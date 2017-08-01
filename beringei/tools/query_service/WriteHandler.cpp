/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "WriteHandler.h"

#include <utility>

#include <folly/DynamicConverter.h>
#include <folly/io/IOBuf.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <thrift/lib/cpp/util/ThriftSerializer.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "mysql_connection.h"
#include "mysql_driver.h"

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>

#include <algorithm>

using apache::thrift::SimpleJSONSerializer;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;
using namespace proxygen;

DEFINE_int32(agg_bucket_seconds, 30, "time aggregation bucket size");

namespace facebook {
namespace gorilla {

WriteHandler::WriteHandler(
    std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
    std::shared_ptr<MySqlClient> mySqlClient,
    std::shared_ptr<BeringeiClient> beringeiClient)
    : RequestHandler(),
      configurationAdapter_(configurationAdapter),
      mySqlClient_(mySqlClient),
      beringeiClient_(beringeiClient) {}

void WriteHandler::onRequest(
    std::unique_ptr<HTTPMessage> /* unused */) noexcept {
  // nothing to do
}

void WriteHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {
  if (body_) {
    body_->prependChain(move(body));
  } else {
    body_ = move(body);
  }
}

int64_t timeCalc(int64_t timeIn) {
  /*
    int64_t timeInSeconds;
    int64_t currentTime = std::time(nullptr);
    int64_t min_s  = 1500000000;
    int64_t min_ms = 1500000000000;
    int64_t min_us = 1500000000000000;
    int64_t min_ns = 1500000000000000000;
    int64_t max_s  = 2000000000;
    int64_t max_ms = 2000000000000;
    int64_t max_us = 2000000000000000;
    int64_t max_ns = 2000000000000000000;
    if (timeIn < max_s && timeIn > min_s) {
      timeInSeconds = timeIn;
    } else if (timeIn < max_ms && timeIn > min_ms) {
      timeInSeconds = timeIn / 1000;
    } else if (timeIn < max_us && timeIn > min_us) {
      timeInSeconds = timeIn / 1000000;
    } else if (timeIn < max_ns && timeIn > min_ns) {
      timeInSeconds = timeIn / 1000000000;
    } else{
      // just use current time
      timeInSeconds = currentTime;
    }

    int64_t timeDiff = currentTime - timeInSeconds;
    if (timeDiff > 30*60 || timeDiff < -30*60) {
      LOG(INFO) << "Timestamp " << timeInSeconds
                << " is out of sync with current time " << currentTime;
      timeInSeconds = currentTime;
    }
    return folly::to<int64_t>(ceil (timeInSeconds / FLAGS_agg_bucket_seconds))
      * FLAGS_agg_bucket_seconds;
  */
  return folly::to<int64_t>(
             ceil(std::time(nullptr) / FLAGS_agg_bucket_seconds)) *
      FLAGS_agg_bucket_seconds;
}

void WriteHandler::writeData(WriteRequest request) {
  std::unordered_map<std::string, MySqlNodeData> unknownNodes;
  std::unordered_map<int64_t, std::unordered_set<std::string>> missingNodeKey;
  std::vector<DataPoint> bRows;

  auto startTime = (int64_t)duration_cast<milliseconds>(
                       system_clock::now().time_since_epoch())
                       .count();

  for (const auto& agent : request.agents) {
    auto nodeId = mySqlClient_->getNodeId(agent.mac);
    if (!nodeId) {
      MySqlNodeData newNode;
      newNode.mac = agent.mac;
      newNode.node = agent.name;
      newNode.site = agent.site;
      newNode.network = request.topology.name;
      unknownNodes[newNode.mac] = newNode;
      LOG(INFO) << "Unknown mac: " << agent.mac;
      continue;
    }

    for (const auto& stat : agent.stats) {
      // check timestamp
      int64_t tsParsed = timeCalc(stat.ts);
      auto keyId = mySqlClient_->getKeyId(*nodeId, stat.key);
      // verify node/key combo exists
      if (keyId) {
        // insert row for beringei
        DataPoint bRow;
        TimeValuePair timePair;
        Key bKey;

        bKey.key = std::to_string(*keyId);
        bRow.key = bKey;
        timePair.unixTime = tsParsed;
        timePair.value = stat.value;
        bRow.value = timePair;
        bRows.push_back(bRow);
      } else {
        VLOG(2) << "Missing cache for " << *nodeId << "/" << stat.key;
        missingNodeKey[*nodeId].insert(stat.key);
      }
    }
  }
  // write newly found macs and node/key combos
  mySqlClient_->addNodes(unknownNodes);
  mySqlClient_->updateNodeKeys(missingNodeKey);

  // insert rows
  if (!bRows.empty()) {
    folly::EventBase eb;
    eb.runInLoop([this, &bRows]() mutable {
      auto pushedPoints = beringeiClient_->putDataPoints(bRows);
      if (!pushedPoints) {
        LOG(ERROR) << "Failed to perform the put!";
      }
    });
    std::thread tEb([&eb]() { eb.loop(); });
    tEb.join();

    auto endTime = (int64_t)duration_cast<milliseconds>(
                       system_clock::now().time_since_epoch())
                       .count();
    LOG(INFO) << "writeData completed. "
              << "Total: " << (endTime - startTime) << "ms.";
  } else {
    LOG(INFO) << "No stats data to write";
  }
}

void WriteHandler::onEOM() noexcept {
  auto body = body_->moveToFbString();
  WriteRequest request;
  try {
    request = SimpleJSONSerializer::deserialize<WriteRequest>(body);
  } catch (const std::exception&) {
    LOG(INFO) << "Error deserializing stats_writer request";
    ResponseBuilder(downstream_)
        .status(500, "OK")
        .header("Content-Type", "application/json")
        .body("Failed de-serializing stats_writer request")
        .sendWithEOM();
    return;
  }
  logRequest(request);
  LOG(INFO) << "Stats writer request from \"" << request.topology.name
            << "\" for " << request.agents.size() << " nodes";

  folly::fbstring jsonResp;
  try {
    writeData(request);
  } catch (const std::exception& ex) {
    LOG(ERROR) << "Unable to handle stats_writer request: " << ex.what();
    ResponseBuilder(downstream_)
        .status(500, "OK")
        .header("Content-Type", "application/json")
        .body("Failed handling stats_writer request")
        .sendWithEOM();
    return;
  }
  ResponseBuilder(downstream_)
      .status(200, "OK")
      .header("Content-Type", "application/json")
      .body(jsonResp)
      .sendWithEOM();
}

void WriteHandler::onUpgrade(UpgradeProtocol /* unused */) noexcept {}

void WriteHandler::requestComplete() noexcept {
  delete this;
}

void WriteHandler::onError(ProxygenError /* unused */) noexcept {
  LOG(ERROR) << "Proxygen reported error";
  // In QueryServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}

void WriteHandler::logRequest(WriteRequest request) {}
}
} // facebook::gorilla
