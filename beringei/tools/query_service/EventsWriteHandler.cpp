/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "EventsWriteHandler.h"

#include <ctime>
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
using std::chrono::microseconds;
using std::chrono::system_clock;
using namespace proxygen;

namespace facebook {
namespace gorilla {

EventsWriteHandler::EventsWriteHandler(std::shared_ptr<MySqlClient> mySqlClient)
    : RequestHandler(), mySqlClient_(mySqlClient) {}

void EventsWriteHandler::onRequest(
    std::unique_ptr<HTTPMessage> /* unused */) noexcept {
  // nothing to do
}

void EventsWriteHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {
  if (body_) {
    body_->prependChain(move(body));
  } else {
    body_ = move(body);
  }
}

std::string EventsWriteHandler::getMySqlTimestamp(int64_t timeInUsec) {
  time_t curr_time;
  tm* curr_tm;
  char date_string[100];

  time(&curr_time);
  curr_tm = localtime(&curr_time);

  strftime(date_string, 50, "%Y-%m-%d %X", curr_tm);
  return std::string(date_string);
}

void EventsWriteHandler::writeData(EventsWriteRequest request) {
  std::unordered_map<std::string, MySqlNodeData> unknownNodes;
  std::unordered_map<int64_t, std::unordered_set<std::string>>
      missingEventCategories;
  std::vector<MySqlEventData> eventsRows;

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

    for (const auto& event : agent.events) {
      // check timestamp
      std::string tsParsed = getMySqlTimestamp(event.ts);
      auto eventCategoryId =
          mySqlClient_->getEventCategoryId(*nodeId, event.category);
      // verify node/category combo exists
      if (eventCategoryId) {
        // insert row for beringei
        MySqlEventData eventsRow;
        eventsRow.sample = event.sample;
        eventsRow.timestamp = tsParsed;
        eventsRow.category_id = *eventCategoryId;
        eventsRows.push_back(eventsRow);
      } else {
        LOG(INFO) << "Missing cache for " << *nodeId << "/" << event.category;
        missingEventCategories[*nodeId].insert(event.category);
      }
    }
  }
  // write newly found macs and node/key combos
  mySqlClient_->addNodes(unknownNodes);
  mySqlClient_->addEventCategories(missingEventCategories);

  if (eventsRows.size()) {
    folly::EventBase eb;
    eb.runInLoop(
        [this, &eventsRows]() mutable { mySqlClient_->addEvents(eventsRows); });
    std::thread tEb([&eb]() { eb.loop(); });
    tEb.join();

    auto endTime = (int64_t)duration_cast<milliseconds>(
                       system_clock::now().time_since_epoch())
                       .count();
    LOG(INFO) << "Writing events complete. "
              << "Total: " << (endTime - startTime) << "ms.";
  } else {
    LOG(INFO) << "No events data to write!";
  }
}

void EventsWriteHandler::onEOM() noexcept {
  auto body = body_->moveToFbString();
  EventsWriteRequest request;
  try {
    request = SimpleJSONSerializer::deserialize<EventsWriteRequest>(body);
  } catch (const std::exception&) {
    LOG(INFO) << "Error deserializing events_writer request";
    ResponseBuilder(downstream_)
        .status(500, "OK")
        .header("Content-Type", "application/json")
        .body("Failed de-serializing events_writer request")
        .sendWithEOM();
    return;
  }
  logRequest(request);
  LOG(INFO) << "Events writer request from \"" << request.topology.name
            << "\" for " << request.agents.size() << " nodes";

  folly::fbstring jsonResp;
  try {
    writeData(request);
  } catch (const std::exception& ex) {
    LOG(ERROR) << "Unable to handle events_writer request: " << ex.what();
    ResponseBuilder(downstream_)
        .status(500, "OK")
        .header("Content-Type", "application/json")
        .body("Failed handling events_writer request")
        .sendWithEOM();
    return;
  }
  ResponseBuilder(downstream_)
      .status(200, "OK")
      .header("Content-Type", "application/json")
      .body(jsonResp)
      .sendWithEOM();
}

void EventsWriteHandler::onUpgrade(UpgradeProtocol /* unused */) noexcept {}

void EventsWriteHandler::requestComplete() noexcept {
  delete this;
}

void EventsWriteHandler::onError(ProxygenError /* unused */) noexcept {
  LOG(ERROR) << "Proxygen reported error";
  // In QueryServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}

void EventsWriteHandler::logRequest(EventsWriteRequest request) {}
}
} // facebook::gorilla
