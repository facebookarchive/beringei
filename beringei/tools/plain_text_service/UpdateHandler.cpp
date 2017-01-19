/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "UpdateHandler.h"

#include <utility>

#include "DateUtils.h"

#include <folly/DynamicConverter.h>
#include <folly/FBString.h>
#include <folly/io/IOBuf.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <thrift/lib/cpp/util/ThriftSerializer.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <iostream>
#include <vector>

using apache::thrift::SimpleJSONSerializer;
using namespace proxygen;

namespace facebook {
namespace gorilla {

UpdateHandler::UpdateHandler(
    std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter)
    : RequestHandler(), configurationAdapter_(configurationAdapter) {}

void UpdateHandler::onRequest(std::unique_ptr<HTTPMessage> msg) noexcept {
  // Nothing to do.
}

void UpdateHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {
  if (body_) {
    body_->prependChain(move(body));
  } else {
    body_ = move(body);
  }
}

void UpdateHandler::onEOM() noexcept {
  if (!body_ || body_->empty()) {
    return ResponseBuilder(downstream_)
        .status(422, "Unprocessable Entity")
        .header("Content-Type", "text/plain")
        .body("a body is required\r\n")
        .sendWithEOM();
  }
  auto body = body_->moveToFbString();

  // get client
  BeringeiClient client(configurationAdapter_, 50, 5, true);

  // prepare query
  std::vector<DataPoint> dps;
  std::istringstream iss(body.toStdString());

  std::string line;
  while (std::getline(iss, line)) {
    std::vector<std::string> parts;
    folly::split(" ", line, parts, true);

    if (parts.size() < 2 || parts.size() > 3) {
      return ResponseBuilder(downstream_)
          .status(422, "Unprocessable Entity")
          .header("Content-Type", "text/plain")
          .body("bad body\r\n")
          .sendWithEOM();
    }

    DataPoint dp;
    dp.key.key = parts[0];
    dp.key.shardId =
        getShardId(dp.key.key, client.getNumShardsFromWriteClient());
    auto value = folly::tryTo<double>(parts[1]);
    if (!value.hasValue()) {
      return ResponseBuilder(downstream_)
          .status(422, "Unprocessable Entity")
          .header("Content-Type", "text/plain")
          .body("bad value\r\n")
          .sendWithEOM();
    }
    dp.value.value = value.value();

    if (parts.size() == 3) {
      auto timestamp = folly::tryTo<int64_t>(parts[2]);
      if (!timestamp.hasValue()) {
        return ResponseBuilder(downstream_)
            .status(422, "Unprocessable Entity")
            .header("Content-Type", "text/plain")
            .body("bad timestamp\r\n")
            .sendWithEOM();
      }
      dp.value.unixTime = timestamp.value();
    } else {
      dp.value.unixTime =
          std::chrono::duration_cast<std::chrono::seconds>(
              std::chrono::system_clock::now().time_since_epoch())
              .count();
    }

    dps.push_back(dp);
  }

  LOG(INFO) << "Num pts: " << dps.size();

  if (!client.putDataPoints(dps)) {
    LOG(ERROR) << "Failed to perform the put!";
    return ResponseBuilder(downstream_)
        .status(500, "Internal Server Error")
        .header("Content-Type", "text/plain")
        .body("failed to perform the put\r\n")
        .sendWithEOM();
  }

  client.flushQueue();

  ResponseBuilder(downstream_)
      .status(200, "OK")
      .header("Content-Type", "application/json")
      .body("ok")
      .sendWithEOM();
}

void UpdateHandler::onUpgrade(UpgradeProtocol /* unused */) noexcept {
  // Handler doesn't support upgrades.
}

void UpdateHandler::requestComplete() noexcept {
  LOG(INFO) << "Query Request Complete";
  // In PlainTextServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}

void UpdateHandler::onError(ProxygenError /* unused */) noexcept {
  LOG(ERROR) << "Proxygen reported error";
  // In PlainTextServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}

int UpdateHandler::getShardId(const std::string& key, const int numShards) {
  std::hash<std::string> hash;
  size_t hashValue = hash(key);

  if (numShards != 0) {
    return hashValue % numShards;
  } else {
    return hashValue;
  }
}
}
} // facebook::gorilla
