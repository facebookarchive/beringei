/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "FetchHandler.h"

#include <utility>

#include "DateUtils.h"

#include <folly/DynamicConverter.h>
#include <folly/io/IOBuf.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <thrift/lib/cpp/util/ThriftSerializer.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <sstream>

using apache::thrift::SimpleJSONSerializer;
using namespace proxygen;

namespace facebook {
namespace gorilla {

FetchHandler::FetchHandler(
    std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter)
    : RequestHandler(), configurationAdapter_(configurationAdapter) {}

void FetchHandler::onRequest(std::unique_ptr<HTTPMessage> msg) noexcept {
  _start = msg->getQueryParam("start");
  _end = msg->getQueryParam("end");
  _key = msg->getQueryParam("key");

  LOG(INFO) << "Query start: " << _start << " end: " << _end
            << " key: " << _key;
}

void FetchHandler::onBody(std::unique_ptr<folly::IOBuf> /* unused */) noexcept {
  // Nothing to do.
}

void FetchHandler::onEOM() noexcept {
  if (_start.empty()) {
    return ResponseBuilder(downstream_)
        .status(422, "Unprocessable Entity")
        .header("Content-Type", "text/plain")
        .body("start is required\r\n")
        .sendWithEOM();
  }

  if (_end.empty()) {
    return ResponseBuilder(downstream_)
        .status(422, "Unprocessable Entity")
        .header("Content-Type", "text/plain")
        .body("end is required\r\n")
        .sendWithEOM();
  }

  if (_key.empty()) {
    return ResponseBuilder(downstream_)
        .status(422, "Unprocessable Entity")
        .header("Content-Type", "text/plain")
        .body("key is required\r\n")
        .sendWithEOM();
  }

  time_t startTime;
  if (!DateUtils::stringParseIsoTimestamp(_start, &startTime)) {
    return ResponseBuilder(downstream_)
        .status(422, "Unprocessable Entity")
        .header("Content-Type", "text/plain")
        .body("start is invalid\r\n")
        .sendWithEOM();
  }
  time_t endTime;
  if (!DateUtils::stringParseIsoTimestamp(_end, &endTime)) {
    return ResponseBuilder(downstream_)
        .status(422, "Unprocessable Entity")
        .header("Content-Type", "text/plain")
        .body("end is invalid\r\n")
        .sendWithEOM();
  }

  BeringeiClient client(
      configurationAdapter_, 1, BeringeiClient::kNoWriterThreads);

  GetDataRequest request;
  request.keys.emplace_back();
  request.keys.back().key = _key;
  request.keys.back().shardId = getShardId(_key, client.getNumShards());
  request.begin = startTime;
  request.end = endTime;

  gorilla::GorillaResultVector result;
  client.get(request, result);

  // Transform datapoints into result.
  std::ostringstream res;
  for (const auto& keyData : result) {
    const auto& keyName = keyData.first.key;
    for (const auto& timeValue : keyData.second) {
      res << keyName << " " << timeValue.value << " " << timeValue.unixTime
          << std::endl;
    }
  }

  ResponseBuilder(downstream_)
      .status(200, "OK")
      .header("Content-Type", "application/json")
      .body(res.str())
      .sendWithEOM();
}

void FetchHandler::onUpgrade(UpgradeProtocol /* unused */) noexcept {
  // Handler doesn't support upgrades.
}

void FetchHandler::requestComplete() noexcept {
  LOG(INFO) << "Query Request Complete";
  // In PlainTextServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}

void FetchHandler::onError(ProxygenError /* unused */) noexcept {
  LOG(ERROR) << "Proxygen reported error";
  // In PlainTextServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}

int FetchHandler::getShardId(const std::string& key, const int numShards) {
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
