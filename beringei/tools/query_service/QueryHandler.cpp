/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "QueryHandler.h"

#include "BeringeiData.h"

#include <utility>

#include <folly/DynamicConverter.h>
#include <folly/io/IOBuf.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <thrift/lib/cpp/util/ThriftSerializer.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

using apache::thrift::SimpleJSONSerializer;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;
using namespace proxygen;

namespace facebook {
namespace gorilla {

const int MAX_COLUMNS = 7;
const int MAX_DATA_POINTS = 60;
const int NUM_HBS_PER_SEC = 39; // approximately

QueryHandler::QueryHandler(
    std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
    std::shared_ptr<BeringeiClient> beringeiClient)
    : RequestHandler(), configurationAdapter_(configurationAdapter),
      beringeiClient_(beringeiClient) {}

void
QueryHandler::onRequest(std::unique_ptr<HTTPMessage> /* unused */) noexcept {
  // nothing to do
}

void QueryHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {
  if (body_) {
    body_->prependChain(move(body));
  } else {
    body_ = move(body);
  }
}

void QueryHandler::onEOM() noexcept {
  auto body = body_->moveToFbString();
  query::QueryRequest request;
  try {
    request = SimpleJSONSerializer::deserialize<query::QueryRequest>(body);
  }
  catch (const std::exception &) {
    LOG(INFO) << "Error deserializing QueryRequest";
    ResponseBuilder(downstream_)
        .status(500, "OK")
        .header("Content-Type", "application/json")
        .body("Failed de-serializing QueryRequest")
        .sendWithEOM();
    return;
  }
  BeringeiData dataFetcher(configurationAdapter_, beringeiClient_, request);
  std::string responseJson;
  try {
    responseJson = folly::toJson(dataFetcher.process());
  }
  catch (const std::runtime_error &ex) {
    LOG(ERROR) << "Failed executing beringei query: " << ex.what();
  }
  ResponseBuilder(downstream_)
      .status(200, "OK")
      .header("Content-Type", "application/json")
      .body(responseJson)
      .sendWithEOM();
}

void QueryHandler::onUpgrade(UpgradeProtocol /* unused */) noexcept {}

void QueryHandler::requestComplete() noexcept { delete this; }

void QueryHandler::onError(ProxygenError /* unused */) noexcept {
  LOG(ERROR) << "Proxygen reported error";
  // In QueryServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}

}
} // facebook::gorilla
