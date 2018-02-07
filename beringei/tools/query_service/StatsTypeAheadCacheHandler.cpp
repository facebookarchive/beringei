/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "mysql_connection.h"
#include "mysql_driver.h"
#include "StatsTypeAheadCacheHandler.h"

#include <algorithm>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <folly/DynamicConverter.h>
#include <folly/Conv.h>
#include <folly/io/IOBuf.h>
#include <map>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <thrift/lib/cpp/util/ThriftSerializer.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <utility>

using apache::thrift::SimpleJSONSerializer;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;
using namespace proxygen;

namespace facebook {
namespace gorilla {

StatsTypeAheadCacheHandler::StatsTypeAheadCacheHandler(
    std::shared_ptr<MySqlClient> mySqlClient,
    std::shared_ptr<TACacheMap> typeaheadCache)
    : RequestHandler(), mySqlClient_(mySqlClient),
      typeaheadCache_(typeaheadCache) {}

void StatsTypeAheadCacheHandler::onRequest(
    std::unique_ptr<HTTPMessage> /* unused */) noexcept {
  // nothing to do
}

void StatsTypeAheadCacheHandler::onBody(
    std::unique_ptr<folly::IOBuf> body) noexcept {
  if (body_) {
    body_->prependChain(move(body));
  } else {
    body_ = move(body);
  }
}

void StatsTypeAheadCacheHandler::onEOM() noexcept {
  auto body = body_->moveToFbString();
  query::Topology request;
  try {
    request = SimpleJSONSerializer::deserialize<query::Topology>(body);
  }
  catch (const std::exception &ex) {
    LOG(INFO) << "Error deserializing stats type ahead request";
    LOG(INFO) << "JSON: " << body;
    ResponseBuilder(downstream_)
        .status(500, "OK")
        .header("Content-Type", "application/json")
        .body("Failed de-serializing stats type ahead request")
        .sendWithEOM();
    return;
  }
  LOG(INFO) << "Stats type-ahead cache request from \"" << request.name << "\"";

  try {
    // insert cache handler
    LOG(INFO) << "Got topology: " << request.name;
    StatsTypeAheadCache taCache(mySqlClient_);
    taCache.fetchMetricNames(request);
    LOG(INFO) << "Type-ahead cache loaded for: " << request.name;
    // TODO - this doesn't update, need concurrent
    typeaheadCache_->insert(std::make_pair(request.name, taCache));
  }
  catch (const std::exception &ex) {
    LOG(ERROR) << "Unable to handle stats type-ahead cache request: "
               << ex.what();
    ResponseBuilder(downstream_)
        .status(500, "OK")
        .header("Content-Type", "application/json")
        .body("Failed handling stats_type_ahead request")
        .sendWithEOM();
    return;
  }
  folly::dynamic dummyResponse = folly::dynamic::object;
  ResponseBuilder(downstream_)
      .status(200, "OK")
      .header("Content-Type", "application/json")
      .body(folly::toJson(dummyResponse))
      .sendWithEOM();
}

void
StatsTypeAheadCacheHandler::onUpgrade(UpgradeProtocol /* unused */) noexcept {}

void StatsTypeAheadCacheHandler::requestComplete() noexcept { delete this; }

void StatsTypeAheadCacheHandler::onError(ProxygenError /* unused */) noexcept {
  LOG(ERROR) << "Proxygen reported error";
  // In QueryServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}
}
} // facebook::gorilla
