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
#include "StatsTypeAheadHandler.h"

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

StatsTypeAheadHandler::StatsTypeAheadHandler(
    std::shared_ptr<MySqlClient> mySqlClient,
    std::shared_ptr<TACacheMap> typeaheadCache)
    : RequestHandler(), mySqlClient_(mySqlClient),
      typeaheadCache_(typeaheadCache) {}

void StatsTypeAheadHandler::onRequest(
    std::unique_ptr<HTTPMessage> /* unused */) noexcept {
  // nothing to do
}

void
StatsTypeAheadHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {
  if (body_) {
    body_->prependChain(move(body));
  } else {
    body_ = move(body);
  }
}

void StatsTypeAheadHandler::onEOM() noexcept {
  auto body = body_->moveToFbString();
  query::TypeAheadRequest request;
  try {
    request = SimpleJSONSerializer::deserialize<query::TypeAheadRequest>(body);
  }
  catch (const std::exception &ex) {
    LOG(INFO) << "Error deserializing stats type ahead request";
    ResponseBuilder(downstream_)
        .status(500, "OK")
        .header("Content-Type", "application/json")
        .body("Failed de-serializing stats type ahead request")
        .sendWithEOM();
    return;
  }
  LOG(INFO) << "Stats type ahead request for \"" << request.input
            << "\" on \"" << request.topologyName << "\"";
  // check for cache client
  auto taIt = typeaheadCache_->find(request.topologyName);
  if (taIt == typeaheadCache_->end()) {
    LOG(ERROR) << "No type-ahead cache for \"" << request.topologyName << "\"";
    ResponseBuilder(downstream_)
        .status(500, "OK")
        .header("Content-Type", "application/json")
        .body("No type-ahead cache found")
        .sendWithEOM();
    return;
  }
  auto taCache = taIt->second;
  auto retMetrics = taCache.searchMetrics(request.input);
  folly::dynamic orderedMetricList = folly::dynamic::array;
  for (const auto& metricList : retMetrics) {
    folly::dynamic keyList = folly::dynamic::array;
    for (const auto& key : metricList) {
      VLOG(1) << "\t\tName: " << key.displayName << ", key: " << key.key
              << ", node: " << key.nodeName;
      keyList.push_back(folly::dynamic::object
        ("displayName", key.displayName)
        ("key", key.key)
        ("keyId", key.keyId)
        ("nodeName", key.nodeName)
        ("siteName", key.siteName)
        ("node", key.node)
      );
    }
    // add to json
    orderedMetricList.push_back(keyList);
  }
  // build type-ahead list
  ResponseBuilder(downstream_)
      .status(200, "OK")
      .header("Content-Type", "application/json")
      .body(folly::toJson(orderedMetricList))
      .sendWithEOM();
}

void StatsTypeAheadHandler::onUpgrade(UpgradeProtocol /* unused */) noexcept {}

void StatsTypeAheadHandler::requestComplete() noexcept { delete this; }

void StatsTypeAheadHandler::onError(ProxygenError /* unused */) noexcept {
  LOG(ERROR) << "Proxygen reported error";
  // In QueryServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}
}
} // facebook::gorilla
