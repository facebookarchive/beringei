/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "TableQueryHandler.h"

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

TableQueryHandler::TableQueryHandler(
    std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
    std::shared_ptr<BeringeiClient> beringeiClient,
    std::shared_ptr<TACacheMap> typeaheadCache)
    : RequestHandler(), configurationAdapter_(configurationAdapter),
      beringeiClient_(beringeiClient),
      typeaheadCache_(typeaheadCache) {}

void
TableQueryHandler::onRequest(std::unique_ptr<HTTPMessage> /* unused */) noexcept {
  // nothing to do
}

void TableQueryHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {
  if (body_) {
    body_->prependChain(move(body));
  } else {
    body_ = move(body);
  }
}

void TableQueryHandler::onEOM() noexcept {
  auto body = body_->moveToFbString();
  query::TableQueryRequest request;
  try {
    request = SimpleJSONSerializer::deserialize<query::TableQueryRequest>(body);
    LOG(INFO) << "Topo: " << request.topologyName
              << ", queries: " << request.nodeQueries.size()
              << ", link queries: " << request.linkQueries.size();
  }
  catch (const std::exception &) {
    LOG(INFO) << "Error deserializing TableQueryRequest";
    ResponseBuilder(downstream_)
        .status(500, "OK")
        .header("Content-Type", "application/json")
        .body("Failed de-serializing TableQueryRequest")
        .sendWithEOM();
    return;
  }
  // match to a type-ahead cache
  auto taCacheIt = typeaheadCache_->find(request.topologyName);
  if (taCacheIt == typeaheadCache_->end()) {
    LOG(INFO) << "\tTopology cache not found: " << request.topologyName;
    ResponseBuilder(downstream_)
        .status(500, "OK")
        .header("Content-Type", "application/json")
        .body("Topology cache not found: " + request.topologyName)
        .sendWithEOM();
    return;
  }
  auto taCache = taCacheIt->second;
  query::QueryRequest queryRequest;
  // build node queries
  std::vector<int64_t> keyIdList;
  std::vector<query::KeyData> keyDataListRenamed;
  std::string lastType;
  int minAgo;
  for (const auto& nodeQuery : request.nodeQueries) {
    VLOG(1) << "\tFetching node query metric: " << nodeQuery.metric;
    // fetch KeyData
    auto keyDataList = taCache.getKeyData(nodeQuery.metric);
    for (auto& keyData : keyDataList) {
      VLOG(1) << "\t\tNode: " << keyData.nodeName
              << ", displayName: " << keyData.displayName
              << ", keyId: " << keyData.keyId;
      keyIdList.push_back(keyData.keyId);
      keyData.displayName = keyData.nodeName;
      keyDataListRenamed.push_back(keyData);
    }
    lastType = nodeQuery.type;
    if (nodeQuery.__isset.min_ago) {
      // use min_ago if set
      minAgo = nodeQuery.min_ago;
    }
  }
  // TODO - the JS does one query for all, which is silly
  // we should switch this to be one per request
  query::Query nodeQuery;
  nodeQuery.type = lastType;
  nodeQuery.key_ids = keyIdList;
  nodeQuery.data = keyDataListRenamed;
  nodeQuery.min_ago = minAgo;
  nodeQuery.__isset.min_ago = true;
  queryRequest.queries.push_back(nodeQuery);
  // build node queries
  keyIdList.clear();
  keyDataListRenamed.clear();
  for (const auto& linkQuery : request.linkQueries) {
    VLOG(1) << "\tFetching link query metric: " << linkQuery.metric;
    // fetch KeyData
    auto keyDataList = taCache.getKeyData(linkQuery.metric);
    for (auto& keyData : keyDataList) {
      VLOG(1) << "\t\tLink: " << keyData.linkName
              << ", displayName: " << keyData.displayName
              << ", keyId: " << keyData.keyId;
      keyIdList.push_back(keyData.keyId);
      keyData.displayName = keyData.linkName;
      keyDataListRenamed.push_back(keyData);
    }
    lastType = linkQuery.type;
    if (linkQuery.__isset.min_ago) {
      // use min_ago if set
      minAgo = linkQuery.min_ago;
    }
  }
  // TODO - same as above
  query::Query linkQuery;
  linkQuery.type = lastType;
  linkQuery.key_ids = keyIdList;
  linkQuery.data = keyDataListRenamed;
  linkQuery.min_ago = minAgo;
  linkQuery.__isset.min_ago = true;
  queryRequest.queries.push_back(linkQuery);
  // build link queries
  BeringeiData dataFetcher(configurationAdapter_, beringeiClient_, queryRequest);
  auto beringeiResults = dataFetcher.process();
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

void TableQueryHandler::onUpgrade(UpgradeProtocol /* unused */) noexcept {}

void TableQueryHandler::requestComplete() noexcept { delete this; }

void TableQueryHandler::onError(ProxygenError /* unused */) noexcept {
  LOG(ERROR) << "Proxygen reported error";
  // In QueryServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}

}
} // facebook::gorilla
