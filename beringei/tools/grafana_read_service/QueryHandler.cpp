/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "QueryHandler.h"

#include <utility>

#include "DateUtils.h"

#include <folly/DynamicConverter.h>
#include <folly/io/IOBuf.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

using apache::thrift::SimpleJSONSerializer;
using namespace proxygen;

namespace facebook {
namespace gorilla {

QueryHandler::QueryHandler(
    std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter)
    : RequestHandler(), configurationAdapter_(configurationAdapter) {}

void QueryHandler::onRequest(
    std::unique_ptr<HTTPMessage> /* unused */) noexcept {
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

  auto request = SimpleJSONSerializer::deserialize<QueryRequest>(body);
  logRequest(request);

  BeringeiClient client(
      configurationAdapter_, 1, BeringeiClient::kNoWriterThreads);

  int numShards = client.getMaxNumShards();
  std::vector<std::pair<Key, std::vector<TimeValuePair>>> beringeiResult;
  auto beringeiRequest = createBeringeiRequest(request, numShards);
  client.get(beringeiRequest, beringeiResult);

  ResponseBuilder(downstream_)
      .status(200, "OK")
      .header("Content-Type", "application/json")
      .body(createJsonResponse(request.targets, beringeiResult))
      .sendWithEOM();
}

void QueryHandler::onUpgrade(UpgradeProtocol /* unused */) noexcept {
  // handler doesn't support upgrades
}

void QueryHandler::requestComplete() noexcept {
  LOG(INFO) << "Query Request Complete";
  // In GrafanaServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}

void QueryHandler::onError(ProxygenError /* unused */) noexcept {
  LOG(ERROR) << "Proxygen reported error";
  // In GrafanaServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}

/* Sample Grafana Query Request

  {
    "panelId": 1,
    "range": {
      "from": "2016-10-31T06:33:44.866Z",
      "to": "2016-10-31T12:33:44.866Z",
      "raw": {
        "from": "now-6h",
        "to": "now"
      }
    },
    "rangeRaw": {
      "from": "now-6h",
      "to": "now"
    },
    "interval": "30s",
    "intervalMs": 30000,
    "targets": [
        { "target": "upper_50", refId: "A" },
        { "target": "upper_75", refId: "B" }
      ],
    "format": "json",
    "maxDataPoints": 550
  }
*/

void QueryHandler::logRequest(QueryRequest request) {
  std::string targetString;
  for (auto target : request.targets) {
    targetString += " " + target.target;
  }

  LOG(INFO) << "Panel Id : " << request.panelId << ", request data from "
            << request.range.from << " to " << request.range.to
            << " at interval " << request.intervalMs << " for targets "
            << targetString;
}

/* Sample Grafana Query Response

[
  {
    "target":"upper_75", // The field being queried for
    "datapoints":[
      [622,1450754160000],  // Metric value as a float , unixtimestamp in
milliseconds
      [365,1450754220000]
    ]
  },
  {
    "target":"upper_90",
    "datapoints":[
      [861,1450754160000],
      [767,1450754220000]
    ]
  }
]
*/

folly::fbstring QueryHandler::createJsonResponse(
    const std::vector<QueryTarget>& targets,
    const std::vector<std::pair<Key, std::vector<TimeValuePair>>>&
        beringeiResponse) {
  folly::fbstring responseBody = "[";
  bool first = true;
  for (auto target : targets) {
    if (!first) {
      responseBody += ",";
    }
    first = false;
    auto response = createResponseObject(target.target, beringeiResponse);
    responseBody += folly::toJson(response);
  }
  responseBody += "]";

  return responseBody;
}

folly::dynamic QueryHandler::createResponseObject(
    const std::string& target,
    const std::vector<std::pair<Key, std::vector<TimeValuePair>>>&
        beringeiResponse) {
  std::locale loc;
  folly::dynamic response = folly::dynamic::object;
  response["target"] = target;
  response["datapoints"] = folly::dynamic::array();

  for (const auto& responsePair : beringeiResponse) {
    if (responsePair.first.key == target) {
      folly::dynamic datapoints = folly::dynamic::array();
      for (const auto& dp : responsePair.second) {
        datapoints.push_back(
            folly::toDynamic(std::make_pair(dp.value, dp.unixTime * 1000)));
      }

      response["datapoints"] = datapoints;
      break;
    }
  }

  return response;
}

int QueryHandler::getShardId(const std::string& key, const int numShards) {
  std::hash<std::string> hash;
  size_t hashValue = hash(key);

  if (numShards != 0) {
    return hashValue % numShards;
  } else {
    return hashValue;
  }
}

GetDataRequest QueryHandler::createBeringeiRequest(
    const QueryRequest& request,
    const int numShards) {
  time_t fromTime;
  time_t toTime;
  DateUtils::stringParseGrafanaTimestamp(request.range.from, &fromTime);
  DateUtils::stringParseGrafanaTimestamp(request.range.to, &toTime);

  GetDataRequest beringeiRequest;
  beringeiRequest.begin = fromTime;
  beringeiRequest.end = toTime;

  for (auto& target : request.targets) {
    Key beringeiKey;
    auto lowerCaseKey = target.target;
    beringeiKey.key = lowerCaseKey;
    beringeiKey.shardId = getShardId(lowerCaseKey, numShards);
    beringeiRequest.keys.push_back(beringeiKey);
  }

  return beringeiRequest;
}
}
} // facebook::gorilla
