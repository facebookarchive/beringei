/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "AggregatorService.h"
#include "BeringeiData.h"

#include "beringei/if/gen-cpp2/Topology_types_custom_protocol.h"
#include <curl/curl.h>
#include <folly/io/async/AsyncTimeout.h>
#include <thrift/lib/cpp/util/ThriftSerializer.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

DEFINE_int32(time_period, 30, "Beringei time period");

extern "C" {
struct HTTPDataStruct {
  char* data;
  size_t size;
};

static size_t
curlWriteCb(void* content, size_t size, size_t nmemb, void* userp) {
  size_t realSize = size * nmemb;
  struct HTTPDataStruct* httpData = (struct HTTPDataStruct*)userp;
  httpData->data =
      (char*)realloc(httpData->data, httpData->size + realSize + 1);
  if (httpData->data == nullptr) {
    printf("Unable to allocate memory (realloc failed)\n");
    return 0;
  }
  memcpy(&(httpData->data[httpData->size]), content, realSize);
  httpData->size += realSize;
  httpData->data[httpData->size] = 0;
  return realSize;
}
}

using apache::thrift::SimpleJSONSerializer;

namespace facebook {
namespace gorilla {

AggregatorService::AggregatorService(
  std::shared_ptr<TACacheMap> typeaheadCache,
  std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
  std::shared_ptr<BeringeiClient> beringeiReadClient,
  std::shared_ptr<BeringeiClient> beringeiWriteClient)
  : typeaheadCache_(typeaheadCache),
    configurationAdapter_(configurationAdapter),
    beringeiReadClient_(beringeiReadClient),
    beringeiWriteClient_(beringeiWriteClient) {

  timer_ = folly::AsyncTimeout::make(eb_, [&] () noexcept {
    timerCallback();
  });
  timer_->scheduleTimeout(FLAGS_time_period * 1000);
}

void
AggregatorService::timerCallback() {
  timer_->scheduleTimeout(FLAGS_time_period * 1000);
  // run some aggregation
  std::unordered_map<std::string /* key name */, double> aggValues;
  auto topology = fetchTopology();
  if (!topology.name.empty() &&
      !topology.nodes.empty() &&
      !topology.links.empty()) {
    int64_t timeStamp = folly::to<int64_t>(
       ceil(std::time(nullptr) / 30.0)) * 30;
    // pop traffic
    std::vector<query::Node> popNodes;
    // nodes up + down
    int onlineNodes = 0;
    for (const auto& node : topology.nodes) {
      onlineNodes += (node.status != query::NodeStatusType::OFFLINE);
      if (node.pop_node) {
        popNodes.push_back(node);
      } 
    }
    aggValues["total_nodes"] = topology.nodes.size();
    aggValues["online_nodes"] = onlineNodes;
    aggValues["online_nodes_perc"] = (double)onlineNodes / topology.nodes.size() * 100.0;
    aggValues["pop_nodes"] = popNodes.size();
    // (wireless) links up + down
    int wirelessLinks = 0;
    int onlineLinks = 0;
    for (const auto& link : topology.links) {
      if (link.link_type != query::LinkType::WIRELESS) {
        continue;
      }
      wirelessLinks++;
      onlineLinks += link.is_alive;
    }
    aggValues["total_wireless_links"] = wirelessLinks;
    aggValues["online_wireless_links"] = onlineLinks;
    aggValues["online_wireless_links_perc"] = (double)onlineLinks / wirelessLinks * 100.0;
    // report metrics somewhere? TBD
    for (const auto& metric : aggValues) {
      LOG(INFO) << "Agg: " << metric.first << " = " << metric.second
                << ", ts: " << timeStamp;
    }
    LOG(INFO) << "--------------------------------------";
    std::vector<DataPoint> bDataPoints;
    // query metric data from beringei
    auto taCacheIt = typeaheadCache_->find(topology.name);
    if (taCacheIt != typeaheadCache_->end()) {
      LOG(INFO) << "Cache found for: " << topology.name;
      // fetch back the metrics we care about (PER, MCS?)
      // and average the values
      buildQuery(aggValues, &taCacheIt->second);
      // find metrics, update beringei
      for (const auto& metric : aggValues) {
        std::vector<query::KeyData> keyData =
          taCacheIt->second.getKeyData(metric.first);
        if (keyData.size() != 1) {
          LOG(INFO) << "Metric not found: " << metric.first;
          continue;
        }
        int keyId = keyData.front().keyId;
        // create beringei data-point
        DataPoint bDataPoint;
        TimeValuePair bTimePair;
        Key bKey;

        bKey.key = std::to_string(keyId);
        bDataPoint.key = bKey;
        bTimePair.unixTime = timeStamp;
        bTimePair.value = metric.second;
        bDataPoint.value = bTimePair;
        bDataPoints.push_back(bDataPoint);
      }
      folly::EventBase eb;
      eb.runInLoop([this, &bDataPoints]() mutable {
        auto pushedPoints = beringeiWriteClient_->putDataPoints(bDataPoints);
        if (!pushedPoints) {
          LOG(ERROR) << "Failed to perform the put!";
        }
      });
      std::thread tEb([&eb]() { eb.loop(); });
      tEb.join();
      LOG(INFO) << "Data-points written";
    } else {
      LOG(ERROR) << "Missing type-ahead cache for: " << topology.name;
    }
    // push metrics into beringei
  } else {
    LOG(INFO) << "Invalid topology";
  }
}

void
AggregatorService::buildQuery(
    std::unordered_map<std::string, double>& values,
    StatsTypeAheadCache* cache) {
  // build queries
  query::QueryRequest queryRequest;
  std::vector<query::Query> queries;
  std::vector<std::string> keyNames = {"per", "mcs", "snr"};
  for (const auto keyName : keyNames) {
    auto keyData = cache->getKeyData(keyName);
    query::Query query;
    query.type = "latest";
    std::vector<int64_t> keyIds;
    std::vector<query::KeyData> keyDataRenamed;
    for (const auto& key : keyData) {
      keyIds.push_back(key.keyId);
      auto newKeyData = key;
      newKeyData.displayName = key.linkName;
      keyDataRenamed.push_back(newKeyData);
    }
    query.key_ids = keyIds;
    query.data = keyDataRenamed;
    query.min_ago = 5;
    query.__isset.min_ago = true;
    queries.push_back(query);
  }
  // fetch the last few minutes to receive the latest data point
  queryRequest.queries = queries;
  BeringeiData dataFetcher(configurationAdapter_, beringeiReadClient_, queryRequest);
  folly::dynamic results = dataFetcher.process();
  int queryIdx = 0;
  for (const auto& query : results) {
    double sum = 0;
    int items = 0;
    for (const auto& pair : query.items()) {
      sum += pair.second.asDouble();
      items++;
    }
    double avg = sum / items;
    std::string keyName = keyNames[queryIdx] + ".avg";
    values[keyName] = avg;
    queryIdx++;
  }
}

query::Topology
AggregatorService::fetchTopology() {
  query::Topology topology;
  try {
    CURL* curl;
    CURLcode res;
    curl = curl_easy_init();
    if (!curl) {
      throw std::runtime_error("Unable to initialize CURL");
    }
    std::string postData("{}");
    // we have to forward the v4 address right now since no local v6
    std::string endpoint("http://10.56.68.18:8088/api/getTopology");
    // we can't verify the peer with our current image/lack of certs
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0);
    curl_easy_setopt(curl, CURLOPT_URL, endpoint.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postData.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, postData.length());
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 0);
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 1000 /* 1 second */);

    // read data from request
    struct HTTPDataStruct dataChunk;
    dataChunk.data = (char*)malloc(1);
    dataChunk.size = 0;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &curlWriteCb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*)&dataChunk);
    res = curl_easy_perform(curl);
    if (res == CURLE_OK) {
      long response_code;
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
      // response code 204 is a success
    }
    // cleanup
    curl_easy_cleanup(curl);
    topology = SimpleJSONSerializer::deserialize<query::Topology>(dataChunk.data);
    free(dataChunk.data);
    if (res != CURLE_OK) {
      LOG(WARNING) << "CURL error for endpoint " << endpoint << ": "
                   << curl_easy_strerror(res);
    }
  } catch (const std::exception& ex) {
    LOG(ERROR) << "CURL Error: " << ex.what();
  }
  return topology;
}

void
AggregatorService::start() {
  eb_.loopForever();
}

}
} // facebook::gorilla
