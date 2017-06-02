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
  columnNames_.clear();
  timeSeries_.clear();
  aggSeries_.clear();
  query_ = request.queries[0];
  LOG(INFO) << "Request for " << query_.key_ids.size() << " key ids of "
            << query_.agg_type << " aggregation, for " << query_.min_ago
            << " minutes ago.";
 
  folly::fbstring jsonResp = handleQuery();
  ResponseBuilder(downstream_)
      .status(200, "OK")
      .header("Content-Type", "application/json")
      .body(jsonResp)
    .sendWithEOM();
}

void QueryHandler::onUpgrade(UpgradeProtocol /* unused */) noexcept { }

void QueryHandler::requestComplete() noexcept {
  delete this;
}

void QueryHandler::onError(ProxygenError /* unused */) noexcept {
  LOG(ERROR) << "Proxygen reported error";
  // In QueryServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}

void QueryHandler::logRequest(QueryRequest request) { }

void QueryHandler::columnNames() {
  // set column names
  if (query_.agg_type == "top" ||
      query_.agg_type == "bottom") {
    // top, bottom, none
    std::unordered_set<std::string> keyNames;
    std::unordered_set<std::string> linkNames;
    std::unordered_set<std::string> displayNames;
    for (const auto& keyData : query_.data) {
      keyNames.insert(keyData.key);
      linkNames.insert(keyData.linkName);
      displayNames.insert(keyData.displayName);
    }
    for (const auto& keyData : query_.data) {
      std::string columnName = keyData.node;
      if (keyData.linkName.length()) {
        columnName = keyData.linkName;
      } else if (keyData.displayName.length() && displayNames.size() > 1) {
        columnName = keyData.displayName;
      } else if (keyNames.size() > 1) {
        columnName = keyData.key;
      } else if (keyData.nodeName.length()) {
        columnName = keyData.nodeName;
      }
      std::replace(columnName.begin(), columnName.end(), '.', ' ');
      columnNames_.push_back(columnName);
    }
  }
}

/**
 * Common metrics
 *
 * Uptime/availability
 * Iterate over all data points
 */
folly::fbstring QueryHandler::eventHandler(int dataPointIncrementMs) {
  int64_t timeBucketCount = (endTime_ - startTime_) / 30;
  int keyCount = beringeiTimeSeries_.size();
  // count is a special aggregation that will be missed due to default values
  int timeSeriesCounts[timeBucketCount]{};
  // pre-allocate the array size
  double timeSeries[keyCount][timeBucketCount]{};
  int keyIndex = 0;
  for (const auto& keyTimeSeries : beringeiTimeSeries_) {
    const std::string& keyName = keyTimeSeries.first.key;
    for (const auto& timePair : keyTimeSeries.second) {
      int timeBucketId = (timePair.unixTime - startTime_) / 30;
      timeSeries[keyIndex][timeBucketId] = timePair.value;
    }
    keyIndex++;
  }
  int expectedDataPoints = 30 * (1000 / (double)dataPointIncrementMs);
  // iterate over all expected data points
  folly::dynamic linkMap = folly::dynamic::object;
  for (int keyIndex = 0; keyIndex < keyCount; keyIndex++) {
    int missingCounter = 0;
    int upPoints = 0;
    double partialSeconds = 0;
    int startOnlineIndex = -1;
    folly::dynamic onlineEvents = folly::dynamic::array;
    for (int timeIndex = 0; timeIndex < timeBucketCount; timeIndex++) {
      double timeVal = timeSeries[keyIndex][timeIndex];
      if (timeVal == 0) {
        missingCounter++;
        // missing data
      } else if (timeVal >= expectedDataPoints) {
        // whole interval is good
        if (missingCounter) {
          // determine how many gaps to fill
          double partialIntervalSec = timeVal / expectedDataPoints;
          double completeIntervals = partialIntervalSec / 30;
          if (completeIntervals >= missingCounter) {
            // we covered up all missing data points
            upPoints += missingCounter;
            LOG(INFO) << "[" << timeIndex << "] Filled all " << missingCounter << " missed data buckets";
            // online events
            if (startOnlineIndex == -1) {
              startOnlineIndex = timeIndex - missingCounter;
            }
            LOG(INFO) << "[" << timeIndex << "] Marked starting index of uptime: " << startOnlineIndex;
          } else {
            // fill partial interval
            int wholeIntervals = (int)completeIntervals;
            upPoints+= wholeIntervals;
            double remainIntervalSec = partialIntervalSec - (wholeIntervals * 30);
            LOG(INFO) << "[" << timeIndex << "] Filled partial missing data buckets. Missing intervals: "
                      << missingCounter << ", covered intervals: " << completeIntervals
                      << " (" << wholeIntervals << "), partial seconds: " << remainIntervalSec;
            partialSeconds += remainIntervalSec;
            // events, ignore partial intervals
            if (startOnlineIndex == -1) {
              startOnlineIndex = timeIndex - wholeIntervals;
            }
          }
          missingCounter = 0;
        }
        if (startOnlineIndex == -1) {
          LOG(INFO) << "Marking start index: " << timeIndex;
          startOnlineIndex = timeIndex;
        }
        upPoints++;
      } else {
        // part of interval is good, calculate how much of it to mark
        double partialIntervalSec = timeVal / expectedDataPoints;
        partialSeconds += partialIntervalSec;
        LOG(INFO) << "[" << timeIndex << "] Some part of interval is up, about " << partialIntervalSec;
        // events
        if (startOnlineIndex >= 0) {
          // partial interval, mark this interval as down
          LOG(INFO) << "[" << timeIndex << "] Partially online interval, marking offline. We were online for: "
                    << (timeIndex - startOnlineIndex) << " intervals, missing counter: " << missingCounter;
          // push event
          onlineEvents.push_back(folly::dynamic::object
            ("startTime", (startTime_ + (startOnlineIndex * 30)))
            ("endTime", (startTime_ + ((timeIndex - missingCounter) * 30)))
            ("title", ("Online Partial " + std::to_string(startOnlineIndex) +
                       " <-> " + std::to_string(timeIndex - missingCounter))));
          startOnlineIndex = -1;
        }
        // reset missing counter, which only tracks whole intervals offline
        missingCounter = 0;
      }
      // finalize events
      if ((timeIndex + 1) == timeBucketCount && startOnlineIndex >= 0) {
        LOG(INFO) << "Start online index set, push event from start: "
                  << startOnlineIndex << " to: " << timeIndex;
        onlineEvents.push_back(folly::dynamic::object
          ("startTime", (startTime_ + (startOnlineIndex * 30)))
          ("endTime", (endTime_))
          ("title", ("Online " + std::to_string(startOnlineIndex) +
                     " <-> " + std::to_string(timeIndex))));
      }
    }
    // seconds of uptime / seconds in period
    double uptimePerc = 0;
    if (upPoints > 0 || partialSeconds > 0) {
      uptimePerc = (upPoints * 30 + partialSeconds) / (timeBucketCount * 30) * 100.0;
    }
    auto& linkName = query_.data[keyIndex].linkName;
    LOG(INFO) << "Key ID: " << query_.data[keyIndex].key
              << ", Link name: " << linkName
              << ", Expected count: " << timeBucketCount
              << ", up count: " << upPoints
              << ", partial seconds: " << partialSeconds
              << ", uptime: " << uptimePerc << "%";
    linkMap[linkName] = folly::dynamic::object;
    linkMap[linkName]["alive"] = uptimePerc;
    linkMap[linkName]["events"] = onlineEvents;
  }
      
  folly::dynamic response = folly::dynamic::object;
  response["links"] = linkMap;
  response["start"] = startTime_ * 1000;
  response["end"] = endTime_ * 1000;
  return folly::toJson(response);

}

folly::fbstring QueryHandler::transform() {
  // time align all data
  int64_t timeBucketCount = (endTime_ - startTime_) / 30;
  int keyCount = beringeiTimeSeries_.size();
  // count is a special aggregation that will be missed due to default values
  int timeSeriesCounts[timeBucketCount]{};
  // pre-allocate the array size
  double timeSeries[keyCount][timeBucketCount]{};
  int keyIndex = 0;
  for (const auto& keyTimeSeries : beringeiTimeSeries_) {
    const std::string& keyName = keyTimeSeries.first.key;
    for (const auto& timePair : keyTimeSeries.second) {
      int timeBucketId = (timePair.unixTime - startTime_) / 30;
      timeSeries[keyIndex][timeBucketId] = timePair.value;
      timeSeriesCounts[timeBucketId]++;
    }
    keyIndex++;
  }
  int dataPointAggCount = 1;
  if (timeBucketCount > MAX_DATA_POINTS) {
    dataPointAggCount = std::ceil((double)timeBucketCount / MAX_DATA_POINTS);
  }
  int condensedBucketCount = timeBucketCount / dataPointAggCount;
  // allocate condensed time series, sum series (by key) for later
  // sorting, and sum of time bucket
  double cTimeSeries[keyCount][condensedBucketCount]{};
  double sumSeries[keyCount]{};
  double sumTimeBucket[condensedBucketCount]{};
  for (int i = 0; i < keyCount; i++) {
    int timeBucketId = 0;
    int startBucketId = 0;
    int endBucketId = 0 + dataPointAggCount;
    while (startBucketId < timeBucketCount) {
      // fetch all data points we need to aggregate
      if (endBucketId >= timeBucketCount) {
        endBucketId = timeBucketCount - 1;
      }
      // aggregations
      double sum = std::accumulate(&timeSeries[i][startBucketId],
                                   &timeSeries[i][endBucketId + 1],
                                   0.0);
      double avg = sum / (double)(endBucketId - startBucketId + 1);
      auto minMax = std::minmax_element(&timeSeries[i][startBucketId],
                                        &timeSeries[i][endBucketId + 1]);
      double min = *minMax.first;
      double max = *minMax.second;
      sumTimeBucket[timeBucketId] += avg;
      if (query_.agg_type == "avg") {
        // the time aggregated bucket for this key
        if (i == 0) {
          aggSeries_["min"].push_back(min);
          aggSeries_["max"].push_back(max);
        } else {
          aggSeries_["min"][timeBucketId] =
            std::min(aggSeries_["min"][timeBucketId], min);
          aggSeries_["max"][timeBucketId] =
            std::max(aggSeries_["max"][timeBucketId], max);
        }
      } else if (query_.agg_type == "count") {
        double countSum = std::accumulate(&timeSeriesCounts[startBucketId],
                                          &timeSeriesCounts[endBucketId + 1],
                                          0);
        double countAvg = countSum / (double)(endBucketId - startBucketId + 1);
        aggSeries_[query_.agg_type].push_back(countAvg);
      } else if (query_.agg_type == "event") {
        // event aggregation
        // I0524 19:17:35.861244 23268 QueryHandler.cpp:190] Bucket[0][2877] = 964
        // I0524 19:17:35.861249 23268 QueryHandler.cpp:190] Bucket[0][2878] = 2136
        // I0524 19:17:35.861253 23268 QueryHandler.cpp:190] Bucket[0][2879] = 3308
        for (int e = startBucketId; e <= endBucketId; e++) {
          LOG(INFO) << "Bucket[" << i << "][" << e << "] = " << timeSeries[i][e];
        }
      } else {
        // no aggregation
        cTimeSeries[i][timeBucketId] = avg;
        if (keyCount > MAX_COLUMNS) {
          sumSeries[i] += sum;
        }
      }
      // set next bucket
      startBucketId += dataPointAggCount;
      endBucketId += dataPointAggCount;
      timeBucketId++;
    }
  }
  // now we have time aggregated data
  // sort by avg value across time series if needed
  folly::dynamic datapoints = folly::dynamic::array();
  for (int i = 0; i < (timeBucketCount / dataPointAggCount); i++) {
    datapoints.push_back(folly::dynamic::array(
      (startTime_ + (i * dataPointAggCount * 30)) * 1000));
  }
  folly::dynamic columns = folly::dynamic::array();
  columns.push_back("time");
  if (query_.agg_type == "bottom" ||
      query_.agg_type == "top") {
    if (keyCount > MAX_COLUMNS) {
      // sort data
      std::vector<std::pair<int, double>> keySums;
      for (int i = 0; i < keyCount; i++) {
        keySums.push_back(std::make_pair(i, sumSeries[i]));
      }
      auto sortLess = [](auto& lhs, auto& rhs) {return lhs.second < rhs.second;};
      auto sortGreater = [](auto& lhs, auto& rhs) {return lhs.second > rhs.second;};
      if (query_.agg_type == "bottom") {
        std::sort(keySums.begin(), keySums.end(), sortLess);
      } else {
        std::sort(keySums.begin(), keySums.end(), sortGreater);
      }
      keySums.resize(MAX_COLUMNS);
      for (const auto& kv : keySums) {
        columns.push_back(columnNames_[kv.first]);
        // loop over time series
        for (int i = 0; i < (timeBucketCount / dataPointAggCount); i++) {
          datapoints[i].push_back(timeSeries[kv.first][i]);
        }
      }
    } else {
      // agg series
      for (int i = 0; i < keyCount; i++) {
        columns.push_back(columnNames_[i]);
        // loop over time series
        for (int e = 0; e < condensedBucketCount; e++) {
          datapoints[e].push_back(cTimeSeries[i][e]);
        }
      }
    }
  } else if (query_.agg_type == "sum") {
    columns.push_back(query_.agg_type);
    for (int i = 0; i < condensedBucketCount; i++) {
      datapoints[i].push_back(sumTimeBucket[i]);
    }
  } else if (query_.agg_type == "count") {
    for (const auto& aggSerie : aggSeries_) {
      columns.push_back(aggSerie.first);
      for (int i = 0; i < condensedBucketCount; i++) {
        datapoints[i].push_back(aggSerie.second[i]);
      }
    }
  } else if (query_.agg_type == "avg") {
    // show agg series
    for (const auto& aggSerie : aggSeries_) {
      columns.push_back(aggSerie.first);
      for (int i = 0; i < condensedBucketCount; i++) {
        datapoints[i].push_back(aggSerie.second[i]);
      }
    }
    columns.push_back("avg");
    for (int i = 0; i < condensedBucketCount; i++) {
      datapoints[i].push_back(sumTimeBucket[i] / keyCount);
    }
  }
  folly::dynamic response = folly::dynamic::object;
  response["name"] = "id";
  response["columns"] = columns;
  response["points"] = datapoints;
  return "[" + folly::toJson(response) + "]";
}

folly::fbstring QueryHandler::handleQuery() {
  auto startTime = (int64_t)duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();
  folly::Promise<TimeSeries> p;
  auto f = p.getFuture();
  folly::EventBase eb;
  eb.runInLoop([this, p = std::move(p)] () mutable {
    BeringeiClient client(
        configurationAdapter_, 1, BeringeiClient::kNoWriterThreads);
    int numShards = client.getNumShards();
    auto beringeiRequest = createBeringeiRequest(query_, numShards);
    client.get(beringeiRequest, beringeiTimeSeries_);
  });
  std::thread tEb([&eb]() { eb.loop(); });
  tEb.join();
  auto fetchTime = (int64_t)duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();
  columnNames();
  auto columnNamesTime = (int64_t)duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();
  folly::fbstring results{};
  if (query_.type == "event") {
    results = eventHandler(26 /* ms for heartbeats */);
  } else {
    results = transform();
  }
  auto endTime = (int64_t)duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();
  // TODO - add fetch
  LOG(INFO) << "Query completed. " 
            << "Fetch: " << (fetchTime - startTime) << "ms, "
            << "Column names: " << (columnNamesTime - fetchTime) << "ms, "
            << "Transform: " << (endTime - columnNamesTime) << "ms, "
            << "Total: " << (endTime - startTime) << "ms.";
  return results;
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
    const Query& request,
    const int numShards) {
  // TODO - absolute time
  startTime_ = std::time(nullptr) - (request.min_ago * 60);
  endTime_ = std::time(nullptr);
  GetDataRequest beringeiRequest;

  beringeiRequest.begin = startTime_;
  beringeiRequest.end = endTime_;

  for (const auto& keyId : request.key_ids) {
    Key beringeiKey;
    beringeiKey.key = std::to_string(keyId);
    // everything is shard 0 on the writer side
    beringeiKey.shardId = 0;
    beringeiRequest.keys.push_back(beringeiKey);
  }

  return beringeiRequest;
}
}
} // facebook::gorilla
