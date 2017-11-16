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
    std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
    std::shared_ptr<BeringeiClient> beringeiClient)
    : RequestHandler(),
      configurationAdapter_(configurationAdapter),
      beringeiClient_(beringeiClient) {}

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
  query::QueryRequest request;
  try {
    request = SimpleJSONSerializer::deserialize<query::QueryRequest>(body);
  } catch (const std::exception&) {
    LOG(INFO) << "Error deserializing QueryRequest";
    ResponseBuilder(downstream_)
        .status(500, "OK")
        .header("Content-Type", "application/json")
        .body("Failed de-serializing QueryRequest")
      .sendWithEOM();
    return;
  }
  logRequest(request);
  columnNames_.clear();
  timeSeries_.clear();
  aggSeries_.clear();
  // one json response
  folly::dynamic response = folly::dynamic::array();
  for (const auto& query : request.queries) {
    query_ = query;
    LOG(INFO) << "Request for " << query_.key_ids.size() << " key ids of "
              << query_.agg_type << " aggregation, for " << query_.min_ago
              << " minutes ago.";
    folly::dynamic jsonQueryResp;
    try {
      jsonQueryResp = handleQuery();
    } catch (const std::exception& ex) {
      LOG(ERROR) << "Unable to handle query: " << ex.what();
      ResponseBuilder(downstream_)
          .status(500, "OK")
          .header("Content-Type", "application/json")
          .body("Failed handling query")
        .sendWithEOM();
      return;
    }
    response.push_back(jsonQueryResp);
  }
  std::string responseJson;
  try {
    responseJson = folly::toJson(response);
  } catch (const std::runtime_error& ex) {
    LOG(ERROR) << "Empty response for query: " << ex.what();
  }
  ResponseBuilder(downstream_)
      .status(200, "OK")
      .header("Content-Type", "application/json")
      .body(responseJson)
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

void QueryHandler::logRequest(query::QueryRequest request) { }

void QueryHandler::columnNames() {
  // set column names
  if (query_.agg_type == "top" ||
      query_.agg_type == "bottom" ||
      query_.agg_type == "none") {
    // top, bottom, none
    std::unordered_set<std::string> keyNames;
    std::unordered_set<std::string> linkNames;
    std::unordered_set<std::string> nodeNames;
    std::unordered_set<std::string> displayNames;
    for (const auto& keyData : query_.data) {
      keyNames.insert(keyData.key);
      // add title append
      if (keyData.__isset.linkTitleAppend) {
        linkNames.insert(keyData.linkName + " " + keyData.linkTitleAppend);
      } else {
        linkNames.insert(keyData.linkName);
      }
      nodeNames.insert(keyData.nodeName);
      displayNames.insert(keyData.displayName);
    }
    for (const auto& keyData : query_.data) {
      std::string columnName = keyData.node;
      if (keyData.linkName.length() &&
          linkNames.size() == query_.key_ids.size()) {
        if (keyData.__isset.linkTitleAppend) {
          columnName = keyData.linkName + " " + keyData.linkTitleAppend;
        } else {
          columnName = keyData.linkName;
        }
      } else if (keyData.displayName.length() &&
                 displayNames.size() == query_.key_ids.size()) {
        columnName = keyData.displayName;
      } else if (keyNames.size() == query_.key_ids.size()) {
        columnName = keyData.key;
      } else if (keyData.nodeName.length() &&
                 nodeNames.size() == query_.key_ids.size()) {
        columnName = keyData.nodeName;
      } else {
        columnName = folly::sformat("{} / {}", keyData.nodeName, keyData.key);
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
folly::dynamic QueryHandler::eventHandler(int dataPointIncrementMs,
                                          const std::string& metricName) {
  int64_t timeBucketCount = (endTime_ - startTime_) / 30;
  int keyCount = beringeiTimeSeries_.size();
  // count is a special aggregation that will be missed due to default values
  int timeSeriesCounts[timeBucketCount]{};
  // pre-allocate the array size
  double *timeSeries = new double[query_.key_ids.size() * timeBucketCount]();
  int keyIndex = 0;
  // map returned key -> index
  std::unordered_map<std::string, int64_t> keyMapIndex;
  for (const auto& keyId : query_.key_ids) {
    keyMapIndex[std::to_string(keyId)] = keyIndex;
    keyIndex++;
  }
  for (const auto& keyTimeSeries : beringeiTimeSeries_) {
    const std::string& keyName = keyTimeSeries.first.key;
    keyIndex = keyMapIndex[keyName];
    for (const auto& timePair : keyTimeSeries.second) {
      int timeBucketId = (timePair.unixTime - startTime_) / 30;
      timeSeries[keyIndex * timeBucketCount + timeBucketId] = timePair.value;
    }
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
      double timeVal = timeSeries[keyIndex * timeBucketCount + timeIndex];
      double prevVal = timeIndex > 0 ? timeSeries[keyIndex * timeBucketCount + timeIndex - 1] : 0;
      if (timeIndex > 0 && prevVal >= 0 && timeVal >= 0) {
        VLOG(3) << "VERBOSE: timeSeries[" << keyIndex << "][" << timeIndex
                << "] = " << timeVal << " | Diff: " << (timeVal - prevVal)
                << " / " << ((timeVal - prevVal) / 30.0);;
      } else {
        VLOG(3) << "VERBOSE: timeSeries[" << keyIndex << "][" << timeIndex
                << "] = " << timeVal;
      }
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
            VLOG(2) << "[" << timeIndex << "] Filled all " << missingCounter
                    << " missed data buckets, complete intervals covered: "
                    << completeIntervals << ", partial seconds (total): "
                    << partialIntervalSec;
            // online events
            if (startOnlineIndex == -1) {
              startOnlineIndex = timeIndex - missingCounter;
            }
            VLOG(2) << "[" << timeIndex << "] Marked starting index of uptime: " << startOnlineIndex;
          } else {
            // fill partial interval
            int wholeIntervals = (int)completeIntervals;
            upPoints+= wholeIntervals;
            double remainIntervalSec = partialIntervalSec - (wholeIntervals * 30);
            VLOG(2) << "[" << timeIndex << "] Filled partial missing data buckets. Missing intervals: "
                    << missingCounter << ", covered intervals: " << completeIntervals
                    << " (" << wholeIntervals << "), partial seconds: " << remainIntervalSec
                    << " start interval: " << startOnlineIndex;
            partialSeconds += remainIntervalSec;
            // only part of the interval recovered, we must have an event
            onlineEvents.push_back(makeEvent(startOnlineIndex,
                                             timeIndex - missingCounter));
            // events, ignore partial intervals
            startOnlineIndex = timeIndex - wholeIntervals;
          }
          missingCounter = 0;
        }
        if (startOnlineIndex == -1) {
          VLOG(2) << "Marking start index: " << timeIndex;
          startOnlineIndex = timeIndex;
        }
        upPoints++;
      } else {
        // part of interval is good, calculate how much of it to mark
        double partialIntervalSec = timeVal / expectedDataPoints;
        partialSeconds += partialIntervalSec;
        VLOG(2) << "[" << timeIndex << "] Some part of interval is up, about " << partialIntervalSec;
        // events
        if (startOnlineIndex >= 0) {
          // partial interval, mark this interval as down
          VLOG(2) << "[" << timeIndex << "] Partially online interval, marking offline. We were online for: "
                  << (timeIndex - missingCounter - startOnlineIndex)
                  << " intervals, missing counter: " << missingCounter
                  << ", event start: " << startOnlineIndex
                  << ", end: " << (timeIndex - missingCounter);;
          onlineEvents.push_back(makeEvent(startOnlineIndex,
                                           timeIndex - missingCounter));
          // we're online now
          startOnlineIndex = timeIndex;
        }
        // reset missing counter, which only tracks whole intervals offline
        missingCounter = 0;
      }
      // finalize events
      if ((timeIndex + 1) == timeBucketCount && startOnlineIndex >= 0) {
        // allow one missing interval to account for delays in incoming data
        if (missingCounter == 1) {
          missingCounter = 0;
        }
        VLOG(2) << "[END] Start online index set, push event from start: "
                << startOnlineIndex << " to: "
                << (timeIndex - missingCounter)
                << ", missing counter: " << missingCounter;
        onlineEvents.push_back(makeEvent(startOnlineIndex,
                                         timeIndex - missingCounter));
      }
    }
    // seconds of uptime / seconds in period
    double uptimePerc = 0;
    if (upPoints > 0 || partialSeconds > 0) {
      uptimePerc = (upPoints * 30 + partialSeconds) / (timeBucketCount * 30) * 100.0;
    }
    auto& name = query_.data[keyIndex].displayName;
    VLOG(2) << "Key ID: " << query_.data[keyIndex].key
            << ", Name: " << name
            << ", Expected count: " << timeBucketCount
            << ", up count: " << upPoints
            << ", partial seconds: " << partialSeconds
            << ", uptime: " << uptimePerc << "%";
    linkMap[name] = folly::dynamic::object;
    linkMap[name][metricName] = uptimePerc;
    linkMap[name]["events"] = onlineEvents;
  }
  delete[] timeSeries;
  folly::dynamic response = folly::dynamic::object;
  response["metrics"] = linkMap;
  response["start"] = startTime_ * 1000;
  response["end"] = endTime_ * 1000;
  return response;

}
std::string QueryHandler::getTimeStr(time_t timeSec) {
  char timeStr[100];
  std::strftime(timeStr,
                sizeof(timeStr),
                "%T",
                std::localtime(&timeSec));
  return std::string(timeStr);
}
/*
 * Create a single event
 */
folly::dynamic QueryHandler::makeEvent(int64_t startIndex, int64_t endIndex) {
  int uptimeDurationSec = (endIndex - startIndex) * 30;
  // Online for [duration text] from [start time] to [end time]
  std::string title = folly::sformat("{} minutes from {} to {}",
                                     (uptimeDurationSec / 60),
                                     getTimeStr(startTime_ + (startIndex * 30)),
                                     getTimeStr(startTime_ + (endIndex * 30)));
  return folly::dynamic::object
            ("startTime", (startTime_ + (startIndex * 30)))
            ("endTime", (startTime_ + (endIndex * 30)))
            ("title", title);
}

folly::dynamic QueryHandler::transform() {
  // time align all data
  int64_t timeBucketCount = (endTime_ - startTime_) / 30;
  int keyCount = beringeiTimeSeries_.size();
  // count is a special aggregation that will be missed due to default values
  int timeSeriesCounts[timeBucketCount]{};
  // pre-allocate the array size
  double *timeSeries = new double[keyCount * timeBucketCount];
  int keyIndex = 0;
  for (const auto& keyTimeSeries : beringeiTimeSeries_) {
    const std::string& keyName = keyTimeSeries.first.key;
    for (const auto& timePair : keyTimeSeries.second) {
      int timeBucketId = (timePair.unixTime - startTime_) / 30;
      timeSeries[keyIndex * timeBucketCount + timeBucketId] = timePair.value;
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
      double sum = std::accumulate(&timeSeries[i * timeBucketCount + startBucketId],
                                   &timeSeries[i * timeBucketCount + endBucketId + 1],
                                   0.0);
      double avg = sum / (double)(endBucketId - startBucketId + 1);
      auto minMax = std::minmax_element(&timeSeries[i * timeBucketCount + startBucketId],
                                        &timeSeries[i * timeBucketCount + endBucketId + 1]);
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
  for (int i = 0; i < condensedBucketCount; i++) {
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
      auto sortLess = [](auto& lhs, auto& rhs) {
        return lhs.second < rhs.second;};
      auto sortGreater = [](auto& lhs, auto& rhs) {
        return lhs.second > rhs.second;};
      if (query_.agg_type == "bottom") {
        std::sort(keySums.begin(), keySums.end(), sortLess);
      } else {
        std::sort(keySums.begin(), keySums.end(), sortGreater);
      }
      keySums.resize(MAX_COLUMNS);
      for (const auto& kv : keySums) {
        columns.push_back(columnNames_[kv.first]);
        // loop over time series
        for (int i = 0; i < condensedBucketCount; i++) {
          datapoints[i].push_back(cTimeSeries[kv.first][i]);
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
  } else if (query_.agg_type == "none") {
    // agg series
    for (int i = 0; i < keyCount; i++) {
      columns.push_back(columnNames_[i]);
      // loop over time series
      for (int e = 0; e < condensedBucketCount; e++) {
        datapoints[e].push_back(cTimeSeries[i][e]);
      }
    }
  }
  delete[] timeSeries;
  folly::dynamic response = folly::dynamic::object;
  response["name"] = "id";
  response["columns"] = columns;
  response["points"] = datapoints;
  return response;
}

folly::dynamic QueryHandler::handleQuery() {
  auto startTime = (int64_t)duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();
  // validate first, prefer to throw here (no futures)
  validateQuery(query_);
  // fetch async data
  folly::EventBase eb;
  eb.runInLoop([this] () mutable {
    BeringeiClient client(
        configurationAdapter_, 1, BeringeiClient::kNoWriterThreads);
    int numShards = client.getNumShards();
    auto beringeiRequest = createBeringeiRequest(query_, numShards);
    beringeiClient_->get(beringeiRequest, beringeiTimeSeries_);
  });
  std::thread tEb([&eb]() { eb.loop(); });
  tEb.join();
  auto fetchTime = (int64_t)duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();
  columnNames();
  auto columnNamesTime = (int64_t)duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();
  folly::dynamic results{};
  if (query_.type == "event") {
//    results = eventHandler(26 /* ms for heartbeats */, "alive");
    // uplink bw request
    results = eventHandler(25 /* ms for heartbeats */, "alive");
  } else if (query_.type == "uptime_sec") {
    results = eventHandler(1000, "minion_uptime");
  } else {
    results = transform();
  }
  auto endTime = (int64_t)duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();
  LOG(INFO) << "Query completed. "
            << "Fetch: " << (fetchTime - startTime) << "ms, "
            << "Column names: " << (columnNamesTime - fetchTime) << "ms, "
            << "Event/Transform: " << (endTime - columnNamesTime) << "ms, "
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

void QueryHandler::validateQuery(
    const query::Query& request) {
  if (request.__isset.start_ts &&
      request.__isset.end_ts) {
    // TODO - sanity check time
    startTime_ = std::ceil(request.start_ts / 30.0) * 30;
    endTime_ = std::ceil(request.end_ts / 30.0) * 30;
    if (endTime_ <= startTime_) {
      LOG(ERROR) << "Request for invalid time window: " << startTime_
                 << " <-> " << endTime_;
      throw std::runtime_error("Request for invalid time window");
    }
  } else if (request.__isset.min_ago) {
    startTime_ = std::time(nullptr) - (60 * request.min_ago);
    endTime_ = std::time(nullptr);
  } else {
    // default to 1 day here
    startTime_ = std::time(nullptr) - (24 * 60 * 60);
    endTime_ = std::time(nullptr);
  }
  LOG(INFO) << "Request for start: " << startTime_ << " <-> " << endTime_;
}

GetDataRequest QueryHandler::createBeringeiRequest(
    const query::Query& request,
    const int numShards) {
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
