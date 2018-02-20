/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "BeringeiData.h"

#include <utility>

#include <folly/DynamicConverter.h>
#include <folly/io/IOBuf.h>
#include <thrift/lib/cpp/util/ThriftSerializer.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

using apache::thrift::SimpleJSONSerializer;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;

namespace facebook {
namespace gorilla {

const int MAX_COLUMNS = 7;
const int MAX_DATA_POINTS = 60;
const int NUM_HBS_PER_SEC = 39; // approximately

BeringeiData::BeringeiData(
    std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
    std::shared_ptr<BeringeiClient> beringeiClient,
    const query::QueryRequest& request)
    : configurationAdapter_(configurationAdapter),
      beringeiClient_(beringeiClient),
      request_(request) {}

folly::dynamic
BeringeiData::process() {
  columnNames_.clear();
  timeSeries_.clear();
  aggSeries_.clear();
  // one json response
  folly::dynamic response = folly::dynamic::array();
  for (const auto &query : request_.queries) {
    query_ = query;
    if (query_.key_ids.empty()) {
      continue;
    }
    LOG(INFO) << "Request for " << query_.key_ids.size() << " key ids of "
              << query_.agg_type << " aggregation, for " << query_.min_ago
              << " minutes ago.";
    folly::dynamic jsonQueryResp;
    jsonQueryResp = handleQuery();
    response.push_back(jsonQueryResp);
  }
  return response;
}

void BeringeiData::columnNames() {
  // set column names
  if (query_.agg_type == "top" || query_.agg_type == "bottom" ||
      query_.agg_type == "none") {
    // top, bottom, none
    std::unordered_set<std::string> keyNames;
    std::unordered_set<std::string> linkNames;
    std::unordered_set<std::string> nodeNames;
    std::unordered_set<std::string> displayNames;
    for (const auto &keyData : query_.data) {
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
    for (const auto &keyData : query_.data) {
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
folly::dynamic BeringeiData::eventHandler(int dataPointIncrementMs,
                                          const std::string &metricName) {
  int64_t timeBucketCount = (endTime_ - startTime_) / 30;
  int keyCount = beringeiTimeSeries_.size();
  // count is a special aggregation that will be missed due to default values
  int timeSeriesCounts[timeBucketCount]{};
  // pre-allocate the array size
  double *timeSeries = new double[query_.key_ids.size() * timeBucketCount]();
  int keyIndex = 0;
  // map returned key -> index
  std::unordered_map<std::string, int64_t> keyMapIndex;
  for (const auto &keyId : query_.key_ids) {
    keyMapIndex[std::to_string(keyId)] = keyIndex;
    keyIndex++;
  }
  for (const auto &keyTimeSeries : beringeiTimeSeries_) {
    const std::string &keyName = keyTimeSeries.first.key;
    keyIndex = keyMapIndex[keyName];
    for (const auto &timePair : keyTimeSeries.second) {
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
      double prevVal =
          timeIndex > 0 ? timeSeries[keyIndex * timeBucketCount + timeIndex - 1]
                        : 0;
      if (timeIndex > 0 && prevVal >= 0 && timeVal >= 0) {
        VLOG(2) << "VERBOSE: timeSeries[" << keyIndex << "][" << timeIndex
                << "] = " << timeVal << " | Diff: " << (timeVal - prevVal)
                << " / " << ((timeVal - prevVal) / 30.0);
        ;
      } else {
        VLOG(2) << "VERBOSE: timeSeries[" << keyIndex << "][" << timeIndex
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
            VLOG(3) << "[" << timeIndex << "] Filled all " << missingCounter
                    << " missed data buckets, complete intervals covered: "
                    << completeIntervals
                    << ", partial seconds (total): " << partialIntervalSec;
            // online events
            if (startOnlineIndex == -1) {
              startOnlineIndex = timeIndex - missingCounter;
            }
            VLOG(3) << "[" << timeIndex << "] Marked starting index of uptime: "
                    << startOnlineIndex;
          } else {
            // fill partial interval
            int wholeIntervals = (int)completeIntervals;
            upPoints += wholeIntervals;
            double remainIntervalSec =
                partialIntervalSec - (wholeIntervals * 30);
            VLOG(2)
                << "[" << timeIndex
                << "] Filled partial missing data buckets. Missing intervals: "
                << missingCounter
                << ", covered intervals: " << completeIntervals << " ("
                << wholeIntervals << "), partial seconds: " << remainIntervalSec
                << " start interval: " << startOnlineIndex;
            partialSeconds += remainIntervalSec;
            // only part of the interval recovered, we must have an event
            onlineEvents.push_back(
                makeEvent(startOnlineIndex, timeIndex - missingCounter));
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
        VLOG(2) << "[" << timeIndex << "] Some part of interval is up, about "
                << partialIntervalSec;
        // events
        if (startOnlineIndex >= 0) {
          // partial interval, mark this interval as down
          VLOG(2) << "[" << timeIndex << "] Partially online interval, marking "
                                         "offline. We were online for: "
                  << (timeIndex - missingCounter - startOnlineIndex)
                  << " intervals, missing counter: " << missingCounter
                  << ", event start: " << startOnlineIndex
                  << ", end: " << (timeIndex - missingCounter);
          ;
          onlineEvents.push_back(
              makeEvent(startOnlineIndex, timeIndex - missingCounter));
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
          upPoints++;
        }
        VLOG(2) << "[END] Start online index set, push event from start: "
                << startOnlineIndex << " to: " << (timeIndex - missingCounter)
                << ", missing counter: " << missingCounter;
        onlineEvents.push_back(
            makeEvent(startOnlineIndex, timeIndex - missingCounter));
      }
    }
    // seconds of uptime / seconds in period
    double uptimePerc = 0;
    if (upPoints > 0 || partialSeconds > 0) {
      uptimePerc =
          (upPoints * 30 + partialSeconds) / (timeBucketCount * 30) * 100.0;
    }
    auto &name = query_.data[keyIndex].displayName;
    VLOG(2) << "Key ID: " << query_.data[keyIndex].key
            << ", Name: " << name
            << ", Expected count: " << timeBucketCount
            << ", up count: " << upPoints
            << ", partial seconds: " << partialSeconds
            << ", uptime: " << uptimePerc << "%";
    auto linkMapIt = linkMap.find(name);
    if (linkMapIt != linkMap.items().end()) {
      auto metricMapIt = linkMapIt->second.find(metricName);
      // replace data if metric is higher on current
      if (metricMapIt != linkMapIt->second.items().end() &&
          uptimePerc < metricMapIt->second.asDouble()) {
        continue;
      }
    }
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

std::string BeringeiData::getTimeStr(time_t timeSec) {
  char timeStr[100];
  std::strftime(timeStr, sizeof(timeStr), "%T", std::localtime(&timeSec));
  return std::string(timeStr);
}

/*
 * Create a single event
 */
folly::dynamic BeringeiData::makeEvent(int64_t startIndex, int64_t endIndex) {
  int uptimeDurationSec = (endIndex - startIndex) * 30;
  // Online for [duration text] from [start time] to [end time]
  std::string title =
      folly::sformat("{} minutes from {} to {}", (uptimeDurationSec / 60),
                     getTimeStr(startTime_ + (startIndex * 30)),
                     getTimeStr(startTime_ + (endIndex * 30)));
  return folly::dynamic::object("startTime", (startTime_ + (startIndex * 30)))(
      "endTime", (startTime_ + (endIndex * 30)))("title", title);
}

folly::dynamic BeringeiData::latest() {
  int keyCount = beringeiTimeSeries_.size();
  int keyIndex = 0;
  // store the latest value for each key
  double latestValue[keyCount]{};
  folly::dynamic response = folly::dynamic::object;
  for (const auto &keyTimeSeries : beringeiTimeSeries_) {
    const std::string &keyName = keyTimeSeries.first.key;
    if (keyTimeSeries.second.size()) {
      response[query_.data[keyIndex].displayName] =
        keyTimeSeries.second.back().value;
    }
    keyIndex++;
  }
  return response;
}

void
BeringeiData::valueOrNull(folly::dynamic& obj, double value, int count) {
  if (value == std::numeric_limits<double>::max() ||
      std::isnan(value) ||
      std::isinf(value)) {
    obj.push_back(nullptr);
  } else if (count > 1) {
    obj.push_back((double)value / count);
  } else {
    obj.push_back(value);
  }
}

folly::dynamic BeringeiData::transform() {
  // time align all data
  int64_t timeBucketCount = (endTime_ - startTime_) / 30;
  int keyCount = beringeiTimeSeries_.size();
  // pre-allocate the array size
  double *timeSeries = new double[keyCount * timeBucketCount]{};
  int dataPointAggCount = 1;
  if (timeBucketCount > MAX_DATA_POINTS) {
    dataPointAggCount = std::ceil((double)timeBucketCount / MAX_DATA_POINTS);
  }
  int condensedBucketCount = std::ceil((double)timeBucketCount / dataPointAggCount);
  int countPerBucket[keyCount][condensedBucketCount + 1]{};
  // allocate condensed time series, sum series (by key) for later
  // sorting, and sum of time bucket
  double cTimeSeries[keyCount][condensedBucketCount + 1]{};
  double sumSeries[keyCount]{};
  // sum of all known data points in each bucket
  double sumTimeBucket[condensedBucketCount + 1]{};
  // sum of the average of known data points in each bucket
  double sumOfAvgTimeBucket[condensedBucketCount + 1]{};
  int countTimeBucket[condensedBucketCount + 1]{};
  int keyIndex = 0;
  // store the latest value for each key
  for (const auto &keyTimeSeries : beringeiTimeSeries_) {
    const std::string &keyName = keyTimeSeries.first.key;
    // fetch the last value, assume 0 for not-found
    for (const auto &timePair : keyTimeSeries.second) {
      int timeBucketId = (timePair.unixTime - startTime_) / 30;
      // log # of dps per bucket for DP smoothing
      int condensedBucketId = timeBucketId > 0 ? (timeBucketId / dataPointAggCount) : 0;
      int oldIndex = (keyIndex * timeBucketCount + timeBucketId);
      int newIndex = (keyIndex * timeBucketCount + (condensedBucketId * dataPointAggCount) + countPerBucket[keyIndex][condensedBucketId]);
      if (isnan(timePair.value)) {
        continue;
      }
      // aggregate count by bucket (per-key)
      countPerBucket[keyIndex][condensedBucketId]++;
      // aggregate count by time bucket (all-keys)
      countTimeBucket[condensedBucketId]++;

      // place the value into the next position in the bucket (don't care
      // about the correct time slot in the bucket)
      timeSeries[newIndex] = timePair.value;
      // sum all data-points in the series
      sumSeries[keyIndex] += timePair.value;
      sumTimeBucket[condensedBucketId] += timePair.value;
    }
    keyIndex++;
  }
  for (int i = 0; i < keyCount; i++) {
    int timeBucketId = 0;
    int startBucketId = 0;
    while (startBucketId < timeBucketCount) {
      // number of data-points in condensed bucket (ignoring missing)
      int bucketDpCount = countPerBucket[i][timeBucketId];
      double sum = std::numeric_limits<double>::max();
      double avg = std::numeric_limits<double>::max();
      double min = std::numeric_limits<double>::max();
      double max = std::numeric_limits<double>::max();
      // perform aggregations only if we have data
      if (bucketDpCount > 0) {
        // sum all non-missing data points
        sum = std::accumulate(
            &timeSeries[i * timeBucketCount + startBucketId],
            &timeSeries[i * timeBucketCount + startBucketId + bucketDpCount/* do we need +1 for .end()? */], 0.0);
        if (!isnan(sum)) {
          // divide by the # of DPs in the bucket, ignoring missing data
          avg = sum / (double)(bucketDpCount);
        }
        sumOfAvgTimeBucket[timeBucketId] += avg;
        // min + max over the real data
        auto minMax = std::minmax_element(
            &timeSeries[i * timeBucketCount + startBucketId],
            &timeSeries[i * timeBucketCount + startBucketId + bucketDpCount/* todo - same question */]);
        min = *minMax.first;
        max = *minMax.second;
      }
      if (query_.agg_type == "avg") {
        // the time aggregated bucket for this key
        if (i == 0) {
          aggSeries_["min"].push_back(min);
          aggSeries_["max"].push_back(max);
        } else {
          aggSeries_["min"][timeBucketId] =
              std::min(aggSeries_["min"][timeBucketId], min);
          // max only added if count is set
          if (bucketDpCount > 0) {
            if (aggSeries_["max"][timeBucketId] == std::numeric_limits<double>::max()) {
              aggSeries_["max"][timeBucketId] = max;
            } else {
              aggSeries_["max"][timeBucketId] =
                  std::max(aggSeries_["max"][timeBucketId], max);
            }
          }
        }
      } else if (query_.agg_type == "count") {
        double countSum = countTimeBucket[timeBucketId];
        double countAvg = countSum == 0 ? 0 : (countSum / (double)(bucketDpCount));
        aggSeries_[query_.agg_type].push_back(countAvg);
      } else {
        // no aggregation
        cTimeSeries[i][timeBucketId] = avg;
      }
      // set next bucket
      startBucketId += dataPointAggCount;
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
  if (query_.agg_type == "bottom" || query_.agg_type == "top") {
    if (keyCount > MAX_COLUMNS) {
      // sort data
      std::vector<std::pair<int, double> > keySums;
      for (int i = 0; i < keyCount; i++) {
        keySums.push_back(std::make_pair(i, sumSeries[i]));
      }
      // sort for 'bottom' aggregation, ignoring nan/inf data
      auto sortLess = [](auto &lhs,
                         auto &rhs) {
        if (std::isnan(rhs.second) || std::isinf(rhs.second)) {
          return true;
        }
        if (std::isnan(lhs.second) || std::isinf(lhs.second)) {
          return false;
        }
        return lhs.second < rhs.second;
      };
      // sort for 'top' aggregation, ignoring nan/inf data
      auto sortGreater = [](auto &lhs,
                            auto &rhs) {
        if (std::isnan(lhs.second) || std::isinf(lhs.second)) {
          return false;
        }
        if (std::isnan(rhs.second) || std::isinf(rhs.second)) {
          return true;
        }
        return lhs.second > rhs.second;
      };
      if (query_.agg_type == "bottom") {
        std::sort(keySums.begin(), keySums.end(), sortLess);
      } else {
        std::sort(keySums.begin(), keySums.end(), sortGreater);
      }
      keySums.resize(MAX_COLUMNS);
      for (const auto &kv : keySums) {
        columns.push_back(columnNames_[kv.first]);
        // loop over time series
        for (int i = 0; i < condensedBucketCount; i++) {
          valueOrNull(datapoints[i], cTimeSeries[kv.first][i]);
        }
      }
    } else {
      // agg series
      for (int i = 0; i < keyCount; i++) {
        columns.push_back(columnNames_[i]);
        // loop over time series
        for (int e = 0; e < condensedBucketCount; e++) {
          valueOrNull(datapoints[e], cTimeSeries[i][e]);
        }
      }
    }
  } else if (query_.agg_type == "sum") {
    columns.push_back(query_.agg_type);
    for (int i = 0; i < condensedBucketCount; i++) {
      // divide by the received data points per interval?
      valueOrNull(datapoints[i], sumOfAvgTimeBucket[i]);
    }
  } else if (query_.agg_type == "count") {
    for (const auto &aggSerie : aggSeries_) {
      columns.push_back(aggSerie.first);
      for (int i = 0; i < condensedBucketCount; i++) {
        valueOrNull(datapoints[i], aggSerie.second[i]);
      }
    }
  } else if (query_.agg_type == "avg") {
    // adds min + max from above
    for (const auto &aggSerie : aggSeries_) {
      columns.push_back(aggSerie.first);
      for (int i = 0; i < condensedBucketCount; i++) {
        valueOrNull(datapoints[i], aggSerie.second[i]);
      }
    }
    // add average from sum values
    columns.push_back("avg");
    for (int i = 0; i < condensedBucketCount; i++) {
      if (countTimeBucket[i] > 0) {
        // divide by the # of received data points for the bucket
        valueOrNull(datapoints[i], sumTimeBucket[i], countTimeBucket[i]);
      } else {
        valueOrNull(datapoints[i], std::numeric_limits<double>::max());
      }
    }
  } else if (query_.agg_type == "none") {
    // agg series
    for (int i = 0; i < keyCount; i++) {
      columns.push_back(columnNames_[i]);
      // loop over time series
      for (int e = 0; e < condensedBucketCount; e++) {
        valueOrNull(datapoints[e], cTimeSeries[i][e]);
      }
    }
  }
  int dpC = 0;
  for (const auto& dpLine : datapoints) {
    try {
      folly::toJson(dpLine);
    } catch (const std::exception& ex) {
      for (const auto& dp : dpLine) {
        LOG(INFO) << "\t\tDP: " << dp;
      }
      LOG(ERROR) << "toJson failed: " << ex.what();
    }
    
    dpC++;
  }
  delete[] timeSeries;
  folly::dynamic response = folly::dynamic::object;
  response["name"] = "id";
  response["columns"] = columns;
  response["points"] = datapoints;
  return response;
}

#define MIN_UPTIME_FOR_CALC 60 /* in seconds */
#define INVALID_VALUE 0xff
#define BUG_FOUND 0xfe
#define MIN_MDPUS_FOR_PER_PER_SEC 100
#define LINK_A 0
#define LINK_Z 1

double BeringeiData::calculateAverage(double *timeSeries, bool *valid,
                                      int timeSeriesStartIndex, int minIdx,
                                      int maxIdx, bool mcsflag) {
  double numValidSamples = 0;
  double accsum = 0;
  double avg = INVALID_VALUE;
  for (int timeindex = minIdx; timeindex <= maxIdx; timeindex++) {
    if (valid[timeSeriesStartIndex + timeindex]) {
      // don't count MCS = 0
      if (mcsflag && timeSeries[timeSeriesStartIndex + timeindex] == 0) {
        continue;
      }
      accsum += timeSeries[timeSeriesStartIndex + timeindex];
      numValidSamples = numValidSamples + 1;
    }
  }

  if (numValidSamples > 0) {
    avg = accsum / numValidSamples;
  }
  return avg;
}

/* analyzerTable takes the results of a Beringei time series query with multiple
 * keys and creates a json result:
 * {"end":time, "start":time,"metrics":{<linknameA>:{"avgper":value,
 * "avgsnr":value, ...}}}
 * function assumes it will be given ssnrEst, txOk, txFail, mcs for multiple
 * nodes
 * input: beringeiTimeWindowS, the minimum time spacing (30s default)
 */
folly::dynamic BeringeiData::analyzerTable(int beringeiTimeWindowS) {
  VLOG(3) << "AT: BeringeiData::analyzerTable";
  // endTime_ and startTime_ are from the request, not necessarily the
  // Beringei response
  int64_t timeBucketCount = (endTime_ - startTime_) / beringeiTimeWindowS;
  const int numKeysReturned = beringeiTimeSeries_.size();
  int keyIndex = 0;
  // map returned key -> index
  std::unordered_map<std::string, int64_t> keyMapIndex;
  // there is one keyIndex for every parameter and every link
  // for example, MCS from A->Z is one keyIndex
  for (const auto &keyId : query_.key_ids) {
    keyMapIndex[std::to_string(keyId)] = keyIndex;
    keyIndex++;
  }
  // it is possible that not all of the keys queried are returned by the query
  const int numKeysQueried = keyIndex;

  // find the number of unique links
  // linkindex is a doubly subscripted array
  // linkindex[displayName][a_or_z] = link number;
  // e.g. linkindex["link-15-30.s2-15-49.s1"]["(Z)"] = 4;
  folly::dynamic linkindex = folly::dynamic::object;
  // linkname[link number] = displayName (e.g. link-15-30.s2-15-49.s1)
  folly::dynamic linkname = folly::dynamic::object;
  // // linkdir[link number] = "(A)" or "(Z)"
  // folly::dynamic linkdir = folly::dynamic::object;
  // keylink maps the keyIndex to the link number
  int keylink[numKeysQueried];
  // numlinks is the total number of unique links (A->Z and Z->A are
  // the same link)
  int numlinks = 0;
  VLOG(3) << "AT: numKeysReturned:" << numKeysReturned
          << " numKeysQueried:" << numKeysQueried;
  for (int keyIndex = 0; keyIndex < numKeysQueried; keyIndex++) {
    auto &key = query_.data[keyIndex].key;
    auto &displayName = query_.data[keyIndex].displayName;
    auto &a_or_z = query_.data[keyIndex].linkTitleAppend;
    // if this link hasn't been seen yet
    if (linkindex.find(displayName) == linkindex.items().end()) {
      linkindex[displayName] = numlinks;
      linkname[numlinks] =
          displayName; // example:
                       // link-terra111.f5.tb.a404-if-terra212.f5.tb.a404-if
      numlinks++;
    }
    keylink[keyIndex] = folly::convertTo<int>(linkindex[displayName]);
  }

  // all values are initialized to zero
  double *timeSeries = new double[query_.key_ids.size() * timeBucketCount]();
  // valid is a boolean array of true/false to indicate if value is valid
  bool *valid = new bool[query_.key_ids.size() * timeBucketCount]();
  // minValidTimeBucketId and maxValidTimeBucketId are the smallest and largest
  // valid time bucket index
  int minValidTimeBucketId[numlinks][2] = {};
  int maxValidTimeBucketId[numlinks][2] = {};
  // numFlaps and upTime should be the same in both directions except
  // for missing data
  int numFlaps[numlinks][2] = {};
  int upTimeSec[numlinks][2] = {};
  bool found[numlinks][2] = {};

  // this loop fills timeSeries[] and valid[]
  // loop is over all keys (a key is, e.g.
  // tgf.38:3a:21:b0:11:e2.phystatus.ssnrEst)
  // the loop also finds the number of link flaps and the beginning and end
  // of the most recent time the link was up
  for (const auto &keyTimeSeries : beringeiTimeSeries_) {
    // the keyName is the unique number stored in mysql
    // e.g. 38910993
    const std::string &keyName = keyTimeSeries.first.key;
    keyIndex = keyMapIndex[keyName];
    auto &displayName = query_.data[keyIndex].displayName;
    int linknum = folly::convertTo<int>(linkindex[displayName]);
    auto &a_or_z = query_.data[keyIndex].linkTitleAppend;
    int link = (a_or_z.find("A") != std::string::npos) ? LINK_A : LINK_Z;
    auto &key = query_.data[keyIndex].key;
    // convert the key names to lower case
    // e.g. tgf.38:3a:21:b0:11:e2.phystatus.ssnrEst
    std::transform(key.begin(), key.end(), key.begin(), ::tolower);
    bool first = true;
    int firstValue = 0;
    unsigned int prevValue = 0;
    for (const auto &timePair : keyTimeSeries.second) {
      int timeBucketId = (timePair.unixTime - startTime_) / beringeiTimeWindowS;
      timeSeries[keyIndex * timeBucketCount + timeBucketId] = timePair.value;
      valid[keyIndex * timeBucketCount + timeBucketId] = true;
      // beringeiTimeSeries_ has "value" and "unixTime"
      // see .../beringei/lib/TimeSeriesStream-inl.h -- addValueToOutput()
      // and TimeSeriesStream::readValues
      // in readValues() it only adds a value to the output if it is valid

      // find the number of link transitions (flaps) and find the beginning of
      // the last time link was up
      if ((key.find("uplinkbwreq") != std::string::npos) ||
          (key.find("keepalive") != std::string::npos) ||
          (key.find("heartbeat") != std::string::npos)) {
        if (first) {
          minValidTimeBucketId[linknum][link] = timeBucketId;
          firstValue = timePair.value;
          first = false;
        }
        int expectedHBcount =
            firstValue + (timeBucketId - minValidTimeBucketId[linknum][link]) *
                             NUM_HBS_PER_SEC * beringeiTimeWindowS;
        found[linknum][link] = true;
        // if the current HB counter is less than the last one or if it is
        // much less than the expectedHBcount, assume the link went down and
        // start over
        if (timePair.value < prevValue ||
            (timePair.value < expectedHBcount * 0.9)) {
          firstValue = timePair.value;
          numFlaps[linknum][link]++;
          minValidTimeBucketId[linknum][link] = timeBucketId;
        }
        upTimeSec[linknum][link] =
            (timeBucketId - minValidTimeBucketId[linknum][link]) *
            beringeiTimeWindowS;
        maxValidTimeBucketId[linknum][link] = timeBucketId;
      }
      prevValue = timePair.value;
    }
  }

  // calculate statistics for each link, we need a separate loop because
  // for example, PER = txFail/(txOk+txFail) - we need to calculate txOK and
  // txFail before we can calculate PER -logic is simpler
  double diffTxOk[numlinks][2];
  double diffTxFail[numlinks][2];
  double avgSnr[numlinks][2];
  double avgMcs[numlinks][2];
  double avgTxPower[numlinks][2];

  // initialize the variables
  for (int keyIndex = 0; keyIndex < numKeysQueried; keyIndex++) {
    auto &displayName = query_.data[keyIndex].displayName;
    int linknum = folly::convertTo<int>(linkindex[displayName]);
    auto &a_or_z = query_.data[keyIndex].linkTitleAppend;
    int link = (a_or_z.find("A") != std::string::npos) ? LINK_A : LINK_Z;

    diffTxOk[linknum][link] = INVALID_VALUE;
    diffTxFail[linknum][link] = INVALID_VALUE;
    avgSnr[linknum][link] = INVALID_VALUE;
    avgMcs[linknum][link] = INVALID_VALUE;
    avgTxPower[linknum][link] = INVALID_VALUE;
  }

  for (const auto &keyTimeSeries : beringeiTimeSeries_) {
    const std::string &keyName = keyTimeSeries.first.key;
    keyIndex = keyMapIndex[keyName];
    auto &key = query_.data[keyIndex].key;
    // convert the key names to lower case
    // e.g. tgf.38:3a:21:b0:11:e2.phystatus.ssnrEst
    std::transform(key.begin(), key.end(), key.begin(), ::tolower);
    auto &displayName = query_.data[keyIndex].displayName;
    auto &a_or_z = query_.data[keyIndex].linkTitleAppend;
    int link = (a_or_z.find("A") != std::string::npos) ? LINK_A : LINK_Z;
    int linknum = folly::convertTo<int>(linkindex[displayName]);
    if (!found[linknum][link]) {
      VLOG(1) << "AT: ERROR: link has no HB:" << displayName << a_or_z;
      avgMcs[linknum][link] = BUG_FOUND;
      continue;
    }

    if (upTimeSec[linknum][link] > MIN_UPTIME_FOR_CALC) {
      bool minMaxTimesValid = valid[keyIndex * timeBucketCount +
                                    minValidTimeBucketId[linknum][link]] &&
                              valid[keyIndex * timeBucketCount +
                                    maxValidTimeBucketId[linknum][link]];

      if (key.find("txok") != std::string::npos) {
        if (minMaxTimesValid) {
          diffTxOk[linknum][link] =
              timeSeries[keyIndex * timeBucketCount +
                         maxValidTimeBucketId[linknum][link]] -
              timeSeries[keyIndex * timeBucketCount +
                         minValidTimeBucketId[linknum][link]];
        } else {
          VLOG(1) << "AT ERROR: min/max times not valid for diffTxOk";
          diffTxOk[linknum][link] = BUG_FOUND;
        }
        if (diffTxOk[linknum][link] < 0) {
          VLOG(1) << "AT ERROR: diffTxOk < 0";
          diffTxOk[linknum][link] = BUG_FOUND;
        }
      }
      if (key.find("txfail") != std::string::npos) {
        if (minMaxTimesValid) {
          diffTxFail[linknum][link] =
              timeSeries[keyIndex * timeBucketCount +
                         maxValidTimeBucketId[linknum][link]] -
              timeSeries[keyIndex * timeBucketCount +
                         minValidTimeBucketId[linknum][link]];
        } else {
          VLOG(1) << "AT ERROR: min/max times not valid for diffTxFail";
          diffTxFail[linknum][link] = BUG_FOUND;
        }
        if (diffTxFail[linknum][link] < 0) {
          VLOG(1) << "AT ERROR: diffTxFail < 0";
          diffTxFail[linknum][link] = BUG_FOUND;
        }
      }
      if (key.find("ssnrest") != std::string::npos) {
        avgSnr[linknum][link] =
            calculateAverage(timeSeries, valid, keyIndex * timeBucketCount,
                             minValidTimeBucketId[linknum][link],
                             maxValidTimeBucketId[linknum][link], false);
      }
      if (key.find("mcs") != std::string::npos) {
        avgMcs[linknum][link] =
            calculateAverage(timeSeries, valid, keyIndex * timeBucketCount,
                             minValidTimeBucketId[linknum][link],
                             maxValidTimeBucketId[linknum][link], true);
      }
      if (key.find("txpowerindex") != std::string::npos) {
        avgTxPower[linknum][link] =
            calculateAverage(timeSeries, valid, keyIndex * timeBucketCount,
                             minValidTimeBucketId[linknum][link],
                             maxValidTimeBucketId[linknum][link], false);
      }
    }
  }

  folly::dynamic linkparams = folly::dynamic::object;
  folly::dynamic metrics = folly::dynamic::object;
  std::vector<std::string> linkdir = { "A", "Z" };
  // return processed statistics
  for (int linknum = 0; linknum < numlinks; linknum++) {
    for (int link = LINK_A; link <= LINK_Z; link++) {
      double avgPer = INVALID_VALUE;
      double dok = diffTxOk[linknum][link];
      double dfail = diffTxFail[linknum][link];
      if (dfail == BUG_FOUND || dok == BUG_FOUND) {
        avgPer = BUG_FOUND;
      } else if (dfail != INVALID_VALUE && dok != INVALID_VALUE &&
                 (dfail + dok > 0)) {
        avgPer = dfail / (dfail + dok);
      }

      // calculate average throughput in packets per second
      double tputPPS = 0.0;
      if (upTimeSec[linknum][link] > 0) {
        tputPPS = (dfail + dok) / (double)(upTimeSec[linknum][link]);
      }

      linkparams["avgper"] = avgPer;
      linkparams["avgsnr"] = avgSnr[linknum][link];
      linkparams["avgtxpower"] = avgTxPower[linknum][link];
      linkparams["avgmcs"] = avgMcs[linknum][link];
      linkparams["tput"] = tputPPS;
      linkparams["flaps"] = numFlaps[linknum][link];
      linkparams["uptime"] = upTimeSec[linknum][link];
      if (!metrics[linkname[linknum]].isObject()) {
        metrics[linkname[linknum]] = folly::dynamic::object;
      }
      metrics[linkname[linknum]][linkdir[link]] = linkparams;
    }
  }
  delete[] timeSeries;
  delete[] valid;
  folly::dynamic response = folly::dynamic::object;
  response["name"] = "analyzerTable";
  response["start"] = startTime_ * 1000;
  response["end"] = endTime_ * 1000;
  response["metrics"] = metrics;
  return response;
}

folly::dynamic BeringeiData::handleQuery() {
  auto startTime = (int64_t)duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()).count();
  // validate first, prefer to throw here (no futures)
  validateQuery(query_);
  // fetch async data
  folly::EventBase eb;
  eb.runInLoop([this]() mutable {
    BeringeiClient client(configurationAdapter_, 1,
                          BeringeiClient::kNoWriterThreads);
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
    // uplink bw request
    results = eventHandler(25 /* ms for heartbeats */, "alive");
  } else if (query_.type == "uptime_sec") {
    results = eventHandler(1000, "minion_uptime");
  } else if (query_.type == "analyzer_table") {
    results = analyzerTable(30);
  } else if (query_.type == "latest") {
    results = latest();
  } else {
    results = transform();
  }
  auto endTime = (int64_t)duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()).count();
  LOG(INFO) << "Query completed. "
            << "Query type \"" << query_.type
            << "\" Fetch: " << (fetchTime - startTime) << "ms, "
            << "Column names: " << (columnNamesTime - fetchTime) << "ms, "
            << "Event/Transform: " << (endTime - columnNamesTime) << "ms, "
            << "Total: " << (endTime - startTime) << "ms.";
  return results;
}

int BeringeiData::getShardId(const std::string &key, const int numShards) {
  std::hash<std::string> hash;
  size_t hashValue = hash(key);

  if (numShards != 0) {
    return hashValue % numShards;
  } else {
    return hashValue;
  }
}

void BeringeiData::validateQuery(const query::Query &request) {
  if (request.__isset.min_ago) {
    startTime_ = std::time(nullptr) - (60 * request.min_ago);
    endTime_ = std::time(nullptr);
  } else if (request.start_ts != 0 && request.end_ts != 0) {
    // TODO - sanity check time
    startTime_ = std::ceil(request.start_ts / 30.0) * 30;
    endTime_ = std::ceil(request.end_ts / 30.0) * 30;
    if (endTime_ <= startTime_) {
      LOG(ERROR) << "Request for invalid time window: " << startTime_ << " <-> "
                 << endTime_;
      throw std::runtime_error("Request for invalid time window");
    }
  } else {
    // default to 1 day here
    startTime_ = std::time(nullptr) - (24 * 60 * 60);
    endTime_ = std::time(nullptr);
  }
  LOG(INFO) << "Request for start: " << startTime_ << " <-> " << endTime_;
}

GetDataRequest BeringeiData::createBeringeiRequest(const query::Query &request,
                                                   const int numShards) {
  GetDataRequest beringeiRequest;

  beringeiRequest.begin = startTime_;
  beringeiRequest.end = endTime_;

  for (const auto &keyId : request.key_ids) {
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
