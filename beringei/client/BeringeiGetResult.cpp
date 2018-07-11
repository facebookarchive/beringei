/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "beringei/client/BeringeiGetResult.h"

#include <algorithm>
#include <vector>

#include <folly/container/Enumerate.h>

#include "beringei/lib/GorillaStatsManager.h"
#include "beringei/lib/TimeSeries.h"

DECLARE_int32(mintimestampdelta);
DECLARE_bool(gorilla_compare_reads);
DECLARE_double(gorilla_compare_epsilon);

namespace {
template <typename T>
static int64_t vectorMemory(const std::vector<T>& ts) {
  return ts.capacity() * sizeof(T);
}

template <typename T>
static int64_t vectorMemory(const std::vector<std::vector<T>>& ts) {
  int64_t size = ts.capacity() * sizeof(std::vector<T>);
  for (const auto& t : ts) {
    size += vectorMemory(t);
  }
  return size;
}
} // namespace

namespace facebook {
namespace gorilla {

static const std::string kMismatchesKey = "gorilla_client.mismatches";

BeringeiGetResultCollector::BeringeiGetResultCollector(
    size_t size,
    size_t services,
    int64_t begin,
    int64_t end)
    : beginTime_(begin),
      endTime_(end),
      numServices_(services),
      complete_(size),
      drops_(1ull << services),
      mismatches_(1ull << services),
      done_(false),
      result_(size),
      queue_(size * services) {
  // This would require an insane amount of memory anyway, but at least make
  // sure we don't overrun the KeyStats structure.
  CHECK_LT(numServices_, 32);
  SYNCHRONIZED(addStats_) {
    addStats_.remainingKeys = size;
    addStats_.count.resize(size);
  }
}

bool BeringeiGetResultCollector::addResults(
    GetDataResult&& results,
    const std::vector<size_t>& indices,
    size_t serviceId) {
  bool ret = false;
  if (done_.load()) {
    return 0;
  }

  SYNCHRONIZED(addStats_) {
    for (auto result : folly::enumerate(results.get_results())) {
      size_t i = indices[result.index];
      switch (result->status) {
        case StatusCode::OK:
          FOLLY_FALLTHROUGH;
        case StatusCode::KEY_MISSING:
          FOLLY_FALLTHROUGH;
        case StatusCode::MISSING_TOO_MUCH_DATA:
          ret |= ++addStats_.count[i] == 1 && --addStats_.remainingKeys == 0;
          break;
        case StatusCode::DONT_OWN_SHARD:
          break;
        case StatusCode::SHARD_IN_PROGRESS:
          break;
        case StatusCode::RPC_FAIL:
        case StatusCode::BUCKET_NOT_FINALIZED:
        case StatusCode::ZIPPY_STORAGE_FAIL:
          // Beringei should not return these result codes.
          LOG(FATAL) << "Invalid error code from Beringei: "
                     << apache::thrift::TEnumTraits<StatusCode>::findName(
                            result->status);
          break;
      }
    }
  }

  Item item;
  item.indices = indices;
  item.serviceId = serviceId;
  item.results = std::move(results);
  bool success = queue_.write(std::move(item));

  // If we failed to write to queue, let all 3 regions come back.
  return ret && success;
}

BeringeiGetResult BeringeiGetResultCollector::finalize(
    bool validate,
    const std::vector<std::string>& serviceNames) {
  done_.store(true);
  Item item;
  while (queue_.read(item)) {
    auto& indices = item.indices;
    auto& results = item.results;
    auto serviceId = item.serviceId;
    for (auto result : folly::enumerate(results.results)) {
      size_t i = indices[result.index];
      switch (result->status) {
        case StatusCode::SHARD_IN_PROGRESS:
          // Include the data in the results, but don't count it as a complete
          // copy.
          merge(i, serviceId, *result);
          complete_[i].received.set(serviceId);
          break;
        case StatusCode::OK:
          FOLLY_FALLTHROUGH;
        case StatusCode::MISSING_TOO_MUCH_DATA:
          // A complete result.
          // We can't count MISSING_TOO_MUCH_DATA as an incomplete result
          // because it's not a transient failure. We don't want to fail the
          // query completely if all replicas are missing data at different time
          // points.
          merge(i, serviceId, *result);
          ++complete_[i].finalizedCount;
          complete_[i].received.set(serviceId);
          break;
        case StatusCode::DONT_OWN_SHARD:
          // While in theory we would want to invalidate the shard map cache and
          // issue a new query to whoever *does* own the shard, in practice the
          // TTL on the cache is much, much shorter than the time it takes to
          // load a shard. If we did successfully issue a new query, we wouldn't
          // get any meaningful data from the new owner.
          break;
        case StatusCode::KEY_MISSING:
          // We successfully found that there was no data.
          complete_[i].received.set(serviceId);
          ++complete_[i].finalizedCount;
          break;
        case StatusCode::RPC_FAIL:
        case StatusCode::BUCKET_NOT_FINALIZED:
        case StatusCode::ZIPPY_STORAGE_FAIL:
          // Beringei should not return these result codes.
          LOG(FATAL) << "Invalid error code from Beringei: "
                     << apache::thrift::TEnumTraits<StatusCode>::findName(
                            result->status);
          break;
      }
    }
  }

  CHECK_EQ(serviceNames.size(), numServices_);
  uint32_t min = numServices_;
  std::vector<int> resultSets(1ull << numServices_);

  for (auto c : complete_) {
    min = std::min(min, c.finalizedCount);
    resultSets[c.received.to_ulong()]++;
  }

  std::vector<int> drops(numServices_);
  std::vector<int> missings(numServices_);

  CHECK_EQ(drops_.size(), resultSets.size());
  for (int bitmask = 0; bitmask < drops_.size(); bitmask++) {
    for (int i = 0; i < numServices_; i++) {
      if (bitmask & (1ull << i)) {
        drops[i] += drops_[bitmask];
      } else {
        missings[i] += resultSets[bitmask];
      }
    }
  }

  BeringeiGetStats& stats = result_.stats;
  for (size_t i = 0; i < numServices_; i++) {
    if (drops[i] > 0) {
      GorillaStatsManager::addStatValue(
          "gorilla_client.missing_points." + serviceNames[i], drops[i], SUM);
      GorillaStatsManager::addStatValue(
          "gorilla_client.missing_points." + serviceNames[i], 1, COUNT);
    }

    if (missings[i] > 0) {
      GorillaStatsManager::addStatValue(
          "gorilla_client.failed_keys." + serviceNames[i], missings[i], SUM);
      GorillaStatsManager::addStatValue(
          "gorilla_client.failed_keys." + serviceNames[i], 1, COUNT);
    }

    stats.mismatches = std::max(stats.mismatches, mismatches_[1ull << i]);
    stats.missingPoints = std::max<int64_t>(stats.missingPoints, drops[i]);
    stats.failedKeys = std::max<int64_t>(stats.failedKeys, missings[i]);
  }

  if (FLAGS_gorilla_compare_reads) {
    if (stats.mismatches > 0) {
      GorillaStatsManager::addStatValue(kMismatchesKey, stats.mismatches, SUM);
      GorillaStatsManager::addStatValue(kMismatchesKey, 1, COUNT);
    }
  }

  int64_t numDatapoints = 0;
  for (auto& ts : result_.results) {
    numDatapoints += ts.size();
  }
  GorillaStatsManager::addStatValue(
      "gorilla_client.num_datapoints", numDatapoints, SUM);
  GorillaStatsManager::addStatValue("gorilla_client.num_queries", 1, SUM);

  result_.stats.memoryEstimate = sizeof(this) + vectorMemory(result_.results) +
      vectorMemory(complete_) + vectorMemory(drops_);

  std::vector<KeyStats>().swap(complete_);
  std::vector<int>().swap(drops_);

  // Call the query successful if we got results from half the services.
  result_.allSuccess = min > 0;
  if (validate && !result_.allSuccess) {
    throw std::runtime_error("Incomplete results from Beringei");
  }

  return std::move(result_);
}

void BeringeiGetResultCollector::merge(
    size_t i,
    size_t service,
    const TimeSeriesData& result) {
  int64_t inSize = 0;
  int64_t mismatches = 0;
  const size_t oldSize = result_.results[i].size();

  TimeSeries::mergeValues(
      result.data,
      result_.results[i],
      beginTime_,
      endTime_,
      FLAGS_mintimestampdelta,
      FLAGS_gorilla_compare_reads,
      FLAGS_gorilla_compare_epsilon,
      &inSize,
      &mismatches);

  // Count the un-matched data points.
  // Missing in existing
  drops_[complete_[i].received.to_ulong()] +=
      (result_.results[i].size() - oldSize);
  // Missing in current result
  drops_[1ull << service] += (result_.results[i].size() - inSize);

  if (complete_[i].finalizedCount == 1) {
    mismatches_[complete_[i].received.to_ulong()] += mismatches;
  }
  mismatches_[1ull << service] += mismatches;
}

} // namespace gorilla
} // namespace facebook
