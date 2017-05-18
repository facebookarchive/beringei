/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "BeringeiGetResult.h"

#include <algorithm>
#include <vector>

#include <folly/Enumerate.h>

#include "beringei/lib/GorillaStatsManager.h"
#include "beringei/lib/TimeSeries.h"

DECLARE_int32(mintimestampdelta);
DEFINE_bool(
    gorilla_compare_reads,
    false,
    "whether to compare the data read from different gorilla services");
DEFINE_double(
    gorilla_compare_epsilon,
    0.1,
    "the allowed error between data for comparison");

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

struct DeltaInserter
    : public std::iterator<std::output_iterator_tag, void, void, void, void> {
  using container_type = std::vector<facebook::gorilla::TimeValuePair>;

  DeltaInserter(
      std::vector<facebook::gorilla::TimeValuePair>& data,
      int64_t delta)
      : data_(&data), delta_(delta) {}

  DeltaInserter& operator=(const facebook::gorilla::TimeValuePair& tv) {
    if (data_->empty() || (tv.unixTime - data_->back().unixTime) >= delta_) {
      data_->push_back(tv);
    }
    return *this;
  }

  DeltaInserter& operator=(facebook::gorilla::TimeValuePair&& tv) {
    if (data_->empty() || (tv.unixTime - data_->back().unixTime) >= delta_) {
      data_->push_back(std::move(tv));
    }
    return *this;
  }

  DeltaInserter& operator*() {
    return *this;
  }
  DeltaInserter& operator++() {
    return *this;
  }
  DeltaInserter& operator++(int) {
    return *this;
  }

  std::vector<facebook::gorilla::TimeValuePair>* data_;
  int64_t delta_;
};

// May be slightly more expensive.
struct DeltaCompareInserter
    : public std::iterator<std::output_iterator_tag, void, void, void, void> {
  using container_type = std::vector<facebook::gorilla::TimeValuePair>;

  DeltaCompareInserter(
      std::vector<facebook::gorilla::TimeValuePair>& data,
      int64_t delta,
      double epsilon,
      int64_t& mismatches)
      : data_(&data),
        delta_(delta),
        epsilon_(epsilon),
        mismatches_(mismatches) {}

  DeltaCompareInserter& operator=(const facebook::gorilla::TimeValuePair& tv) {
    if (data_->empty() || (tv.unixTime - data_->back().unixTime) >= delta_) {
      data_->push_back(tv);
    } else if (
        std::abs(tv.value - data_->back().value) >
        std::max(epsilon_ * std::abs(data_->back().value), epsilon_)) {
      mismatches_++;
    }
    return *this;
  }

  DeltaCompareInserter& operator=(facebook::gorilla::TimeValuePair&& tv) {
    if (data_->empty() || (tv.unixTime - data_->back().unixTime) >= delta_) {
      data_->push_back(std::move(tv));
    } else if (
        std::abs(tv.value - data_->back().value) >
        std::max(epsilon_ * std::abs(data_->back().value), epsilon_)) {
      mismatches_++;
    }
    return *this;
  }

  DeltaCompareInserter& operator*() {
    return *this;
  }
  DeltaCompareInserter& operator++() {
    return *this;
  }
  DeltaCompareInserter& operator++(int) {
    return *this;
  }

  std::vector<facebook::gorilla::TimeValuePair>* data_;
  int64_t delta_;
  double epsilon_;
  int64_t& mismatches_;
};
}

namespace facebook {
namespace gorilla {

static const std::string kMismatchesKey = ".gorilla_client.mismatches";

BeringeiGetResultCollector::BeringeiGetResultCollector(
    size_t size,
    size_t services,
    int64_t begin,
    int64_t end)
    : beginTime_(begin),
      endTime_(end),
      numServices_(services),
      remainingKeys_(size),
      complete_(size),
      drops_(1ull << services),
      mismatches_(1ull << services),
      done_(false),
      result_(size) {
  // This would require an insane amount of memory anyway, but at least make
  // sure we don't overrun the KeyStats structure.
  CHECK_LT(numServices_, 32);
}

bool BeringeiGetResultCollector::addResults(
    const GetDataResult& results,
    const std::vector<size_t>& indices,
    size_t serviceId) {
  bool ret = false;

  lock_.lock();
  if (done_) {
    lock_.unlock();
    return false;
  }

  for (auto result : folly::enumerate(results.results)) {
    size_t i = indices[result.index];
    switch (result->status) {
      case StatusCode::OK:
      case StatusCode::MISSING_TOO_MUCH_DATA:
        // A complete result.
        // We can't count MISSING_TOO_MUCH_DATA as an incomplete result
        // because it's not a transient failure. We don't want to fail the query
        // completely if all replicas are missing data at different time points.
        merge(i, serviceId, *result);
        complete_[i].received.set(serviceId);
        ret |= ++complete_[i].count == 1 && --remainingKeys_ == 0;
        break;
      case StatusCode::DONT_OWN_SHARD:
        // While in theory we would want to invalidate the shard map cache and
        // issue a new query to whoever *does* own the shard, in practice the
        // TTL on the cache is much, much shorter than the time it takes to load
        // a shard. If we did successfully issue a new query, we wouldn't get
        // any meaningful data from the new owner.
        break;
      case StatusCode::KEY_MISSING:
        // We successfully found that there was no data.
        complete_[i].received.set(serviceId);
        ret |= ++complete_[i].count == 1 && --remainingKeys_ == 0;
        break;
      case StatusCode::SHARD_IN_PROGRESS:
        // Include the data in the results, but don't count it as a complete
        // copy.
        merge(i, serviceId, *result);
        complete_[i].received.set(serviceId);
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
  lock_.unlock();
  return ret;
}

BeringeiGetResult BeringeiGetResultCollector::finalize(
    bool validate,
    const std::vector<std::string>& serviceNames) {
  // From here on out, future calls to addResults() will do nothing.
  lock_.lock();
  done_ = true;
  lock_.unlock();

  CHECK_EQ(serviceNames.size(), numServices_);

  uint32_t min = numServices_;
  std::vector<int> resultSets(1ull << numServices_);

  for (auto c : complete_) {
    min = std::min(min, c.count);
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

  int64_t maxMismatches = 0;
  for (size_t i = 0; i < numServices_; i++) {
    GorillaStatsManager::addStatValue(
        ".gorilla_client.missing_points." + serviceNames[i], drops[i], SUM);
    GorillaStatsManager::addStatValue(
        ".gorilla_client.failed_keys." + serviceNames[i], missings[i], SUM);

    if (FLAGS_gorilla_compare_reads) {
      if (maxMismatches < mismatches_[1ull << i]) {
        maxMismatches = mismatches_[1ull << i];
      }
    }
  }

  if (FLAGS_gorilla_compare_reads) {
    if (maxMismatches > 0) {
      GorillaStatsManager::addStatValue(kMismatchesKey, maxMismatches, SUM);
      GorillaStatsManager::addStatValue(kMismatchesKey, 1, COUNT);
    }
  }

  int64_t numDatapoints = 0;
  for (auto& ts : result_.results) {
    numDatapoints += ts.size();
  }
  GorillaStatsManager::addStatValue(
      ".gorilla_client.num_datapoints", numDatapoints, SUM);

  result_.memoryEstimate = sizeof(this) + vectorMemory(result_.results) +
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
  // Decompress the incoming data.
  std::vector<TimeValuePair> resultData;
  for (const auto& block : result.data) {
    TimeSeries::getValues(block, resultData, beginTime_, endTime_);
  }

  auto& haveData = result_.results[i];

  // This is the first copy.
  if (complete_[i].count == 0) {
    std::swap(haveData, resultData);
    return;
  }

  // Merge the results.
  std::vector<TimeValuePair> newData;
  newData.reserve(std::max(haveData.size(), resultData.size()));

  if (FLAGS_gorilla_compare_reads) {
    int64_t mismatches = 0;
    std::merge(
        haveData.begin(),
        haveData.end(),
        resultData.begin(),
        resultData.end(),
        DeltaCompareInserter(
            newData,
            FLAGS_mintimestampdelta,
            FLAGS_gorilla_compare_epsilon,
            mismatches));
    if (complete_[i].count == 1) {
      mismatches_[complete_[i].received.to_ulong()] += mismatches;
    }
    mismatches_[1ull << service] += mismatches;
  } else {
    std::merge(
        haveData.begin(),
        haveData.end(),
        resultData.begin(),
        resultData.end(),
        DeltaInserter(newData, FLAGS_mintimestampdelta));
  }

  // Count the un-matched data points.
  drops_[complete_[i].received.to_ulong()] +=
      (newData.size() - haveData.size());
  drops_[1ull << service] += (newData.size() - resultData.size());
  std::swap(haveData, newData);
}
}
}
