/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <algorithm>
#include <bitset>
#include <mutex>

#include <folly/container/Enumerate.h>
#include <glog/logging.h>

#include "beringei/lib/TimeSeries.h"

#include "BeringeiScanShardResult.h"

DECLARE_int32(mintimestampdelta);
DECLARE_bool(gorilla_compare_reads);
DECLARE_double(gorilla_compare_epsilon);

namespace {
template <class S>
auto& operator<<(S& lhs, const facebook::gorilla::ScanShardRequest& rhs) {
  lhs << "{ shard=" << rhs.shardId << ", begin=" << rhs.begin
      << ", end=" << rhs.end << ", subshard=" << rhs.subshard
      << ", numSubshards=" << rhs.numSubshards << " }";
  return lhs;
}

struct KeyData {
  explicit KeyData(int services) : data_(services), queriedRecently_(false) {}

  void set(
      int service,
      std::vector<facebook::gorilla::TimeSeriesBlock>&& rhs,
      bool queriedRecently) {
    CHECK_LT(service, data_.size());
    data_[service] = std::move(rhs);
    queriedRecently_ |= queriedRecently;
  }

  std::vector<std::vector<facebook::gorilla::TimeSeriesBlock>> data_;
  bool queriedRecently_;
};
} // namespace

namespace facebook {
namespace gorilla {
bool BeringeiScanShardResult::operator==(
    const facebook::gorilla::BeringeiScanShardResult& rhs) const {
  // Fail predictably on *this and rhs class invariant violations
  CHECK_EQ(data.size(), keys.size());
  CHECK_EQ(queriedRecently.size(), keys.size());
  CHECK_EQ(rhs.data.size(), rhs.keys.size());
  CHECK_EQ(rhs.queriedRecently.size(), rhs.keys.size());

  bool ret = status == rhs.status && keys.size() == rhs.keys.size();
  ret = ret && beginTime == rhs.beginTime;
  ret = ret && endTime == rhs.endTime;

  if (ret) {
    std::map<std::string, size_t> lhsToIndex;
    for (const auto& i : folly::enumerate(keys)) {
      const auto insertResult = lhsToIndex.insert(std::make_pair(*i, i.index));
      CHECK(insertResult.second);
    }

    std::map<unsigned, unsigned> lhsToRhs;
    for (size_t i = 0; ret && i < rhs.keys.size(); ++i) {
      const auto lhsIndex = lhsToIndex.find(rhs.keys[i]);
      ret = ret && lhsIndex != lhsToIndex.end();
      if (ret) {
        const auto insertResult =
            lhsToRhs.insert(std::make_pair(lhsIndex->second, i));
        CHECK(insertResult.second);
      }
    }

    for (size_t lhsIndex = 0; ret && lhsIndex < keys.size(); ++lhsIndex) {
      const unsigned rhsIndex = lhsToRhs[lhsIndex];
      const auto lhsData = getUncompressedData(lhsIndex);
      const auto rhsData = rhs.getUncompressedData(rhsIndex);
      ret = ret && lhsData == rhsData;
      ret = ret && queriedRecently[lhsIndex] == rhs.queriedRecently[rhsIndex];
    }
  }

  return ret;
}

std::vector<TimeValuePair> BeringeiScanShardResult::getUncompressedData(
    size_t key) const {
  std::vector<TimeValuePair> ret;
  for (unsigned service = 0; service < data[key].size(); ++service) {
    int64_t inSize = 0;
    int64_t mismatches = 0;

    TimeSeries::mergeValues(
        data[key][service],
        ret,
        beginTime,
        endTime,
        FLAGS_mintimestampdelta,
        FLAGS_gorilla_compare_reads,
        FLAGS_gorilla_compare_epsilon,
        &inSize,
        &mismatches);
  }
  return ret;
}

std::vector<TimeValuePair> BeringeiScanShardResult::takeUncompressedData(
    size_t key) {
  std::vector<TimeValuePair> ret = getUncompressedData(key);
  std::vector<std::vector<TimeSeriesBlock>>().swap(data[key]);
  return ret;
}

BeringeiScanShardResultCollector::BeringeiScanShardResultCollector(
    size_t services,
    int64_t begin,
    int64_t end)
    : beginTime_(begin),
      endTime_(end),
      numServices_(services),
      remainingServices_(services),
      results_(services),
      status_(StatusCode::RPC_FAIL),
      allSuccess_(false),
      done_(false) {}

bool BeringeiScanShardResultCollector::addResult(
    ScanShardResult&& result,
    const ScanShardRequest& request,
    size_t service) {
  std::lock_guard<decltype(lock_)> lock(lock_);

  if (done_) {
    return false;
  }

  StatusCode resultStatus = result.status;

  std::string mismatchName;
  size_t mismatchValue;
  if ((mismatchValue = result.data.size()) != result.keys.size()) {
    mismatchName = "data";
  } else if (
      (mismatchValue = result.queriedRecently.size()) != result.keys.size()) {
    mismatchName = "queriedRecently";
  } else {
    mismatchValue = 0;
  }

  if (!mismatchName.empty()) {
    LOG(ERROR) << "scanShard request to service " << service << " " << request
               << " result " << mismatchName << " size " << mismatchValue
               << " != keys size " << result.keys.size();
    if (resultStatus == StatusCode::OK) {
      resultStatus = StatusCode::RPC_FAIL;
    }
  } else {
    results_[service] = std::make_unique<ScanShardResult>(std::move(result));
  }

  // Favor success or more specific error message
  if (status_ == StatusCode::RPC_FAIL) {
    status_ = resultStatus;
  }

  if (result.status != StatusCode::OK) {
    allSuccess_ = false;
  }

  CHECK_GE(remainingServices_, 1);
  --remainingServices_;
  return remainingServices_ == 0;
}

BeringeiScanShardResult BeringeiScanShardResultCollector::finalize(
    bool validate,
    const std::vector<std::string>& serviceNames) {
  CHECK_EQ(serviceNames.size(), numServices_);

  {
    std::lock_guard<decltype(lock_)> lock(lock_);
    CHECK(!done_);
    done_ = true;
  }

  std::map<std::string, KeyData> keyData;
  for (const auto& result : folly::enumerate(results_)) {
    if (*result) {
      CHECK_EQ((*result)->data.size(), (*result)->keys.size());
      CHECK_EQ((*result)->queriedRecently.size(), (*result)->keys.size());

      for (const auto& key : folly::enumerate((*result)->keys)) {
        auto i = keyData.find(*key);
        if (i == keyData.end()) {
          auto inserted =
              keyData.insert(std::make_pair(*key, KeyData(numServices_)));
          i = inserted.first;
        }
        i->second.set(
            result.index /* service */,
            std::move((*result)->data[key.index]),
            (*result)->queriedRecently[key.index]);
      }
    }
  }

  BeringeiScanShardResult ret(
      beginTime_, endTime_, keyData.size(), serviceNames.size());

  for (size_t i = 0; !keyData.empty(); ++i) {
    auto key = keyData.begin();

    ret.keys[i] = key->first;
    ret.data[i] = std::move(key->second.data_);
    ret.queriedRecently[i] = key->second.queriedRecently_;

    keyData.erase(key);
  }

  ret.status = status_;
  ret.allSuccess = allSuccess_;

  if (validate && !ret.allSuccess) {
    throw std::runtime_error("Incomplete results from Beringei ScanShard");
  }

  return ret;
}
} // namespace gorilla
} // namespace facebook
