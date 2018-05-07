/**
 * Copyright (c) 2018-present, Facebook, Inc.
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
struct KeyData {
  explicit KeyData(int services) : data_(services), queriedRecently_(false) {}

  void set(
      int service,
      std::vector<facebook::gorilla::TimeSeriesBlock>&& rhs,
      bool queriedRecently) {
    CHECK_LT(service, data_.size());
    CHECK(!have_[service]);
    data_[service] = std::move(rhs);
    have_[service] = true;
    queriedRecently_ |= queriedRecently;
  }

  std::vector<std::vector<facebook::gorilla::TimeSeriesBlock>> data_;
  std::bitset<32> have_;
  bool queriedRecently_;
};

template <class S>
auto& operator<<(S& lhs, const facebook::gorilla::ScanShardRequest& rhs) {
  lhs << "{ shard=" << rhs.shardId << ", begin=" << rhs.begin
      << ", end=" << rhs.end << ", subshard=" << rhs.subshard
      << ", numSubshards=" << rhs.numSubshards << " }";
  return lhs;
}

template <typename T>
void add(std::vector<T>& lhs, const std::vector<T>& rhs) {
  CHECK_EQ(lhs.size(), rhs.size());
  for (size_t i = 0; i < lhs.size(); ++i) {
    lhs[i] += rhs[i];
  }
}
} // namespace

namespace facebook {
namespace gorilla {
void BeringeiScanStats::updateByEntry(std::bitset<32> services) {
  ++total_;

  unsigned count = 0;
  for (unsigned i = 0; i < numServices_; ++i) {
    if (enabled_[i] && !services[i]) {
      ++missingByService_[i];
      ++count;
    }
  }

  ++missingByNumServices_[count];
}

void BeringeiScanStats::updateByService(
    size_t service,
    int64_t newTotal,
    int64_t added,
    int64_t missing) {
  CHECK_GT(numServices_, 0);
  CHECK_GE(added, 0);
  CHECK_GE(missing, 0);

  total_ += added;

  for (unsigned i = 0; i < service; ++i) {
    if (enabled_[i]) {
      missingByService_[i] += added;
    }
  }
  // Don't count missing data in partial results with error returns
  if (enabled_[service]) {
    missingByService_[service] += missing;
  }

  if (missingByNumServicesValid_) {
    if (numServices_ == 1) {
      missingByNumServices_[0] = newTotal;
    } else if (numServices_ == 2) {
      // updateByService must be called sequentially for all services
      // 0 <= service < numServices_
      if (service == 1) {
        missingByNumServices_[0] = newTotal - added - missing;
        missingByNumServices_[1] = added + missing;
      }
    } else {
      missingByNumServicesValid_ = false;
    }
  }
}

void BeringeiScanStats::merge(const BeringeiScanStats& rhs) {
  CHECK_EQ(numServices_, rhs.numServices_);
  CHECK_EQ(enabled_, rhs.enabled_);

  total_ += rhs.total_;

  missingByServiceValid_ &= rhs.missingByServiceValid_;
  if (missingByServiceValid_) {
    add(missingByService_, rhs.missingByService_);
  }

  missingByNumServicesValid_ &= rhs.missingByNumServicesValid_;
  if (missingByNumServicesValid_) {
    add(missingByNumServices_, rhs.missingByNumServices_);
  }
}

bool BeringeiScanStats::getMissingByService(size_t service, int64_t* out)
    const {
  if (missingByServiceValid_) {
    CHECK_LT(service, numServices_);
    *out = missingByService_[service];
  }
  return missingByServiceValid_;
}

bool BeringeiScanStats::getMissingByNumServices(size_t service, int64_t* out)
    const {
  if (missingByNumServicesValid_) {
    CHECK_LT(service, numServices_);
    *out = missingByNumServices_[service];
  }
  return missingByNumServicesValid_;
}

void BeringeiScanStats::setMissingByService(size_t service, int64_t value) {
  CHECK_LT(service, numServices_);
  missingByService_[service] = value;
  missingByServiceValid_ = true;
}

void BeringeiScanStats::setMissingByService(const std::vector<int64_t>& in) {
  missingByService_ = in;
  missingByServiceValid_ = true;
}

void BeringeiScanStats::setMissingByNumServices(size_t service, int64_t value) {
  CHECK_LT(service, numServices_);
  missingByNumServices_[service] = value;
  missingByNumServicesValid_ = true;
}

void BeringeiScanStats::setMissingByNumServices(
    const std::vector<int64_t>& in) {
  missingByNumServices_ = in;
  missingByNumServicesValid_ = true;
}

std::vector<TimeValuePair> BeringeiScanShardResult::getUncompressedData(
    size_t key,
    BeringeiScanStats* stats,
    int64_t* mergeDeletedDataKeys) const {
  std::vector<TimeValuePair> ret;

  if (stats && !stats->getNumServices()) {
    *stats = BeringeiScanStats(numServices, serviceDataValid);
  }

  std::unique_ptr<BeringeiScanStats> newStats;
  if (stats) {
    newStats =
        std::make_unique<BeringeiScanStats>(numServices, serviceDataValid);
  }

  int64_t lastService = -1;
  int64_t lastMismatches = -1;
  int64_t lastInSize = -1;
  for (unsigned service = 0; service < data[key].size(); ++service) {
    int64_t oldSize = ret.size();
    int64_t inSize;
    int64_t mismatches = 0;

    if (lastService != -1 && data[key][service] == data[key][lastService]) {
      // Merge is a no-op. Mismatches is the same since the original values are
      // preferred over new values when timestamps are the same.
      inSize = lastInSize;
      mismatches = lastMismatches;
    } else {
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

    if (inSize > 0) {
      lastService = service;
      lastMismatches = mismatches;
      lastInSize = inSize;
    }

    if (newStats) {
      const int64_t added = ret.size() - oldSize;
      const int64_t missing = ret.size() - inSize;

      if (added >= 0 && missing >= 0) {
        newStats->updateByService(service, ret.size(), added, missing);
      } else {
        newStats.reset();
        if (mergeDeletedDataKeys) {
          if (*mergeDeletedDataKeys == 0) {
            LOG(INFO) << "scanShard request to service " << service << " of "
                      << numServices << " " << serviceNames[service] << " "
                      << request << " data shrunk on merge key " << keys[key]
                      << " added " << added << " missing " << missing;
          }
          ++*mergeDeletedDataKeys;
        }
      }
    }
  }

  if (stats && newStats) {
    stats->merge(*newStats);
  }
  return ret;
}

std::vector<TimeValuePair> BeringeiScanShardResult::takeUncompressedData(
    size_t key,
    BeringeiScanStats* stats,
    int64_t* mergeDeletedDataKeys) {
  std::vector<TimeValuePair> ret =
      getUncompressedData(key, stats, mergeDeletedDataKeys);
  std::vector<std::vector<TimeSeriesBlock>>().swap(data[key]);
  return ret;
}

BeringeiScanShardResultCollector::BeringeiScanShardResultCollector(
    size_t services,
    const ScanShardRequest& request)
    : request_(request),
      numServices_(services),
      remainingServices_(services),
      results_(services),
      status_(StatusCode::RPC_FAIL),
      allSuccess_(false),
      done_(false) {}

bool BeringeiScanShardResultCollector::addResult(
    ScanShardResult&& result,
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
    LOG(ERROR) << "scanShard request to service " << service << " " << request_
               << " result " << mismatchName << " size " << mismatchValue
               << " != keys size " << result.keys.size();
    if (resultStatus == StatusCode::OK) {
      resultStatus = StatusCode::RPC_FAIL;
    }
  } else {
    results_[service] = std::make_unique<ScanShardResult>(std::move(result));
  }

  // Favor success or more specific error message
  if (status_ == StatusCode::RPC_FAIL ||
      (status_ != StatusCode::OK && resultStatus == StatusCode::OK)) {
    status_ = resultStatus;
  }

  if (allSuccess_ && resultStatus != StatusCode::OK) {
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

  std::bitset<32> serviceDataValid;
  std::map<std::string, KeyData> keyData;
  for (const auto& result : folly::enumerate(results_)) {
    if (*result) {
      // Responses not satisfying this invariant were set to NULL in addResult
      CHECK_EQ((*result)->data.size(), (*result)->keys.size());
      CHECK_EQ((*result)->queriedRecently.size(), (*result)->keys.size());

      serviceDataValid[result.index] = (*result)->status == StatusCode::OK;

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
      request_, keyData.size(), serviceNames, serviceDataValid);

  for (size_t i = 0; !keyData.empty(); ++i) {
    auto key = keyData.begin();

    ret.keys[i] = key->first;
    ret.data[i] = std::move(key->second.data_);
    ret.queriedRecently[i] = key->second.queriedRecently_;

    ret.keyStats.updateByEntry(key->second.have_);

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
