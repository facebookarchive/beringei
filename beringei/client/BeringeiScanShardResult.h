/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include <folly/fibers/TimedMutex.h>

#include "beringei/if/gen-cpp2/beringei_data_types.h"

namespace facebook {
namespace gorilla {

// A BeringeiScanStat object can be used to track ScanShard statistics for
// keys or time series data points
class BeringeiScanStats {
 public:
  explicit BeringeiScanStats(size_t services = 0, std::bitset<32> enabled = 0)
      : numServices_(services),
        enabled_(enabled),
        total_(),
        missingByService_(services),
        missingByServiceValid_(numServices_ != 0),
        missingByNumServices_(services),
        missingByNumServicesValid_(numServices_ != 0) {}

  // Call once per data point or key.
  // Do not call updateByService for the same input
  //
  // Can track missingByNumServices_ and missingByService_ for all numServices
  //
  // @param[in] services bit field of regions containing the data point or key
  void updateByEntry(std::bitset<32> services);

  // Call for each service with data sequentially for each service
  // 0 <= service < numServices_
  //
  // Can track missingByNumServices_ for numServices <= 2,
  // missingByService_ for all numServices
  //
  // @param[in] newSize size of unique data points or keys per service
  // when all entities in all smaller numbered services are known
  // @param[in] added number of unique data points or keys added by the
  // response from service.  Must be non-negative.
  // @param[in] missing number of data points or keys in the response from
  // service.  Must be non-negative.
  void updateByService(
      size_t service,
      int64_t newSize,
      int64_t added,
      int64_t missing);

  // Merge rhs into this
  void merge(const BeringeiScanStats& rhs);

  size_t getNumServices() const {
    return numServices_;
  }

  // @param[in] service < numServices_
  bool getMissingByService(size_t service, int64_t* out) const;

  // @param[in] service < numServices_
  bool getMissingByNumServices(size_t service, int64_t* out) const;

  int64_t getTotal() const {
    return total_;
  }

  // only for test.  Imply counts for all services are valid
  void setMissingByService(size_t service, int64_t value);
  void setMissingByService(const std::vector<int64_t>& in);
  void setMissingByNumServices(size_t service, int64_t value);
  void setMissingByNumServices(const std::vector<int64_t>& in);
  void setMissingByNumServicesInvalid() {
    missingByNumServicesValid_ = false;
  }

  void setTotal(int64_t value) {
    total_ = value;
  }

 private:
  size_t numServices_;
  std::bitset<32> enabled_;

  uint64_t total_;
  std::vector<int64_t> missingByService_;
  bool missingByServiceValid_;

  // Number of data points missing from the indexed number of replicas,
  // with replicas having no data (boost::none) not counted to provide
  // more consistent statistics when replicas are unreachable
  std::vector<int64_t> missingByNumServices_;
  bool missingByNumServicesValid_;
};

// Result for ScanShard operations potentially merging several responses
// for multi-master operation.  Results are stored as compressed time series,
// then uncompressed and combined on a per-key basis to limit memory footprint
struct BeringeiScanShardResult {
  BeringeiScanShardResult()
      : beginTime(),
        endTime(),
        status(StatusCode::OK),
        numServices(),
        allSuccess() {}
  BeringeiScanShardResult(
      const ScanShardRequest& requestArg,
      size_t keys,
      const std::vector<std::string>& serviceNamesArg,
      std::bitset<32> serviceDataValidArg)
      : request(requestArg),
        beginTime(request.begin),
        endTime(request.end),
        status(StatusCode::OK),
        keys(keys),
        data(keys),
        queriedRecently(keys),
        keyStats(serviceNamesArg.size(), serviceDataValidArg),
        numServices(serviceNamesArg.size()),
        serviceNames(serviceNamesArg),
        serviceDataValid(serviceDataValidArg),
        allSuccess(false) {}
  BeringeiScanShardResult(const BeringeiScanShardResult&) = delete;
  BeringeiScanShardResult& operator=(const BeringeiScanShardResult&) = delete;
  BeringeiScanShardResult(BeringeiScanShardResult&&) = default;
  BeringeiScanShardResult& operator=(BeringeiScanShardResult&&) = default;

  // Return uncompressed time series for key[index] transferring ownership
  //
  // @param[inout] stats *stats is updated when stats is non-null and
  // data[index][key] merge without dropping data points for
  // 0 <= key < numServices.  On the first call, stats must reference a default
  // constructed BeringeiScanStats object.
  std::vector<TimeValuePair> takeUncompressedData(
      size_t index,
      BeringeiScanStats* stats = nullptr,
      int64_t* mergeDeletedDataKeys = nullptr);

  // Return uncompressed time series for key[index] with no ownership transfer
  // Only for testing
  std::vector<TimeValuePair> getUncompressedData(
      size_t index,
      BeringeiScanStats* stats = nullptr,
      int64_t* mergeDeletedDataKeys = nullptr) const;

  ScanShardRequest request;

  int64_t beginTime;
  int64_t endTime;

  StatusCode status;

  // Class invariant is keys.size() == data.size() == queriedRecently.size()
  std::vector<std::string> keys;

  // data[key][service][block], with data[key].size == numServices
  std::vector<std::vector<std::vector<TimeSeriesBlock>>> data;
  // queriedRecently[i] corresponds to keys[i]
  std::vector<bool> queriedRecently;

  BeringeiScanStats keyStats;

  size_t numServices;

  // serviceNames[i] corresponds to data[key][i] and serviceDataValid[i]
  std::vector<std::string> serviceNames;

  // serviceDataValid[i] corresponds to data[key][i] and serviceNames[i]
  std::bitset<32> serviceDataValid;

  bool allSuccess;
};

// Accumulate and consolidate Beringei ScanShard responses
class BeringeiScanShardResultCollector {
 public:
  BeringeiScanShardResultCollector(
      size_t services,
      const ScanShardRequest& request);

  // @return true on completion
  bool addResult(ScanShardResult&& result, size_t service);

  // Finalize data, record stats, and extract the result structure.
  // Throws an exception on incomplete results if requested to do so.
  //
  // After this point, further calls to `addResults()` will be ignored.
  //
  // Can only be called once
  BeringeiScanShardResult finalize(
      bool validate,
      const std::vector<std::string>& serviceNames);

 private:
  const ScanShardRequest request_;

  // Number of services being queried
  const size_t numServices_;

  // Protects faollowing fields.
  folly::fibers::TimedMutex lock_;

  // Count of services which have not yet returned
  size_t remainingServices_;

  std::vector<std::unique_ptr<ScanShardResult>> results_;

  // Aggregate status
  StatusCode status_;

  // Every service returned StatusCode::OK
  bool allSuccess_;

  // this->finalize called (only once allowed)
  bool done_;
};
} // namespace gorilla
} // namespace facebook
