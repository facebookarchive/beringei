/**
 * Copyright (c) 2016-present, Facebook, Inc.
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

struct BeringeiScanShardResult {
  BeringeiScanShardResult()
      : beginTime(),
        endTime(),
        status(StatusCode::OK),
        services(),
        allSuccess() {}
  BeringeiScanShardResult(
      int64_t begin,
      int64_t end,
      size_t keys,
      size_t servicesArg)
      : beginTime(begin),
        endTime(end),
        status(StatusCode::OK),
        keys(keys),
        data(keys),
        queriedRecently(keys),
        services(servicesArg),
        allSuccess(false) {}
  BeringeiScanShardResult(const BeringeiScanShardResult&) = delete;
  BeringeiScanShardResult& operator=(const BeringeiScanShardResult&) = delete;
  BeringeiScanShardResult(BeringeiScanShardResult&&) = default;
  BeringeiScanShardResult& operator=(BeringeiScanShardResult&&) = default;

  // @pre *this and rhs do not violate the class invariant
  bool operator==(const BeringeiScanShardResult& rhs) const;

  // Return uncompressed time series for key[index] transferring ownership
  std::vector<TimeValuePair> takeUncompressedData(size_t index);

  // Return uncompressed time series for key[index] with no ownership transfer
  // Only for testing, with refactored code doing conversion
  std::vector<TimeValuePair> getUncompressedData(size_t index) const;

  int64_t beginTime, endTime;

  StatusCode status;

  // Class invariant is keys.size() == data.size() == queriedRecently.size()
  std::vector<std::string> keys;

  // data[key][service][block]
  std::vector<std::vector<std::vector<TimeSeriesBlock>>> data;
  std::vector<bool> queriedRecently;

  // XXX: dreweckhardt 2018=01=31 numServices is more consistent
  size_t services;

  bool allSuccess;
};

class BeringeiScanShardResultCollector {
 public:
  BeringeiScanShardResultCollector(size_t services, int64_t begin, int64_t end);

  // @return true on completion
  bool addResult(
      ScanShardResult&& result,
      const ScanShardRequest& request,
      size_t service);

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
  // Begin and end time for the query to remove extraneous data.
  const int64_t beginTime_, endTime_;

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
