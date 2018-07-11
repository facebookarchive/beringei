/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <bitset>
#include <mutex>
#include <vector>

#include <folly/MPMCQueue.h>
#include <folly/fibers/TimedMutex.h>
#include <folly/futures/Future.h>

#include "beringei/client/BeringeiNetworkClient.h"
#include "beringei/if/gen-cpp2/beringei_data_types.h"

namespace facebook {
namespace gorilla {
// The stats for a completed BeringeiQuery.
// memoryEstimate is an estimate of how much memory the query consumed in bytes.
// missingPoints is the max number of missing points from a replica that are
// present in another replica.
// failedKeys is the max number of missing keys from a replica.
// mismatches is the max number of mismatched datapoints from a replica.
struct BeringeiGetStats {
  size_t memoryEstimate = 0;
  int64_t missingPoints = 0;
  int64_t failedKeys = 0;
  int64_t mismatches = 0;
};

// The results of a BeringeiQuery.
// Values are returned in the same order as they were queried.
// Keys that were not found have empty result vectors.
//
// allSuccess is set to true if we were able to get a full copy of the results.
struct BeringeiGetResult {
  BeringeiGetResult() : allSuccess(false) {}
  explicit BeringeiGetResult(size_t size) : results(size), allSuccess(false) {}
  BeringeiGetResult(const BeringeiGetResult&) = delete;
  BeringeiGetResult& operator=(const BeringeiGetResult&) = delete;
  BeringeiGetResult(BeringeiGetResult&&) = default;
  BeringeiGetResult& operator=(BeringeiGetResult&&) = default;

  std::vector<std::vector<TimeValuePair>> results;
  bool allSuccess;
  BeringeiGetStats stats;
};

// This class records results for a Beringei query as they arrive from multiple
// replicas of the service, tracking how much data was lost from each replica.
//
// Note: to do this quickly, it uses memory exponential in the number of
// replicas. As a typical setup is unlikely to have more than 3 replicas of the
// data, this is probably fine.
class BeringeiGetResultCollector {
 public:
  BeringeiGetResultCollector(
      size_t keys,
      size_t services,
      int64_t begin,
      int64_t end);

  // Insert received data into internal queue, and return number of keys read.
  // @param[in] results Results received from Gorilla.
  // @param[in] indices Indices of keys in the requests for a host.
  //  For example, host A has keys 0, 3, 4. Host B has keys 1, 2.
  // @param[in] service Index of the region.
  // @returns Number valid keys from {results}.
  bool addResults(
      GetDataResult&& results,
      const std::vector<size_t>& indices,
      size_t service);

  // Finalize data, record stats, and extract the result structure.
  // Throws an exception on incomplete results if requested to do so.
  // After this point, further calls to `addResults()` will be ignored.
  BeringeiGetResult finalize(
      bool validate,
      const std::vector<std::string>& serviceNames);

  // Use in tests only.
  const std::vector<int64_t>& getMismatchesForTesting() const {
    return mismatches_;
  }

 private:
  // Add results.
  void merge(size_t i, size_t service, const TimeSeriesData& result);

  // Begin and end time for the query to remove extraneous data.
  int64_t beginTime_, endTime_;

  // How many copies we're expecting for each key.
  size_t numServices_;

  struct AddStats {
    // How many keys have no results.
    size_t remainingKeys;

    // How many services have reported this key.
    std::vector<uint32_t> count;
  };
  folly::Synchronized<AddStats, std::mutex> addStats_;

  // Which services have reported which keys.
  struct KeyStats {
    uint32_t finalizedCount;
    std::bitset<32> received;
  };
  std::vector<KeyStats> complete_;

  // How much data was missing from each service.
  std::vector<int> drops_;

  // Mismatches per each service's first merge, indexed by 1ull << service.
  std::vector<int64_t> mismatches_;

  std::atomic<bool> done_;
  BeringeiGetResult result_;

  struct Item {
    GetDataResult results;
    std::vector<size_t> indices;
    size_t serviceId;
  };

  folly::MPMCQueue<Item> queue_;
};

} // namespace gorilla
} // namespace facebook
