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
#include <vector>

#include <folly/synchronization/SmallLocks.h>
#include "BucketStorage.h"
#include "TimeSeriesStream.h"
#include "beringei/if/gen-cpp2/beringei_data_types.h"

namespace facebook {
namespace gorilla {

class BucketMap;
// Holds a rolling window of TimeSeries data.
class BucketedTimeSeries {
 public:
  BucketedTimeSeries();
  ~BucketedTimeSeries();

  // Initialize a BucketedTimeSeries with n historical buckets and
  // one active bucket. The BucketedTimeSeries will ignore any points
  // that predate minTimestamp and loaded block files that predate minBucket.
  // Not thread-safe.
  void reset(uint8_t n, uint32_t minBucket, int64_t minTimestamp);

  // Add a data point to the given bucket. Returns true if data was
  // added, false if it was dropped. If category pointer is defined,
  // sets the category.
  bool put(
      uint32_t i,
      const TimeValuePair& value,
      BucketStorage* storage,
      uint32_t timeSeriesId,
      uint16_t* category);

  using FetchType = BucketStorage::FetchType;

  class GetCounts {
   public:
    GetCounts() : blockCountBySource_{}, coldBlocks_() {}

    void blockFetched(FetchType source, bool cold = false) {
      ++blockCountBySource_[source];
      if (cold) {
        ++coldBlocks_;
      }
    }

    uint8_t getBlockCount(FetchType source) const {
      return blockCountBySource_[source];
    }

    uint8_t getColdBlocks() const {
      return coldBlocks_;
    }

   private:
    std::array<uint8_t, FetchType::MAX> blockCountBySource_;
    uint8_t coldBlocks_;
  };

  // Read out buckets between begin and end inclusive, including current one.
  typedef std::vector<TimeSeriesBlock> Output;
  void get(
      uint32_t begin,
      uint32_t end,
      Output& out,
      BucketStorage* storage,
      GetCounts* = nullptr);

  // Returns a tuple representing:
  //   1) the number of points in the active stream.
  //   2) the number of bytes used by the stream.
  //   3) the number of bytes allocated for the stream.
  std::tuple<uint32_t, uint32_t, uint32_t> getActiveTimeSeriesStreamInfo() {
    folly::MSLGuard guard(lock_);
    return std::make_tuple(count_, stream_.size(), stream_.capacity());
  }

  // Returns how many buckets ago this value was queried.
  // Will return 255 if it has never been queried.
  uint8_t getQueriedBucketsAgo() {
    return queriedBucketsAgo_;
  }

  // Sets that this time series was just queried.
  void setQueried();

  void setDataBlock(
      uint32_t position,
      BucketStorage* storage,
      BucketStorage::BucketStorageId id);

  // Sets the current bucket. Flushes data from the previous bucket to
  // BucketStorage. No-op if this time series is already at
  // currentBucket.
  void setCurrentBucket(
      uint32_t currentBucket,
      BucketStorage* storage,
      uint32_t timeSeriesId);

  // Returns true if there are data points for this time series.
  bool hasDataPoints(uint8_t numBuckets);

  // Returns the ODS category associated with this time series.
  uint16_t getCategory() const;

  // Sets the ODS category for this time series.
  void setCategory(uint16_t category);

  int32_t getFirstUpdateTime(BucketStorage* storage, const BucketMap& map);
  uint32_t getLastUpdateTime(BucketStorage* storage, const BucketMap& map);

  // Return age of bucket relative to current
  int32_t getBucketAge(uint32_t bucket) const;

  bool ready() const {
    return ready_;
  }

  void setReady() {
    ready_ = true;
  }

  // Return whether this time series is cold
  bool getCold() const;

 private:
  // Open the next bucket for writes.
  void open(uint32_t next, BucketStorage* storage, uint32_t timeSeriesId);

  uint8_t queriedBucketsAgo_;

  mutable folly::MicroSpinLock lock_;

  // Number of points in the active bucket (stream_).
  uint16_t count_;

  // Currently active bucket.
  uint32_t current_;

  // Blocks of metadata for previous data.
  std::unique_ptr<BucketStorage::BucketStorageId[]> blocks_;

  // Current stream of data.
  TimeSeriesStream stream_;

  // Whether this timeseries is ready for logging.
  bool ready_;
};
}
} // facebook::gorilla
