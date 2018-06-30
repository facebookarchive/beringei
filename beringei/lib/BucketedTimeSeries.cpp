/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "beringei/lib/BucketedTimeSeries.h"

#include <folly/synchronization/CallOnce.h>

#include "beringei/lib/BucketMap.h"
#include "beringei/lib/GorillaStatsManager.h"

DEFINE_int32(
    mintimestampdelta,
    30,
    "Values coming in faster than this are considered spam");

DEFINE_uint32(
    cold_bucket_threshold,
    0,
    "Time series queried more buckets ago are considered cold");

DEFINE_bool(
    enable_cold_writes,
    false,
    "Enable persistently flagging cold data buckets");

DEFINE_bool(
    all_buckets_memory,
    false,
    "Keep buckets in memory and just use eviction thresholds for stats.");

namespace facebook {
namespace gorilla {

static const uint16_t kDefaultCategory = 0;
// Number of buckets expired with data points
static const std::string kBucketsExpiredTotal = "buckets_expired_total";
// Subset which were never queried
static const std::string kBucketsExpiredQueried = "buckets_expired_queried";

// Number of time series streams committed to buckets
static const std::string TimeSeriesStreamsTotal = "time_series_streams_total";
// Subset which were never queried
static const std::string TimeSeriesStreamsQueried =
    "time_series_streams_queried";

BucketedTimeSeries::BucketedTimeSeries() : ready_(false) {
  static folly::once_flag flag;
  folly::call_once(flag, [&]() {
    GorillaStatsManager::addStatExportType(kBucketsExpiredTotal, SUM);
    GorillaStatsManager::addStatExportType(kBucketsExpiredTotal, AVG);
    GorillaStatsManager::addStatExportType(kBucketsExpiredQueried, SUM);
    GorillaStatsManager::addStatExportType(kBucketsExpiredQueried, AVG);

    GorillaStatsManager::addStatExportType(TimeSeriesStreamsTotal, SUM);
    GorillaStatsManager::addStatExportType(TimeSeriesStreamsTotal, AVG);
    GorillaStatsManager::addStatExportType(TimeSeriesStreamsQueried, SUM);
    GorillaStatsManager::addStatExportType(TimeSeriesStreamsQueried, AVG);
  });
}

BucketedTimeSeries::~BucketedTimeSeries() {}

void BucketedTimeSeries::reset(
    uint8_t n,
    uint32_t minBucket,
    int64_t minTimestamp) {
  queriedBucketsAgo_ = std::numeric_limits<uint8_t>::max();
  lock_.init();
  current_ = minBucket;
  blocks_.reset(new BucketStorage::BucketStorageId[n]);

  for (int i = 0; i < n; i++) {
    // Blacklist older buckets if `minBucket` was set.
    blocks_[i] = (minBucket == 0) ? BucketStorage::kInvalidId
                                  : BucketStorage::kDisabledId;
  }
  blocks_[current_ % n] = BucketStorage::kInvalidId;
  count_ = 0;
  stream_.reset(minTimestamp, FLAGS_mintimestampdelta);
  stream_.extraData = kDefaultCategory;
}

bool BucketedTimeSeries::put(
    uint32_t i,
    const TimeValuePair& value,
    BucketStorage* storage,
    uint32_t timeSeriesId,
    uint16_t* category) {
  folly::MSLGuard guard(lock_);
  if (i < current_) {
    return false;
  }

  if (i != current_) {
    open(i, storage, timeSeriesId);
  }

  if (!stream_.append(value, FLAGS_mintimestampdelta)) {
    return false;
  }

  if (category) {
    stream_.extraData = *category;
  }

  count_++;
  return true;
}

void BucketedTimeSeries::get(
    uint32_t begin,
    uint32_t end,
    std::vector<TimeSeriesBlock>& out,
    BucketStorage* storage,
    GetCounts* countsOut) {
  uint8_t n = storage->numBuckets();
  out.reserve(out.size() + std::min<uint32_t>(n + 1, end - begin + 1));

  folly::MSLGuard guard(lock_);
  bool getCurrent = begin <= current_ && end >= current_;

  end = std::min(end, current_ >= 1 ? current_ - 1 : 0);
  begin = std::max(begin, current_ >= n ? current_ - n : 0);

  GetCounts counts{};

  for (int i = begin; i <= end; i++) {
    TimeSeriesBlock outBlock;
    uint16_t count;

    BucketStorage::BucketStorageId id = blocks_[i % n];
    BucketStorage::FetchType type = BucketStorage::FetchType::NONE;
    BucketStorage::FetchStatus status =
        storage->fetch(i, id, outBlock.data, count, &type);
    if (status == BucketStorage::FetchStatus::SUCCESS) {
      outBlock.count = count;
      out.push_back(std::move(outBlock));
      bool cold = BucketStorage::coldId(id);
      if (!FLAGS_all_buckets_memory) {
        counts.blockFetched(type, cold);
      } else if (i + storage->numMemoryBuckets(cold) + 1 >= current_) {
        counts.blockFetched(BucketStorage::FetchType::MEMORY, cold);
      } else {
        counts.blockFetched(BucketStorage::FetchType::DISK, cold);
      }
    } else {
      counts.blockFetched(BucketStorage::FetchType::INVALID, false /* cold */);
    }
  }

  if (getCurrent) {
    out.emplace_back();
    out.back().count = count_;
    stream_.readData(out.back().data);
    counts.blockFetched(BucketStorage::FetchType::MEMORY, false /* cold */);
  }

  if (countsOut) {
    *countsOut = counts;
  }
}

void BucketedTimeSeries::setCurrentBucket(
    uint32_t currentBucket,
    BucketStorage* storage,
    uint32_t timeSeriesId) {
  folly::MSLGuard guard(lock_);
  if (current_ < currentBucket) {
    open(currentBucket, storage, timeSeriesId);
  }
}

void BucketedTimeSeries::open(
    uint32_t next,
    BucketStorage* storage,
    uint32_t timeSeriesId) {
  if (current_ == 0) {
    // Skip directly to the new value.
    current_ = next;
    blocks_[current_ % storage->numBuckets()] = BucketStorage::kInvalidId;
    return;
  }

  int bucketsExpired = 0;
  int bucketsExpiredQueried = 0;

  // Wipe all the blocks in between.
  while (current_ != next) {
    // Reset the block we're about to replace.
    auto& block = blocks_[current_ % storage->numBuckets()];

    if (block != BucketStorage::kInvalidId &&
        block != BucketStorage::kDisabledId) {
      ++bucketsExpired;
      // Can't distinguish between queries for current_ and newer
      if (queriedBucketsAgo_ <= storage->numBuckets()) {
        ++bucketsExpiredQueried;
      }
    }

    if (count_ > 0) {
      GorillaStatsManager::addStatValue(TimeSeriesStreamsTotal);
      if (queriedBucketsAgo_ == 0) {
        GorillaStatsManager::addStatValue(TimeSeriesStreamsQueried);
      }
      // Copy out the active data.
      block = storage->store(
          current_,
          stream_.getDataPtr(),
          stream_.size(),
          count_,
          timeSeriesId,
          getCold() && FLAGS_enable_cold_writes);
    } else {
      block = BucketStorage::kInvalidId;
    }

    // Prepare for writes.
    count_ = 0;
    stream_.reset();
    current_++;

    if (queriedBucketsAgo_ < std::numeric_limits<uint8_t>::max()) {
      queriedBucketsAgo_++;
    }
  }

  GorillaStatsManager::addStatValue(kBucketsExpiredTotal, bucketsExpired);
  GorillaStatsManager::addStatValue(
      kBucketsExpiredQueried, bucketsExpiredQueried);
}

void BucketedTimeSeries::setQueried() {
  queriedBucketsAgo_ = 0;
}

void BucketedTimeSeries::setDataBlock(
    uint32_t position,
    BucketStorage* storage,
    BucketStorage::BucketStorageId id) {
  folly::MSLGuard guard(lock_);

  // We just loaded block data that is newer than the current bucket.
  // This is likely because we just haven't received any data for it yet, but
  // just in case, throw out any data we do have to replace it with what we just
  // loaded.
  if (position >= current_) {
    count_ = 0;
    stream_.reset();
    open(position + 1, storage, 0);
  }

  // Don't store any data older than the configured minimum bucket.
  // This allows safe shard movement even when timeseries IDs get reused.
  auto numBuckets = storage->numBuckets();
  auto& block = blocks_[position % numBuckets];
  if (block != BucketStorage::kDisabledId) {
    blocks_[position % numBuckets] = id;
  }
}

bool BucketedTimeSeries::hasDataPoints(uint8_t numBuckets) {
  folly::MSLGuard guard(lock_);
  if (count_ > 0) {
    return true;
  }

  for (int i = 0; i < numBuckets; i++) {
    if (blocks_[i] != BucketStorage::kInvalidId &&
        blocks_[i] != BucketStorage::kDisabledId) {
      return true;
    }
  }

  return false;
}

uint16_t BucketedTimeSeries::getCategory() const {
  folly::MSLGuard guard(lock_);
  return stream_.extraData;
}

void BucketedTimeSeries::setCategory(uint16_t category) {
  folly::MSLGuard guard(lock_);
  stream_.extraData = category;
}

int32_t BucketedTimeSeries::getFirstUpdateTime(
    BucketStorage* storage,
    const BucketMap& map) {
  folly::MSLGuard guard(lock_);

  auto n = storage->numBuckets();

  // Find the first block that has data and return its starting timestamp.
  for (int i = n; i > 0; i--) {
    int position = (int)current_ - i;
    if (position < 0) {
      continue;
    }

    auto& block = blocks_[position % storage->numBuckets()];
    if (block != BucketStorage::kInvalidId &&
        block != BucketStorage::kDisabledId) {
      return map.timestamp(position);
    }
  }

  return stream_.getFirstTimeStamp();
}

uint32_t BucketedTimeSeries::getLastUpdateTime(
    BucketStorage* storage,
    const BucketMap& map) {
  folly::MSLGuard guard(lock_);
  uint32_t lastUpdateTime = stream_.getPreviousTimeStamp();
  if (lastUpdateTime != 0) {
    return lastUpdateTime;
  }

  // Nothing in the stream, find the latest block that has data and
  // return the end time of that block. The return value will just be
  // an estimate.
  for (int i = 0; i < storage->numBuckets(); i++) {
    int position = (int)current_ - 1 - i;
    if (position < 0) {
      break;
    }

    auto& block = blocks_[position % storage->numBuckets()];
    if (block != BucketStorage::kInvalidId &&
        block != BucketStorage::kDisabledId) {
      return map.timestamp(position + 1);
    }
  }

  return 0;
}

int32_t BucketedTimeSeries::getBucketAge(uint32_t bucket) const {
  return current_ - bucket;
}

bool BucketedTimeSeries::getCold() const {
  return queriedBucketsAgo_ > FLAGS_cold_bucket_threshold;
}

} // namespace gorilla
} // namespace facebook
