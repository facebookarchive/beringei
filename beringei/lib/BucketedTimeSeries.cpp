/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "BucketedTimeSeries.h"
#include "BucketMap.h"

DEFINE_int32(
    mintimestampdelta,
    30,
    "Values coming in faster than this are considered spam");

namespace facebook {
namespace gorilla {

static const uint16_t kDefaultCategory = 0;

BucketedTimeSeries::BucketedTimeSeries() {}

BucketedTimeSeries::~BucketedTimeSeries() {}

void BucketedTimeSeries::reset(uint8_t n) {
  queriedBucketsAgo_ = std::numeric_limits<uint8_t>::max();
  lock_.init();
  current_ = 0;
  blocks_.reset(new BucketStorage::BucketStorageId[n]);

  for (int i = 0; i < n; i++) {
    blocks_[i] = BucketStorage::kInvalidId;
  }
  count_ = 0;
  stream_.reset();
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
    BucketStorage* storage) {
  uint8_t n = storage->numBuckets();
  out.reserve(out.size() + std::min<uint32_t>(n + 1, end - begin + 1));

  folly::MSLGuard guard(lock_);
  bool getCurrent = begin <= current_ && end >= current_;

  end = std::min(end, current_ >= 1 ? current_ - 1 : 0);
  begin = std::max(begin, current_ >= n ? current_ - n : 0);

  for (int i = begin; i <= end; i++) {
    TimeSeriesBlock outBlock;
    uint16_t count;

    BucketStorage::FetchStatus status =
        storage->fetch(i, blocks_[i % n], outBlock.data, count);
    if (status == BucketStorage::FetchStatus::SUCCESS) {
      outBlock.count = count;
      out.push_back(std::move(outBlock));
    }
  }

  if (getCurrent) {
    out.emplace_back();
    out.back().count = count_;
    stream_.readData(out.back().data);
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
    return;
  }

  // Wipe all the blocks in between.
  while (current_ != next) {
    // Reset the block we're about to replace.
    auto& block = blocks_[current_ % storage->numBuckets()];

    if (count_ > 0) {
      // Copy out the active data.
      block = storage->store(
          current_, stream_.getDataPtr(), stream_.size(), count_, timeSeriesId);
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
}

void BucketedTimeSeries::setQueried() {
  queriedBucketsAgo_ = 0;
}

void BucketedTimeSeries::setDataBlock(
    uint32_t position,
    uint8_t numBuckets,
    BucketStorage::BucketStorageId id) {
  folly::MSLGuard guard(lock_);

  // Needed for time series that receive data very rarely.
  if (position >= current_) {
    current_ = position + 1;
    count_ = 0;
    stream_.reset();
  }

  blocks_[position % numBuckets] = id;
}

bool BucketedTimeSeries::hasDataPoints(uint8_t numBuckets) {
  folly::MSLGuard guard(lock_);
  if (count_ > 0) {
    return true;
  }

  for (int i = 0; i < numBuckets; i++) {
    if (blocks_[i] != BucketStorage::kInvalidId) {
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

    if (blocks_[position % storage->numBuckets()] !=
        BucketStorage::kInvalidId) {
      return map.timestamp(position + 1);
    }
  }

  return 0;
}
}
} // facebook::gorilla
