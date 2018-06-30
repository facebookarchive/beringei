/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "BucketStorage.h"

#include "GorillaStatsManager.h"
#include "TimeSeriesStream.h"

#include <folly/compression/Compression.h>
#include <folly/io/IOBuf.h>

DEFINE_int32(
    pla_period,
    60,
    "Lossily compress time series to 1 point every X seconds");

namespace facebook {
namespace gorilla {

// To fit in 15 bits.
const uint16_t kMaxItemCount = 32767;

// To fit in 15 bits.
const uint16_t kMaxDataLength = 32767;

// For page index to fit in 17 bits.
const uint32_t kMaxPageCount = 131072;

// Store data in 64K chunks.
// kMaxPageCount * kPageSize = 8GB
const uint32_t BucketStorage::kPageSize = kDataBlockSize;

const uint8_t BucketStorage::kDefaultToNumBuckets = 0;

// Zero can be used as the invalid ID because no valid ID will ever be zero
const BucketStorage::BucketStorageId BucketStorage::kInvalidId = 0;

// Also an invalid ID because offset + length will be > page size.
const BucketStorage::BucketStorageId BucketStorage::kDisabledId = ~0;

static const size_t kLargeFileBuffer = 1024 * 1024;

static const std::string kBlockFileReadFailures = "block_file_read_failures";
static const std::string kDedupedTimeSeriesSize = "timeseries_block_dedup_size";
static const std::string kWrittenTimeSeriesSize =
    "timeseries_block_written_size";
static const std::string kExpiredBucketFetch = "expired_bucket_fetches";

BucketStorage::BucketStorage(uint8_t numBuckets, int shardId)
    : numBuckets_(numBuckets), shardId_(shardId) {}

BucketStorage::~BucketStorage() {}

BucketStorageSingle::BucketStorageSingle(
    uint8_t numBuckets,
    int shardId,
    const std::string& dataDirectory,
    uint8_t numMemoryBuckets,
    DataBlockVersion writeVersion)
    : BucketStorage(numBuckets, shardId),
      newestPosition_(0),
      dataBlockIO_(shardId, dataDirectory, writeVersion),
      numMemoryBuckets_(
          numMemoryBuckets == kDefaultToNumBuckets ? numBuckets
                                                   : numMemoryBuckets) {
  data_.reset(new BucketData[numBuckets]);
  enable();
}

BucketStorageSingle::~BucketStorageSingle() {}

void BucketStorageSingle::createDirectories() {
  dataBlockIO_.createDirectories();
}

BucketStorage::BucketStorageId BucketStorageSingle::store(
    uint32_t position,
    const char* data,
    uint16_t dataLength,
    uint16_t itemCount,
    uint32_t timeSeriesId,
    bool cold) {
  if (dataLength > kMaxDataLength || itemCount > kMaxItemCount) {
    LOG(ERROR) << "Attempted to insert too much data. Length : " << dataLength
               << " Count : " << itemCount;
    return kInvalidId;
  }

  uint32_t pageIndex;
  uint32_t pageOffset;
  uint8_t bucket = position % numBuckets_;

  std::unique_lock<std::mutex> guard(data_[bucket].pagesMutex);

  if (data_[bucket].disabled) {
    return kInvalidId;
  }

  // Check if this is the first time this position is seen. If it
  // is, buckets are rotated and an old bucket is now the active
  // one.
  if (position > data_[bucket].position) {
    // Need this lock to prevent reading from deleted memory in fetch.
    folly::RWSpinLock::WriteHolder writeGuard(data_[bucket].fetchLock);

    if (data_[bucket].activePages < data_[bucket].pages.size()) {
      // Only delete memory if the pages were not fully used the
      // previous time around. This means that with 12 2-hour buckets
      // if there's a spike in the amount of data on day 1, the extra
      // memory will be freed on day 2.
      data_[bucket].pages.resize(data_[bucket].activePages);
    }

    data_[bucket].activePages = 0;
    data_[bucket].lastPageBytesUsed = 0;
    data_[bucket].position = position;
    data_[bucket].storageIds.clear();
    data_[bucket].timeSeriesIds.clear();
    data_[bucket].finalized = false;
    data_[bucket].storageIdsLookupMap.clear();
    newestPosition_ = position;
  }

  if (data_[bucket].position != position) {
    LOG(ERROR) << "Trying to write data to an expired bucket";
    return kInvalidId;
  }

  if (data_[bucket].finalized) {
    LOG(ERROR) << "Trying to write data to a finalized bucket";
    return kInvalidId;
  }

  BucketStorageId id = kInvalidId;
  uint64_t hash = folly::hash::SpookyHashV2::Hash64(data, dataLength, 0);
  const auto& matches = data_[bucket].storageIdsLookupMap.equal_range(hash);
  for (auto iter = matches.first; iter != matches.second; ++iter) {
    uint32_t index, offset;
    uint16_t length, count;
    parseId(iter->second, index, offset, length, count);
    if (length == dataLength && count == itemCount &&
        (memcmp(data, data_[bucket].pages[index]->data + offset, dataLength) ==
         0)) {
      id = iter->second;

      GorillaStatsManager::addStatValue(kDedupedTimeSeriesSize, dataLength);
      break;
    }
  }

  if (id == kInvalidId) {
    if (data_[bucket].activePages == 0 ||
        data_[bucket].lastPageBytesUsed + dataLength > kPageSize) {
      if (data_[bucket].activePages == data_[bucket].pages.size()) {
        // All allocated pages used, need to allocate more pages.
        if (data_[bucket].pages.size() == kMaxPageCount) {
          LOG(ERROR) << "All pages are already in use.";
          return kInvalidId;
        }

        data_[bucket].pages.emplace_back(new DataBlock);
      }

      // Use the next page.
      data_[bucket].activePages++;
      data_[bucket].lastPageBytesUsed = 0;
    }

    pageIndex = data_[bucket].activePages - 1;
    pageOffset = data_[bucket].lastPageBytesUsed;
    data_[bucket].lastPageBytesUsed += dataLength;

    memcpy(data_[bucket].pages[pageIndex]->data + pageOffset, data, dataLength);
    id = createId(pageIndex, pageOffset, dataLength, itemCount, cold);
    data_[bucket].storageIdsLookupMap.insert(std::make_pair(hash, id));
    GorillaStatsManager::addStatValue(kWrittenTimeSeriesSize, dataLength);
  }
  data_[bucket].timeSeriesIds.push_back(timeSeriesId);
  data_[bucket].storageIds.push_back(id);

  return id;
}

BucketStorage::FetchStatus BucketStorageSingle::fetch(
    uint32_t position,
    BucketStorage::BucketStorageId id,
    std::string& data,
    uint16_t& itemCount,
    FetchType* type) {
  if (id == kInvalidId || id == kDisabledId) {
    return FAILURE;
  }

  uint32_t pageIndex;
  uint32_t pageOffset;
  uint16_t dataLength;
  parseId(id, pageIndex, pageOffset, dataLength, itemCount);
  uint8_t bucket = position % numBuckets_;

  if (pageOffset + dataLength > kPageSize) {
    LOG(ERROR) << "Corrupt storage id:" << id << " pageIndex:" << pageIndex
               << " pageOffset:" << pageOffset << " dataLength:" << dataLength
               << " itemCount:" << itemCount;
    return FAILURE;
  }

  folly::RWSpinLock::ReadHolder readGuard(&data_[bucket].fetchLock);
  if (data_[bucket].disabled) {
    return FAILURE;
  }

  if (data_[bucket].position != position && data_[bucket].position != 0) {
    VLOG(0) << "Tried to fetch data for an expired bucket:" << size_t(bucket)
            << " position:" << position;
    GorillaStatsManager::addStatValue(kExpiredBucketFetch, 1);
    return FAILURE;
  }

  if (pageIndex < data_[bucket].pages.size() &&
      data_[bucket].pages[pageIndex]) {
    data.assign(data_[bucket].pages[pageIndex]->data + pageOffset, dataLength);
    if (type) {
      *type = MEMORY;
    }
    return SUCCESS;
  }

  return FAILURE;
}

std::set<uint32_t> BucketStorageSingle::findCompletedPositions() {
  return dataBlockIO_.findCompletedBlockFiles();
}

bool BucketStorageSingle::loadPosition(
    uint32_t position,
    std::vector<uint32_t>& timeSeriesIds,
    std::vector<uint64_t>& storageIds) {
  uint8_t bucket = position % numBuckets_;

  {
    folly::RWSpinLock::WriteHolder writeGuard(&data_[bucket].fetchLock);

    // Verify that we should actually do this.
    if (!sanityCheck(bucket, position)) {
      return false;
    }

    // Ignore buckets that have been completely read from disk or are being
    // actively filled by store().
    if (data_[bucket].activePages != 0) {
      return false;
    }
  }

  // Actually load the data.
  auto blocks = dataBlockIO_.readBlocks(position, timeSeriesIds, storageIds);
  if (blocks.size() == 0) {
    GorillaStatsManager::addStatValue(kBlockFileReadFailures, 1);
    return false;
  }

  // Grab the lock and do the same sanity checks again.
  folly::RWSpinLock::WriteHolder writeGuard(&data_[bucket].fetchLock);
  if (!sanityCheck(bucket, position) || data_[bucket].activePages != 0) {
    return false;
  }

  data_[bucket].pages.resize(blocks.size());
  data_[bucket].activePages = blocks.size();

  for (int i = 0; i < blocks.size(); i++) {
    data_[bucket].pages[i] = std::move(blocks[i]);
  }
  return true;
}

void BucketStorageSingle::clearAndDisable() {
  for (int i = 0; i < numBuckets_; i++) {
    std::unique_lock<std::mutex> guard(data_[i].pagesMutex);
    folly::RWSpinLock::WriteHolder writeGuard(data_[i].fetchLock);

    data_[i].position = 0;
    data_[i].disabled = true;
    std::vector<std::shared_ptr<DataBlock>>().swap(data_[i].pages);
    data_[i].activePages = 0;
    data_[i].lastPageBytesUsed = 0;
    std::unordered_multimap<uint64_t, uint64_t>().swap(
        data_[i].storageIdsLookupMap);
    data_[i].finalized = false;
  }
}

void BucketStorageSingle::enable() {
  for (int i = 0; i < numBuckets_; i++) {
    std::unique_lock<std::mutex> guard(data_[i].pagesMutex);
    folly::RWSpinLock::WriteHolder writeGuard(data_[i].fetchLock);

    data_[i].position = 0;
    data_[i].disabled = false;
    data_[i].activePages = 0;
    data_[i].lastPageBytesUsed = 0;
  }
}

uint8_t BucketStorageSingle::numMemoryBuckets(bool /* unused */) const {
  return numMemoryBuckets_;
}

BucketStorage::BucketStorageId BucketStorage::createId(
    uint32_t pageIndex,
    uint32_t pageOffset,
    uint16_t dataLength,
    uint16_t itemCount,
    bool cold) {
  // Store all the values in 64 bits.
  BucketStorageId id = 0;

  // Using the first bit.
  id += (uint64_t)cold << 63;

  // Using the next 17 bits.
  id += (uint64_t)pageIndex << 46;

  // The next 16 bits.
  id += (uint64_t)pageOffset << 30;

  // The next 15 bits.
  id += (uint64_t)dataLength << 15;

  // The last 15 bits.
  id += itemCount;

  return id;
}

void BucketStorage::parseId(
    BucketStorageId id,
    uint32_t& pageIndex,
    uint32_t& pageOffset,
    uint16_t& dataLength,
    uint16_t& itemCount) {
  pageIndex = (id >> 46) & (kMaxPageCount - 1);
  pageOffset = (id >> 30) & (kPageSize - 1);
  dataLength = (id >> 15) & kMaxDataLength;
  itemCount = id & kMaxItemCount;
}

bool BucketStorage::coldId(BucketStorageId id) {
  return id != kInvalidId && id != kDisabledId && ((id >> 63) & 1);
}

void BucketStorageSingle::finalizeBucket(uint32_t position) {
  std::vector<std::shared_ptr<DataBlock>> pages;
  std::vector<uint32_t> timeSeriesIds;
  std::vector<BucketStorageId> storageIds;
  uint32_t activePages;
  const uint8_t bucket = position % numBuckets_;

  {
    std::lock_guard<std::mutex> guard(data_[bucket].pagesMutex);
    const uint32_t pageIndex = data_[bucket].activePages - 1;

    if (data_[bucket].disabled) {
      LOG(ERROR) << "Trying to finalize a disabled bucket: " << bucket
                 << " position:" << position;
      return;
    }

    if (data_[bucket].position != position) {
      if (data_[bucket].activePages > 0) {
        LOG(ERROR) << "Trying to finalize an expired bucket: " << bucket
                   << " position:" << position;
      }
      return;
    }

    if (data_[bucket].finalized) {
      LOG(ERROR) << "This bucket has already been finalized: " << bucket
                 << " position:" << position;
      return;
    }

    pages = data_[bucket].pages;
    timeSeriesIds = std::move(data_[bucket].timeSeriesIds);
    storageIds = std::move(data_[bucket].storageIds);
    activePages = data_[bucket].activePages;
    std::vector<uint32_t>().swap(data_[bucket].timeSeriesIds);
    std::vector<BucketStorageId>().swap(data_[bucket].storageIds);
    std::unordered_multimap<uint64_t, uint64_t>().swap(
        data_[bucket].storageIdsLookupMap);

    data_[bucket].finalized = true;
  }

  if (activePages > 0 && timeSeriesIds.size() > 0) {
    // Delete files older than 24h.
    dataBlockIO_.remove(position - numBuckets_);
    dataBlockIO_.write(position, pages, activePages, timeSeriesIds, storageIds);
  }
}

bool BucketStorageSingle::sanityCheck(uint8_t bucket, uint32_t position) {
  if (data_[bucket].disabled) {
    LOG(WARNING) << "Tried to fetch bucket for disabled shard";
    return false;
  }

  if (data_[bucket].position == 0) {
    // First time this bucket is used for anything. Mark the
    // position.
    data_[bucket].position = position;
    return true;
  }

  if (data_[bucket].position == position) {
    return true;
  }

  LOG(WARNING) << "Tried to fetch expired bucket";
  return false;
}

void BucketStorageSingle::deleteBucketsOlderThan(uint32_t position) {
  dataBlockIO_.clearTo(position);
}

void BucketStorage::startMonitoring() {
  GorillaStatsManager::addStatExportType(kBlockFileReadFailures, SUM);
  GorillaStatsManager::addStatExportType(kDedupedTimeSeriesSize, SUM);
  GorillaStatsManager::addStatExportType(kDedupedTimeSeriesSize, COUNT);
  GorillaStatsManager::addStatExportType(kWrittenTimeSeriesSize, SUM);
  GorillaStatsManager::addStatExportType(kWrittenTimeSeriesSize, COUNT);
  GorillaStatsManager::addStatExportType(kExpiredBucketFetch, COUNT);
}

std::pair<uint64_t, uint64_t> BucketStorageSingle::getPagesSize() {
  uint64_t activePagesSize = 0;
  uint64_t totalPagesSize = 0;
  for (int i = 0; i < numBuckets_; i++) {
    std::unique_lock<std::mutex> guard(data_[i].pagesMutex);
    activePagesSize += data_[i].activePages * (uint64_t)kDataBlockSize;
    totalPagesSize += data_[i].pages.size() * (uint64_t)kDataBlockSize;
  }
  return std::make_pair(activePagesSize, totalPagesSize);
}
} // namespace gorilla
} // namespace facebook
