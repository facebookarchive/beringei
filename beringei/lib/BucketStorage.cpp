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

// For page index to fit in 18 bits.
const uint32_t kMaxPageCount = 262144;

// Store data in 64K chunks.
// kMaxPageCount * kPageSize = 16GB
const uint32_t BucketStorage::kPageSize = kDataBlockSize;

// Zero can be used as the invalid ID because no valid ID will ever be zero
const BucketStorage::BucketStorageId BucketStorage::kInvalidId = 0;

// Also an invalid ID because offset + length will be > page size.
const BucketStorage::BucketStorageId BucketStorage::kDisabledId = ~0;

const std::string BucketStorage::kDataPrefix = "block_data";
const std::string BucketStorage::kCompletePrefix = "complete_block";

static const size_t kLargeFileBuffer = 1024 * 1024;

static const std::string kBlockFileReadFailures = "block_file_read_failures";
static const std::string kDedupedTimeSeriesSize = "timeseries_block_dedup_size";
static const std::string kWrittenTimeSeriesSize =
    "timeseries_block_written_size";

BucketStorage::BucketStorage(
    uint8_t numBuckets,
    int shardId,
    const std::string& dataDirectory)
    : numBuckets_(numBuckets),
      newestPosition_(0),
      dataBlockReader_(shardId, dataDirectory),
      dataFiles_(shardId, kDataPrefix, dataDirectory),
      completeFiles_(shardId, kCompletePrefix, dataDirectory) {
  data_.reset(new BucketData[numBuckets]);
  enable();
}

BucketStorage::BucketStorageId BucketStorage::store(
    uint32_t position,
    const char* data,
    uint16_t dataLength,
    uint16_t itemCount,
    uint32_t timeSeriesId) {
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
  if (position > newestPosition_) {
    // Need this lock to prevent reading from deleted memory in fetch.
    folly::RWSpinLock::WriteHolder writeGuard(data_[bucket].fetchLock);

    if (data_[bucket].activePages < data_[bucket].pages.size()) {
      // Only delete memory if the pages were not fully used the
      // previous time around. This means that if there's a spike in
      // the amount of data on day 1, the extra memory will be freed
      // on day 3.
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
    id = createId(pageIndex, pageOffset, dataLength, itemCount);
    data_[bucket].storageIdsLookupMap.insert(std::make_pair(hash, id));
    GorillaStatsManager::addStatValue(kWrittenTimeSeriesSize, dataLength);
  }
  data_[bucket].timeSeriesIds.push_back(timeSeriesId);
  data_[bucket].storageIds.push_back(id);

  return id;
}

BucketStorage::FetchStatus BucketStorage::fetch(
    uint32_t position,
    BucketStorage::BucketStorageId id,
    std::string& data,
    uint16_t& itemCount) {
  if (id == kInvalidId || id == kDisabledId) {
    return FAILURE;
  }

  uint32_t pageIndex;
  uint32_t pageOffset;
  uint16_t dataLength;
  parseId(id, pageIndex, pageOffset, dataLength, itemCount);
  uint8_t bucket = position % numBuckets_;

  if (pageOffset + dataLength > kPageSize) {
    LOG(ERROR) << "Corrupt ID";
    return FAILURE;
  }

  folly::RWSpinLock::ReadHolder readGuard(&data_[bucket].fetchLock);
  if (data_[bucket].disabled) {
    return FAILURE;
  }

  if (data_[bucket].position != position && data_[bucket].position != 0) {
    LOG(WARNING) << "Tried to fetch data for an expired bucket.";
    return FAILURE;
  }

  if (pageIndex < data_[bucket].pages.size() &&
      data_[bucket].pages[pageIndex]) {
    data.assign(data_[bucket].pages[pageIndex]->data + pageOffset, dataLength);
    return SUCCESS;
  }

  return FAILURE;
}

bool BucketStorage::loadPosition(
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
  auto blocks =
      dataBlockReader_.readBlocks(position, timeSeriesIds, storageIds);
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

void BucketStorage::clearAndDisable() {
  for (int i = 0; i < numBuckets_; i++) {
    std::unique_lock<std::mutex> guard(data_[i].pagesMutex);
    folly::RWSpinLock::WriteHolder writeGuard(data_[i].fetchLock);

    data_[i].disabled = true;
    std::vector<std::shared_ptr<DataBlock>>().swap(data_[i].pages);
    data_[i].activePages = 0;
    data_[i].lastPageBytesUsed = 0;
  }
}

void BucketStorage::enable() {
  for (int i = 0; i < numBuckets_; i++) {
    std::unique_lock<std::mutex> guard(data_[i].pagesMutex);
    folly::RWSpinLock::WriteHolder writeGuard(data_[i].fetchLock);

    data_[i].disabled = false;
    data_[i].activePages = 0;
    data_[i].lastPageBytesUsed = 0;
  }
}

BucketStorage::BucketStorageId BucketStorage::createId(
    uint32_t pageIndex,
    uint32_t pageOffset,
    uint16_t dataLength,
    uint16_t itemCount) {
  // Store all the values in 64 bits.
  BucketStorageId id = 0;

  // Using the first 18 bits.
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
  pageIndex = id >> 46;
  pageOffset = (id >> 30) & (kPageSize - 1);
  dataLength = (id >> 15) & kMaxDataLength;
  itemCount = id & kMaxItemCount;
}

void BucketStorage::finalizeBucket(uint32_t position) {
  std::vector<std::shared_ptr<DataBlock>> pages;
  std::vector<uint32_t> timeSeriesIds;
  std::vector<BucketStorageId> storageIds;
  uint32_t activePages;
  const uint8_t bucket = position % numBuckets_;

  {
    std::lock_guard<std::mutex> guard(data_[bucket].pagesMutex);
    const uint32_t pageIndex = data_[bucket].activePages - 1;

    if (data_[bucket].disabled) {
      LOG(ERROR) << "Trying to finalize a disabled bucket";
      return;
    }

    if (data_[bucket].position != position) {
      LOG(ERROR) << "Trying to finalize an expired bucket";
      return;
    }

    if (data_[bucket].finalized) {
      LOG(ERROR) << "This bucket has already been finalized " << position;
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
    write(position, pages, activePages, timeSeriesIds, storageIds);
  }
}

void BucketStorage::write(
    uint32_t position,
    const std::vector<std::shared_ptr<DataBlock>>& pages,
    uint32_t activePages,
    const std::vector<uint32_t>& timeSeriesIds,
    const std::vector<BucketStorageId>& storageIds) {
  CHECK_EQ(timeSeriesIds.size(), storageIds.size());

  // Delete files older than 24h.
  dataFiles_.remove(position - numBuckets_);
  completeFiles_.remove(position - numBuckets_);

  auto dataFile = dataFiles_.open(position, "wb", kLargeFileBuffer);
  if (!dataFile.file) {
    LOG(ERROR) << "Opening data file failed";
    return;
  }

  uint32_t count = timeSeriesIds.size();
  size_t dataLen = sizeof(uint32_t) + // count
      sizeof(uint32_t) + // active pages
      count * sizeof(uint32_t) + // time series ids
      count * sizeof(uint64_t) + // storage ids
      activePages * kDataBlockSize; // blocks

  std::unique_ptr<char[]> buffer(new char[dataLen]);
  char* ptr = buffer.get();

  memcpy(ptr, &count, sizeof(uint32_t));
  ptr += sizeof(uint32_t);
  memcpy(ptr, &activePages, sizeof(uint32_t));
  ptr += sizeof(uint32_t);

  memcpy(ptr, &timeSeriesIds[0], sizeof(uint32_t) * count);
  ptr += sizeof(uint32_t) * count;

  memcpy(ptr, &storageIds[0], sizeof(uint64_t) * count);
  ptr += sizeof(uint64_t) * count;

  for (int i = 0; i < activePages; i++) {
    memcpy(ptr, pages[i]->data, kDataBlockSize);
    ptr += kDataBlockSize;
  }

  CHECK_EQ(ptr - buffer.get(), dataLen);

  try {
    auto ioBuffer = folly::IOBuf::wrapBuffer(buffer.get(), dataLen);
    auto codec = folly::io::getCodec(
        folly::io::CodecType::ZLIB, folly::io::COMPRESSION_LEVEL_BEST);
    auto compressed = codec->compress(ioBuffer.get());
    compressed->coalesce();

    if (fwrite(
            compressed->data(),
            sizeof(char),
            compressed->length(),
            dataFile.file) != compressed->length()) {
      PLOG(ERROR) << "Writing to data file " << dataFile.name << " failed";
      fclose(dataFile.file);
      return;
    }

    LOG(INFO) << "Wrote compressed data block file. " << dataLen << " -> "
              << compressed->length();

  } catch (std::exception& e) {
    LOG(ERROR) << e.what();
    fclose(dataFile.file);
    return;
  }

  fclose(dataFile.file);

  auto completeFile = completeFiles_.open(position, "wb", 0);
  if (!completeFile.file) {
    LOG(ERROR) << "Opening marker file failed";
    return;
  }
  fclose(completeFile.file);
}

bool BucketStorage::sanityCheck(uint8_t bucket, uint32_t position) {
  if (data_[bucket].disabled) {
    LOG(WARNING) << "Tried to fetch bucket for disabled shard";
    return false;
  }

  if (data_[bucket].position != position) {
    if (data_[bucket].position == 0) {
      // First time this bucket is used for anything. Mark the
      // position.
      data_[bucket].position = position;
    } else {
      LOG(WARNING) << "Tried to fetch expired bucket";
      return false;
    }
  }
  return true;
}

void BucketStorage::deleteBucketsOlderThan(uint32_t position) {
  completeFiles_.clearTo(position);
  dataFiles_.clearTo(position);
}

void BucketStorage::startMonitoring() {
  GorillaStatsManager::addStatExportType(kBlockFileReadFailures, SUM);
  GorillaStatsManager::addStatExportType(kDedupedTimeSeriesSize, SUM);
  GorillaStatsManager::addStatExportType(kDedupedTimeSeriesSize, COUNT);
  GorillaStatsManager::addStatExportType(kWrittenTimeSeriesSize, SUM);
  GorillaStatsManager::addStatExportType(kWrittenTimeSeriesSize, COUNT);
}

std::pair<uint64_t, uint64_t> BucketStorage::getPagesSize() {
  uint64_t activePagesSize = 0;
  uint64_t totalPagesSize = 0;
  for (int i = 0; i < numBuckets_; i++) {
    std::unique_lock<std::mutex> guard(data_[i].pagesMutex);
    activePagesSize += data_[i].activePages * (uint64_t)kDataBlockSize;
    totalPagesSize += data_[i].pages.size() * (uint64_t)kDataBlockSize;
  }
  return std::make_pair(activePagesSize, totalPagesSize);
}
}
} // facebook:gorilla
