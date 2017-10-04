/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <stdint.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "DataBlock.h"
#include "DataBlockReader.h"

#include <folly/RWSpinLock.h>

namespace facebook {
namespace gorilla {

// class BucketStorage
//
// This class stores data for each bucket in 64K blocks. The reason
// for storing data in a single place (or single place for each shard)
// is to avoid the memory overhead and fragmentation that comes from
// allocating millions of ~500 byte blocks.
class BucketStorage {
 public:
  typedef uint64_t BucketStorageId;

  static const BucketStorageId kInvalidId;
  static const BucketStorageId kDisabledId;
  static const uint32_t kPageSize;

  static const std::string kDataPrefix;
  static const std::string kMetadataPrefix;

  // These files are only used as marker files to indicate which
  // blocks have been completed. The files are empty but the file name
  // has the id of the completed block.
  static const std::string kCompletePrefix;

  BucketStorage(
      uint8_t numBuckets,
      int shardId,
      const std::string& dataDirectory);

  virtual ~BucketStorage() {}

  // Stores data.
  //
  // `position` is the bucket position from the beginning before modulo.
  // `data` is the data to be stored.
  // `dataLength` is the length of the data in bytes
  // `itemCount` is the item count that will be returned when fetching data
  //
  // Returns an id that can be used to fetch data later, or kInvalidId if data
  // could not be stored. This can happen if data is tried to be stored for
  // a position that is too old, i.e., more than numBuckets behind the current
  // position.
  BucketStorageId store(
      uint32_t position,
      const char* data,
      uint16_t dataLength,
      uint16_t itemCount,
      uint32_t timeSeriesId = 0);

  enum FetchStatus { SUCCESS, FAILURE };

  // Fetches data.
  //
  // Returns SUCCESS on success and fills `data` and `itemCount`,
  // FAILURE on failure.
  virtual FetchStatus fetch(
      uint32_t position,
      BucketStorageId id,
      std::string& data,
      uint16_t& itemCount);

  // Read all blocks for a given position into memory.
  //
  // Returns true if the position was successfully read from disk and
  // false if it wasn't, due to disk failure or the position being
  // expired or disabled. Fills in timeSeriesIds and storageIds with
  // the metadata associated with the blocks.
  bool loadPosition(
      uint32_t position,
      std::vector<uint32_t>& timeSeriesIds,
      std::vector<uint64_t>& storageIds);

  // This clears and disables the buckets for reads and writes.
  void clearAndDisable();

  // Enables a previously disabled storage.
  void enable();

  uint8_t numBuckets() {
    return numBuckets_;
  }

  static void parseId(
      BucketStorageId id,
      uint32_t& pageIndex,
      uint32_t& pageOffset,
      uint16_t& dataLength,
      uint16_t& itemCount);

  // Finalizes a bucket at the given position. After calling this no
  // more data can be stored in this bucket.
  void finalizeBucket(uint32_t position);

  void deleteBucketsOlderThan(uint32_t position);

  static void startMonitoring();

  // Returns the total size of active and all in-memory pages
  // (active pages size; all pages size)
  std::pair<uint64_t, uint64_t> getPagesSize();

 private:
  BucketStorageId createId(
      uint32_t pageIndex,
      uint32_t pageOffset,
      uint16_t dataLength,
      uint16_t itemCount);

  void write(
      uint32_t position,
      const std::vector<std::shared_ptr<DataBlock>>& pages,
      uint32_t activePages,
      const std::vector<uint32_t>& timeSeriesIds,
      const std::vector<BucketStorageId>& storageIds);

  // Verify that the given position is active and not disabled.
  // Caller must hold the write lock because this can open a new bucket.
  bool sanityCheck(uint8_t bucket, uint32_t position);

  struct BucketData {
    BucketData()
        : activePages(0),
          lastPageBytesUsed(0),
          position(0),
          disabled(false),
          finalized(false) {}

    std::vector<std::shared_ptr<DataBlock>> pages;
    uint32_t activePages;
    uint32_t lastPageBytesUsed;
    uint32_t position;
    bool disabled;
    bool finalized;

    // Two separate vectors for metadata to save memory.
    std::vector<uint32_t> timeSeriesIds;
    std::vector<BucketStorageId> storageIds;

    std::unordered_multimap<uint64_t, uint64_t> storageIdsLookupMap;

    // To control that reads will always work, i.e., allocated pages
    // won't be deleted.
    folly::RWSpinLock fetchLock;

    // To control modifying pages vector and the other data in this struct.
    std::mutex pagesMutex;
  };

  const uint8_t numBuckets_;
  int newestPosition_;
  std::unique_ptr<BucketData[]> data_;
  DataBlockReader dataBlockReader_;

  FileUtils dataFiles_;
  FileUtils completeFiles_;
};
}
} // facebook:gorilla
