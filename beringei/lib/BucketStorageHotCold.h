/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <array>
#include <memory>

#include "BucketStorage.h"

namespace facebook {
namespace gorilla {
// class BucketStorageHotCold
//
// Wrapper around separate BucketStorageSingle instances for hot
// and cold data placed according to the store cold parameter.
class BucketStorageHotCold : public BucketStorage {
 public:
  BucketStorageHotCold(
      uint8_t numBuckets,
      int shardId,
      const std::string& dataDirectory,
      uint8_t numColdMemoryBuckets = kDefaultToNumBuckets);

  virtual ~BucketStorageHotCold() override;

  virtual void createDirectories() override;

  virtual BucketStorageId store(
      uint32_t position,
      const char* data,
      uint16_t dataLength,
      uint16_t itemCount,
      uint32_t timeSeriesId = 0,
      bool cold = false) override;

  virtual FetchStatus fetch(
      uint32_t position,
      BucketStorageId id,
      std::string& data,
      uint16_t& itemCount,
      FetchType* type = nullptr) override;

  virtual std::set<uint32_t> findCompletedPositions() override;

  virtual bool loadPosition(
      uint32_t position,
      std::vector<uint32_t>& timeSeriesIds,
      std::vector<uint64_t>& storageIds) override;

  virtual void clearAndDisable() override;

  virtual void enable() override;

  virtual uint8_t numMemoryBuckets(bool /* unused */) const override;

  virtual void finalizeBucket(uint32_t position) override;

  virtual void deleteBucketsOlderThan(uint32_t position) override;

  virtual std::pair<uint64_t, uint64_t> getPagesSize() override;

 private:
  BucketStorageHotCold(const BucketStorageHotCold&) = delete;
  BucketStorageHotCold& operator=(const BucketStorageHotCold&) = delete;

  static std::string getColdSubdirectory(const std::string& dataDirectory);

  // Call fn with args for both hot and cold BucketStorage objects
  template <typename... Types>
  void fanout(void (BucketStorage::*fn)(Types...), Types... args) {
    (storage_.cold.get()->*fn)(args...);
    // Hot storage last so that its completed implies cold completed
    (storage_.hot.get()->*fn)(args...);
  }

  const struct {
    std::unique_ptr<BucketStorage> hot;
    std::unique_ptr<BucketStorage> cold;
  } storage_;
};

} // namespace gorilla
} // namespace facebook
