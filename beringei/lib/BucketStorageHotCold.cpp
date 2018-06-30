/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <algorithm>
#include <array>
#include <iterator>
#include <sstream>

#include "BucketStorageHotCold.h"

#include "beringei/lib/GorillaStatsManager.h"

#include <folly/synchronization/CallOnce.h>

namespace {
template <typename T>
void append(std::vector<T>& out, const std::vector<T>& extra) {
  out.reserve(out.size() + extra.size());
  std::copy(extra.cbegin(), extra.cend(), std::back_inserter(out));
}

const std::string kColdSubdirectory = "cold";

const std::string kPositionMissingCold = "position_missing_cold";
const std::string kPositionMissingHot = "position_missing_hot";
} // namespace

namespace facebook {
namespace gorilla {

BucketStorageHotCold::BucketStorageHotCold(
    uint8_t numBuckets,
    int shardId,
    const std::string& dataDirectory,
    uint8_t numColdMemoryBuckets)
    : BucketStorage(numBuckets, shardId),
      storage_{std::make_unique<BucketStorageSingle>(
                   numBuckets,
                   shardId,
                   dataDirectory,
                   numBuckets,
                   // Backwards compatible for downgrade
                   DataBlockVersion::V_0),
               std::make_unique<BucketStorageSingle>(
                   numBuckets,
                   shardId,
                   getColdSubdirectory(dataDirectory),
                   numColdMemoryBuckets,
                   // Supports paging
                   DataBlockVersion::V_0_UNCOMPRESSED)} {
  static folly::once_flag flag;
  folly::call_once(flag, [&]() {
    GorillaStatsManager::addStatExportType(kPositionMissingCold, SUM);
    GorillaStatsManager::addStatExportType(kPositionMissingHot, SUM);
  });
}

BucketStorageHotCold::~BucketStorageHotCold() {}

void BucketStorageHotCold::createDirectories() {
  fanout(&BucketStorage::createDirectories);
}

BucketStorage::BucketStorageId BucketStorageHotCold::store(
    uint32_t position,
    const char* data,
    uint16_t dataLength,
    uint16_t itemCount,
    uint32_t timeSeriesId,
    bool cold) {
  return (cold ? storage_.cold : storage_.hot)
      ->store(position, data, dataLength, itemCount, timeSeriesId, cold);
}

BucketStorage::FetchStatus BucketStorageHotCold::fetch(
    uint32_t position,
    BucketStorage::BucketStorageId id,
    std::string& data,
    uint16_t& itemCount,
    FetchType* type) {
  return (coldId(id) ? storage_.cold : storage_.hot)
      ->fetch(position, id, data, itemCount, type);
}

namespace {
template <typename T>
size_t logOneWayDifferences(
    int shardId,
    const T& lhs,
    const T& rhs,
    const std::string& rhsName) {
  std::vector<typename T::value_type> difference;
  std::set_difference(
      lhs.cbegin(),
      lhs.cend(),
      rhs.cbegin(),
      rhs.cend(),
      std::back_insert_iterator<decltype(difference)>(difference));
  if (!difference.empty()) {
    std::string out;
    folly::join(", ", difference, out);
    LOG(WARNING) << "Shard " << shardId << ' ' << rhsName << " missing " << out;
  }
  return difference.size();
}

template <typename T>
void logHotColdDifferences(int shardId, const T& hot, const T& cold) {
  // Expect missing position files in "cold" on migration from configurations
  // without separate cold position files.
  size_t missingCold = logOneWayDifferences(shardId, hot, cold, "cold");
  GorillaStatsManager::addStatValue(kPositionMissingCold, missingCold);

  // Missing position files in "hot" come from process shutdown or
  // BucketStorage::disableAndClear calls due to shard movement between
  // completing cold and hot finalization.
  size_t missingHot = logOneWayDifferences(shardId, cold, hot, "hot");
  GorillaStatsManager::addStatValue(kPositionMissingHot, missingHot);
}
} // namespace

std::set<uint32_t> BucketStorageHotCold::findCompletedPositions() {
  std::set<uint32_t> hot = storage_.hot->findCompletedPositions();
  std::set<uint32_t> cold = storage_.cold->findCompletedPositions();
  logHotColdDifferences(shardId_, hot, cold);
  // Ignore cold entries not in hot because they're not yet written
  //
  // Callers should achieve durability in this situation via a separate
  // write-ahead-log which they need to avoid data loss from
  // BucketStorage persisting the entire position as a single operation
  return hot;
}

bool BucketStorageHotCold::loadPosition(
    uint32_t position,
    std::vector<uint32_t>& timeSeriesIds,
    std::vector<uint64_t>& storageIds) {
  std::vector<uint32_t> coldTimeSeriesIds;
  std::vector<uint64_t> coldStorageIds;
  const struct {
    bool hot;
    bool cold;
  } loaded{
      storage_.hot->loadPosition(position, timeSeriesIds, storageIds),
      storage_.cold->loadPosition(position, coldTimeSeriesIds, coldStorageIds)};

  if (loaded.cold) {
    append(timeSeriesIds, coldTimeSeriesIds);
    append(storageIds, coldStorageIds);
  }

  if (loaded.hot != loaded.cold) {
    std::array<std::string, 2> success = {"failed", "succeeded"};

    // "hot load succeeded cold load failed" is expected on migration from
    // configurations without separate cold position files.
    //
    // "hot load failed cold load succeeded" should only happen
    // by callers not following the normal use case of calling
    // loadPosition on the findCompletedPositions output.
    //
    // Otherwise it suggests a file corruption problem, as does
    // "hot load failed cold load failed"
    LOG(WARNING) << "Shard " << shardId_ << " position " << position
                 << " hot load " << success[loaded.hot] << " cold load "
                 << success[loaded.cold];
  }

  return loaded.hot || loaded.cold;
}

void BucketStorageHotCold::clearAndDisable() {
  fanout(&BucketStorage::clearAndDisable);
}

void BucketStorageHotCold::enable() {
  fanout(&BucketStorage::enable);
}

uint8_t BucketStorageHotCold::numMemoryBuckets(bool cold) const {
  return (cold ? storage_.cold : storage_.hot)->numMemoryBuckets(cold);
}

void BucketStorageHotCold::finalizeBucket(uint32_t position) {
  fanout(&BucketStorage::finalizeBucket, position);
}

void BucketStorageHotCold::deleteBucketsOlderThan(uint32_t position) {
  fanout(&BucketStorage::deleteBucketsOlderThan, position);
}

std::pair<uint64_t, uint64_t> BucketStorageHotCold::getPagesSize() {
  auto hot = storage_.hot->getPagesSize();
  auto cold = storage_.cold->getPagesSize();
  return std::make_pair(hot.first + cold.first, hot.second + cold.second);
}

std::string BucketStorageHotCold::getColdSubdirectory(
    const std::string& dataDirectory) {
  return FileUtils::joinPaths(dataDirectory, kColdSubdirectory);
}
} // namespace gorilla
} // namespace facebook
