/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "beringei/lib/BucketMap.h"

#include <folly/Format.h>
#include <folly/MapUtil.h>

#include "beringei/lib/BucketLogWriter.h"
#include "beringei/lib/BucketStorage.h"
#include "beringei/lib/BucketStorageHotCold.h"
#include "beringei/lib/BucketUtils.h"
#include "beringei/lib/DataLog.h"
#include "beringei/lib/GorillaStatsManager.h"
#include "beringei/lib/GorillaTimeConstants.h"
#include "beringei/lib/LogReader.h"
#include "beringei/lib/TimeSeries.h"

DEFINE_int32(
    data_point_queue_size,
    1000,
    "The size of the qeueue that holds the data points in memory before they "
    "can be handled. This queue is only used when shards are being added.");

DEFINE_int64(
    missing_logs_threshold_secs,
    600, // 10 minute default
    "Count gaps longer than this as holes in the log files.");

DEFINE_bool(check_keys_consistency, false, "Check of consistency in key maps");
DEFINE_int64(
    consistency_check_interval_s,
    120,
    "Interval to check for row consistency");

namespace facebook {
namespace gorilla {

// When performing initial insertion, add this much buffer to the vector
// on each resize.
const int kRowsAtATime = 10000;

static const std::string kMsPerKeyListRead = "ms_per_key_list_read";
static const std::string kMsPerLogFilesRead = "ms_per_log_files_read";
static const std::string kMsPerBlockFileRead = "ms_per_block_file_read";
static const std::string kMsPerQueueProcessing = "ms_per_queue_processing";
static const std::string kDataPointQueueDropped = "data_point_queue_dropped";
static const std::string kCorruptKeyFiles = "corrupt_key_files";
static const std::string kCorruptLogFiles = "corrupt_log_files";
static const std::string kUnknownKeysInLogFiles = "unknown_keys_in_log_files";
static const std::string kUnknownKeysInBlockMetadataFiles =
    "unknown_keys_in_block_metadata_files";
static const std::string kDataHoles = "missing_blocks_and_logs";
static const std::string kMissingLogs = "missing_seconds_of_log_data";
static const std::string kDeletionRaces = "key_deletion_failures";
static const std::string kDuplicateKeys = "duplicate_keys_in_key_list";
static const std::string kDropMissingKeys = "drop_missing_keys";
static const std::string kNumStreamMessages = "num_stream_msg";
static const std::string kNumRowsAddedFromStreaming = "num_rows_from_stream";
static const std::string kNumEvictedKeys = "num_evicted_keys";
static const std::string kNumDeleteMsg = "num_delete_msg";
static const std::string kNumNotReadyDataPoint = "num_not_ready_dp";
static const std::string kNumDuplicateWhileReset = "num_duplicate_while_reset";
static const std::string kInvalidDelete = "num_invalid_delete";
static const std::string kFailedDeleteOldKey = "num_failed_delete_old_key";

static const size_t kMaxAllowedKeyLength = 400;

const int BucketMap::kNotOwned = -1;

DECLARE_int32(max_allowed_timeseries_id);

BucketMap::BucketMap(
    uint8_t buckets,
    uint64_t windowSize,
    int shardId,
    const std::string& dataDirectory,
    std::shared_ptr<KeyListWriter> keyWriter,
    std::shared_ptr<BucketLogWriterIf> logWriter,
    BucketMap::State state,
    std::shared_ptr<LogReaderFactory> logReaderFactory,
    std::shared_ptr<KeyListReaderFactory> keyReaderFactory,
    bool usePrimaryTopology,
    bool enableHotColdBuckets)
    : n_(buckets),
      windowSize_(windowSize),
      reliableDataStartTime_(0),
      lock_(),
      tableSize_(0),
      storage_(
          enableHotColdBuckets ? std::unique_ptr<BucketStorage>(
                                     std::make_unique<BucketStorageHotCold>(
                                         buckets,
                                         shardId,
                                         dataDirectory))
                               : std::unique_ptr<BucketStorage>(
                                     std::make_unique<BucketStorageSingle>(
                                         buckets,
                                         shardId,
                                         dataDirectory))),
      state_(state),
      shardId_(shardId),
      dataDirectory_(dataDirectory),
      keyWriter_(keyWriter),
      logWriter_(logWriter),
      lastFinalizedBucket_(0),
      logReaderFactory_(logReaderFactory),
      keyReaderFactory_(keyReaderFactory),
      primary_(!usePrimaryTopology),
      usePrimaryTopology_(usePrimaryTopology),
      sequence_(0) {
  if (FLAGS_check_keys_consistency) {
    // Periodically check for consistency.
    consistencyCheck_.addFunction(
        [this]() {
          // Only run when it's owned.
          if (state_ == OWNED) {
            CHECK(consistencyCheck());
          }
        },
        std::chrono::seconds(FLAGS_consistency_check_interval_s));
    consistencyCheck_.start();
  }
}

BucketMap::~BucketMap() {
  if (usePrimaryTopology_) {
    stopStreamKeys();
  }
}

void BucketMap::createDirectories() {
  storage_->createDirectories();
}

// Insert the given data point, creating a new row if necessary.
// Returns the number of new rows created (0 or 1) and the number of
// data points successfully inserted (0 or 1) as a pair of ints.
// Returns {kNotOwned,kNotOwned} if this map is currenly not owned.
std::pair<int, int> BucketMap::put(
    const std::string& key,
    const TimeValuePair& value,
    uint16_t category,
    bool skipStateCheck) {
  State state;
  uint32_t id;
  auto existingItem = getInternal(key, state, id);

  if (!existingItem && !primary_.load()) {
    // Drop early if we're not allowed to create new timeseries.
    GorillaStatsManager::addStatValue(kDropMissingKeys);
    return {0, 0};
  }

  // State check can only skipped when processing data points from the
  // queue. Data points that come in externally during processing will
  // still be queued.
  if (skipStateCheck) {
    CHECK_EQ(PROCESSING_QUEUED_DATA_POINTS, state);
  } else {
    switch (state) {
      case UNOWNED:
        return {kNotOwned, kNotOwned};
      case PRE_OWNED:
      case READING_KEYS:
        queueDataPointWithKey(key, value, category);

        // Assume the data point will be added and no new keys will be
        // added. This might not be the case but these return values
        // are only used for counters.
        return {0, 1};
      case READING_KEYS_DONE:
      case READING_LOGS:
      case PROCESSING_QUEUED_DATA_POINTS:
        if (existingItem) {
          queueDataPointWithId(id, value, category);
        } else {
          queueDataPointWithKey(key, value, category);
        }
        return {0, 1};
      case READING_BLOCK_DATA:
      case OWNED:
      case PRE_UNOWNED:
        // Continue normal processing. PRE_UNOWNED is still completely
        // considered to be owned.
        break;

        // No default case to let compiler warn if new states are added
        // without adding a case for them.
    }
  }

  if (existingItem) {
    bool added = putDataPointWithId(&existingItem->second, id, value, category);
    return {0, added ? 1 : 0};
  }

  uint32_t b = bucket(value.unixTime);

  // Prepare a row now to minimize critical section.
  auto newRow = std::make_shared<std::pair<std::string, BucketedTimeSeries>>();
  newRow->first = key;
  newRow->second.reset(n_, b, value.unixTime);
  newRow->second.put(
      b, value, storage_.get(), static_cast<uint32_t>(-1), &category);

  int index = 0;
  bool keyAdded = false;
  {
    // Lock the map again.
    folly::RWSpinLock::WriteHolder writeGuard(lock_);

    // The value here doesn't matter because it will be replaced later.
    auto ret = map_.insert(std::make_pair(newRow->first.c_str(), -1));
    keyAdded = ret.second;

    if (keyAdded) {
      // Find a row in the vector.
      if (freeList_.size()) {
        index = *freeList_.rbegin();
        freeList_.erase(index);
      } else {
        tableSize_++;
        rows_.emplace_back();
        index = rows_.size() - 1;
      }

      // If we don't use primary-secondary, the key is always ready for write,
      // since we don't have to sync it with anyone else.
      if (!usePrimaryTopology_) {
        newRow->second.setReady();
      }

      // If we failed to enqueue to the key writer, remove the key from the map
      // and drop the data point.
      if (!keyWriter_->addKey(
              shardId_, index, newRow->first, category, value.unixTime)) {
        CHECK_GT(map_.erase(newRow->first.c_str()), 0);
        CHECK(freeList_.insert(index).second);
        return {0, 0};
      }

      rows_[index] = newRow;
      ret.first->second = index;
    }
  }

  // The key was just added right before this by other request. It's fine. Retry
  // this request again.
  if (!keyAdded) {
    // Run this call again. This time the key should have been created.
    return put(key, value, category, skipStateCheck);
  }

  // Only log if the timeseries is ready.
  if (rows_[index]->second.ready()) {
    logWriter_->logData(shardId_, index, value.unixTime, value.value);
  } else {
    GorillaStatsManager::addStatValue(kNumNotReadyDataPoint);
  }
  return {1, 1};
}

// Get a shared_ptr to a TimeSeries.
BucketMap::Item BucketMap::get(const std::string& key) {
  State state;
  uint32_t id;
  return getInternal(key, state, id);
}

// Get all the TimeSeries.
void BucketMap::getEverything(std::vector<Item>& out) {
  out.reserve(tableSize_);
  folly::RWSpinLock::ReadHolder guard(lock_);
  out.insert(out.end(), rows_.begin(), rows_.end());
}

bool BucketMap::getSome(std::vector<Item>& out, int offset, int count) {
  out.reserve(count);
  folly::RWSpinLock::ReadHolder guard(lock_);
  if (offset >= rows_.size()) {
    return false;
  } else if (offset + count >= rows_.size()) {
    out.insert(out.end(), rows_.begin() + offset, rows_.end());
    return false;
  } else {
    out.insert(
        out.end(), rows_.begin() + offset, rows_.begin() + offset + count);
    return true;
  }
}

void BucketMap::erase(uint32_t index, const char* key, uint16_t category) {
  if (!primary_.load()) {
    return;
  }

  bool success = false;
  {
    folly::RWSpinLock::WriteHolder guard(lock_);
    success = eraseBasedOnKeyList(index, key);
  }

  if (success) {
    keyWriter_->deleteKey(shardId_, index, key, category);
  }
}

uint32_t BucketMap::bucket(uint64_t unixTime) const {
  return BucketUtils::bucket(unixTime, windowSize_, shardId_);
}

uint64_t BucketMap::timestamp(uint32_t bucket) const {
  return BucketUtils::timestamp(bucket, windowSize_, shardId_);
}

uint64_t BucketMap::duration(uint32_t buckets) const {
  return BucketUtils::duration(buckets, windowSize_);
}

uint32_t BucketMap::buckets(uint64_t duration) const {
  return BucketUtils::buckets(duration, windowSize_);
}

BucketStorage* BucketMap::getStorage() {
  return storage_.get();
}

bool BucketMap::setState(BucketMap::State state) {
  Timer timer(true);

  // If we have to drop a shard, move the data here, then free all the memory
  // outside of any locks, as this can take a long time.
  folly::F14FastMap<const char*, int, CaseHash, CaseEq> tmpMap;
  std::set<size_t> tmpQueue;
  std::vector<Item> tmpVec;
  std::vector<std::vector<uint32_t>> tmpDeviations;

  std::unique_lock<std::mutex> stateGuard(stateChangeMutex_);
  folly::RWSpinLock::WriteHolder guard(lock_);
  if (!isAllowedStateTransition(state_, state)) {
    LOG(WARNING) << "Illegal transition from " << state_ << " to " << state;
    return false;
  }

  if (state == PRE_OWNED) {
    addTimer_.start();

    // Start keywriter and log writer for this shard. Note that even though key
    // writer is started, keys won't be logged unless this replica is primary.
    keyWriter_->startShard(shardId_, true);
    logWriter_->startShard(shardId_);

    dataPointQueue_ = std::make_shared<folly::MPMCQueue<QueuedDataPoint>>(
        FLAGS_data_point_queue_size);

    // Deviations are indexed per minute.
    deviations_.resize(duration(n_) / kGorillaSecondsPerMinute);
  } else if (state == UNOWNED) {
    tmpMap.swap(map_);
    tmpQueue.swap(freeList_);
    tmpVec.swap(rows_);
    tmpDeviations.swap(deviations_);
    tableSize_ = 0;

    // These operations do block, but only to enqueue flags, not drain the
    // queues to disk.
    keyWriter_->stopShard(shardId_);
    logWriter_->stopShard(shardId_);
  } else if (state == OWNED) {
    // Calling this won't hurt even if the timer isn't running.
    addTimer_.stop();

    if (!primary_.load()) {
      startStreamKeys();
    }
  }

  BucketMap::State oldState = state_;
  state_ = state;
  guard.reset();

  // Enable/disable storage outside the lock because it might take a
  // while and the the storage object has its own locking.
  if (state == PRE_OWNED) {
    storage_->enable();
  } else if (state == UNOWNED) {
    storage_->clearAndDisable();
  }

  LOG(INFO) << "Changed state of shard " << shardId_ << " from " << oldState
            << " to " << state << " in " << timer.get() << "us";

  return true;
}

BucketMap::State BucketMap::getState() const {
  folly::RWSpinLock::ReadHolder guard(lock_);
  return state_;
}

Timer::TimeVal BucketMap::getAddTime() {
  return addTimer_.get() / kGorillaUsecPerMs;
}

bool BucketMap::cancelUnowning() {
  folly::RWSpinLock::WriteHolder guard(lock_);
  if (state_ != PRE_UNOWNED) {
    return false;
  }

  state_ = OWNED;
  return true;
}

bool BucketMap::isAllowedStateTransition(State from, State to) {
  return to > from || (from == OWNED && to == PRE_UNOWNED);
}

int BucketMap::finalizeBuckets(uint32_t lastBucketToFinalize) {
  if (getState() != OWNED) {
    return 0;
  }

  // This code assumes that only one thread will be calling this at a
  // time. If this isn't the case anymore, locks need to be added.
  uint32_t bucketToFinalize;
  if (lastFinalizedBucket_ == 0) {
    bucketToFinalize = lastBucketToFinalize;
  } else {
    bucketToFinalize = lastFinalizedBucket_ + 1;
  }

  if (bucketToFinalize <= lastFinalizedBucket_ ||
      bucketToFinalize > lastBucketToFinalize) {
    return 0;
  }

  // There might be more than one bucket to finalize if the server was
  // restarted or shards moved.
  int bucketsToFinalize = lastBucketToFinalize - bucketToFinalize + 1;
  std::vector<BucketMap::Item> timeSeriesData;
  getEverything(timeSeriesData);

  for (uint32_t bucket = bucketToFinalize; bucket <= lastBucketToFinalize;
       bucket++) {
    for (int i = 0; i < timeSeriesData.size(); i++) {
      if (timeSeriesData[i].get()) {
        timeSeriesData[i]->second.setCurrentBucket(
            bucket + 1,
            getStorage(),
            i); // `i` is the id of the time series
      }
    }

    getStorage()->finalizeBucket(bucket);
  }

  lastFinalizedBucket_ = lastBucketToFinalize;
  return bucketsToFinalize;
}

bool BucketMap::isBehind(uint32_t bucketToFinalize) const {
  return getState() == OWNED && lastFinalizedBucket_ != 0 &&
      bucketToFinalize > lastFinalizedBucket_ + 1;
}

void BucketMap::shutdown() {
  if (getState() == OWNED) {
    logWriter_->stopShard(shardId_);
    keyWriter_->stopShard(shardId_, true);

    // Set the state directly without calling setState which would try
    // to deallocate memory.
    std::unique_lock<std::mutex> stateGuard(stateChangeMutex_);
    folly::RWSpinLock::WriteHolder guard(lock_);
    state_ = UNOWNED;
  }
}

void BucketMap::compactKeyList(bool force) {
  if (usePrimaryTopology_ && !force) {
    // If we use primary secondary architecture, there should be a separate
    // service that does the compacting the keylist based on synchronized key
    // log.
    return;
  }
  std::vector<Item> items;
  getEverything(items);

  uint32_t i = static_cast<uint32_t>(-1);
  keyWriter_->compact(shardId_, [&]() {
    for (i++; i < items.size(); i++) {
      // Only checkpoint keys that are ready.
      if (items[i].get() && items[i]->second.ready()) {
        return std::make_tuple(
            i,
            items[i]->first.c_str(),
            items[i]->second.getCategory(),
            items[i]->second.getFirstUpdateTime(getStorage(), *this));
      }
    }
    return std::make_tuple<uint32_t, const char*, uint16_t, int32_t>(
        0, nullptr, 0, 0);
  });
}

void BucketMap::deleteOldBlockFiles() {
  // Start far enough back that we can't possibly interfere with anything.
  storage_->deleteBucketsOlderThan(bucket(time(nullptr)) - n_ - 1);
}

void BucketMap::startMonitoring() {
  GorillaStatsManager::addStatExportType(kMsPerKeyListRead, AVG);
  GorillaStatsManager::addStatExportType(kMsPerLogFilesRead, AVG);
  GorillaStatsManager::addStatExportType(kMsPerBlockFileRead, AVG);
  GorillaStatsManager::addStatExportType(kMsPerBlockFileRead, COUNT);
  GorillaStatsManager::addStatExportType(kMsPerQueueProcessing, AVG);
  GorillaStatsManager::addStatExportType(kDataPointQueueDropped, SUM);
  GorillaStatsManager::addStatExportType(kCorruptLogFiles, SUM);
  GorillaStatsManager::addStatExportType(kCorruptKeyFiles, SUM);
  GorillaStatsManager::addStatExportType(kUnknownKeysInLogFiles, SUM);
  GorillaStatsManager::addStatExportType(kUnknownKeysInBlockMetadataFiles, SUM);
  GorillaStatsManager::addStatExportType(kDataHoles, SUM);
  GorillaStatsManager::addStatExportType(kMissingLogs, SUM);
  GorillaStatsManager::addStatExportType(kMissingLogs, AVG);
  GorillaStatsManager::addStatExportType(kMissingLogs, COUNT);
  GorillaStatsManager::addStatExportType(kDeletionRaces, SUM);
  GorillaStatsManager::addStatExportType(kDuplicateKeys, SUM);
  GorillaStatsManager::addStatExportType(kDropMissingKeys, COUNT);
  GorillaStatsManager::addStatExportType(kNumStreamMessages, COUNT);
  GorillaStatsManager::addStatExportType(kNumRowsAddedFromStreaming, COUNT);
  GorillaStatsManager::addStatExportType(kNumEvictedKeys, COUNT);
  GorillaStatsManager::addStatExportType(kNumDeleteMsg, COUNT);
  GorillaStatsManager::addStatExportType(kNumNotReadyDataPoint, COUNT);
  GorillaStatsManager::addStatExportType(kNumDuplicateWhileReset, COUNT);
  GorillaStatsManager::addStatExportType(kInvalidDelete, COUNT);
  GorillaStatsManager::addStatExportType(kFailedDeleteOldKey, COUNT);
}

BucketMap::Item
BucketMap::getInternal(const std::string& key, State& state, uint32_t& id) {
  folly::RWSpinLock::ReadHolder guard(lock_);

  state = state_;
  if (state_ >= UNOWNED && state_ <= READING_KEYS) {
    // Either the state is UNOWNED or keys are being read. In both
    // cases do not try to find the key.
    return nullptr;
  }

  const auto& it = map_.find(key.c_str());
  if (it != map_.end()) {
    id = it->second;
    return rows_[id];
  }

  return nullptr;
}

void BucketMap::readData() {
  bool success = setState(READING_LOGS);
  CHECK(success) << "Setting state failed";

  Timer timer(true);

  {
    std::unique_lock<std::mutex> guard(unreadBlockFilesMutex_);
    int missingFiles = 0;
    try {
      unreadBlockFiles_ = storage_->findCompletedPositions();
      if (unreadBlockFiles_.size() > 0) {
        missingFiles = checkForMissingBlockFiles();
        lastFinalizedBucket_ = *unreadBlockFiles_.rbegin();
      }
    } catch (std::exception& e) {
      LOG(ERROR) << "Failed listing completed block files for shard "
                 << shardId_ << " : " << e.what();
      missingFiles = n_;
    }
    if (missingFiles > 0) {
      logMissingBlockFiles(missingFiles);
    }
  }

  readLogFiles(lastFinalizedBucket_);
  GorillaStatsManager::addStatValue(
      kMsPerLogFilesRead, timer.reset() / kGorillaUsecPerMs);
  CHECK_EQ(getState(), READING_LOGS);

  success = setState(PROCESSING_QUEUED_DATA_POINTS);
  CHECK(success);

  // Skip state check when processing queued data points.
  processQueuedDataPoints(true);

  // There's a tiny chance that incoming data points will think that
  // the state is PROCESSING_QUEUED_DATA_POINTS and they will be
  // queued after the second call to processQueuedDataPoints.
  success = setState(READING_BLOCK_DATA);
  CHECK(success);

  // Process queued data points again, just to be sure that the queue
  // is empty because it is possible that something was inserted into
  // the queue after it was emptied and before the state was set to
  // READING_BLOCK_DATA.
  processQueuedDataPoints(false);
  GorillaStatsManager::addStatValue(
      kMsPerQueueProcessing, timer.reset() / kGorillaUsecPerMs);

  // Take a copy of the shared pointer to avoid freeing the memory
  // while holding the write lock. Not the most elegant solution but it
  // guarantees that freeing memory won't block anything else.
  std::shared_ptr<folly::MPMCQueue<QueuedDataPoint>> copy;
  {
    folly::RWSpinLock::WriteHolder guard(lock_);
    copy = dataPointQueue_;
    dataPointQueue_.reset();
  }

  // Probably not needed because this object will fall out of scope,
  // but I am afraid of compiler optimizations that might end up
  // freeing the memory inside the write lock.
  copy.reset();
}

bool BucketMap::readBlockFiles() {
  uint32_t position;
  {
    std::unique_lock<std::mutex> guard(unreadBlockFilesMutex_);
    if (unreadBlockFiles_.empty()) {
      bool success = setState(OWNED);
      CHECK(success);
      // Done reading block files.
      return false;
    }

    position = *unreadBlockFiles_.rbegin();
    unreadBlockFiles_.erase(position);
  }

  std::vector<uint32_t> timeSeriesIds;
  std::vector<uint64_t> storageIds;

  LOG(INFO) << "Reading blockfiles for shard " << shardId_ << ": " << position;
  Timer timer(true);
  if (storage_->loadPosition(position, timeSeriesIds, storageIds)) {
    folly::RWSpinLock::ReadHolder guard(lock_);

    for (int i = 0; i < timeSeriesIds.size(); i++) {
      if (timeSeriesIds[i] < rows_.size() && rows_[timeSeriesIds[i]].get()) {
        DCHECK_LT(i, storageIds.size());
        rows_[timeSeriesIds[i]]->second.setDataBlock(
            position, storage_.get(), storageIds[i]);
      } else {
        GorillaStatsManager::addStatValue(kUnknownKeysInBlockMetadataFiles);
      }
    }

    GorillaStatsManager::addStatValue(
        kMsPerBlockFileRead, timer.reset() / kGorillaUsecPerMs);
    LOG(INFO) << "Done reading blockfiles for shard " << shardId_ << ": "
              << position;
  } else {
    // This could just be because we've read the data before, but that shouldn't
    // happen (it gets cleared on shard drop). Bump the counter anyway.
    LOG(ERROR) << "Failed to read blockfiles for shard " << shardId_ << ": "
               << position << ". Already loaded?";
  }

  return true;
}

bool BucketMap::eraseBasedOnKeyList(uint32_t id, const char* key) {
  if (id >= rows_.size()) {
    return false;
  }

  if (!rows_[id]) {
    GorillaStatsManager::addStatValue(kInvalidDelete);

    // Trying to delete an non-existing row. This row is probably already
    // deleted earlier.
    LOG(ERROR) << folly::sformat(
        "Shard: {}. Trying to delete non-existing row: {}, with key: {}",
        shardId_,
        id,
        key);
    return false;
  }

  auto& currentKey = rows_[id]->first;
  if (!CaseEq()(currentKey.c_str(), key)) {
    GorillaStatsManager::addStatValue(kInvalidDelete);

    // For some reason, trying to delete a different key that is supposed to
    // be in this slot.
    LOG(ERROR) << folly::sformat(
        "Shard: {}. Trying to delete a different key in row: {},"
        " current key: {}, deleting key: {}",
        shardId_,
        id,
        currentKey,
        key);
    return false;
  }

  // Delete the key from the map also.
  // Note: since the key to {map_} is a pointer to the string in {rows_}, we
  // **need** to remove from the map first before attemping to deallocate from
  // rows_.
  auto it = map_.find(key);
  if (it != map_.end()) {
    map_.erase(it);
  }

  // We're safe to delete key at this point.
  rows_[id].reset();
  freeList_.insert(id);

  GorillaStatsManager::addStatValue(kNumDeleteMsg);
  return true;
}

void BucketMap::readKeyList() {
  LOG(INFO) << "Reading keys for shard " << shardId_;
  Timer timer(true);

  if (state_ == PRE_OWNED) {
    bool success = setState(READING_KEYS);
    CHECK(success) << "Setting state failed";
  }

  // No reason to lock because nothing is touching the rows_ or map_
  // while this is running.

  // Read all the keys from disk into the vector.
  auto keyReader = keyReaderFactory_->getKeyReader(shardId_, dataDirectory_);
  ssize_t numKeys = keyReader->readKeys([&](uint32_t id,
                                            const char* key,
                                            uint16_t category,
                                            int32_t timestamp,
                                            bool isAppend,
                                            uint64_t seq) {
    sequence_ = std::max(sequence_, seq);
    if (strlen(key) >= kMaxAllowedKeyLength) {
      LOG(ERROR) << "Key too long. Key file is corrupt for shard " << shardId_;
      GorillaStatsManager::addStatValue(kCorruptKeyFiles);

      // Don't continue reading from this file anymore.
      return false;
    }

    if (id > FLAGS_max_allowed_timeseries_id) {
      LOG(ERROR) << "ID is too large. Key file is corrupt for shard "
                 << shardId_;
      GorillaStatsManager::addStatValue(kCorruptKeyFiles);

      // Don't continue reading from this file anymore.
      return false;
    }

    if (id >= rows_.size()) {
      resizeRows(id + kRowsAtATime);
    }

    if (isAppend) {
      insertBasedOnKeyList(id, key, category, timestamp);
    } else {
      eraseBasedOnKeyList(id, key);
    }
    return true;
  });

  if (numKeys < 0) {
    // Don't change the state if we failed to read key list. This would force to
    // try reading key list again.
    map_.clear();
    rows_.clear();
    freeList_.clear();
    tableSize_ = rows_.size();
    return;
  }

  tableSize_ = rows_.size();
  LOG(INFO) << folly::sformat("Shard: {}. Done reading keys!", shardId_);
  GorillaStatsManager::addStatValue(
      kMsPerKeyListRead, timer.reset() / kGorillaUsecPerMs);

  bool success = setState(READING_KEYS_DONE);
  CHECK(success) << "Setting state failed";
}

void BucketMap::readLogFiles(uint32_t lastBlock) {
  LOG(INFO) << "Reading logs for shard " << shardId_;
  auto ingestData = [this](
                        uint32_t key,
                        int64_t unixTime,
                        double value,
                        uint32_t& unknownKeys,
                        int64_t& lastTimestamp) {
    folly::RWSpinLock::ReadHolder guard(lock_);
    if (key < rows_.size() && rows_[key].get()) {
      TimeValuePair tv;
      tv.unixTime = unixTime;
      tv.value = value;
      rows_[key]->second.put(
          bucket(unixTime), tv, storage_.get(), key, nullptr);
    } else {
      unknownKeys++;
    }

    int64_t gap = unixTime - lastTimestamp;
    if (gap > FLAGS_missing_logs_threshold_secs &&
        lastTimestamp > timestamp(1)) {
      LOG(ERROR) << folly::sformat(
          "Shard: {}. {} seconds of missing logs from {} to {}.",
          shardId_,
          gap,
          lastTimestamp,
          unixTime);
      GorillaStatsManager::addStatValue(kDataHoles, 1);
      GorillaStatsManager::addStatValue(kMissingLogs, gap);
      reliableDataStartTime_ = unixTime;
    }
    lastTimestamp = std::max(lastTimestamp, unixTime);
  };

  uint32_t unknownKeys = 0;
  int64_t lastTimestamp = timestamp(lastBlock + 1);
  auto logReader = logReaderFactory_->getLogReader(
      shardId_, windowSize_, std::move(ingestData));
  logReader->readLog(lastBlock, lastTimestamp, unknownKeys);

  int64_t now = time(nullptr);
  int64_t gap = now - lastTimestamp;
  if (gap > FLAGS_missing_logs_threshold_secs && lastTimestamp > timestamp(1)) {
    LOG(ERROR) << folly::sformat(
        "Shard: {}. {} seconds of missing logs from {} to now ({}).",
        shardId_,
        gap,
        lastTimestamp,
        now);
    GorillaStatsManager::addStatValue(kDataHoles, 1);
    GorillaStatsManager::addStatValue(kMissingLogs, gap);
    reliableDataStartTime_ = now;
  }

  LOG(INFO) << folly::sformat("Shard: {}. Done reading logs.", shardId_);
  LOG(INFO) << folly::sformat(
      "Shard: {}. {} unknown keys found.", shardId_, unknownKeys);
  GorillaStatsManager::addStatValue(kUnknownKeysInLogFiles, unknownKeys);
}

void BucketMap::queueDataPointWithKey(
    const std::string& key,
    const TimeValuePair& value,
    uint16_t category) {
  if (key == "") {
    LOG(WARNING) << "Not queueing with empty key";
    return;
  }

  QueuedDataPoint dp;
  dp.key = key;
  dp.unixTime = value.unixTime;
  dp.value = value.value;
  dp.category = category;

  queueDataPoint(dp);
}

void BucketMap::queueDataPointWithId(
    uint32_t id,
    const TimeValuePair& value,
    uint16_t category) {
  QueuedDataPoint dp;

  // Leave key string empty to indicate that timeSeriesId is used.
  dp.timeSeriesId = id;
  dp.unixTime = value.unixTime;
  dp.value = value.value;
  dp.category = category;

  queueDataPoint(dp);
}

void BucketMap::queueDataPoint(QueuedDataPoint& dp) {
  std::shared_ptr<folly::MPMCQueue<QueuedDataPoint>> queue;
  {
    folly::RWSpinLock::ReadHolder guard(lock_);
    queue = dataPointQueue_;
  }

  if (!queue) {
    LOG(ERROR) << "Queue was deleted!";
    GorillaStatsManager::addStatValue(kDataPointQueueDropped);
    reliableDataStartTime_ = time(nullptr);
    return;
  }

  if (!queue->write(std::move(dp))) {
    GorillaStatsManager::addStatValue(kDataPointQueueDropped);
    reliableDataStartTime_ = time(nullptr);
  }
}

void BucketMap::processQueuedDataPoints(bool skipStateCheck) {
  std::shared_ptr<folly::MPMCQueue<QueuedDataPoint>> queue;

  {
    // Take a copy of the shared pointer for the queue. Even if this
    // shard is let go while processing the queue, nothing will cause
    // a segfault and the data points are just skipped.
    folly::RWSpinLock::ReadHolder guard(lock_);
    queue = dataPointQueue_;
  }

  if (!queue) {
    LOG(WARNING) << "Could not process data points. The queue was deleted!";
    return;
  }

  QueuedDataPoint dp;
  while (queue->read(dp)) {
    TimeValuePair value;
    value.unixTime = dp.unixTime;
    value.value = dp.value;

    if (dp.key.length() == 0) {
      // Time series id is known. It's possbible to take a few
      // shortcuts to make adding the data point faster.

      Item item;
      State state;
      {
        folly::RWSpinLock::ReadHolder guard(lock_);
        CHECK_LT(dp.timeSeriesId, rows_.size());
        item = rows_[dp.timeSeriesId];
        state = state_;
      }

      if (!skipStateCheck && state != OWNED && state != PRE_UNOWNED) {
        // Extremely rare corner case. We just set the state to owned
        // and the queue should be really tiny or empty but still
        // state was changed.
        continue;
      }

      putDataPointWithId(&item->second, dp.timeSeriesId, value, dp.category);
    } else {
      // Run these through the normal workflow.
      put(dp.key, value, dp.category, skipStateCheck);
    }
  }
}

bool BucketMap::putDataPointWithId(
    BucketedTimeSeries* timeSeries,
    uint32_t timeSeriesId,
    const TimeValuePair& value,
    uint16_t category) {
  uint32_t b = bucket(value.unixTime);
  bool added =
      timeSeries->put(b, value, storage_.get(), timeSeriesId, &category);
  if (added) {
    logWriter_->logData(shardId_, timeSeriesId, value.unixTime, value.value);
  }
  return added;
}

int64_t BucketMap::getReliableDataStartTime() {
  return reliableDataStartTime_;
}

int BucketMap::getShardId() const {
  return shardId_;
}

int BucketMap::checkForMissingBlockFiles() {
  // Just look for holes in the progression of files.
  // Gaps between log and block files will be checked elsewhere.

  int missingFiles = 0;
  for (auto it = unreadBlockFiles_.begin();
       std::next(it) != unreadBlockFiles_.end();
       it++) {
    if (*it + 1 != *std::next(it)) {
      missingFiles++;
    }
  }
  return missingFiles;
}

void BucketMap::logMissingBlockFiles(int missingFiles) {
  uint32_t now = bucket(time(nullptr));

  std::stringstream error;
  error << missingFiles << " completed block files are missing. Got blocks";
  for (uint32_t id : unreadBlockFiles_) {
    error << " " << id;
  }
  error << ". Expected blocks in range [" << now - n_ << ", " << now - 1 << "]"
        << " for shard " << shardId_;

  LOG(ERROR) << error.str();
  GorillaStatsManager::addStatValue(kDataHoles, missingFiles);
  reliableDataStartTime_ = time(nullptr);
}

int BucketMap::indexDeviatingTimeSeries(
    uint32_t deviationStartTime,
    uint32_t indexingStartTime,
    uint32_t endTime,
    double minimumSigma) {
  if (getState() != OWNED) {
    return 0;
  }

  int totalMinutes = duration(n_) / kGorillaSecondsPerMinute;

  CHECK_EQ(totalMinutes, deviations_.size());

  uint32_t begin = bucket(deviationStartTime);
  uint32_t end = bucket(endTime);

  std::vector<Item> timeSeriesData;
  getEverything(timeSeriesData);

  // Low estimate for the number of time series that have a deviation
  // to avoid constant reallocation.
  int initialSize = timeSeriesData.size() / pow(10, minimumSigma);
  std::vector<std::vector<uint32_t>> deviations(totalMinutes);
  for (int i = indexingStartTime; i <= endTime; i += kGorillaSecondsPerMinute) {
    deviations[i / kGorillaSecondsPerMinute % totalMinutes].reserve(
        initialSize);
  }

  for (int i = 0; i < timeSeriesData.size(); i++) {
    auto& timeSeries = timeSeriesData[i];
    if (!timeSeries.get()) {
      continue;
    }

    std::vector<TimeSeriesBlock> out;
    timeSeries->second.get(begin, end, out, getStorage());
    std::vector<TimeValuePair> values;
    for (auto& block : out) {
      TimeSeries::getValues(block, values, deviationStartTime, endTime);
    }

    if (values.size() == 0) {
      continue;
    }

    // Calculate the mean and standard deviation.
    double sum = 0;
    for (auto& v : values) {
      sum += v.value;
    }

    double avg = sum / values.size();
    double variance = 0.0;
    for (auto& value : values) {
      variance += (value.value - avg) * (value.value - avg);
    }
    variance /= values.size();

    if (variance == 0) {
      continue;
    }

    // Index values that are over the limit.
    double stddev = std::sqrt(variance);
    double limit = minimumSigma * stddev;
    for (auto& v : values) {
      if (v.unixTime >= indexingStartTime && v.unixTime <= endTime &&
          fabs(v.value - avg) >= limit) {
        uint32_t time = (v.unixTime / kGorillaSecondsPerMinute) % totalMinutes;
        deviations[time].push_back(i);
      }
    }
  }

  folly::RWSpinLock::WriteHolder guard(lock_);
  if (state_ != OWNED) {
    guard.reset();
    LOG(WARNING) << "Shard " << shardId_
                 << " ownership change while indexing deviations.";
    return 0;
  }
  int deviationsIndexed = 0;
  for (int i = indexingStartTime; i <= endTime; i += kGorillaSecondsPerMinute) {
    int pos = i / kGorillaSecondsPerMinute % totalMinutes;
    deviationsIndexed += deviations[pos].size();
    deviations_[pos] = std::move(deviations[pos]);
  }

  return deviationsIndexed;
}

std::vector<BucketMap::Item> BucketMap::getDeviatingTimeSeries(
    uint32_t unixTime) {
  if (getState() != OWNED) {
    return {};
  }

  int totalMinutes = duration(n_) / kGorillaSecondsPerMinute;
  CHECK_EQ(totalMinutes, deviations_.size());

  std::vector<BucketMap::Item> deviations;
  int time = unixTime / kGorillaSecondsPerMinute % totalMinutes;

  folly::RWSpinLock::ReadHolder guard(lock_);
  deviations.reserve(deviations_.size());
  for (auto& row : deviations_[time]) {
    if (row < rows_.size()) {
      deviations.push_back(rows_[row]);
    }
  }

  return deviations;
}

bool BucketMap::setRole(bool primary) {
  if (!usePrimaryTopology_) {
    VLOG(1) << folly::sformat(
        "Shard: {}: Not using primary topology, always primary", shardId_);
    // If not using primary-secondary topology, all regions are primary.
    CHECK(primary_);
    return true;
  }

  bool changed = primary_ != primary;

  if (state_ == OWNED) {
    if (primary) {
      keyWriter_->startShard(shardId_, true);
      stopStreamKeys();
    } else {
      keyWriter_->stopShard(shardId_, true);
      if (changed) {
        Timer timer(true);
        clearNotReadyRows();
        auto duration = timer.get();
        LOG(INFO) << folly::sformat(
            "Shard: {}. Clearing not ready row takes {} us",
            shardId_,
            duration);
      }
      startStreamKeys();
    }
  }

  primary_ = primary;
  LOG_IF(INFO, changed && state_ == OWNED) << folly::sformat(
      "Shard: {}: Setting role from {} to {}", shardId_, !primary, primary);

  return changed;
}

bool BucketMap::getRole() const {
  return primary_.load();
}

bool BucketMap::shouldUseKeyWriter() const {
  return primary_.load();
}

BucketMap::Item BucketMap::createNewRow(
    const char* key,
    uint16_t category,
    int64_t unixTime) const {
  auto b = bucket(unixTime);
  auto newRow = std::make_shared<std::pair<std::string, BucketedTimeSeries>>();
  newRow->first = key;
  newRow->second.reset(n_, b, unixTime);
  newRow->second.setCategory(category);
  return newRow;
}

void BucketMap::startStreamKeys() {
  CHECK(usePrimaryTopology_)
      << "Non-primary topology shouldn't use key streaming";
  SYNCHRONIZED(streamer_) {
    if (streamer_.started) {
      CHECK(streamer_.marker.load());
      return;
    }

    streamer_.marker.store(true);
    streamer_.readingThread = std::thread([&]() {
      if (FLAGS_check_keys_consistency) {
        CHECK(consistencyCheck());
      }

      auto reader = keyReaderFactory_->getKeyReader(shardId_, dataDirectory_);
      auto cb = [&](uint32_t id,
                    const char* key,
                    uint16_t category,
                    int32_t unixTime,
                    bool isAppend,
                    uint64_t sequence) -> bool {
        VLOG(2) << folly::sformat(
            "Shard: {}. Streaming key: {}", shardId_, key);
        return keyStreamCallback(
            id, key, category, unixTime, isAppend, sequence);
      };

      // Start streaming keys.
      LOG(INFO) << folly::sformat(
          "Shard: {}. Start streaming keys from sequence number: {}",
          shardId_,
          sequence_);
      reader->streamKeys(cb, streamer_.marker, sequence_);
    });

    streamer_.started = true;
  }
}

void BucketMap::stopStreamKeys() {
  CHECK(usePrimaryTopology_)
      << "Non-primary topology shouldn't use key streaming";

  VLOG(1) << folly::sformat("Shard: {}. Stop streaming keys.", shardId_);

  SYNCHRONIZED(streamer_) {
    if (!streamer_.started) {
      CHECK(!streamer_.marker.load());
      return;
    }
    streamer_.marker.store(false);
    if (streamer_.readingThread.joinable()) {
      streamer_.readingThread.join();
    }
    streamer_.started = false;
    LOG(INFO) << folly::sformat(
        "Shard: {}. End streaming keys from sequence number: {}",
        shardId_,
        sequence_);
  }

  if (FLAGS_check_keys_consistency) {
    CHECK(consistencyCheck());
  }
}

void BucketMap::markTimeSeriesReady(
    uint32_t id,
    const char* key,
    uint64_t seq) {
  sequence_ = seq;

  folly::RWSpinLock::ReadHolder readGuard(lock_);
  if (id >= rows_.size()) {
    return;
  }

  auto row = rows_[id];
  if (!row) {
    return;
  }

  if (!CaseEq()(key, row->first.c_str())) {
    return;
  }

  row->second.setReady();
}

bool BucketMap::keyStreamCallback(
    uint32_t id,
    const char* key,
    uint16_t category,
    int32_t unixTime,
    bool isAppend,
    uint64_t sequence) {
  sequence_ = std::max(sequence_, sequence);
  folly::RWSpinLock::WriteHolder writeGuard(lock_);
  if (isAppend) {
    insertBasedOnKeyList(id, key, category, unixTime);
  } else {
    eraseBasedOnKeyList(id, key);
  }
  return true;
}

void BucketMap::insertBasedOnKeyList(
    uint32_t id,
    const char* key,
    uint16_t category,
    int32_t unixTime) {
  GorillaStatsManager::addStatValue(kNumStreamMessages);
  if (id >= rows_.size()) {
    resizeRows(id + kRowsAtATime);
  }

  auto row = rows_[id];
  if (row) {
    if (!CaseEq()(key, row->first.c_str())) {
      // Eviction. Must delete from the map first!
      auto it = map_.find(row->first.c_str());
      CHECK(it != map_.end());
      CHECK_EQ(it->second, id);

      // Only mark evicted if this TS is ready.
      if (row->second.ready()) {
        LOG(ERROR) << folly::sformat(
            "Shard: {}. Evicting key: {} at id: {}, for new key: {}",
            shardId_,
            row->first,
            id,
            key);
        GorillaStatsManager::addStatValue(kNumEvictedKeys);
      }

      map_.erase(it);
      rows_[id].reset();
      freeList_.insert(id);
    } else {
      VLOG(1) << folly::sformat(
          "Shard: {}. Key {} already exists!", shardId_, key);
      // Received this key from streaming means it's ready.
      row->second.setReady();
      return;
    }
  }

  // rows_[id] is null, which is good. check for if the key already exists in
  // the map.
  auto it = map_.find(key);
  if (it != map_.end()) {
    // This TS existed somewhere else. We just need to move the row to the
    // right slot.
    auto prevId = it->second;
    CHECK(CaseEq()(rows_[prevId]->first.c_str(), key));

    // Update the map.
    it->second = id;

    // Move the row.
    rows_[id] = rows_[prevId];
    rows_[prevId].reset();

    // Update the freelist accordingly.
    freeList_.insert(prevId);
    freeList_.erase(id);

    // Set ready.
    rows_[id]->second.setReady();
    return;
  }

  // Now rows_[id] is free AND there is no key in the map.
  CHECK(rows_[id] == nullptr);
  CHECK(map_.find(key) == map_.end());

  auto newRow = createNewRow(key, category, unixTime);
  newRow->second.setReady();
  rows_[id] = newRow;
  freeList_.erase(id);

  // This call should succeed.
  CHECK(map_.emplace(newRow->first.c_str(), id).second);
}

bool BucketMap::consistencyCheck() const {
  folly::RWSpinLock::ReadHolder lock(lock_);
  for (size_t i = 0; i < rows_.size(); ++i) {
    auto row = rows_[i];
    if (row) {
      auto it = map_.find(row->first.c_str());
      if (it == map_.end()) {
        LOG(ERROR) << folly::sformat(
            "Shard: {}. Can't find key {} in map", shardId_, row->first);
        return false;
      } else if (it->second != i) {
        LOG(ERROR) << folly::sformat(
            "Shard: {}. Index in map is different: {} vs {}",
            shardId_,
            it->second,
            i);
        return false;
      } else if (freeList_.count(i) > 0) {
        LOG(ERROR) << folly::sformat(
            "Shard: {}. Free list has {}, even though it's not free",
            shardId_,
            i);
        return false;
      }
    } else {
      if (freeList_.count(i) == 0) {
        LOG(ERROR) << folly::sformat(
            "Shard: {}. Free list doesn't have {}, even though it's free",
            shardId_,
            i);
        return false;
      }
    }
  }

  for (const auto& kv : map_) {
    auto key = kv.first;
    auto id = kv.second;
    if (id >= rows_.size()) {
      LOG(ERROR) << folly::sformat(
          "Shard: {}. ID is too large: {}", shardId_, id);
      return false;
    } else if (!rows_[id]) {
      LOG(ERROR) << folly::sformat(
          "Shard: {}. ID is not in rows: {}", shardId_, id);
      return false;
    } else if (!CaseEq()(key, rows_[id]->first.c_str())) {
      LOG(ERROR) << folly::sformat(
          "Shard: {}. Keys are different: {} vs {}",
          shardId_,
          key,
          rows_[id]->first);
      return false;
    } else if (freeList_.count(id) > 0) {
      LOG(ERROR) << folly::sformat(
          "Shard: {}. Freelist has {}, even though it's in the map",
          shardId_,
          id);
      return false;
    }
  }

  return true;
}

void BucketMap::resizeRows(size_t size) {
  size_t prev = rows_.size();
  if (size <= prev) {
    return;
  }

  rows_.resize(size);
  tableSize_.store(size);

  for (size_t i = prev; i < size; ++i) {
    freeList_.insert(i);
  }
}

bool BucketMap::isDrained() const {
  return keyWriter_->isDrained(shardId_);
}

uint64_t BucketMap::getSequence() const {
  return sequence_;
}

void BucketMap::clearNotReadyRows() {
  std::vector<Item> rows;
  getEverything(rows);
  for (size_t i = 0; i < rows.size(); ++i) {
    auto item = rows[i];
    if (item && !item->second.ready()) {
      folly::RWSpinLock::WriteHolder guard(lock_);
      eraseBasedOnKeyList(i, item->first.c_str());
    }
  }
}

} // namespace gorilla
} // namespace facebook
