/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "BucketLogWriter.h"
#include "BucketStorage.h"
#include "BucketedTimeSeries.h"
#include "CaseUtils.h"
#include "KeyListWriter.h"
#include "PersistentKeyList.h"
#include "Timer.h"

#include <folly/RWSpinLock.h>

namespace facebook {
namespace gorilla {

// This class handles all the time series in a shard. It loads shard
// information when the shard is added. It also keeps a track of the
// state of the shard.
class BucketMap {
 public:
  typedef std::shared_ptr<std::pair<std::string, BucketedTimeSeries>> Item;

  static const int kNotOwned;

  enum State {
    // The order here matters. It's only possible to go to a bigger
    // state and from OWNED to PRE_UNOWNED.

    // About to be unowned. No resources have been released yet. Can
    // be owned again by just calling `cancelUnowning` function.
    PRE_UNOWNED = 0,

    // Unowned. No resources allocated. To own the shard, it must be
    // moved to PRE_OWNED state.
    UNOWNED = 1,

    // Pre-owned. Resources allocated and reading keys and logs can
    // start. If no keys/data need to be read, it can be moved to
    // OWNED state after this state directly.
    PRE_OWNED = 2,

    // Currently reading keys.
    READING_KEYS = 3,

    // Reading keys is done and logs can be read next.
    READING_KEYS_DONE = 4,

    // Currenly reading logs.
    READING_LOGS = 5,

    // Processing queued data points.
    PROCESSING_QUEUED_DATA_POINTS = 6,

    // Reading block files.
    READING_BLOCK_DATA = 7,

    // Everything is read and owned.
    OWNED = 8,
  };

  BucketMap(
      uint8_t buckets,
      uint64_t windowSize,
      int shardId,
      const std::string& dataDirectory,
      std::shared_ptr<KeyListWriter> keyWriter,
      std::shared_ptr<BucketLogWriter> logWriter,
      State state);
  virtual ~BucketMap() {}
  // Insert the given data point, creating a new row if necessary.
  // Returns the number of new rows created (0 or 1) and the number of
  // data points successfully inserted (0 or 1) as a pair of ints.
  // Returns {kNotOwned,kNotOwned} if this map is currenly not owned.

  virtual std::pair<int, int> put(
      const std::string& key,
      const TimeValuePair& value,
      uint16_t category,
      bool skipStateCheck = false);

  // Get a shared_ptr to a TimeSeries.
  Item get(const std::string& key);

  // Get all the TimeSeries.
  void getEverything(std::vector<Item>& out);

  // Get some of the TimeSeries. Follows the amazing naming convention
  // of `getEverything`. Returns true if there is more data left.
  bool getSome(std::vector<Item>& out, int offset, int count);

  void erase(int index, Item item);

  // Conversions between bucket number and timestamp.
  static uint32_t bucket(uint64_t unixTime, uint64_t windowSize, int shardId);
  uint32_t bucket(uint64_t unixTime) const;
  static uint64_t timestamp(uint32_t bucket, uint64_t windowSize, int shardId);
  uint64_t timestamp(uint32_t bucket) const;

  // Conversions between duration and number of buckets.
  static uint64_t duration(uint32_t buckets, uint64_t windowSize);
  uint64_t duration(uint32_t buckets) const;
  static uint32_t buckets(uint64_t duration, uint64_t windowSize);
  uint32_t buckets(uint64_t duration) const;

  BucketStorage* getStorage();

  void flushKeyList();

  void compactKeyList();

  void deleteOldBlockFiles();

  static void startMonitoring();

  // Reads the key list. This function should be called after moving
  // to PRE_OWNED state.
  void readKeyList();

  // Raads the data. The function should be called after calling
  // readKeyList.
  void readData();

  // Reads compressed block files for the newest unread time window.
  // This function should be called repeatedly after calling readData.
  // Returns true if there might be more files to read, in which case the caller
  // should call again later.
  bool readBlockFiles();

  // Sets the state. Returns true if state was set, false if the state
  // transition is not allowed or already in that state.
  bool setState(State state);

  State getState();

  // Returns the time in milliseconds it took to add this shard from
  // PRE_OWNED state to OWNED state. If called before the shard is
  // added, will return zero.
  Timer::TimeVal getAddTime();

  // Cancels unowning. This should only be called if current state is
  // PRE_UNOWNED. Returns true if unowning was successful. State will
  // be OWNED after a successful call.
  bool cancelUnowning();

  // Returns true if the state transition is allowed.
  static bool isAllowedStateTransition(State from, State to);

  // Finalizes all the buckets which haven't been finalized up to the
  // given position. Returns the number of buckets that were
  // finalized. If the shard is not owned, will return immediately
  // with 0. This function is not thread-safe.
  int finalizeBuckets(uint32_t bucketToFinalize);

  // Returns whether this BucketMap is behind more than 1 bucket.
  bool isBehind(uint32_t bucketToFinalize) const;

  // Process is shutting down. Closes any open files. State will be
  // UNOWNED after this.
  void shutdown();

  // Returns list of time series that deviatated from the mean at the
  // given time.
  std::vector<BucketMap::Item> getDeviatingTimeSeries(uint32_t unixTime);

  // Indexes deviating time series. `deviationStartTime` and `endTime`
  // is the time range for calculating the mean and standard
  // deviation. `indexingStartTime` and `endTime` is the time range
  // for which the deviations are indexed. `minimumSigma` sepcifies
  // the minimum number of standard deviations the value has to differ
  // from the mean before it's indexed.
  //
  // Returns the total number of deviations that were indexed.
  int indexDeviatingTimeSeries(
      uint32_t deviationStartTime,
      uint32_t indexingStartTime,
      uint32_t endTime,
      double minimumSigma);

  uint32_t getLastFinalizedBucket() {
    return lastFinalizedBucket_;
  }

  // returns the earliest timestamp (inclusive) from which gorilla is
  // unaware of any missing data.  initialized to 0 and returns 0
  // if a shard has no missing data
  int64_t getReliableDataStartTime();

 private:
  // Load all the datapoints out of the logfiles for this shard that
  // are newer than what is covered by the lastBlock.
  void readLogFiles(uint32_t lastBlock);

  // Returns a shared_ptr to the item if found. Always sets
  // `state`. Sets `id` if item is found. If keyList is not nullptr,
  // sets that.
  BucketMap::Item
  getInternal(const std::string& key, State& state, uint32_t& id);

  void queueDataPointWithKey(
      const std::string& key,
      const TimeValuePair& value,
      uint16_t category);
  void queueDataPointWithId(
      uint32_t id,
      const TimeValuePair& value,
      uint16_t category);

  void processQueuedDataPoints(bool skipStateCheck);

  bool putDataPointWithId(
      BucketedTimeSeries* timeSeries,
      uint32_t timeSeriesId,
      const TimeValuePair& value,
      uint16_t category);

  void checkForMissingBlockFiles();

  const uint8_t n_;
  const int64_t windowSize_;

  int64_t reliableDataStartTime_;

  mutable folly::RWSpinLock lock_;

  std::unordered_map<const char*, int, CaseHash, CaseEq> map_;

  // Always equal to rows_.size();
  std::atomic<int> tableSize_;

  std::vector<Item> rows_;
  std::priority_queue<int, std::vector<int>, std::less<int>> freeList_;
  BucketStorage storage_;
  State state_;
  int shardId_;
  const std::string dataDirectory_;

  std::shared_ptr<KeyListWriter> keyWriter_;
  std::shared_ptr<BucketLogWriter> logWriter_;
  Timer addTimer_;
  std::mutex stateChangeMutex_;

  struct QueuedDataPoint {
    uint32_t timeSeriesId;

    // 32 bits for the timestamp to save memory. 64-bits not needed
    // because the timestamp is turned into seconds before coming to
    // BucketMap.
    uint32_t unixTime;

    // Empty string will indicate that timeSeriesId is used.
    std::string key;
    double value;
    uint16_t category;
  };

  void queueDataPoint(QueuedDataPoint& dp);
  std::shared_ptr<folly::MPMCQueue<QueuedDataPoint>> dataPointQueue_;
  uint32_t lastFinalizedBucket_;

  std::mutex unreadBlockFilesMutex_;
  std::set<uint32_t> unreadBlockFiles_;

  // Circular vector for the deviations.
  std::vector<std::vector<uint32_t>> deviations_;
};
}
} // facebook::gorilla
