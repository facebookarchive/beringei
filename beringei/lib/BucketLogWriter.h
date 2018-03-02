/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <memory>
#include <thread>
#include <unordered_map>

#include <folly/MPMCQueue.h>
#include <gtest/gtest.h>

#include "beringei/lib/DataLog.h"
#include "beringei/lib/FileUtils.h"

namespace facebook {
namespace gorilla {

class BucketLogWriterIf {
 public:
  virtual ~BucketLogWriterIf() {}

  /// This will push the given data entry to a queue for logging.
  /// @param[in] shardId Shard for this log.
  /// @param[in] index Index into the internal vector of TimeSeries.
  /// @param[in] unixTime Time of the data point.
  /// @param[in] value Value of the data point.
  virtual void
  logData(int64_t shardId, int32_t index, int64_t unixTime, double value) = 0;

  /// Starts writing points for this shard.
  /// @param[in] shard Shard to start writing data point.
  virtual void startShard(int64_t shardId) = 0;

  /// Stops writing points for this shard and closes all the open files.
  /// @param[in] shard Shard to stop writing data point.
  virtual void stopShard(int64_t shardId) = 0;
};

class BucketLogWriter : public BucketLogWriterIf {
 public:
  static const std::string kLogFilePrefix;

  BucketLogWriter(
      int windowSize,
      const std::string& dataDirectory,
      size_t queueSize,
      uint32_t allowedTimestampBehind);

  ~BucketLogWriter();

  /// @see BucketLogWriterIf.
  void logData(int64_t shardId, int32_t index, int64_t unixTime, double value)
      override;

  /// @see BucketLogWriterIf.
  void startShard(int64_t shardId) override;

  /// @see BucketLogWriterIf.
  void stopShard(int64_t shardId) override;

  /// Initialize all monitoring for this class.
  static void startMonitoring();

  /// Flush the current buffered queue.
  void flushQueue();

 private:
  // Writes a single entry from the queue. Does not need to be called
  // if `writerThread` was defined in the constructor.
  bool writeOneLogEntry(bool blockingRead);
  void startWriterThread();
  void stopWriterThread();

  uint32_t bucket(uint64_t unixTime, int shardId) const;
  uint64_t timestamp(uint32_t bucket, int shardId) const;
  uint64_t duration(uint32_t buckets) const;

 private:
  struct LogDataInfo {
    int32_t index;

    // Cheating and using only 32-bits for the shard id to save some
    // memory, because currently we are using only 6000 shards.
    int32_t shardId;
    int64_t unixTime;
    double value;
  };

  int windowSize_;
  folly::MPMCQueue<LogDataInfo> logDataQueue_;
  std::unique_ptr<std::thread> writerThread_;
  std::atomic<bool> stopThread_;
  const std::string dataDirectory_;
  const uint32_t waitTimeBeforeClosing_;
  const uint32_t keepLogFilesAroundTime_;

  struct ShardWriter {
    std::unordered_map<int, std::unique_ptr<DataLogWriter>> logWriters;
    std::unique_ptr<FileUtils> fileUtils;
    uint32_t nextClearTimeSecs;
  };

  std::unordered_map<int64_t, ShardWriter> shardWriters_;
};

} // namespace gorilla
} // namespace facebook
