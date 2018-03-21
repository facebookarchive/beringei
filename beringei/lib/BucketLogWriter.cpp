/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "beringei/lib/BucketLogWriter.h"

#include <glog/logging.h>

#include "beringei/lib/BucketUtils.h"
#include "beringei/lib/GorillaStatsManager.h"
#include "beringei/lib/GorillaTimeConstants.h"
#include "beringei/lib/Timer.h"

namespace facebook {
namespace gorilla {

DECLARE_int32(data_log_buffer_size);

static const int kLogFileBufferSize = FLAGS_data_log_buffer_size;
static const std::string kLogDataDequeueLatencyUs =
    "log_data_dequeue_latency_us";
static const std::string kLogDataFailures = "log_data_failures";
static const std::string kLogDataEnqueueFailures = "log_data_enqueue_failures";
static const std::string kLogFileOpenRetries = "log_file_open_retries";
static const std::string kLogFilesystemFailures =
    "failed_writes.log_filesystem";

// These are not valid indexes so they can be used to control starting
// and stopping shards.
static const int kStartShardIndex = -1;
static const int kStopShardIndex = -2;
static const int kNoOpIndex = -3;

static const int kMaxActiveBuckets = 2;

static const int kFileOpenRetries = 5;
static const int kSleepUsBetweenFailures = 100 * kGorillaUsecPerMs; // 100 us
const std::string BucketLogWriter::kLogFilePrefix = "log";

BucketLogWriter::BucketLogWriter(
    int windowSize,
    const std::string& dataDirectory,
    size_t queueSize,
    uint32_t allowedTimestampBehind)
    : windowSize_(windowSize),
      logDataQueue_(queueSize),
      writerThread_(nullptr),
      stopThread_(false),
      dataDirectory_(dataDirectory),

      // One `allowedTimestampBehind` delay to allow the data to come in
      // and one more delay to allow the data to be dequeued and written.
      waitTimeBeforeClosing_(allowedTimestampBehind * 2),
      keepLogFilesAroundTime_(BucketUtils::duration(2, windowSize)) {
  CHECK_GT(windowSize, allowedTimestampBehind)
      << "Window size " << windowSize
      << " must be larger than allowedTimestampBehind "
      << allowedTimestampBehind;

  startWriterThread();
}

BucketLogWriter::~BucketLogWriter() {
  stopWriterThread();
}

void BucketLogWriter::startWriterThread() {
  stopThread_ = false;
  writerThread_.reset(new std::thread([&]() {
    while (true) {
      try {
        if (!writeOneLogEntry(true)) {
          break;
        }
      } catch (std::exception& e) {
        // Most likely a problem with filesystem.
        LOG(ERROR) << e.what();
        GorillaStatsManager::addStatValue(kLogFilesystemFailures, 1);
        usleep(kSleepUsBetweenFailures);
      }
    }
  }));
}

void BucketLogWriter::stopWriterThread() {
  if (writerThread_) {
    stopThread_ = true;

    // Wake up and stop the writer thread. Just sends zeros to do
    // that.
    logData(0, kNoOpIndex, 0, 0);
    writerThread_->join();
  }
}

uint32_t BucketLogWriter::bucket(uint64_t unixTime, int shardId) const {
  return BucketUtils::bucket(unixTime, windowSize_, shardId);
}

uint64_t BucketLogWriter::timestamp(uint32_t bucket, int shardId) const {
  return BucketUtils::timestamp(bucket, windowSize_, shardId);
}

uint64_t BucketLogWriter::duration(uint32_t buckets) const {
  return BucketUtils::duration(buckets, windowSize_);
}

void BucketLogWriter::flushQueue() {
  stopWriterThread();
  while (writeOneLogEntry(false))
    ;
  startWriterThread();
}

void BucketLogWriter::logData(
    int64_t shardId,
    int32_t index,
    int64_t unixTime,
    double value) {
  LogDataInfo info;
  info.shardId = shardId;
  info.index = index;
  info.unixTime = unixTime;
  info.value = value;

  if (!logDataQueue_.write(std::move(info))) {
    GorillaStatsManager::addStatValue(kLogDataEnqueueFailures, 1);
  }
}

bool BucketLogWriter::writeOneLogEntry(bool blockingRead) {
  // This code assumes that there's only a single thread running here!
  std::vector<LogDataInfo> data;
  LogDataInfo info;

  if (stopThread_ && blockingRead) {
    return false;
  }

  Timer dequeueTimer(true);
  if (blockingRead) {
    // First read is blocking then as many as possible without blocking.
    logDataQueue_.blockingRead(info);
    data.push_back(std::move(info));
  }

  while (logDataQueue_.read(info)) {
    data.push_back(std::move(info));
  }
  GorillaStatsManager::addStatValue(
      kLogDataDequeueLatencyUs, dequeueTimer.get());

  bool onePreviousLogWriterCleared = false;

  for (const auto& info : data) {
    if (info.index == kStartShardIndex) {
      ShardWriter writer;

      // Select the next clear time to be the start of a bucket between
      // windowSize_ and windowSize_ * 2 to spread out the clear operations.
      writer.nextClearTimeSecs = BucketUtils::floorTimestamp(
          time(nullptr) + duration(2), windowSize_, info.shardId);
      writer.fileUtils.reset(
          new FileUtils(info.shardId, kLogFilePrefix, dataDirectory_));
      shardWriters_.insert(std::make_pair(info.shardId, std::move(writer)));
    } else if (info.index == kStopShardIndex) {
      LOG(INFO) << "Stopping shard " << info.shardId;
      shardWriters_.erase(info.shardId);
    } else if (info.index != kNoOpIndex) {
      auto iter = shardWriters_.find(info.shardId);
      if (iter == shardWriters_.end()) {
        LOG(ERROR) << "Trying to write to a shard that is not enabled for "
                      "writing "
                   << info.shardId;
        continue;
      }

      int b = bucket(info.unixTime, info.shardId);
      ShardWriter& shardWriter = iter->second;
      auto& logWriter = shardWriter.logWriters[b];

      // If this bucket doesn't have a file open yet, open it now.
      if (!logWriter) {
        for (int i = 0; i < kFileOpenRetries; i++) {
          GorillaStatsManager::addStatValue(kLogFileOpenRetries, i);
          auto f = shardWriter.fileUtils->open(
              info.unixTime, "wb", kLogFileBufferSize);
          if (f.file) {
            logWriter.reset(new DataLogWriter(std::move(f), info.unixTime));
            break;
          }
          if (i == kFileOpenRetries - 1) {
            LOG(ERROR) << "Failed too many times to open log file " << f.name;
            GorillaStatsManager::addStatValue(kLogFilesystemFailures, 1);
          }
          usleep(kSleepUsBetweenFailures);
        }
      }

      // Open files for the next bucket in the last 1/10 of the time window.
      uint32_t openNextFileTime =
          timestamp(b, info.shardId) + windowSize_ * 0.9;
      if (time(nullptr) > openNextFileTime &&
          shardWriter.logWriters.find(b + 1) == shardWriter.logWriters.end()) {
        uint32_t baseTime = timestamp(b + 1, info.shardId);
        LOG(INFO) << "Opening file in advance for shard " << info.shardId;
        for (int i = 0; i < kFileOpenRetries; i++) {
          GorillaStatsManager::addStatValue(kLogFileOpenRetries, i);
          auto f =
              shardWriter.fileUtils->open(baseTime, "wb", kLogFileBufferSize);
          if (f.file) {
            shardWriter.logWriters[b + 1].reset(
                new DataLogWriter(std::move(f), baseTime));
            break;
          }

          if (i == kFileOpenRetries - 1) {
            // This is kind of ok. We'll try again above.
            LOG(ERROR) << "Failed too many times to open log file " << f.name;
            GorillaStatsManager::addStatValue(kLogFilesystemFailures, 1);
          }
          usleep(kSleepUsBetweenFailures);
        }
      }

      if (logWriter) {
        logWriter->append(info.index, info.unixTime, info.value);
      } else {
        GorillaStatsManager::addStatValue(kLogDataFailures, 1);
      }

      // Only clear at most one previous bucket because the operation
      // is really slow and queue might fill up if multiple buckets
      // are cleared.
      auto now = time(nullptr);
      int nowBucket = bucket(now, info.shardId);
      if (!onePreviousLogWriterCleared &&
          now - BucketUtils::floorTimestamp(now, windowSize_, info.shardId) >
              waitTimeBeforeClosing_ &&
          shardWriter.logWriters.find(nowBucket - 1) !=
              shardWriter.logWriters.end()) {
        shardWriter.logWriters.erase(nowBucket - 1);
        onePreviousLogWriterCleared = true;
      }

      if (now > shardWriter.nextClearTimeSecs) {
        shardWriter.fileUtils->clearTo(time(nullptr) - keepLogFilesAroundTime_);
        shardWriter.nextClearTimeSecs += duration(1);
      }
    }
  }

  // Don't flush any of the logWriters. DataLog class will handle the
  // flushing when there's enough data.
  return !data.empty();
}

void BucketLogWriter::startShard(int64_t shardId) {
  LogDataInfo info;
  info.shardId = shardId;
  info.index = kStartShardIndex;
  logDataQueue_.blockingWrite(std::move(info));
}

void BucketLogWriter::stopShard(int64_t shardId) {
  LogDataInfo info;
  info.shardId = shardId;
  info.index = kStopShardIndex;
  logDataQueue_.blockingWrite(std::move(info));
}

void BucketLogWriter::startMonitoring() {
  GorillaStatsManager::addStatExportType(kLogDataEnqueueFailures, SUM);
  GorillaStatsManager::addStatExportType(kLogDataDequeueLatencyUs, AVG);
  GorillaStatsManager::addStatExportType(kLogFileOpenRetries, SUM);
  GorillaStatsManager::addStatExportType(kLogDataFailures, SUM);
  GorillaStatsManager::addStatExportType(kLogFilesystemFailures, SUM);
}

} // namespace gorilla
} // namespace facebook
