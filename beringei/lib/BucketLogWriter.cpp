/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "BucketLogWriter.h"

#include "GorillaStatsManager.h"

#include "glog/logging.h"
#include "beringei/lib/GorillaTimeConstants.h"

namespace facebook {
namespace gorilla {

DECLARE_int32(data_log_buffer_size);

static const int kLogFileBufferSize = FLAGS_data_log_buffer_size;
static const std::string kLogDataFailures = ".log_data_failures";
static const std::string kLogFilesystemFailures =
    ".failed_writes.log_filesystem";

// These are not valid indexes so they can be used to control starting
// and stopping shards.
static const int kStartShardIndex = -1;
static const int kStopShardIndex = -2;
static const int kNoOpIndex = -3;

static const int kMaxActiveBuckets = 2;

static const int kFileOpenRetries = 5;
static const int kSleepUsBetweenFailures = 100 * kGorillaUsecPerMs; // 100 us
const std::string BucketLogWriter::kLogFilePrefix = "log";

uint32_t BucketLogWriter::numShards_ = 1;

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
      keepLogFilesAroundTime_(windowSize * 2) {
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
    GorillaStatsManager::addStatValue(kLogDataFailures, 1);
  }
}

bool BucketLogWriter::writeOneLogEntry(bool blockingRead) {
  // This code assumes that there's only a single thread running here!
  std::vector<LogDataInfo> data;
  LogDataInfo info;

  if (stopThread_ && blockingRead) {
    return false;
  }

  if (blockingRead) {
    // First read is blocking then as many as possible without blocking.
    logDataQueue_.blockingRead(info);
    data.push_back(std::move(info));
  }

  while (logDataQueue_.read(info)) {
    data.push_back(std::move(info));
  }

  bool onePreviousLogWriterCleared = false;

  for (const auto& info : data) {
    if (info.index == kStartShardIndex) {
      ShardWriter writer;

      // Randomly select the next clear time between windowSize_ and
      // windowSize_ * 2 to spread out the clear operations.
      writer.nextClearTimeSecs =
          time(nullptr) + windowSize_ + (random() % windowSize_);
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

      int bucket = info.unixTime / windowSize_;
      ShardWriter& shardWriter = iter->second;
      auto& logWriter = shardWriter.logWriters[bucket];

      // If this bucket doesn't have a file open yet, open it now.
      if (!logWriter) {
        for (int i = 0; i < kFileOpenRetries; i++) {
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

      // Spread out opening the files for the next bucket in the last
      // 1/4 of the time window based on the shard ID. This will avoid
      // opening a lot of files simultaneously.
      uint32_t openNextFileTime =
          windowSize_ * (bucket + 0.75 + 0.25 * info.shardId / numShards_);
      if (time(nullptr) > openNextFileTime &&
          shardWriter.logWriters.find(bucket + 1) ==
              shardWriter.logWriters.end()) {
        uint32_t baseTime = (bucket + 1) * windowSize_;
        LOG(INFO) << "Opening file in advance for shard " << info.shardId;
        for (int i = 0; i < kFileOpenRetries; i++) {
          auto f =
              shardWriter.fileUtils->open(baseTime, "wb", kLogFileBufferSize);
          if (f.file) {
            shardWriter.logWriters[bucket + 1].reset(
                new DataLogWriter(std::move(f), baseTime));
            break;
          }

          if (i == kFileOpenRetries - 1) {
            // This is kind of ok. We'll try again above.
            LOG(ERROR) << "Failed too many times to open log file " << f.name;
          }
          usleep(kSleepUsBetweenFailures);
        }
      }

      // Only clear at most one previous bucket because the operation
      // is really slow and queue might fill up if multiple buckets
      // are cleared.
      if (!onePreviousLogWriterCleared &&
          time(nullptr) % windowSize_ > waitTimeBeforeClosing_ &&
          shardWriter.logWriters.find(bucket - 1) !=
              shardWriter.logWriters.end()) {
        shardWriter.logWriters.erase(bucket - 1);
        onePreviousLogWriterCleared = true;
      }

      if (time(nullptr) > shardWriter.nextClearTimeSecs) {
        shardWriter.fileUtils->clearTo(time(nullptr) - keepLogFilesAroundTime_);
        shardWriter.nextClearTimeSecs += windowSize_;
      }

      if (logWriter) {
        logWriter->append(info.index, info.unixTime, info.value);
      } else {
        GorillaStatsManager::addStatValue(kLogDataFailures, 1);
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
  GorillaStatsManager::addStatExportType(kLogDataFailures, SUM);
  GorillaStatsManager::addStatExportType(kLogFilesystemFailures, SUM);
}
}
} // facebook:gorilla
