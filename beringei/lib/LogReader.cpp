// Copyright 2004-present Facebook. All Rights Reserved.

#include "beringei/lib/LogReader.h"

#include <folly/Range.h>

#include "beringei/lib/BucketUtils.h"
#include "beringei/lib/DataLog.h"
#include "beringei/lib/FileUtils.h"
#include "beringei/lib/GorillaStatsManager.h"

namespace facebook {
namespace gorilla {

static constexpr folly::StringPiece kLogFilePrefix = "log";
static constexpr folly::StringPiece kCorruptLogFiles = "corrupt_log_files";

LocalLogReader::LocalLogReader(
    uint32_t shardId,
    const std::string& dataDir,
    int64_t windowSize,
    DataPointCallback&& cb)
    : shardId_(shardId),
      dataDirectory_(dataDir),
      windowSize_(windowSize),
      cb_(std::move(cb)) {}

void LocalLogReader::readLog(
    uint32_t lastBlock,
    int64_t& lastTimestamp,
    uint32_t& unknownKeys) {
  FileUtils files(shardId_, kLogFilePrefix.str(), dataDirectory_);

  for (int64_t id : files.ls()) {
    if (id < BucketUtils::timestamp(lastBlock + 1, windowSize_, shardId_)) {
      LOG(INFO) << "Skipping log file " << id << " because it's already "
                << "covered by a block";
      continue;
    }

    auto file = files.open(id, "rb", 0);
    if (!file.file) {
      LOG(ERROR) << "Could not open shard " << shardId_ << " logfile";
      continue;
    }

    LOG(INFO) << "Reading logfile " << file.name;
    uint32_t b = BucketUtils::bucket(id, windowSize_, shardId_);
    DataLogReader::readLog(
        file, id, [&](uint32_t key, int64_t unixTime, double value) {
          if (unixTime < BucketUtils::timestamp(b, windowSize_, shardId_) ||
              unixTime > BucketUtils::timestamp(b + 1, windowSize_, shardId_)) {
            LOG(ERROR) << "Unix time is out of the expected range: " << unixTime
                       << " ["
                       << BucketUtils::timestamp(b, windowSize_, shardId_)
                       << ","
                       << BucketUtils::timestamp(b + 1, windowSize_, shardId_)
                       << "]";
            GorillaStatsManager::addStatValue(kCorruptLogFiles.str());

            // It's better to stop reading this log file here because
            // none of the data can be trusted after this.
            return false;
          }
          cb_(key, unixTime, value, unknownKeys, lastTimestamp);
          return true;
        });
    fclose(file.file);
    LOG(INFO) << "Finished reading logfile " << file.name;
  }
}

LocalLogReaderFactory::LocalLogReaderFactory(const std::string& dir)
    : dataDir_(dir) {}

std::unique_ptr<LogReader> LocalLogReaderFactory::getLogReader(
    uint32_t shardId,
    int64_t windowSize,
    DataPointCallback&& func) const {
  return std::make_unique<LocalLogReader>(
      shardId, dataDir_, windowSize, std::move(func));
}

} // namespace gorilla
} // namespace facebook
