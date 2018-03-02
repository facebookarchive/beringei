/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "beringei/lib/DataLog.h"

#include <folly/GroupVarint.h>

#include "beringei/lib/BitUtil.h"
#include "beringei/lib/FileUtils.h"
#include "beringei/lib/GorillaStatsManager.h"

namespace {
const static int kPreviousValuesVectorSizeIncrement = 1000;
}

namespace facebook {
namespace gorilla {

DEFINE_int32(
    data_log_buffer_size,
    65536,
    "The size of the internal buffer when logging data. Buffer size of 64K "
    "equals roughly to 3 seconds of data before it's written to disk");

DEFINE_int32(
    max_allowed_timeseries_id,
    10000000,
    "This is the maximum allowed id for a time series in a shard. This is used "
    "for sanity checking that the file isn't corrupt and to avoid allocating "
    "too much memory");

const static std::string kFailedCounter = "failed_writes.log";
const static std::string kPartialWriteCounter = "partial_writes.log";

DataLogWriter::DataLogWriter(FileUtils::File&& out, int64_t baseTime)
    : out_(out),
      lastTimestamp_(baseTime),
      buffer_(new char[FLAGS_data_log_buffer_size]),
      bufferSize_(0) {
  GorillaStatsManager::addStatExportType(kFailedCounter, SUM);
  GorillaStatsManager::addStatExportType(kPartialWriteCounter, SUM);
}

DataLogWriter::~DataLogWriter() {
  if (out_.file) {
    flushBuffer();
    FileUtils::closeFile(out_, FLAGS_gorilla_async_file_close);
  }
}

void DataLogWriter::append(uint32_t id, int64_t unixTime, double value) {
  folly::fbstring bits;
  uint32_t numBits = 0;

  if (id > FLAGS_max_allowed_timeseries_id) {
    LOG(ERROR) << "ID:" << id
               << " too large. Increase max_allowed_timeseries_id?";
    return;
  }
  DataLogUtil::appendId(id, bits, numBits);

  // Optimize for zero delta case and increase used bits 8 at a time
  // to fill bytes.
  int64_t delta = unixTime - lastTimestamp_;
  DataLogUtil::appendTimestampDelta(delta, bits, numBits);

  if (id >= previousValues_.size()) {
    // If the value hasn't been seen before, assume that the previous
    // value is zero.
    previousValues_.resize(id + kPreviousValuesVectorSizeIncrement, 0);
  }

  uint64_t* v = (uint64_t*)&value;
  uint64_t* previousValue = (uint64_t*)&previousValues_[id];
  uint64_t xorWithPrevious = *v ^ *previousValue;
  DataLogUtil::appendValueXor(xorWithPrevious, bits, numBits);

  previousValues_[id] = value;
  lastTimestamp_ = unixTime;

  if (bits.length() + bufferSize_ > FLAGS_data_log_buffer_size) {
    flushBuffer();
  }

  memcpy(buffer_.get() + bufferSize_, bits.data(), bits.length());
  bufferSize_ += bits.length();
}

bool DataLogWriter::flushBuffer() {
  char* buffer = buffer_.get();
  bool success = true;
  clearerr(out_.file);
  int written = fwrite(buffer, sizeof(char), bufferSize_, out_.file);

  // Check all the possible cases when fwrite() returns something other
  // than bufferSize_ (feof, ferror, partial writes).
  if (written != bufferSize_) {
    success = false;

    if (feof(out_.file)) {
      PLOG(ERROR) << "Reached EOF when writing to buffer: " << out_.name;
    } else if (ferror(out_.file)) {
      PLOG(ERROR) << "Flushing buffer failed: " << out_.name;
    } else if (written > bufferSize_) {
      // This should never happen.
      LOG(ERROR) << "Unstable storage: Written " << written << " of "
                 << bufferSize_ << " to " << out_.name;
    } else {
      GorillaStatsManager::addStatValue(kPartialWriteCounter, 1);
      LOG(INFO) << "Partial write: Written " << written << " of " << bufferSize_
                << " to " << out_.name;
    }

    GorillaStatsManager::addStatValue(kFailedCounter, 1);
  }

  bufferSize_ = 0;
  return success;
}

int DataLogReader::readLog(
    const FileUtils::File& file,
    int64_t baseTime,
    std::function<bool(uint32_t, int64_t, double)> out) {
  fseek(file.file, 0, SEEK_END);
  size_t len = ftell(file.file);
  if (len == 0) {
    return 0;
  }

  std::unique_ptr<char[]> buffer(new char[len]);
  fseek(file.file, 0, SEEK_SET);
  if (fread(buffer.get(), 1, len, file.file) != len) {
    PLOG(ERROR) << "Failed to read entire file " << file.name;
    return -1;
  }

  return DataLogUtil::readLog(
      buffer.get(), len, baseTime, FLAGS_max_allowed_timeseries_id, out);
}

} // namespace gorilla
} // namespace facebook
