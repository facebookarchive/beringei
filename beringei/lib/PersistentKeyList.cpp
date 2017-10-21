/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "PersistentKeyList.h"

#include <folly/compression/Compression.h>
#include <folly/io/IOBuf.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "GorillaStatsManager.h"

namespace facebook {
namespace gorilla {

// For reads and compaction. Can probably be arbitrarily large.
const static size_t kLargeBufferSize = 1 << 24;

// Flush after 4k of keys.
const static size_t kSmallBufferSize = 1 << 12;

const static int kTempFileId = 0;
const int KRetryFileOpen = 3;

const static std::string kFileType = "key_list";
const static std::string kFailedCounter = "failed_writes." + kFileType;

// Marker bytes to determine if the file is compressed or not and if
// there are categories or not.
const static char kCompressedFileMarker = 'C';
const static char kUncompressedFileMarker = 'U';
const static char kCompressedFileWithCategoriesMarker = '0';
const static char kUncompressedFileWithCategoriesMarker = '1';
const static char kCompressedFileWithTimestampsMarker = '2';
const static char kUncompressedFileWithTimestampsMarker = '3';

const static uint32_t kHardFlushIntervalSecs = 120;

PersistentKeyList::PersistentKeyList(
    int64_t shardId,
    const std::string& dataDirectory)
    : activeList_({nullptr, ""}),
      files_(shardId, kFileType, dataDirectory),
      lock_(),
      shard_(shardId) {
  GorillaStatsManager::addStatExportType(kFailedCounter, SUM);

  // Randomly select the next flush time within the interval to spread
  // the fflush calls between shards.
  nextHardFlushTimeSecs_ = time(nullptr) + random() % kHardFlushIntervalSecs;

  openNext();
}

int PersistentKeyList::readKeys(
    int64_t shardId,
    const std::string& dataDirectory,
    std::function<bool(uint32_t, const char*, uint16_t, int32_t)> f) {
  LOG(INFO) << "Reading keys from shard " << shardId;

  FileUtils files(shardId, kFileType, dataDirectory);

  // Read all the keys from all the relevant files.
  std::vector<int64_t> ids = files.ls();
  int keys = 0;
  for (int64_t fileId : ids) {
    // Ignore leftover files from a failed call to compact().
    if (fileId == kTempFileId) {
      continue;
    }

    auto file = files.open(fileId, "rb", 0);
    if (!file.file) {
      LOG(ERROR) << "Opening file failed: " << file.name;
      continue;
    }

    // Read the entire file.
    fseek(file.file, 0, SEEK_END);
    size_t len = ftell(file.file);

    if (len <= 1) {
      fclose(file.file);
      continue;
    }

    std::unique_ptr<char[]> buffer(new char[len]);
    fseek(file.file, 0, SEEK_SET);
    if (fread(buffer.get(), 1, len, file.file) != len) {
      PLOG(ERROR) << "Failed to read " << file.name;
      fclose(file.file);
      continue;
    }

    int keysFound = 0;
    if (buffer[0] == kCompressedFileMarker ||
        buffer[0] == kCompressedFileWithCategoriesMarker ||
        buffer[0] == kCompressedFileWithTimestampsMarker) {
      try {
        auto codec = folly::io::getCodec(
            folly::io::CodecType::ZLIB, folly::io::COMPRESSION_LEVEL_BEST);
        auto ioBuffer = folly::IOBuf::wrapBuffer(buffer.get() + 1, len - 1);
        auto uncompressed = codec->uncompress(ioBuffer.get());

        // It's a chained buffer. This will make it a single buffer.
        uncompressed->coalesce();
        keysFound = readKeysFromBuffer(
            (const char*)uncompressed->data(),
            uncompressed->length(),
            buffer[0] != kCompressedFileMarker,
            buffer[0] == kCompressedFileWithTimestampsMarker,
            f);
      } catch (std::exception& e) {
        LOG(ERROR) << "Uncompression failed: " << e.what();
      }
    } else if (
        buffer[0] == kUncompressedFileMarker ||
        buffer[0] == kUncompressedFileWithCategoriesMarker ||
        buffer[0] == kUncompressedFileWithTimestampsMarker) {
      keysFound = readKeysFromBuffer(
          buffer.get() + 1,
          len - 1,
          buffer[0] != kUncompressedFileMarker,
          buffer[0] == kUncompressedFileWithTimestampsMarker,
          f);
    } else {
      LOG(ERROR) << "Unknown marker byte " << buffer[0];
    }

    if (keysFound == 0) {
      LOG(ERROR) << file.name << " contains no valid data";
    }
    keys += keysFound;

    fclose(file.file);
  }

  LOG(INFO) << "Read " << keys << " keys from " << ids.size()
            << " files for shard " << shardId;
  return keys;
}

bool PersistentKeyList::appendKey(
    uint32_t id,
    const char* key,
    uint16_t category,
    int32_t timestamp) {
  std::lock_guard<std::mutex> guard(lock_);
  if (activeList_.file == nullptr) {
    return false;
  }

  writeKey(id, key, category, timestamp);
  return true;
}

void PersistentKeyList::compact(
    std::function<std::tuple<uint32_t, const char*, uint16_t, int32_t>()>
        generator) {
  // Direct appends to a new file.
  int64_t prev = openNext();

  // Create a temporary compressed file.
  auto tempFile = files_.open(kTempFileId, "wb", kLargeBufferSize);

  if (!tempFile.file) {
    PLOG(ERROR) << "Could not open a temp file for writing keys";
    GorillaStatsManager::addStatValue(kFailedCounter, 1);
    return;
  }

  folly::fbstring buffer;
  for (auto key = generator(); std::get<1>(key) != nullptr; key = generator()) {
    appendBuffer(
        buffer,
        std::get<0>(key),
        std::get<1>(key),
        std::get<2>(key),
        std::get<3>(key));
  }

  if (buffer.length() == 0) {
    fclose(tempFile.file);
    return;
  }

  try {
    auto ioBuffer = folly::IOBuf::wrapBuffer(buffer.data(), buffer.length());
    auto codec = folly::io::getCodec(
        folly::io::CodecType::ZLIB, folly::io::COMPRESSION_LEVEL_BEST);
    auto compressed = codec->compress(ioBuffer.get());
    compressed->coalesce();

    if (fwrite(
            &kCompressedFileWithTimestampsMarker,
            sizeof(char),
            1,
            tempFile.file) != 1 ||
        fwrite(
            compressed->data(),
            sizeof(char),
            compressed->length(),
            tempFile.file) != compressed->length()) {
      PLOG(ERROR) << "Could not write to the temporary key file "
                  << tempFile.name;
      GorillaStatsManager::addStatValue(kFailedCounter, 1);
      fclose(tempFile.file);
      return;
    }

    LOG(INFO) << "Compressed key list from " << buffer.length() << " bytes to "
              << compressed->length();
  } catch (std::exception& e) {
    LOG(ERROR) << "Compression failed:" << e.what();
    fclose(tempFile.file);
    return;
  }

  // Swap the new data in for the old.
  fclose(tempFile.file);
  files_.rename(kTempFileId, prev);

  // Clean up remaining files.
  files_.clearTo(prev);
}

void PersistentKeyList::flush(bool hardFlush) {
  if (activeList_.file == nullptr) {
    openNext();
  }
  if (activeList_.file != nullptr) {
    if (buffer_.length() > 0) {
      size_t written = fwrite(
          buffer_.data(), sizeof(char), buffer_.length(), activeList_.file);
      if (written != buffer_.length()) {
        PLOG(ERROR) << "Failed to flush key list file " << activeList_.name;
        GorillaStatsManager::addStatValue(kFailedCounter, 1);
      }
      buffer_ = "";
    }

    if (hardFlush) {
      fflush(activeList_.file);
    }
  } else {
    // No file to flush to.
    LOG(ERROR) << "Could not flush key list for shard " << shard_
               << " to disk. No open key_list file";
    GorillaStatsManager::addStatValue(kFailedCounter, 1);
  }
}

void PersistentKeyList::clearEntireListForTests() {
  files_.clearAll();
  openNext();
}

int64_t PersistentKeyList::openNext() {
  std::lock_guard<std::mutex> guard(lock_);
  if (activeList_.file != nullptr) {
    fclose(activeList_.file);
  }

  std::vector<int64_t> ids = files_.ls();
  int64_t activeId = ids.empty() ? 1 : ids.back() + 1;
  activeList_ = files_.open(activeId, "wb", kSmallBufferSize);

  int i = 0;
  while (activeList_.file == nullptr && i < KRetryFileOpen) {
    activeList_ = files_.open(activeId, "wb", kSmallBufferSize);
    i++;
  }

  if (activeList_.file == nullptr) {
    PLOG(ERROR) << "Couldn't open key_list." << activeId
                << " for writes (shard " << shard_ << ")";
    return activeId - 1;
  }

  if (fwrite(
          &kUncompressedFileWithTimestampsMarker,
          sizeof(char),
          1,
          activeList_.file) != 1) {
    PLOG(ERROR) << "Could not write to the key list file " << activeList_.name;
    GorillaStatsManager::addStatValue(kFailedCounter, 1);
  }

  return activeId - 1;
}

void PersistentKeyList::appendBuffer(
    folly::fbstring& buffer,
    uint32_t id,
    const char* key,
    uint16_t category,
    int32_t timestamp) const {
  const char* bytes = (const char*)&id;
  for (int i = 0; i < sizeof(id); i++) {
    buffer += bytes[i];
  }
  const char* categoryBytes = (const char*)&category;
  for (int i = 0; i < sizeof(category); i++) {
    buffer += categoryBytes[i];
  }
  const char* timestampBytes = (const char*)&timestamp;
  for (int i = 0; i < sizeof(timestamp); i++) {
    buffer += timestampBytes[i];
  }

  buffer += key;
  buffer += '\0';
}

void PersistentKeyList::writeKey(
    uint32_t id,
    const char* key,
    uint16_t category,
    int32_t timestamp) {
  // Write to the internal buffer and only flush when needed.
  appendBuffer(buffer_, id, key, category, timestamp);

  bool flushHard = time(nullptr) > nextHardFlushTimeSecs_;
  if (flushHard) {
    nextHardFlushTimeSecs_ = time(nullptr) + kHardFlushIntervalSecs;
  }

  if (buffer_.length() >= kSmallBufferSize || flushHard) {
    flush(flushHard);
  }
}

int PersistentKeyList::readKeysFromBuffer(
    const char* buffer,
    size_t len,
    bool categoryPresent,
    bool timestampPresent,
    std::function<bool(uint32_t, const char*, uint16_t, int32_t)> f) {
  // Back up until the buffer ends with a zero byte.
  // This should come from the last byte in a string, but it could be a byte
  // in an id.
  while (buffer[len - 1] != '\0') {
    len--;
    if (len == 0) {
      return 0;
    }
  }

  int keys = 0;

  size_t minRecordLength = sizeof(uint32_t) + 1;
  if (categoryPresent) {
    minRecordLength += sizeof(uint16_t);
  }
  if (timestampPresent) {
    minRecordLength += sizeof(uint32_t);
  }

  // Read the records one-by-one until too few bytes remain.
  // A minimum record is an uint32 (+uint16) (+uint32) and a zero-length string.
  const char* pos = buffer;
  const char* endPos = pos + len - minRecordLength;
  uint16_t defaultCategory = 0;
  int32_t defaultTimestamp = 0;
  while (pos <= endPos) {
    uint32_t* id;
    const char* key;
    uint16_t* category = &defaultCategory;
    int32_t* timestamp = &defaultTimestamp;

    id = (uint32_t*)pos;
    pos += sizeof(uint32_t);
    if (categoryPresent) {
      category = (uint16_t*)pos;
      pos += sizeof(uint16_t);
    }
    if (timestampPresent) {
      timestamp = (int32_t*)pos;
      pos += sizeof(int32_t);
    }
    key = pos;

    if (!f(*id, key, *category, *timestamp)) {
      // Callback doesn't accept more keys.
      break;
    }
    keys++;
    pos += strlen(key) + 1;
  }

  return keys;
}
}
} // facebook:gorilla
