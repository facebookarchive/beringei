/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "beringei/lib/PersistentKeyList.h"

#include <folly/compression/Compression.h>
#include <folly/io/IOBuf.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "beringei/lib/GorillaStatsManager.h"

namespace facebook {
namespace gorilla {

// For reads and compaction. Can probably be arbitrarily large.
const static size_t kLargeBufferSize = 1 << 24;

// Flush after 4k of keys.
const static size_t kSmallBufferSize = 1 << 12;

const static int kTempFileId = 0;
const int KRetryFileOpen = 3;

constexpr static char kCompressedFileWithTimestampsMarker = '2';
constexpr static char kUncompressedFileWithTimestampsMarker = '3';
constexpr static char kAppendMarker = 'a';
constexpr static char kDeleteMarker = 'd';

const static std::string kFileType = "key_list";
const static std::string kFailedCounter = "failed_writes." + kFileType;

// Marker bytes to determine if the file is compressed or not and if
// there are categories or not.
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

bool PersistentKeyList::compactToFile(
    FILE* f,
    const std::string& fileName,
    std::function<std::tuple<uint32_t, const char*, uint16_t, int32_t>()>
        generator) {
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
    fclose(f);
    return false;
  }

  try {
    auto ioBuffer = folly::IOBuf::wrapBuffer(buffer.data(), buffer.length());
    auto codec = folly::io::getCodec(
        folly::io::CodecType::ZLIB, folly::io::COMPRESSION_LEVEL_BEST);
    auto compressed = codec->compress(ioBuffer.get());
    compressed->coalesce();

    if (fwrite(&kCompressedFileWithTimestampsMarker, sizeof(char), 1, f) != 1 ||
        fwrite(compressed->data(), sizeof(char), compressed->length(), f) !=
            compressed->length()) {
      PLOG(ERROR) << "Could not write to the temporary key file " << fileName;
      GorillaStatsManager::addStatValue(kFailedCounter, 1);
      fclose(f);
      return false;
    }

    LOG(INFO) << "Compressed key list from " << buffer.length() << " bytes to "
              << compressed->length();
  } catch (std::exception& e) {
    LOG(ERROR) << "Compression failed:" << e.what();
    fclose(f);
    return false;
  }

  // Swap the new data in for the old.
  fclose(f);
  return true;
}

bool PersistentKeyList::compactToBuffer(
    std::function<std::tuple<uint32_t, const char*, uint16_t, int32_t>()>
        generator,
    uint64_t seq,
    folly::fbstring& out) {
  folly::fbstring buffer;
  for (auto key = generator(); std::get<1>(key) != nullptr; key = generator()) {
    appendBuffer(
        buffer,
        std::get<0>(key),
        std::get<1>(key),
        std::get<2>(key),
        std::get<3>(key));
  }

  if (buffer.empty()) {
    return false;
  }

  try {
    auto ioBuffer = folly::IOBuf::wrapBuffer(buffer.data(), buffer.length());
    auto codec = folly::io::getCodec(
        folly::io::CodecType::ZLIB, folly::io::COMPRESSION_LEVEL_BEST);
    auto compressed = codec->compress(ioBuffer.get());
    compressed->coalesce();

    out.reserve(compressed->length() + 1 + 8);
    out.append((char*)(&seq), 8);
    out.append(&kCompressedFileWithTimestampsMarker, 1);
    out.append((char*)compressed->data(), compressed->length());
  } catch (const std::exception& e) {
    LOG(ERROR) << "Compression failed:" << e.what();
    return false;
  }

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

  if (!compactToFile(tempFile.file, tempFile.name, generator)) {
    return;
  }
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
    int32_t timestamp) {
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

std::unique_ptr<PersistentKeyListIf>
LocalPersistentKeyListFactory::getPersistentKeyList(
    int64_t shardId,
    const std::string& dir) const {
  return std::make_unique<PersistentKeyList>(shardId, dir);
}

void PersistentKeyList::appendMarker(folly::fbstring& buffer, bool compressed) {
  if (compressed) {
    buffer += kCompressedFileWithTimestampsMarker;
  } else {
    buffer += kUncompressedFileWithTimestampsMarker;
  }
}

void PersistentKeyList::appendOpMarker(folly::fbstring& buffer, bool append) {
  if (append) {
    buffer += kAppendMarker;
  } else {
    buffer += kDeleteMarker;
  }
}

} // namespace gorilla
} // namespace facebook
