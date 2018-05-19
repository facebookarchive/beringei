/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <cstdio>
#include <mutex>
#include <vector>

#include <folly/FBString.h>

#include "beringei/lib/FileUtils.h"

namespace facebook {
namespace gorilla {

// class PersistentKeyList
//
// Keeps an on-disk list of (key_id, key_string) pairs.
// This class is thread-safe, but no attempt is made at managing the concurrency
// of the file itself.
class PersistentKeyList {
 public:
  explicit PersistentKeyList(int64_t shardId, const std::string& dataDirectory);
  ~PersistentKeyList() {
    if (activeList_.file != nullptr) {
      flush(true);
      fclose(activeList_.file);
    }
  }

  // Must not be called until after a call to readKeys().
  // Returns false on failure.
  bool appendKey(
      uint32_t id,
      const char* key,
      uint16_t category,
      int32_t firstTimestamp);

  // Rewrite and compress the file to contain only the generated
  // entries. Continues generating until receiving a nullptr key.
  // This function should only be called by a single thread at a time,
  // but concurrent calls to appendKey() are safe.
  void compact(
      std::function<std::tuple<uint32_t, const char*, uint16_t, int32_t>()>
          generator);

  // Writes the internal buffer to disk. If `hardFlush` is set to true
  // forces new keys out to disk instead of leaving them in the OS
  // buffer. This is called internally once a minute so that too much
  // data isn't lost if we go OOM or segfault or get kill -9'd by
  // tupperware.
  void flush(bool hardFlush);

  void clearEntireListForTests();

 private:
  // Prepare a new file for writes. Returns the id of the previous one.
  int64_t openNext();

  // Appends id, key, category to the given buffer. The buffer can be
  // later written to disk. Does not clear the buffer before
  // appending.
  void appendBuffer(
      folly::fbstring& buffer,
      uint32_t id,
      const char* key,
      uint16_t category,
      int32_t timestamp) const;

  // Writes new key to internal buffer. Flushes to disk when buffer is
  // big enough or enough time has passed since the last flush time.
  void
  writeKey(uint32_t id, const char* key, uint16_t category, int32_t timestamp);

  FileUtils::File activeList_;

  FileUtils files_;
  std::mutex lock_;

  int64_t shard_;
  folly::fbstring buffer_;
  uint32_t nextHardFlushTimeSecs_;
};

} // namespace gorilla
} // namespace facebook
