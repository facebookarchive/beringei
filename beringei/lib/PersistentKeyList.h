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

class PersistentKeyListIf {
 public:
  virtual ~PersistentKeyListIf() {}

  // Must not be called until after a call to readKeys().
  // Returns false on failure.
  virtual bool appendKey(
      uint32_t id,
      const char* key,
      uint16_t category,
      int32_t firstTimestamp) = 0;

  // Remove the key from persistent storage, if necessary.
  virtual bool deleteKey(uint32_t id, const char* key, uint16_t category) = 0;

  // Rewrite and compress the file to contain only the generated
  // entries. Continues generating until receiving a nullptr key.
  // This function should only be called by a single thread at a time,
  // but concurrent calls to appendKey() are safe.
  virtual void compact(
      std::function<std::tuple<uint32_t, const char*, uint16_t, int32_t>()>
          generator) = 0;

  // Writes the internal buffer to disk. If `hardFlush` is set to true
  // forces new keys out to disk instead of leaving them in the OS
  // buffer. This is called internally once a minute so that too much
  // data isn't lost if we go OOM or segfault or get kill -9'd by
  // tupperware.
  virtual void flush(bool hardFlush) = 0;
};

// class PersistentKeyList
//
// Keeps an on-disk list of (key_id, key_string) pairs.
// This class is thread-safe, but no attempt is made at managing the concurrency
// of the file itself.
class PersistentKeyList : public PersistentKeyListIf {
 public:
  explicit PersistentKeyList(int64_t shardId, const std::string& dataDirectory);
  ~PersistentKeyList() {
    if (activeList_.file != nullptr) {
      flush(true);
      fclose(activeList_.file);
    }
  }

  /// @see PersistentKeyListIf.
  bool appendKey(
      uint32_t id,
      const char* key,
      uint16_t category,
      int32_t firstTimestamp) override;
  /// @see PersistentKeyListIf.
  bool deleteKey(uint32_t, const char*, uint16_t) override {
    // For on-disk persistent key list, we don't need to delete. We will
    // override the id later.
    return true;
  }

  /// @see PersistentKeyListIf.
  void compact(
      std::function<std::tuple<uint32_t, const char*, uint16_t, int32_t>()>
          generator) override;

  /// @see PersistentKeyListIf.
  void flush(bool hardFlush) override;

  void clearEntireListForTests();

  // Appends id, key, category to the given buffer. The buffer can be
  // later written to disk. Does not clear the buffer before
  // appending.
  static void appendBuffer(
      folly::fbstring& buffer,
      uint32_t id,
      const char* key,
      uint16_t category,
      int32_t timestamp);

  static void appendMarker(folly::fbstring& buffer, bool compressed);

  // Append operation marker to the buffer, whether it's append or delete.
  static void appendOpMarker(folly::fbstring& buffer, bool append);

  static bool compactToFile(
      FILE* f,
      const std::string& name,
      std::function<std::tuple<uint32_t, const char*, uint16_t, int32_t>()>
          generator);

  static bool compactToBuffer(
      std::function<std::tuple<uint32_t, const char*, uint16_t, int32_t>()>
          generator,
      uint64_t seq,
      folly::fbstring& out);

 private:
  // Prepare a new file for writes. Returns the id of the previous one.
  int64_t openNext();

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

class PersistentKeyListFactory {
 public:
  virtual ~PersistentKeyListFactory() {}
  virtual std::unique_ptr<PersistentKeyListIf> getPersistentKeyList(
      int64_t shardId,
      const std::string& dir) const = 0;
};

class LocalPersistentKeyListFactory : public PersistentKeyListFactory {
 public:
  std::unique_ptr<PersistentKeyListIf> getPersistentKeyList(
      int64_t shardId,
      const std::string& dir) const override;
};

} // namespace gorilla
} // namespace facebook
