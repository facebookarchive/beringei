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

#include "beringei/lib/PersistentKeyList.h"

namespace facebook {
namespace gorilla {

class KeyListWriter {
 public:
  static const std::string kLogFilePrefix;

  KeyListWriter(const std::string& dataDirectory, size_t queueSize);

  ~KeyListWriter();

  // Copy a new key onto the queue for writing.
  bool addKey(
      int64_t shardId,
      uint32_t id,
      const std::string& key,
      uint16_t category,
      int32_t timestamp);

  void deleteKey(
      int64_t shardId,
      uint32_t id,
      const std::string& key,
      uint16_t category);

  // Pass a compaction call down to the appropriate PersistentKeyList.
  void compact(
      int64_t shardId,
      std::function<std::tuple<uint32_t, const char*, uint16_t, int32_t>()>
          generator);

  void startShard(int64_t shardId, bool force = false);
  void stopShard(int64_t shardId, bool force = false);

  static void startMonitoring();

  void flushQueue();

  void setKeyListFactory(std::shared_ptr<PersistentKeyListFactory> factory);

  bool isDrained(size_t shardId) const;

 private:
  std::shared_ptr<PersistentKeyListIf> get(int64_t shardId);
  void enable(int64_t shardId);
  void disable(int64_t shardId);

  void stopWriterThread();
  void startWriterThread();

  // Write a single entry from the queue.
  bool writeOneKey();

  struct KeyInfo {
    int64_t shardId;
    std::string key;
    int32_t keyId;
    enum { STOP_THREAD, START_SHARD, STOP_SHARD, WRITE_KEY, DELETE_KEY } type;
    uint16_t category;
    int32_t timestamp;
  };

  folly::MPMCQueue<KeyInfo> keyInfoQueue_;
  std::unique_ptr<std::thread> writerThread_;
  const std::string dataDirectory_;

  mutable std::mutex lock_;
  std::unordered_map<int64_t, std::shared_ptr<PersistentKeyListIf>> keyWriters_;
  std::shared_ptr<PersistentKeyListFactory> persistentKeyListFactory_;
};

} // namespace gorilla
} // namespace facebook
