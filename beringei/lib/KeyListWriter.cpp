/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "beringei/lib/KeyListWriter.h"

#include <folly/Format.h>
#include <folly/MapUtil.h>
#include <glog/logging.h>

#include "beringei/lib/GorillaStatsManager.h"

namespace facebook {
namespace gorilla {

static const std::string kKeyListFailures = "key_list_write_failures";
static const std::string kKeysPopped = "key_list_queue_popped";

KeyListWriter::KeyListWriter(const std::string& dataDirectory, size_t queueSize)
    : keyInfoQueue_(queueSize),
      writerThread_(nullptr),
      dataDirectory_(dataDirectory) {
  startWriterThread();
}

KeyListWriter::~KeyListWriter() {
  stopWriterThread();
}

bool KeyListWriter::addKey(
    int64_t shardId,
    uint32_t id,
    const std::string& key,
    uint16_t category,
    int32_t timestamp) {
  KeyInfo info;
  info.shardId = shardId;
  info.key = key;
  info.keyId = id;
  info.type = KeyInfo::WRITE_KEY;
  info.category = category;
  info.timestamp = timestamp;
  return keyInfoQueue_.write(std::move(info));
}

void KeyListWriter::deleteKey(
    int64_t shardId,
    uint32_t id,
    const std::string& key,
    uint16_t category) {
  KeyInfo info;
  info.shardId = shardId;
  info.keyId = id;
  info.type = KeyInfo::DELETE_KEY;
  info.key = key;
  info.category = category;

  // It's better to delay the thrift handler than to completely lose a key.
  keyInfoQueue_.blockingWrite(std::move(info));
}

void KeyListWriter::compact(
    int64_t shardId,
    std::function<std::tuple<uint32_t, const char*, uint16_t, int32_t>()>
        generator) {
  auto writer = get(shardId);
  if (!writer) {
    LOG(ERROR) << "Trying to compact non-enabled shard " << shardId;
    return;
  }
  writer->compact(generator);
}

void KeyListWriter::startShard(int64_t shardId, bool force) {
  if (force) {
    enable(shardId);
  } else {
    KeyInfo info;
    info.shardId = shardId;
    info.type = KeyInfo::START_SHARD;
    keyInfoQueue_.blockingWrite(std::move(info));
  }
}

void KeyListWriter::stopShard(int64_t shardId, bool force) {
  if (force) {
    disable(shardId);
  } else {
    KeyInfo info;
    info.shardId = shardId;
    info.type = KeyInfo::STOP_SHARD;
    keyInfoQueue_.blockingWrite(std::move(info));
  }
}

void KeyListWriter::startMonitoring() {
  GorillaStatsManager::addStatExportType(kKeyListFailures, SUM);
  GorillaStatsManager::addStatExportType(kKeysPopped, AVG);
  GorillaStatsManager::addStatExportType(kKeysPopped, SUM);
}

std::shared_ptr<PersistentKeyListIf> KeyListWriter::get(int64_t shardId) {
  std::unique_lock<std::mutex> guard(lock_);
  auto iter = keyWriters_.find(shardId);
  if (iter == keyWriters_.end()) {
    return nullptr;
  }
  return iter->second;
}

void KeyListWriter::enable(int64_t shardId) {
  std::unique_lock<std::mutex> guard(lock_);

  // Check if this shard has been started.
  auto it = keyWriters_.find(shardId);
  if (it != keyWriters_.end() && it->second != nullptr) {
    return;
  }

  // This shard has never been stared. Start now.
  LOG(INFO) << folly::sformat("Shard: {}. Enabling key list writer", shardId);
  if (persistentKeyListFactory_) {
    keyWriters_[shardId] = persistentKeyListFactory_->getPersistentKeyList(
        shardId, dataDirectory_);
  } else {
    keyWriters_[shardId].reset(new PersistentKeyList(shardId, dataDirectory_));
  }
}

void KeyListWriter::disable(int64_t shardId) {
  std::unique_lock<std::mutex> guard(lock_);
  keyWriters_.erase(shardId);
}

void KeyListWriter::flushQueue() {
  // Stop thread to flush keys
  stopWriterThread();
  startWriterThread();
}

void KeyListWriter::startWriterThread() {
  writerThread_.reset(new std::thread([&]() {
    while (true) {
      try {
        if (!writeOneKey()) {
          break;
        }
      } catch (std::exception& e) {
        LOG(ERROR) << e.what();
      }
    }
  }));
}

void KeyListWriter::stopWriterThread() {
  if (writerThread_) {
    // Wake up and stop the writer thread.
    KeyInfo info;
    info.type = KeyInfo::STOP_THREAD;
    keyInfoQueue_.blockingWrite(std::move(info));

    writerThread_->join();
  }
}

bool KeyListWriter::writeOneKey() {
  // This code assumes that there's only a single thread running here!

  std::vector<KeyInfo> keys;

  {
    KeyInfo info;
    keyInfoQueue_.blockingRead(info);

    keys.push_back(std::move(info));
    while (keyInfoQueue_.read(info)) {
      keys.push_back(std::move(info));
    }
  }

  GorillaStatsManager::addStatValue(kKeysPopped, keys.size());

  for (auto& info : keys) {
    switch (info.type) {
      case KeyInfo::STOP_THREAD:
        return false;
      case KeyInfo::START_SHARD:
        enable(info.shardId);
        break;
      case KeyInfo::STOP_SHARD:
        disable(info.shardId);
        break;
      case KeyInfo::WRITE_KEY: {
        auto writer = get(info.shardId);
        if (!writer) {
          LOG(ERROR) << "Trying to write key to non-enabled shard "
                     << info.shardId;
          continue;
        }
        if (!writer->appendKey(
                info.keyId, info.key.c_str(), info.category, info.timestamp)) {
          LOG(ERROR) << "Failed to write key '" << info.key
                     << "' to log for shard " << info.shardId;
          GorillaStatsManager::addStatValue(kKeyListFailures);

          // Try to put it back in the queue for later.
          keyInfoQueue_.write(std::move(info));
        }
        break;
      }
      case KeyInfo::DELETE_KEY: {
        auto writer = get(info.shardId);
        if (!writer) {
          LOG(ERROR) << "Trying to delete key for non-enabled shard "
                     << info.shardId;
          continue;
        }
        if (!writer->deleteKey(info.keyId, info.key.c_str(), info.category)) {
          LOG(ERROR) << folly::sformat(
              "Shard: {}. Failed to delete id: {}.", info.shardId, info.keyId);
          GorillaStatsManager::addStatValue(kKeyListFailures);

          // Try to put it back in the queue for later.
          keyInfoQueue_.write(std::move(info));
        }
        break;
      }
    }
  }

  return true;
}

void KeyListWriter::setKeyListFactory(
    std::shared_ptr<PersistentKeyListFactory> factory) {
  std::unique_lock<std::mutex> guard(lock_);
  persistentKeyListFactory_ = factory;
}

bool KeyListWriter::isDrained(size_t shardId) const {
  std::unique_lock<std::mutex> guard(lock_);
  return keyWriters_.find(shardId) == keyWriters_.end();
}

} // namespace gorilla
} // namespace facebook
