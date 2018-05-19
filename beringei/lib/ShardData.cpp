/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "ShardData.h"

#include <folly/container/Enumerate.h>
#include "beringei/lib/GorillaStatsManager.h"
#include "beringei/lib/GorillaTimeConstants.h"

namespace facebook {
namespace gorilla {

const static std::string kNumShards = "num_shards";
const static std::string kMsPerShardAdd = "ms_per_shard_add";
const static std::string kShardsAdded = "shards_added";
const static std::string kShardsBeingAdded = "shards_being_added";
const static std::string kShardsDropped = "shards_dropped";

const int ShardData::kAsyncDropShardsDelaySecs = 30;

static const int kStopThreadShardId = -1;

ShardData::ShardData(int totalShards, int threads)
    : totalShards_(totalShards),
      numShards_(0),
      numShardsBeingAdded_(0),
      addShardQueue_(totalShards),
      readBlocksShardQueue_(totalShards),
      dropShardQueue_(totalShards) {
  // The number of threads for each thread pool must exceed 0.
  CHECK_GT(threads, 0);

  for (int i = 0; i < threads; i++) {
    addShardThreads_.push_back(std::thread(&ShardData::addShardThread, this));
  }
  dropShardThread_ = std::thread(&ShardData::dropShardThread, this);

  GorillaStatsManager::addStatExportType(kMsPerShardAdd, AVG);
  GorillaStatsManager::addStatExportType(kShardsAdded, SUM);
  GorillaStatsManager::addStatExportType(kShardsDropped, SUM);

  GorillaStatsManager::setCounter(kNumShards, 0);
  GorillaStatsManager::setCounter(kShardsBeingAdded, 0);
}

ShardData::~ShardData() {
  dropShardQueue_.write(std::make_pair(0, kStopThreadShardId));
  dropShardThread_.join();

  for (auto& t : addShardThreads_) {
    addShardQueue_.write(kStopThreadShardId);
  }
  for (auto& t : addShardThreads_) {
    t.join();
  }
}

void ShardData::initialize(int64_t shard, std::unique_ptr<BucketMap> map) {
  CHECK_EQ(data_.size(), shard);
  CHECK_LT(shard, totalShards_);
  data_.emplace_back(std::move(map));
}

void ShardData::setShards(const std::set<int64_t>& shards, int dropDelay) {
  std::vector<int64_t> shardsToBeAdded;
  std::vector<int64_t> shardsToBeDropped;

  for (int i = 0; i < data_.size(); i++) {
    BucketMap::State state = data_[i]->getState();
    bool shouldBeOwned = shards.find(i) != shards.end();

    // Don't not touch the shards that are being added. We will attempt
    // to drop them again periodically
    if (shouldBeOwned) {
      if (state == BucketMap::UNOWNED) {
        shardsToBeAdded.push_back(i);
      } else if (state == BucketMap::PRE_UNOWNED) {
        // This shard was queued to be unowned but we never got that far.
        // Just mark it as owned and thread unowning this shard won't do
        // anything
        data_[i]->cancelUnowning();
      }
    } else if (state == BucketMap::OWNED) {
      shardsToBeDropped.push_back(i);
    }
  }

  for (auto& shard : shardsToBeAdded) {
    LOG(INFO) << "Adding shard " << shard << " based on current config.";
    addShardAsync(shard);
  }

  for (auto& shard : shardsToBeDropped) {
    LOG(INFO) << "Dropping shard " << shard << " based on current config.";
    dropShardAsync(shard, dropDelay);
  }
}

std::set<int64_t> ShardData::getShards() {
  std::set<int64_t> shards;

  for (int i = 0; i < data_.size(); i++) {
    // Return anything that is owned or being added.
    if (data_[i]->getState() >= BucketMap::PRE_OWNED) {
      shards.insert(i);
    }
  }

  return shards;
}

int64_t ShardData::getNumShards() {
  return numShards_;
}

int64_t ShardData::getNumShardsOwnedInProgress() {
  return numShards_ + numShardsBeingAdded_;
}

int64_t ShardData::getTotalNumShards() {
  return totalShards_;
}

void ShardData::processOneShardAddition(int64_t shardId) {
  auto map = getShardMap(shardId);
  if (map) {
    BucketMap::State state = map->getState();
    if (state == BucketMap::PRE_OWNED || state == BucketMap::READING_KEYS) {
      // Attempt to read key list again when it's in READING_KEYS state. Being
      // in this state means previous attempt to read keys failed.
      map->readKeyList();

      // Put this shard back in the queue to read data.
      addShardQueue_.write(shardId);
    } else if (state == BucketMap::READING_KEYS_DONE) {
      map->readData();
      numShardsBeingAdded_--;
      numShards_++;
      GorillaStatsManager::setCounter(kShardsBeingAdded, numShardsBeingAdded_);
      GorillaStatsManager::setCounter(kNumShards, numShards_);
      GorillaStatsManager::addStatValue(kShardsAdded);
      GorillaStatsManager::addStatValue(
          kMsPerShardAdd, map->getAddTime() / kGorillaUsecPerMs);

      // Enqueue to read the compressed block files.
      readBlocksShardQueue_.write(shardId);
    } else {
      // Should never be reached.
      CHECK(false);
    }
  }
}

ShardData::BeringeiShardState ShardData::addShardAsync(int64_t shardId) {
  auto map = getShardMap(shardId);
  if (map) {
    BucketMap::State state = map->getState();
    if (state >= BucketMap::PRE_OWNED) {
      return BeringeiShardState::SUCCESS;
    }
    if (!map->setState(BucketMap::PRE_OWNED)) {
      // Setting to pre owned failed which means it's currently being
      // dropped.
      return BeringeiShardState::IN_PROGRESS;
    }

    numShardsBeingAdded_++;
    GorillaStatsManager::setCounter(kShardsBeingAdded, numShardsBeingAdded_);
    addShardQueue_.write(shardId);
    return BeringeiShardState::SUCCESS;
  }
  return BeringeiShardState::ERROR;
}

ShardData::BeringeiShardState ShardData::dropShardAsync(
    int64_t shardId,
    int64_t delay) {
  auto map = getShardMap(shardId);
  if (map) {
    BucketMap::State state = map->getState();
    if (state == BucketMap::UNOWNED) {
      return BeringeiShardState::SUCCESS;
    } else if (state != BucketMap::OWNED) {
      // Anything else other than OWNED and UNOWNED is considered to
      // be inprogess. This could mean that the shard is being added
      // or being dropped after a delay.
      return BeringeiShardState::IN_PROGRESS;
    }

    // PRE_UNOWNED is state that indicates that the shard will be
    // dropped after the delay.
    if (!map->setState(BucketMap::PRE_UNOWNED)) {
      return BeringeiShardState::IN_PROGRESS;
    }

    std::pair<uint32_t, int64_t> shard;
    shard.first = time(nullptr) + delay;
    shard.second = shardId;
    dropShardQueue_.write(shard);
    return BeringeiShardState::IN_PROGRESS;
  }

  return BeringeiShardState::ERROR;
}

void ShardData::addShardThread() {
  // Read key lists for all the shards first before reading any data
  // to make reading key lists as fast as possible.

  while (true) {
    try {
      int64_t shardId;

      // Prioritize addShardQueue_ over readBlocksShardQueue_.
      // It's ok to do the blocking read on addShardQueue_ because these are the
      // only threads that insert into readBlocksShardQueue_.
      if (addShardQueue_.read(shardId)) {
        processOneShardAddition(shardId);
      } else if (readBlocksShardQueue_.read(shardId)) {
        auto map = getShardMap(shardId);
        if (map && map->readBlockFiles()) {
          // Put this shard back in the queue to read more block files.
          // This way, all shards are read starting from now and working back.
          readBlocksShardQueue_.write(shardId);
        }

      } else {
        addShardQueue_.blockingRead(shardId);
        processOneShardAddition(shardId);
      }

      if (shardId == kStopThreadShardId) {
        break;
      }
    } catch (std::exception& e) {
      LOG(ERROR) << e.what();
    }
  }
}

void ShardData::dropShardThread() {
  while (true) {
    try {
      std::pair<uint32_t, int64_t> shard;
      dropShardQueue_.blockingRead(shard);
      if (shard.second == kStopThreadShardId) {
        break;
      }

      int delay = shard.first - time(nullptr);
      if (delay > 0) {
        // FIFO queue with a constant delay, so sleeping is always fine.
        /* sleep override */ sleep(delay);
      }

      auto map = getShardMap(shard.second);
      if (map) {
        if (map->getState() == BucketMap::PRE_UNOWNED) {
          // Only drop the shard if it's in PRE_UNOWNED state. This
          // guarantees that it is not being added back. If shard
          // manager really wants to get rid of this shard, it will ask
          // again.
          if (map->setState(BucketMap::UNOWNED)) {
            numShards_--;
            GorillaStatsManager::setCounter(kNumShards, numShards_);
            GorillaStatsManager::addStatValue(kShardsDropped);
          }
        }
      }
    } catch (std::exception& e) {
      LOG(ERROR) << e.what();
    }
  }
}

BucketMap* ShardData::getShardMap(int64_t shardId) {
  if (shardId < 0 || shardId >= data_.size()) {
    LOG(ERROR) << "Invalid shard " << shardId;
    return nullptr;
  }

  return data_[shardId].get();
}

void ShardData::addShardForTests(int64_t shardId) {
  addShardAsync(shardId);
  while (data_[shardId]->getState() != BucketMap::OWNED) {
    /* sleep override */ usleep(20000);
  }
}

void ShardData::dropShardForTests(int64_t shardId) {
  dropShardAsync(shardId, 0);
  while (data_[shardId]->getState() != BucketMap::UNOWNED) {
    /* sleep override */ usleep(20000);
  }
}

void ShardData::setShardsForTests(const std::set<int64_t>& shards) {
  setShards(shards, 0);

  bool done = false;
  while (!done) {
    /* sleep override */ usleep(20000);
    done = true;
    for (auto map : folly::enumerate(data_)) {
      done &= (*map)->getState() ==
          (shards.count(map.index) ? BucketMap::OWNED : BucketMap::UNOWNED);
    }
  }
}

} // namespace gorilla
} // namespace facebook
