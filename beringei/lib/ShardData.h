/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "beringei/lib/BucketMap.h"

namespace facebook {
namespace gorilla {

class ShardData {
 public:
  static const int kAsyncDropShardsDelaySecs;

  ShardData(int totalShards, int threads);

  ~ShardData();

  // Make it easy to iterate over shards with for(:).
  auto begin() {
    return data_.begin();
  }
  auto end() {
    return data_.end();
  }

  void initialize(int64_t shard, std::unique_ptr<BucketMap> map);

  BucketMap* getShardMap(int64_t shardId);
  BucketMap* operator[](int64_t shardId) {
    return getShardMap(shardId);
  }

  enum BeringeiShardState {
    SUCCESS, // success
    ERROR, // retryable error
    IN_PROGRESS, // async operation in progress, may succeed or fail
    PERMANENT_ERROR, // non-retryable error
  };

  BeringeiShardState addShardAsync(int64_t shardId);

  // Drops a single shard asynchronously. Delay is the seconds to wait
  // before actually dropping the shard.
  BeringeiShardState dropShardAsync(int64_t shardId, int64_t delay);

  // Sets the currently owned shards asynchronously. Anything that is
  // not in this set is considered to be not owned.
  void setShards(
      const std::set<int64_t>& shards,
      int dropDelay = kAsyncDropShardsDelaySecs);

  // Returns the set of owned shards.
  std::set<int64_t> getShards();

  // Returns the number of owned and total shards respectively.
  int64_t getNumShards();
  int64_t getNumShardsOwnedInProgress();
  int64_t getTotalNumShards();

  // Synchronously add and drop shards by spinning until the corresponding
  // async methods succeed.
  void addShardForTests(int64_t shardId);
  void dropShardForTests(int64_t shardId);
  void setShardsForTests(const std::set<int64_t>& shards);

 private:
  void processOneShardAddition(int64_t shardId);

  void addShardThread();
  void dropShardThread();

  std::vector<std::unique_ptr<BucketMap>> data_;

  const int64_t totalShards_;
  std::atomic<int> numShards_;
  std::atomic<int> numShardsBeingAdded_;

  folly::MPMCQueue<int64_t> addShardQueue_;
  folly::MPMCQueue<int64_t> readBlocksShardQueue_;
  std::vector<std::thread> addShardThreads_;

  folly::MPMCQueue<std::pair<uint32_t, int64_t>> dropShardQueue_;
  std::thread dropShardThread_;
};
} // namespace gorilla
} // namespace facebook
