/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <folly/SharedMutex.h>
#include <folly/experimental/FunctionScheduler.h>
#include <mutex>

#include "beringei/client/BeringeiConfigurationAdapterIf.h"
#include "beringei/if/gen-cpp2/BeringeiService.h"
#include "beringei/lib/BucketMap.h"
#include "beringei/lib/MemoryUsageGuardIf.h"

/* using override */
using facebook::gorilla::BeringeiServiceSvIf;

class BeringeiServiceHandlerTest;

namespace facebook {
namespace gorilla {

class BeringeiServiceHandler : virtual public BeringeiServiceSvIf {
  friend class ::BeringeiServiceHandlerTest;

 public:
  static const int kAsyncDropShardsDelaySecs;

  BeringeiServiceHandler(
      std::shared_ptr<BeringeiConfigurationAdapterIf> configAdapter,
      std::shared_ptr<MemoryUsageGuardIf> memoryUsageGuard,
      const std::string& serviceName,
      int port);

  virtual ~BeringeiServiceHandler();

  virtual void getData(GetDataResult& ret, std::unique_ptr<GetDataRequest> req)
      override;

  virtual void putDataPoints(
      PutDataResult& response,
      std::unique_ptr<PutDataRequest> req) override;

  virtual void getShardDataBucket(
      GetShardDataBucketResult& ret,
      int64_t beginTs,
      int64_t endTs,
      int64_t shardId,
      int32_t offset,
      int32_t limit) override;

  virtual BucketMap* getShardMap(int64_t shardId);

  void purgeThread();

  // Purges time series that have no data in the active bucket and not
  // in any of the `numBuckets` older buckets.
  int purgeTimeSeries(uint8_t numBuckets);

  void finalizeBucket(const uint64_t timestamp);
  void finalizeBucketsThread();

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

 private:
  void processOneShardAddition(int64_t shardId);

  void addShardThread();
  void dropShardThread();

  // Reads shard map (via configAdapter_) to learn of shards that have been
  // added or dropped.  Invoked periodically via function scheduler.
  void refreshShardConfig();

  std::vector<std::unique_ptr<BucketMap>> data_;

  std::shared_ptr<BeringeiConfigurationAdapterIf> configAdapter_;
  std::shared_ptr<MemoryUsageGuardIf> memoryUsageGuard_;
  const std::string serviceName_;
  const int32_t port_;

  std::atomic<int> numShards_;
  std::atomic<int> numShardsBeingAdded_;

  folly::MPMCQueue<int64_t> addShardQueue_;
  folly::MPMCQueue<int64_t> readBlocksShardQueue_;
  std::vector<std::thread> addShardThreads_;

  folly::MPMCQueue<std::pair<uint32_t, int64_t>> dropShardQueue_;
  std::thread dropShardThread_;

  int32_t getTotalNumShards();
  int32_t getNumShards();

  folly::FunctionScheduler purgeThread_;
  folly::FunctionScheduler bucketFinalizerThread_;

  folly::FunctionScheduler refreshShardConfigThread_;
};
}
} // facebook::gorilla
