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
#include "beringei/lib/ShardData.h"

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

  virtual void getLastUpdateTimes(
      GetLastUpdateTimesResult& ret,
      std::unique_ptr<GetLastUpdateTimesRequest> req) override;

  void purgeThread();
  void cleanThread();

  // Purges time series that have no data in the active bucket and not
  // in any of the `numBuckets` older buckets.
  int purgeTimeSeries(uint8_t numBuckets);

  void finalizeBucket(const uint64_t timestamp);
  void finalizeBucketsThread();

 private:
  // Reads shard map (via configAdapter_) to learn of shards that have been
  // added or dropped.  Invoked periodically via function scheduler.
  void refreshShardConfig();

  ShardData shards_;

  std::shared_ptr<BeringeiConfigurationAdapterIf> configAdapter_;
  std::shared_ptr<MemoryUsageGuardIf> memoryUsageGuard_;
  const std::string serviceName_;
  const int32_t port_;

  folly::FunctionScheduler purgeThread_;
  folly::FunctionScheduler cleanThread_;
  folly::FunctionScheduler bucketFinalizerThread_;
  folly::FunctionScheduler refreshShardConfigThread_;
};
}
} // facebook::gorilla
