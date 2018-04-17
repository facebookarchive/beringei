/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <condition_variable>
#include <string>
#include <unordered_set>
#include <vector>

#include <folly/io/async/EventBaseManager.h>
#include <folly/synchronization/RWSpinLock.h>

#include "beringei/client/BeringeiConfigurationAdapterIf.h"
#include "beringei/if/gen-cpp2/BeringeiService.h"

using folly::EventBaseManager;

namespace facebook {
namespace fb303 {
class FacebookBase2;
}
} // namespace facebook

namespace facebook {
namespace gorilla {

class BeringeiNetworkClient {
 public:
  BeringeiNetworkClient(
      const std::string& serviceName,
      std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
      bool shadow);

  virtual ~BeringeiNetworkClient() {}

  typedef std::unordered_map<std::pair<std::string, int>, PutDataRequest>
      PutRequestMap;

  typedef std::unordered_map<
      std::pair<std::string, int>,
      std::pair<GetDataRequest, GetDataResult>>
      GetRequestMap;

  typedef std::unordered_map<
      std::pair<std::string, int>,
      std::pair<GetDataRequest, std::vector<size_t>>>
      MultiGetRequestMap;

  // Fire off a putData request. Returns the dropped data
  // points. Might move data points from the requests.
  virtual std::vector<DataPoint> performPut(PutRequestMap& requests);

  virtual folly::Future<std::vector<DataPoint>> futurePerformPut(
      PutDataRequest& request,
      const std::pair<std::string, int>& hostInfo);

  // Fire off a getData request.
  virtual void performGet(GetRequestMap& requests);

  virtual folly::Future<GetDataResult> performGet(
      const std::pair<std::string, int>& hostInfo,
      const GetDataRequest& request,
      folly::EventBase* eb = getEventBase());

  // Fetches the last update times from all the servers in parallel
  // and calls the callback multiple times with partial results. The
  // callback should return false if it doesn't want more results, and
  // the operation will be stopped. The call is synchronous and will
  // return once all the results have been found or the callback has
  // returned false or the timeout has been reached. The callback will
  // be called from multiple different threads.
  virtual void getLastUpdateTimes(
      uint32_t minLastUpdateTime,
      uint32_t maxKeysPerRequest,
      uint32_t timeoutSeconds,
      std::function<bool(const std::vector<KeyUpdateTime>& keys)> callback);

  // Adds a data point to a request. Returns true if more points should be
  // added to this request, false otherwise. `dropped` will be set to true
  // if the data point was not added to the request.
  virtual bool
  addDataPointToRequest(DataPoint& dp, PutRequestMap& requests, bool& dropped);

  // Adds a key to a get request.
  virtual void addKeyToGetRequest(const Key& key, GetRequestMap& requests);

  // Like above, but MultiGetRequestMap also records the index of each key into
  // the corresponding result structure.
  virtual void addKeyToGetRequest(
      size_t index,
      const Key& key,
      MultiGetRequestMap& requests);

  // Invalidate the DirectoryService cache for a certain set of shard ids
  virtual void invalidateCache(const std::unordered_set<int64_t>& shardIds);

  virtual std::string getServiceName();

  bool isCorrespondingService(const std::string& serviceName);

  // Stops all outstanding requests
  virtual void stopRequests();

  virtual int64_t getNumShards() {
    return shardCache_.size();
  }

  virtual void performScanShard(
      const ScanShardRequest& request,
      ScanShardResult& result);

  virtual folly::Future<ScanShardResult> performScanShard(
      const std::pair<std::string, int>& hostInfo,
      const ScanShardRequest& request,
      folly::EventBase* eb = getEventBase());

  static uint32_t getTimeoutMs();

  virtual std::shared_ptr<BeringeiServiceAsyncClient> getBeringeiThriftClient(
      const std::pair<std::string, int>& hostInfo,
      folly::EventBase* eb = getEventBase());

  virtual int getShardForDataPoint(const DataPoint& dp);

  // Gets keys stored in specified shard. Returns true if there are more keys
  // to be fetched.
  virtual bool getShardKeys(
      int shardNumber,
      int limit,
      int offset,
      std::vector<KeyUpdateTime>& keys);

  static folly::EventBase* getEventBase() {
    return folly::EventBaseManager::get()->getEventBase();
  }

  virtual bool getHostForScanShard(
      const ScanShardRequest& request,
      std::pair<std::string, int>& hostInfo) {
    return getHostForShard(request.shardId, hostInfo);
  }

  bool isShadow() const;

  virtual bool getHostForShard(
      int64_t shardId,
      std::pair<std::string, int>& hostInfo);

 protected:
  // Default constructor that doesn't do any initialization. Should be
  // only used from tests.
  BeringeiNetworkClient() {}

  template <typename T>
  void addKeyToRequest(const Key& key, T& requests) {
    std::pair<std::string, int> hostInfo;
    bool success = getHostForShard(key.shardId, hostInfo);
    if (!success) {
      return;
    }

    requests[hostInfo].first.keys.push_back(key);
  }

 private:
  bool getHostForShardOnFailure(
      bool cachedEntry,
      int64_t shardId,
      std::pair<std::string, int>& hostInfo);

  void useStaleCacheEntry(
      int64_t shardId,
      const std::pair<std::string, int>& hostInfo);

  void addCacheEntry(
      int64_t shardId,
      const std::pair<std::string, int>& hostInfo);

  void getLastUpdateTimesForHost(
      uint32_t minLastUpdateTime,
      uint32_t maxKeysPerRequest,
      const std::string& host,
      int port,
      const std::vector<int64_t>& shards,
      uint32_t timeoutSeconds,
      std::function<bool(const std::vector<KeyUpdateTime>& keys)> callback);

  struct ShardCacheEntry {
    std::string hostAddress;
    int port;
    time_t updateTime;
  };

 protected:
  std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter_;
  std::string serviceName_;
  std::atomic<bool> stopRequests_;
  std::condition_variable stopping_;
  std::mutex stoppingMutex_;

 private:
  std::vector<std::unique_ptr<ShardCacheEntry>> shardCache_;
  folly::RWSpinLock shardCacheLock_;
  bool isShadow_ = false;
};

} // namespace gorilla
} // namespace facebook
