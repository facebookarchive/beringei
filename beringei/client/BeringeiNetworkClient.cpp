/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "BeringeiNetworkClient.h"

#include <atomic>

#include <folly/Conv.h>
#include <folly/SocketAddress.h>
#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

#include "beringei/client/BeringeiConfigurationAdapterIf.h"
#include "beringei/lib/GorillaStatsManager.h"
#include "beringei/lib/GorillaTimeConstants.h"

DEFINE_int32(
    gorilla_max_batch_size,
    10000,
    "approximate max data points to send to one gorilla service");
DEFINE_int32(gorilla_shard_cache_ttl, 5, "Shard cache TTL");
DEFINE_int32(gorilla_negative_shard_cache_ttl, 5, "Negative shard cache TTL");
DEFINE_int32(
    gorilla_processing_timeout,
    0,
    "Processing timeout for talking to Gorilla hosts.");

using namespace apache::thrift;
using std::vector;
using std::unique_ptr;

namespace facebook {
namespace gorilla {

static const std::string kStaleShardInfoUsed =
    "gorilla_network_client.stale_shard_info_used";

static const int kDefaultThriftTimeoutMs =
    2 * kGorillaMsPerSecond; // 60 seconds

const static int kSleepBetweenRetrySecs = 10;

BeringeiNetworkClient::BeringeiNetworkClient(
    const std::string& serviceName,
    std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
    bool shadow)
    : configurationAdapter_(configurationAdapter),
      serviceName_(serviceName),
      stopRequests_(false),
      isShadow_(shadow) {
  int shardCount = configurationAdapter_->getShardCount(serviceName_);
  LOG(INFO) << shardCount << " shards in " << serviceName_;
  shardCache_.resize(shardCount);

  // Initialize counters.
  GorillaStatsManager::addStatExportType(kStaleShardInfoUsed, SUM);
}

class RequestHandler : public apache::thrift::RequestCallback {
 public:
  RequestHandler(
      bool oneway,
      std::function<void(bool, ClientReceiveState& state)> callback)
      : oneway_(oneway), callback_(callback) {}

  void requestSent() override {
    if (oneway_) {
      ClientReceiveState dummyState;
      callback_(true, dummyState);
    }
  }

  void replyReceived(ClientReceiveState&& state) override {
    if (!oneway_) {
      callback_(true, state);
    }
  }

  void requestError(ClientReceiveState&& state) override {
    callback_(false, state);
  }

 private:
  bool oneway_;
  std::function<void(bool, ClientReceiveState&)> callback_;
};

vector<DataPoint> BeringeiNetworkClient::performPut(PutRequestMap& requests) {
  std::atomic<int> numActiveRequests(0);
  std::vector<std::shared_ptr<BeringeiServiceAsyncClient>> clients;
  std::vector<DataPoint> dropped;
  std::mutex droppedMutex;

  for (auto& request : requests) {
    try {
      auto client = getBeringeiThriftClient(request.first);

      // Keep clients alive
      clients.push_back(client);
      std::unique_ptr<apache::thrift::RequestCallback> callback(
          new RequestHandler(
              false, [&](bool success, ClientReceiveState& state) {
                if (success) {
                  try {
                    PutDataResult putDataResult;
                    client->recv_putDataPoints(putDataResult, state);

                    std::lock_guard<std::mutex> guard(droppedMutex);
                    dropped.insert(
                        dropped.end(),
                        std::make_move_iterator(putDataResult.data.begin()),
                        std::make_move_iterator(putDataResult.data.end()));
                  } catch (const std::exception& e) {
                    LOG(ERROR) << "Exception from recv_putData: " << e.what();
                    std::lock_guard<std::mutex> guard(droppedMutex);
                    dropped.insert(
                        dropped.end(),
                        std::make_move_iterator(request.second.data.begin()),
                        std::make_move_iterator(request.second.data.end()));
                  }
                } else {
                  auto exn = state.exceptionWrapper();
                  auto error = exn.what().toStdString();
                  LOG(ERROR) << "putDataPoints Failed. Reason: " << error;

                  std::lock_guard<std::mutex> guard(droppedMutex);
                  dropped.insert(
                      dropped.end(),
                      std::make_move_iterator(request.second.data.begin()),
                      std::make_move_iterator(request.second.data.end()));
                }

                if (--numActiveRequests == 0) {
                  getEventBase()->terminateLoopSoon();
                }
              }));

      client->putDataPoints(std::move(callback), request.second);
      numActiveRequests++;
    } catch (std::exception& e) {
      LOG(ERROR) << e.what();
      std::lock_guard<std::mutex> guard(droppedMutex);
      dropped.insert(
          dropped.end(),
          std::make_move_iterator(request.second.data.begin()),
          std::make_move_iterator(request.second.data.end()));
    }
  }

  if (numActiveRequests > 0) {
    getEventBase()->loopForever();
  }
  return dropped;
}

void markRequestResultFailed(const GetDataRequest& req, GetDataResult& res) {
  res.results.clear();
  res.results.resize(req.keys.size());
  for (auto& tsData : res.results) {
    tsData.status = StatusCode::RPC_FAIL;
  }
}

void BeringeiNetworkClient::performGet(GetRequestMap& requests) {
  std::atomic<int> numActiveRequests(0);
  std::vector<std::shared_ptr<BeringeiServiceAsyncClient>> clients;

  std::shared_ptr<BeringeiServiceAsyncClient> client = nullptr;
  for (auto& request : requests) {
    try {
      client = getBeringeiThriftClient(request.first);
    } catch (const std::exception& e) {
      LOG(ERROR) << "Failed to construct BeringeiServiceAsyncClient for"
                 << " host:port " << request.first.first << ":"
                 << request.first.second << " with exception: " << e.what();
      // Mark all the results as RPC_FAIL
      markRequestResultFailed(request.second.first, request.second.second);
      continue;
    }

    // Keep clients alive
    clients.push_back(client);
    std::unique_ptr<apache::thrift::RequestCallback> callback(
        new RequestHandler(false, [&](bool success, ClientReceiveState& state) {
          if (success) {
            try {
              client->recv_getData(request.second.second, state);
            } catch (const std::exception& e) {
              LOG(ERROR) << "Exception from recv_getData: " << e.what();
              // Mark all the results as RPC_FAIL
              markRequestResultFailed(
                  request.second.first, request.second.second);
            }
          } else {
            auto exn = state.exceptionWrapper();
            auto error = exn.what().toStdString();
            LOG(ERROR) << "getData failed. Reason: " << error;

            markRequestResultFailed(
                request.second.first, request.second.second);
          }

          if (--numActiveRequests == 0) {
            getEventBase()->terminateLoopSoon();
          }
        }));

    client->getData(std::move(callback), request.second.first);
    numActiveRequests++;
  }

  if (numActiveRequests > 0) {
    getEventBase()->loopForever();
  }
}

folly::Future<GetDataResult> BeringeiNetworkClient::performGet(
    const std::pair<std::string, int>& hostInfo,
    const GetDataRequest& request) {
  return getBeringeiThriftClient(hostInfo)->future_getData(request);
}

void BeringeiNetworkClient::performShardDataBucketGet(
    int64_t begin,
    int64_t end,
    int64_t shardId,
    int32_t offset,
    int32_t limit,
    GetShardDataBucketResult& result) {
  std::pair<std::string, int> hostInfo;
  bool success = getHostForShard(shardId, hostInfo);

  if (!success) {
    result.status = StatusCode::RPC_FAIL;
    LOG(ERROR) << "Could not get host for shard " << shardId;
    return;
  }

  std::shared_ptr<BeringeiServiceAsyncClient> client = nullptr;
  try {
    client = this->getBeringeiThriftClient(hostInfo);
  } catch (const std::exception& e) {
    result.status = StatusCode::RPC_FAIL;
    LOG(ERROR) << "Failed to construct BeringeiServiceAsyncClient for"
               << " host:port " << hostInfo.first << ":" << hostInfo.second
               << " with exception: " << e.what();
    return;
  }

  try {
    client->sync_getShardDataBucket(result, begin, end, shardId, offset, limit);
  } catch (const std::exception& e) {
    result.status = StatusCode::RPC_FAIL;
    LOG(ERROR) << "Got exception talking to Gorilla: " << e.what();
  }
}

bool BeringeiNetworkClient::getShardKeys(
    int shardNumber,
    int limit,
    int offset,
    std::vector<KeyUpdateTime>& keys) {
  std::pair<std::string, int> hostInfo;
  if (!getHostForShard(shardNumber, hostInfo)) {
    throw std::runtime_error(
        folly::format("Couldn't find shard owner {}", shardNumber).str());
  }

  std::shared_ptr<BeringeiServiceAsyncClient> client =
      getBeringeiThriftClient(hostInfo);

  GetLastUpdateTimesRequest req;
  GetLastUpdateTimesResult result;
  req.offset = offset;
  req.shardId = shardNumber;
  req.limit = limit;

  client->sync_getLastUpdateTimes(result, req);
  keys = std::move(result.keys);
  return result.moreResults;
}

void BeringeiNetworkClient::getLastUpdateTimesForHost(
    uint32_t minLastUpdateTime,
    uint32_t maxKeysPerRequest,
    const std::string& host,
    int port,
    const std::vector<int64_t>& shards,
    uint32_t timeoutSeconds,
    std::function<bool(const std::vector<KeyUpdateTime>& keys)> callback) {
  time_t startTime = time(nullptr);

  stopRequests_ = false;

  std::unique_lock<std::mutex> lock(stoppingMutex_);

  try {
    auto client = getBeringeiThriftClient({host, port});
    for (auto& shard : shards) {
      GetLastUpdateTimesRequest req;
      GetLastUpdateTimesResult result;
      req.offset = 0;
      req.shardId = shard;
      req.limit = maxKeysPerRequest;
      bool continueOperation = true;

      do {
        bool success = false;
        try {
          client->sync_getLastUpdateTimes(result, req);
          success = true;
        } catch (std::exception& e) {
          LOG(ERROR) << e.what();
        }

        if (success) { // we got results back
          continueOperation = callback(result.keys);
          if (!result.moreResults) {
            break;
          }
          req.offset += maxKeysPerRequest;
        }

        if (continueOperation) {
          continueOperation = time(nullptr) - startTime < timeoutSeconds &&
              !stopRequests_.load();
        }

        if (continueOperation && !success) {
          // Let's not hammer with immediate requests in the loop and
          // let Beringei to take a rest for a little bit.
          continueOperation = !stopping_.wait_for(
              lock, std::chrono::seconds(kSleepBetweenRetrySecs), [this]() {
                return stopRequests_.load();
              });
        }
      } while (continueOperation);

      if (!continueOperation) {
        LOG(INFO) << "Operation stopped by the caller or a timeout was reached";
        break;
      }
    }
  } catch (std::exception& e) {
    LOG(ERROR) << e.what();
  }
}

void BeringeiNetworkClient::getLastUpdateTimes(
    uint32_t minLastUpdateTime,
    uint32_t maxKeysPerRequest,
    uint32_t timeoutSeconds,
    std::function<bool(const std::vector<KeyUpdateTime>& keys)> callback) {
  int numShards = getNumShards();
  std::map<std::pair<std::string, int>, std::vector<int64_t>> shardsPerHost;

  for (int i = 0; i < numShards; i++) {
    std::pair<std::string, int> hostInfo;
    if (getHostForShard(i, hostInfo)) {
      shardsPerHost[hostInfo].push_back(i);
    } else {
      LOG(WARNING) << "Nobody owns shard " << i;
    }
  }

  std::vector<std::thread> threads;
  for (auto& iter : shardsPerHost) {
    threads.push_back(std::thread(
        &BeringeiNetworkClient::getLastUpdateTimesForHost,
        this,
        minLastUpdateTime,
        maxKeysPerRequest,
        iter.first.first,
        iter.first.second,
        iter.second,
        timeoutSeconds,
        callback));
  }

  for (auto& thread : threads) {
    thread.join();
  }
}

bool BeringeiNetworkClient::addDataPointToRequest(
    DataPoint& dp,
    PutRequestMap& requests,
    bool& dropped) {
  std::pair<std::string, int> hostInfo;
  bool success = getHostForShard(dp.key.shardId, hostInfo);
  if (!success) {
    if (!isShadow_) {
      dropped = true;
    }
    return true;
  }

  requests[hostInfo].data.push_back(dp);
  return requests[hostInfo].data.size() < FLAGS_gorilla_max_batch_size;
}

void BeringeiNetworkClient::addKeyToGetRequest(
    const Key& key,
    GetRequestMap& requests) {
  addKeyToRequest<GetRequestMap>(key, requests);
}

void BeringeiNetworkClient::addKeyToGetRequest(
    size_t index,
    const Key& key,
    MultiGetRequestMap& requests) {
  std::pair<std::string, int> hostInfo;
  bool success = getHostForShard(key.shardId, hostInfo);
  if (!success) {
    return;
  }

  requests[hostInfo].first.keys.push_back(key);
  requests[hostInfo].second.push_back(index);
}

uint32_t BeringeiNetworkClient::getTimeoutMs() {
  return (FLAGS_gorilla_processing_timeout == 0)
      ? kDefaultThriftTimeoutMs
      : FLAGS_gorilla_processing_timeout;
}

std::shared_ptr<BeringeiServiceAsyncClient>
BeringeiNetworkClient::getBeringeiThriftClient(
    const std::pair<std::string, int>& hostInfo) {
  folly::SocketAddress address(hostInfo.first, hostInfo.second, true);
  auto socket =
      apache::thrift::async::TAsyncSocket::newSocket(getEventBase(), address);
  auto channel =
      apache::thrift::HeaderClientChannel::newChannel(std::move(socket));
  channel->setTimeout(getTimeoutMs());
  return std::make_shared<BeringeiServiceAsyncClient>(std::move(channel));
}

void BeringeiNetworkClient::invalidateCache(
    const std::unordered_set<int64_t>& shardIds) {
  folly::RWSpinLock::WriteHolder guard(&shardCacheLock_);
  for (int64_t shardId : shardIds) {
    if (shardId < 0 || shardId >= shardCache_.size()) {
      // Ignore invalid shardId
      continue;
    }
    auto& entry = shardCache_[shardId];
    if (entry) {
      entry.reset(nullptr);
    }
  }
}

bool BeringeiNetworkClient::getHostForShard(
    int64_t shardId,
    std::pair<std::string, int>& hostInfo) {
  if (shardId < 0 || shardId >= shardCache_.size()) {
    if (!isShadow_) {
      LOG(ERROR) << "Invalid shard id : " << shardId;
    }
    return false;
  }

  bool cachedEntry = false;
  {
    folly::RWSpinLock::ReadHolder guard(&shardCacheLock_);

    auto& entry = shardCache_[shardId];
    if (entry) {
      cachedEntry = true;
      hostInfo = make_pair(entry->hostAddress, entry->port);
      if (hostInfo.first != "" &&
          entry->updateTime > time(nullptr) - FLAGS_gorilla_shard_cache_ttl) {
        return true;
      } else if (
          hostInfo.first == "" &&
          entry->updateTime >
              time(nullptr) - FLAGS_gorilla_negative_shard_cache_ttl) {
        // Negatively cached entry to avoid querying servicerouter
        // constantly.
        return false;
      } // else the cached entry has outlived its TTL.
    }
  }

  try {
    // It is totally possible that multiple threads enter this section
    // and fetch the host information for the same shard id, but it
    // doesn't actually hurt that much because fetching the host
    // information is really fast.
    if (configurationAdapter_->getHostForShardId(
            shardId, serviceName_, hostInfo)) {
      addCacheEntry(shardId, hostInfo);
      return true;
    } else {
      LOG(WARNING) << "No host in directory service for shard : " << shardId;
      return getHostForShardOnFailure(cachedEntry, shardId, hostInfo);
    }
  } catch (std::exception& e) {
    LOG(ERROR) << e.what();
    return getHostForShardOnFailure(cachedEntry, shardId, hostInfo);
  }

  return false;
}

bool BeringeiNetworkClient::getHostForShardOnFailure(
    bool cachedEntry,
    int64_t shardId,
    std::pair<std::string, int>& hostInfo) {
  if (cachedEntry) {
    useStaleCacheEntry(shardId, hostInfo);
    return hostInfo.first != "";
  } else {
    // Store an empty cache entry to avoid constantly querying the
    // same shard from service router. TTL will expire this entry
    // like all the other entries.
    addCacheEntry(shardId, {"", 0});
    return false;
  }
}

void BeringeiNetworkClient::useStaleCacheEntry(
    int64_t shardId,
    const std::pair<std::string, int>& hostInfo) {
  LOG(WARNING) << "Using possibly stale cache entry for another "
               << FLAGS_gorilla_shard_cache_ttl << " seconds.";
  addCacheEntry(shardId, hostInfo);
  GorillaStatsManager::addStatValue(kStaleShardInfoUsed);
}

void BeringeiNetworkClient::addCacheEntry(
    int64_t shardId,
    const std::pair<std::string, int>& hostInfo) {
  ShardCacheEntry* entryPtr = new ShardCacheEntry;

  entryPtr->hostAddress = hostInfo.first;
  entryPtr->port = hostInfo.second;
  entryPtr->updateTime = time(nullptr);

  folly::RWSpinLock::WriteHolder guard(&shardCacheLock_);
  auto& entry = shardCache_[shardId];
  entry.reset(entryPtr);
}

void BeringeiNetworkClient::stopRequests() {
  stopRequests_ = true;
  stopping_.notify_all();
}

std::string BeringeiNetworkClient::getServiceName() {
  return serviceName_;
}

bool BeringeiNetworkClient::isCorrespondingService(
    const std::string& serviceName) {
  return serviceName_ == serviceName;
}
}
} // facebook:gorilla
