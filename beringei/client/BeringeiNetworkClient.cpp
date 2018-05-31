/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "beringei/client/BeringeiNetworkClient.h"

#include <atomic>

#include <folly/Conv.h>
#include <folly/SocketAddress.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/futures/Future.h>
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
using std::unique_ptr;
using std::vector;

namespace facebook {
namespace gorilla {

static const std::string kStaleShardInfoUsed =
    "gorilla_network_client.stale_shard_info_used";

static const int kDefaultThriftTimeoutMs = 2 * kGorillaMsPerSecond;

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

folly::Future<std::vector<DataPoint>> BeringeiNetworkClient::futurePerformPut(
    PutDataRequest& request,
    const std::pair<std::string, int>& hostInfo) {
  auto client =
      getBeringeiThriftClient(hostInfo, folly::getIOExecutor()->getEventBase());
  if (isShadow()) {
    client->future_putDataPoints(request);
    return folly::makeFuture(std::vector<DataPoint>({}));
  } else {
    return client->future_putDataPoints(request)
        .then([](PutDataResult&& result) { return std::move(result.data); })
        .onError([dps = request.data](const std::exception& e) mutable {
          LOG(ERROR) << "putDataPoints failed: " << e.what();
          return std::move(dps);
        });
  }
}

vector<DataPoint> BeringeiNetworkClient::performPut(PutRequestMap& requests) {
  std::vector<folly::Future<std::vector<DataPoint>>> pendingResponses;
  pendingResponses.reserve(requests.size());
  for (auto& mapEntry : requests) {
    auto& hostInfo = mapEntry.first;
    auto& request = mapEntry.second;
    pendingResponses.push_back(futurePerformPut(request, hostInfo));
  }

  std::vector<DataPoint> result;

  folly::collectAllSemiFuture(pendingResponses)
      .toUnsafeFuture()
      .then([&](auto&& responses) {
        for (auto& maybeDropped : responses) {
          auto& dps = maybeDropped.value();
          result.insert(
              result.end(),
              std::make_move_iterator(dps.begin()),
              std::make_move_iterator(dps.end()));
        }
      })
      .get();
  return result;
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
            auto exn = state.exception();
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
    const GetDataRequest& request,
    folly::EventBase* eb) {
  return getBeringeiThriftClient(hostInfo, eb)->future_getData(request);
}

void BeringeiNetworkClient::performScanShard(
    const ScanShardRequest& request,
    ScanShardResult& result) {
  std::pair<std::string, int> hostInfo;
  bool success = getHostForShard(request.shardId, hostInfo);

  if (!success) {
    result.status = StatusCode::RPC_FAIL;
    LOG(ERROR) << "Could not get host for shard " << request.shardId;
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
    client->sync_scanShard(result, request);
  } catch (const std::exception& e) {
    result.status = StatusCode::RPC_FAIL;
    LOG(ERROR) << "Got exception talking to Gorilla: " << e.what();
  }
}

folly::Future<ScanShardResult> BeringeiNetworkClient::performScanShard(
    const std::pair<std::string, int>& hostInfo,
    const ScanShardRequest& request,
    folly::EventBase* eb) {
  return getBeringeiThriftClient(hostInfo, eb)->future_scanShard(request);
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
    uint32_t /*minLastUpdateTime*/,
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

int BeringeiNetworkClient::getShardForDataPoint(const DataPoint& dp) {
  return dp.key.shardId;
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
    const std::pair<std::string, int>& hostInfo,
    folly::EventBase* eb) {
  folly::SocketAddress address(hostInfo.first, hostInfo.second, true);
  auto socket = apache::thrift::async::TAsyncSocket::newSocket(eb, address);
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

bool BeringeiNetworkClient::isShadow() const {
  return isShadow_;
}

} // namespace gorilla
} // namespace facebook
