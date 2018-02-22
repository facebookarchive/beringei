/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <string>
#include <vector>

#include <folly/Executor.h>
#include <folly/experimental/FunctionScheduler.h>

#ifndef BERINGEI_AUTOSETUP
#include <folly/executors/GlobalExecutor.h>
#include <folly/synchronization/RWSpinLock.h>
#else
#include <wangle/concurrent/GlobalExecutor.h>
#include <folly/RWSpinLock.h>
#endif

#include "beringei/client/BeringeiConfigurationAdapterIf.h"
#include "beringei/client/BeringeiGetResult.h"
#include "beringei/client/BeringeiNetworkClient.h"
#include "beringei/client/RequestBatchingQueue.h"

namespace facebook {
namespace fb303 {
class FacebookBase2;
}
}

namespace facebook {
namespace gorilla {
using GorillaResultVector =
    std::vector<std::pair<Key, std::vector<TimeValuePair>>>;
using GorillaServicesVector = std::vector<std::string>;

class BeringeiClientImpl {
 public:
  // Don't create writer threads.
  static const int kNoWriterThreads;

  // Don't update read services from SMC.
  static const int kNoReadServicesUpdates;

  // Use the default interval for updating read services.
  static const int kDefaultReadServicesUpdateInterval;

  // Read-Write Gorilla Client. If properties are not provided,
  // command-line flags will be used instead.
  // testClient allows unit tests to pass in mocks.
  explicit BeringeiClientImpl(
      std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
      bool throwExceptionOnTransientFailure = false);

  virtual ~BeringeiClientImpl();

  // Enqueue data points to be sent to Gorilla. After this call, the
  // DataPoints will no longer be valid. Returns true if all the data
  // points were pushed to any of the writers. False means that data
  // was definitely dropped.
  virtual bool putDataPoints(std::vector<DataPoint>& values);

  // @see BeringeiNetworkClient
  void getLastUpdateTimes(
      uint32_t minLastUpdateTime,
      uint32_t maxKeysPerRequest,
      uint32_t timeoutSeconds,
      std::function<bool(const std::vector<KeyUpdateTime>& keys)> callback);

  // Get compressed data points from Gorilla.
  // If set, serviceOverride bypasses the gorilla_read_services property.
  virtual void get(
      GetDataRequest& request,
      GetDataResult& result,
      const std::string& serviceOverride = "");

  // Get unpacked data points from Gorilla.
  // If set, serviceOverride bypasses the gorilla_read_services property.
  void get(
      GetDataRequest& request,
      GorillaResultVector& result,
      const std::string& serviceOverride = "");

  // Get unpacked data points from Gorilla.
  // If set, serviceOverride bypasses the gorilla_read_services property.
  virtual BeringeiGetResult get(
      GetDataRequest& request,
      const std::string& serviceOverride = "");

  folly::Future<BeringeiGetResult> futureGet(
      GetDataRequest& request,
      folly::EventBase* eb,
#ifndef BERINGEI_AUTOSETUP
      folly::Executor* workExecutor = folly::getCPUExecutor().get(),
#else
      folly::Executor* workExecutor = wangle::getCPUExecutor().get(),
#endif
      const std::string& serviceOverride = "");

  // Returns true if reading from gorilla is enabled, false otherwise.
  bool isReadingEnabled() {
    folly::RWSpinLock::ReadHolder guard(&readClientLock_);
    return readClients_.size() > 0;
  }

  int64_t getNumShards();

  // Return the number of shards from the write client.
  // Used in cases where only write clients are created.
  int64_t getNumShardsFromWriteClient();

  void stopRequests();

  // Fetch all data for the given shard for a time-window range.
  void scanShard(const ScanShardRequest& request, ScanShardResult& result);

  void flushQueue();

  virtual std::shared_ptr<BeringeiNetworkClient> createNetworkClient(
      const std::string& serviceName,
      std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
      bool shadow);

  virtual std::unique_ptr<BeringeiNetworkClient> createUniqueNetworkClient(
      const std::string& serviceName,
      std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
      bool shadow);

  struct WriteClient {
    WriteClient(
        std::unique_ptr<BeringeiNetworkClient> networkClient,
        size_t queueCapacity,
        size_t queueSize)
        : queue(queueCapacity, queueSize), client(std::move(networkClient)) {}
    WriteClient(
        BeringeiNetworkClient* networkClient,
        size_t queueCapacity,
        size_t queueSize)
        : queue(queueCapacity, queueSize), client(networkClient) {}
    RequestBatchingQueue queue;
    std::unique_ptr<BeringeiNetworkClient> client;
  };

  void initialize(
      int queueCapacity,
      int writerThreads,
      int readServicesUpdateInterval);

  void initializeTestClients(
      int queueCapacity,
      int writerThreads,
      BeringeiNetworkClient* testClient,
      BeringeiNetworkClient* shadowTestClient);

 protected:
  // Constructor that does nothing. Used from tests.
  explicit BeringeiClientImpl() {}

  std::vector<std::shared_ptr<BeringeiNetworkClient>> getAllReadClients(
      const std::string& serviceOverride);
  std::shared_ptr<BeringeiNetworkClient> getReadClientCopy();

  std::vector<std::unique_ptr<WriteClient>> writeClients_;

 private:
  // Make a get request with a specific BeringeiNetworkClient
  // Populates result with the results, foundKeys with keys that were
  // retrieved, unownedKeys with keys that hosts responded they did
  // not own the shard for, and inProgressKeys with keys that are owned
  // but not fully loaded yet. If inProgressKeys is nullptr, foundKeys
  // is also populated with keys that are in progress, and the equivalent
  // is true of partialDataKeys (those keys where there is a hole in the data).
  void getWithClient(
      BeringeiNetworkClient& readClient,
      const GetDataRequest& request,
      GetDataResult& result,
      std::vector<Key>& foundKeys,
      std::vector<Key>& unownedKeys,
      std::vector<Key>* inProgressKeys,
      std::vector<Key>* partialDataKeys);

  // Send data until reading an empty request.
  void writeDataPointsForever(WriteClient* writeClient);

  std::vector<std::string> selectReadServices();

  void updateReadServices();

  void retryThread();

  void logDroppedDataPoints(
      BeringeiNetworkClient* client,
      uint32_t dropped,
      const std::string& msg);

  std::vector<DataPoint> putWithStats(
      BeringeiNetworkClient* client,
      int points,
      BeringeiNetworkClient::PutRequestMap& requestMap);

  void initBeringeiNetworkClients(
      std::vector<std::shared_ptr<BeringeiNetworkClient>>& clients,
      const std::vector<std::string>& readServices);

  void startWriterThreads(int numWriterThreads);
  void stopWriterThreads();

  void setQueueCapacity(int& capacity);
  void setNumWriterThreads(int& writerThreads);

  std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter_;

  std::vector<std::shared_ptr<BeringeiNetworkClient>> readClients_;

  std::vector<std::thread> writers_;

  std::vector<std::string> currentReadServices_;
  folly::FunctionScheduler readServicesUpdateScheduler_;
  folly::RWSpinLock readClientLock_;

  struct RetryOperation {
    BeringeiNetworkClient* client;
    std::vector<DataPoint> dataPoints;
    uint32_t retryTimeSecs;
  };

  bool throwExceptionOnTransientFailure_;
  folly::MPMCQueue<RetryOperation> retryQueue_;
  std::atomic<int> numRetryQueuedDataPoints_;
  std::vector<std::thread> retryWriters_;
};
}
} // facebook:gorilla
