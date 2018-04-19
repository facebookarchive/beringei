#pragma once

#include <thread>

#include <folly/experimental/FunctionScheduler.h>
#include <folly/experimental/observer/Observer.h>
#include <folly/experimental/observer/SimpleObservable.h>
#include <folly/stop_watch.h>

#include "beringei/client/BeringeiHostWriter.h"
#include "beringei/client/BeringeiNetworkClient.h"
#include "beringei/client/RequestBatchingQueue.h"

DECLARE_int32(gorilla_retry_queue_capacity);
DECLARE_int32(gorilla_retry_delay_secs);
DECLARE_int32(gorilla_write_retry_threads);
DECLARE_int32(gorilla_max_retry_batch_size);
DECLARE_int32(gorilla_retry_batch_timeout_ms);

namespace facebook {
namespace gorilla {
class WriteClient {
 public:
  WriteClient(
      std::unique_ptr<BeringeiNetworkClient> networkClient,
      size_t queueCapacity,
      size_t queueSize);

  size_t getNumShards() const;
  void start();
  void stop();
  void retry(std::vector<DataPoint> dropped);

  void putWithRetry(
      PutDataRequest& request,
      std::shared_ptr<BeringeiHostWriter> hostWriter);

  std::vector<DataPoint> putWithStats(
      int points,
      BeringeiNetworkClient::PutRequestMap& requestMap);

  template <typename PointCollection>
  void drop(PointCollection points);

  std::size_t getSnapshotVersion();
  // Return the hostInfo for reach shard.
  std::vector<std::pair<std::string, int>> getHostsSnapshot();

  RequestBatchingQueue queue;
  std::unique_ptr<BeringeiNetworkClient> client;

 private:
  // Send data until reading an empty request.
  void writeDataPointsForever();

  struct RetryOperation {
    std::vector<DataPoint> dataPoints;
    uint32_t retryTimeSecs;
  };

  using ShardCacheEntry = std::pair<std::string, int>;
  using ShardCache = std::vector<ShardCacheEntry>;

  // Retry logic
  void retryThread();
  void logDroppedDataPoints(uint32_t dropped, const std::string& msg);

  std::vector<DataPoint> dropBatch_;
  folly::stop_watch<std::chrono::milliseconds> dropWatch_;
  folly::SharedMutex dropLock_;

  folly::MPMCQueue<RetryOperation> retryQueue_;
  std::atomic<int> numRetryQueuedDataPoints_{0};
  std::vector<std::thread> retryWriters_;
  // End Retry

  // Shard Cache
  folly::observer::SimpleObservable<ShardCache> shardCache_;
  folly::observer::TLObserver<ShardCache> shardCacheObserver_;
  folly::FunctionScheduler shardUpdaterThread_;

  std::shared_ptr<ShardCache> initShardCache();
  void updateShardCache();

  // End Shard Cache

  std::string counterUsPerPut_;
  std::string counterPutKey_;
  std::string counterPutRetryKey_;
  std::string counterPutRejected_;
  std::string counterPutDroppedKey_;
  std::string counterThrottle_;
};

template <typename PointCollection>
void WriteClient::drop(PointCollection points) {
  folly::SharedMutex::WriteHolder guard(dropLock_);

  if (dropBatch_.size() == 0) {
    dropWatch_.reset();
  }

  dropBatch_.insert(
      dropBatch_.end(),
      std::make_move_iterator(points.begin()),
      std::make_move_iterator(points.end()));

  if (dropBatch_.size() >= FLAGS_gorilla_max_retry_batch_size ||
      dropWatch_.elapsed().count() >= FLAGS_gorilla_retry_batch_timeout_ms) {
    retry(std::move(dropBatch_));
    dropBatch_.clear();
  }
}

} // namespace gorilla
} // namespace facebook
