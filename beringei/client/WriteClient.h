#pragma once

#include <thread>

#include "beringei/client/BeringeiNetworkClient.h"
#include "beringei/client/RequestBatchingQueue.h"

DECLARE_int32(gorilla_retry_queue_capacity);
DECLARE_int32(gorilla_retry_delay_secs);
DECLARE_int32(gorilla_write_retry_threads);

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
  std::vector<DataPoint> putWithStats(
      int points,
      BeringeiNetworkClient::PutRequestMap& requestMap);

  RequestBatchingQueue queue;
  std::unique_ptr<BeringeiNetworkClient> client;

 private:
  struct RetryOperation {
    std::vector<DataPoint> dataPoints;
    uint32_t retryTimeSecs;
  };

  // Retry logic
  void retryThread();
  void logDroppedDataPoints(uint32_t dropped, const std::string& msg);

  folly::MPMCQueue<RetryOperation> retryQueue_;
  std::atomic<int> numRetryQueuedDataPoints_{0};
  std::vector<std::thread> retryWriters_;
  // End Retry

  std::string counterUsPerPut_;
  std::string counterPutKey_;
  std::string counterPutRetryKey_;
  std::string counterPutDroppedKey_;
};

} // namespace gorilla
} // namespace facebook
