#pragma once

#include "beringei/client/RequestBatchingQueue.h"

namespace facebook {
namespace gorilla {

class WriteClient {
 public:
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

  size_t getNumShards() const {
    return client->getNumShards();
  }

  RequestBatchingQueue queue;
  std::unique_ptr<BeringeiNetworkClient> client;
};

} // namespace gorilla
} // namespace facebook
