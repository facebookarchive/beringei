/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <functional>
#include <vector>

#include <folly/MPMCQueue.h>

#include "beringei/if/gen-cpp2/beringei_data_types.h"

namespace facebook {
namespace gorilla {

// This class acts as a queue but batches similar items together.
class RequestBatchingQueue {
 public:
  // Capacity is the the total number of DataPoint objects that the
  // queue can hold.
  explicit RequestBatchingQueue(size_t queueCapacity, size_t queueSize)
      : capacity_(queueCapacity), queue_(queueSize), numQueuedDataPoints_(0) {}

  // Pushes points in the queue. Returns true when all the points were
  // pushed to the queue. Returns false when the points were not
  // pushed. The vector is modified (points are moved) on a successful
  // call.
  bool push(std::vector<DataPoint>& points);

  // Pops elements from the queue and calls the callback for each data
  // point. Callback should return false when popping should be
  // stopped. Some items will still be sent to the callback after this
  // because the queue actually stores vectors and not individual data
  // points.
  //
  // Returns the number of points popped and whether the caller should continue
  // asking for more data in the future.
  std::pair<bool, int> pop(std::function<bool(DataPoint& dp)> popCallback);

  // Same as pop but it doesn't return anything and keeps popping until the
  // popCallback returns false, even if the queue is empty.
  void popForever(
      std::function<bool(DataPoint& dp)> popCallback,
      std::function<bool()> timeoutCallback);

  // Causes n future calls to pop() to return false once everything currently in
  // the queue has been processed.
  void flush(int n);

  int size() {
    return numQueuedDataPoints_;
  }

 private:
  const size_t capacity_;
  folly::MPMCQueue<std::vector<DataPoint>> queue_;
  std::atomic<int> numQueuedDataPoints_;
};

} // namespace gorilla
} // namespace facebook
