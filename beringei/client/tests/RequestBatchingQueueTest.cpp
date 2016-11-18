/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <gtest/gtest.h>

#include "beringei/client/RequestBatchingQueue.h"

using namespace ::testing;
using namespace facebook::gorilla;
using namespace std;

std::function<bool(DataPoint& dp)> getPopCallback(
    std::vector<PutDataRequest>& requests,
    int limit) {
  return [&requests, limit](DataPoint& dp) {
    requests[dp.key.shardId].data.push_back(dp);
    return requests[dp.key.shardId].data.size() < limit;
  };
}

TEST(BatchingQueueTest, PushPop) {
  vector<DataPoint> points;
  for (int i = 0; i < 5; i++) {
    DataPoint point;
    point.key.shardId = i % 3;
    points.push_back(point);
  }

  RequestBatchingQueue queue(5, 5);
  std::vector<DataPoint> v0 = {points[0], points[1], points[2]};
  std::vector<DataPoint> v1 = {points[3], points[4]};

  std::vector<DataPoint> r0 = {points[0], points[3]};
  std::vector<DataPoint> r1 = {points[1], points[4]};
  std::vector<DataPoint> r2 = {points[2]};

  EXPECT_TRUE(queue.push(v0));
  EXPECT_TRUE(queue.push(v1));
  ASSERT_EQ(5, queue.size());

  std::vector<PutDataRequest> requests(3);
  EXPECT_EQ(5, queue.pop(getPopCallback(requests, 100)).second);
  EXPECT_EQ(3, requests.size());
  EXPECT_EQ(r0, requests[0].data);
  EXPECT_EQ(r1, requests[1].data);
  EXPECT_EQ(r2, requests[2].data);
}

TEST(BatchingQueueTest, PushTooMany) {
  vector<DataPoint> points;
  for (int i = 0; i < 5; i++) {
    DataPoint point;
    point.key.shardId = i % 3;
    points.push_back(point);
  }

  RequestBatchingQueue queue(4, 4);
  std::vector<DataPoint> v0 = {points[0], points[1]};
  std::vector<DataPoint> v1 = {points[2], points[3]};
  std::vector<DataPoint> v2 = {points[4]};

  EXPECT_TRUE(queue.push(v0));
  EXPECT_TRUE(queue.push(v1));
  EXPECT_FALSE(queue.push(v2));

  std::vector<DataPoint> r0 = {points[0], points[3]};
  std::vector<DataPoint> r1 = {points[1]};
  std::vector<DataPoint> r2 = {points[2]};

  std::vector<PutDataRequest> requests(3);
  EXPECT_EQ(4, queue.pop(getPopCallback(requests, 100)).second);
  EXPECT_EQ(r0, requests[0].data);
  EXPECT_EQ(r1, requests[1].data);
  EXPECT_EQ(r2, requests[2].data);
}

TEST(BatchingQueueTest, LimitedPop) {
  RequestBatchingQueue queue(10, 10);
  for (int i = 0; i < 10; i++) {
    vector<DataPoint> points;
    DataPoint point;
    point.key.shardId = i % 3;
    points.push_back(point);
    ASSERT_TRUE(queue.push(points));
  }

  std::vector<PutDataRequest> requests(3);
  EXPECT_EQ(4, queue.pop(getPopCallback(requests, 2)).second);

  requests.clear();
  requests.resize(3);
  EXPECT_EQ(4, queue.pop(getPopCallback(requests, 2)).second);

  requests.clear();
  requests.resize(3);
  EXPECT_EQ(2, queue.pop(getPopCallback(requests, 2)).second);
}

TEST(BatchingQueueTest, Flush) {
  RequestBatchingQueue queue(10, 10);

  vector<DataPoint> points = {DataPoint{}};
  queue.push(points);
  queue.flush(1);
  points = {DataPoint{}, DataPoint{}};
  queue.push(points);
  queue.flush(2);
  points = {DataPoint{}};
  queue.push(points);

  std::vector<std::pair<bool, int>> results = {
      {false, 1}, {false, 2}, {false, 0}, {true, 1}};

  std::vector<PutDataRequest> requests(3);
  for (int i = 0; i < results.size(); i++) {
    EXPECT_EQ(results[i], queue.pop(getPopCallback(requests, 5)));
  }
  EXPECT_EQ(0, queue.size());
}
