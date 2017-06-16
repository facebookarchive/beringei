/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "beringei/client/tests/MockConfigurationAdapter.h"
#include "beringei/lib/BucketMap.h"
#include "beringei/lib/FileUtils.h"
#include "beringei/lib/GorillaStatsManager.h"
#include "beringei/lib/GorillaTimeConstants.h"
#include "beringei/lib/TimeSeries.h"
#include "beringei/lib/tests/MockMemoryUsageGuard.h"
#include "beringei/service/BeringeiServiceHandler.h"

#include <folly/Range.h>
#include <folly/String.h>

using namespace facebook;
using namespace facebook::gorilla;
using namespace std;

DECLARE_string(data_directory);
DECLARE_int32(add_shard_threads);
DECLARE_bool(create_directories);
DECLARE_int32(buckets);
DECLARE_int32(bucket_size);
DECLARE_int32(allowed_timestamp_behind);
DECLARE_int32(gorilla_shards);
DECLARE_int32(allowed_timestamp_ahead);
DECLARE_bool(disable_shard_refresh);

const double kDefaultValue = 12345;

// Using this value in a key or entity will lead to data loss
constexpr folly::StringPiece kDelimiter = "\x01";
const static int kMaxEntityLength = 128;
const static int kMaxKeyLength = 236;

static bool allCharactersAllowed(const std::string& str) {
  for (auto& c : str) {
    if (c < 32 || c > 126) {
      return false;
    }
  }

  return true;
}

static bool toKey(
    const std::string& entity,
    const std::string& key,
    int numShards,
    Key& out) {
  if (entity.length() == 0 || key.length() == 0 ||
      entity.length() > kMaxEntityLength || key.length() > kMaxKeyLength) {
    return false;
  }

  if (!allCharactersAllowed(entity) || !allCharactersAllowed(key)) {
    return false;
  }

  std::string keyEntity = key + kDelimiter.str() + entity;
  out.key = keyEntity + kDelimiter.str() + "1";

  // Downcase before sharding to ensure case-insensitivity.
  for (char& c : keyEntity) {
    c = tolower(c);
  }
  std::hash<std::string> hash;
  size_t hashValue = hash(keyEntity);
  if (numShards != 0) {
    out.shardId = hashValue % numShards;
  } else {
    out.shardId = hashValue;
  }

  return true;
}

class BeringeiServiceHandlerTest : public testing::Test {
 public:
  void SetUp() override {
    FLAGS_add_shard_threads = 1;
    FLAGS_create_directories = true;
    FLAGS_buckets = 6;
    FLAGS_bucket_size = 4 * kGorillaSecondsPerHour;
    FLAGS_disable_shard_refresh = true;
    FLAGS_gorilla_shards = 100;
  }

  std::unique_ptr<GetDataRequest> generateGetRequest(
      int numKeys,
      int64_t begin,
      int64_t end,
      string keyPrefix = "dummy_key",
      int64_t shardId = 0) {
    std::unique_ptr<GetDataRequest> request(new GetDataRequest);
    for (int i = 0; i < numKeys; i++) {
      Key key;
      key.key = keyPrefix + to_string(i);
      key.shardId = shardId;
      request->keys.push_back(key);
    }
    request->begin = begin;
    request->end = end;

    return request;
  }

  std::unique_ptr<PutDataRequest> generatePutRequest(
      int numKeys,
      int64_t begin,
      int64_t end,
      string keyPrefix = "dummy_key",
      int64_t shardId = 0) {
    int simCategoryId = 0;
    std::unordered_map<int32_t, int64_t> tmp;
    std::unique_ptr<PutDataRequest> request(new PutDataRequest);
    for (int i = 0; i < numKeys; i++) {
      Key key;
      key.key = keyPrefix + to_string(i);
      key.shardId = shardId;
      for (int j = begin; j <= end; j += 60) {
        DataPoint dp;
        dp.key = key;
        dp.value.unixTime = j;
        dp.value.value = kDefaultValue;
        dp.categoryId = (simCategoryId++) % 800;
        request->data.push_back(dp);
      }
    }

    return request;
  }

  std::unique_ptr<PutDataRequest> generatePutRequest(
      int numKeys,
      int64_t begin,
      int64_t end,
      uint16_t categoryId,
      string entity,
      string keyPrefix,
      int numShards) {
    std::unique_ptr<PutDataRequest> request(new PutDataRequest);
    for (int i = 0; i < numKeys; i++) {
      auto key = keyPrefix + to_string(i);
      for (int j = begin; j <= end; j += 60) {
        DataPoint dp;
        toKey(entity, key, numShards, dp.key);
        dp.value.unixTime = j;
        dp.value.value = kDefaultValue;
        dp.categoryId = categoryId;
        request->data.push_back(dp);
      }
    }

    return request;
  }

  void putDataPoints(
      BeringeiServiceHandler& handler,
      std::unique_ptr<PutDataRequest> putDataRequest) {
    PutDataResult result;
    handler.putDataPoints(result, std::move(putDataRequest));
    EXPECT_EQ(0, result.data.size());
  }

  void checkGeneratedGetResults(GetDataResult& result, int numKeys, int total) {
    ASSERT_EQ(numKeys, result.results.size());
    for (int i = 0; i < numKeys; i++) {
      EXPECT_NE(0, result.results[i].data.size());
      int sum = 0;
      for (auto& block : result.results[i].data) {
        sum += block.count;
      }
      EXPECT_EQ(total, sum);
      EXPECT_EQ(StatusCode::OK, result.results[i].status);
    }
  }

  void checkGeneratedGetResults(
      GetDataResult& result,
      int numKeys,
      int64_t begin,
      int64_t end) {
    checkGeneratedGetResults(result, numKeys, (end - begin) / 60 + 1);
  }

  void checkAtLeastOneGetResults(GetDataResult& result, int numKeys) {
    ASSERT_EQ(numKeys, result.results.size());
    for (int i = 0; i < numKeys; i++) {
      ASSERT_NE(0, result.results[i].data.size());
      EXPECT_NE(0, result.results[i].data[0].count);
      EXPECT_EQ(StatusCode::OK, result.results[i].status);
    }
  }

  void checkBadGeneratedGetResults(
      GetDataResult& result,
      int numKeys,
      StatusCode code) {
    ASSERT_EQ(numKeys, result.results.size());
    for (int i = 0; i < numKeys; i++) {
      EXPECT_EQ(0, result.results[i].data.size());
      EXPECT_EQ(code, result.results[i].status);
    }
  }

  void dropShardAndWait(BeringeiServiceHandler* handler, int64_t shardId) {
    handler->shards_.dropShardForTests(shardId);
  }

  void addShardAndWait(BeringeiServiceHandler* handler, int64_t shardId) {
    handler->shards_.addShardForTests(shardId);
  }

  void setShardsAndWait(
      BeringeiServiceHandler* handler,
      std::set<int64_t> shards) {
    handler->shards_.setShardsForTests(shards);
  }

  auto getShards(BeringeiServiceHandler* handler) {
    return handler->shards_.getShards();
  }

  auto addShardAsync(BeringeiServiceHandler* handler, int64_t shardId) {
    return handler->shards_.addShardAsync(shardId);
  }

  void shardTest(
      BeringeiServiceHandler* handler,
      int64_t shardId,
      bool expectData,
      bool allData = true) {
    int64_t startTime = time(nullptr) - 1800;
    int64_t endTime = startTime + 1800;
    string keyPrefix = "key";

    auto putRequest =
        generatePutRequest(1000, startTime, endTime, keyPrefix, shardId);
    PutDataResult putDataResult;
    handler->putDataPoints(putDataResult, std::move(putRequest));

    if (expectData) {
      EXPECT_EQ(0, putDataResult.data.size());
    } else {
      EXPECT_NE(0, putDataResult.data.size());
    }

    GetDataResult result;
    auto getRequest =
        generateGetRequest(1000, startTime, endTime, keyPrefix, shardId);
    handler->getData(result, std::move(getRequest));

    if (expectData) {
      if (allData) {
        checkGeneratedGetResults(result, 1000, startTime, endTime);
      } else {
        checkAtLeastOneGetResults(result, 1000);
      }
    } else {
      checkBadGeneratedGetResults(result, 1000, StatusCode::DONT_OWN_SHARD);
    }
  }
};

class BeringeiServiceHandlerForTest : public BeringeiServiceHandler {
 public:
  explicit BeringeiServiceHandlerForTest(bool adjustTimestamps = false)
      : BeringeiServiceHandler(
            std::make_shared<MockConfigurationAdapter>(),
            std::make_shared<MockMemoryUsageGuard>(),
            "mock_beringei_service",
            9999,
            adjustTimestamps) {}
};

TEST_F(BeringeiServiceHandlerTest, EmptyTimeSeries) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandlerForTest handler;

  GetDataResult result;

  auto request = generateGetRequest(1000, 1000, 9999);
  handler.getData(result, std::move(request));
  checkBadGeneratedGetResults(result, 1000, StatusCode::KEY_MISSING);

  // Run again to get better performance numbers.
  // The first run is much slower than the seconds run because
  // the threads are created in the first run.
  request = generateGetRequest(1000, 1000, 9999);
  handler.getData(result, std::move(request));
}

TEST_F(BeringeiServiceHandlerTest, GetShardDataBucketTimeElapse) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();
  FLAGS_allowed_timestamp_ahead = kGorillaSecondsPerHour * 30;

  BeringeiServiceHandlerForTest handler;

  // FLAGS_bucket_size datapoints shift one bucket back.
  int64_t oneBucketBack = time(nullptr) - FLAGS_bucket_size;
  int64_t startTime = oneBucketBack - oneBucketBack % FLAGS_bucket_size;
  int64_t endTime = startTime + FLAGS_bucket_size;

  int64_t shardId = 0;
  string keyPrefix = "key";

  // 1000 keys, FLAGS_bucket_size datapoints.
  auto putRequest =
      generatePutRequest(1000, startTime, endTime, keyPrefix, shardId);
  putDataPoints(handler, std::move(putRequest));

  handler.finalizeBucket(endTime);
  GetShardDataBucketResult getDataResult;
  handler.getShardDataBucket(getDataResult, endTime, endTime, shardId, 0, 1000);

  EXPECT_EQ(StatusCode::OK, getDataResult.status);

  // Should have 1000 keys.
  EXPECT_EQ(1000, getDataResult.keys.size());

  // Each key should have TimeSeriesBlock
  // since we put data points in the time window.
  EXPECT_EQ(1000, getDataResult.data.size());

  // Each key should have recentRead info associated.
  EXPECT_EQ(1000, getDataResult.recentRead.size());

  for (bool recentRead : getDataResult.recentRead) {
    // The associated BucketedTimeSeries of key are not recently queried.
    EXPECT_FALSE(recentRead);
  }

  GetDataResult result;
  auto getRequest =
      generateGetRequest(1000, startTime, endTime, keyPrefix, shardId);
  handler.getData(result, std::move(getRequest));
  checkGeneratedGetResults(result, 1000, startTime, endTime);

  handler.finalizeBucket(endTime);
  GetShardDataBucketResult getDataResultAfter;
  handler.getShardDataBucket(
      getDataResultAfter, endTime, endTime, shardId, 0, 10000);

  EXPECT_EQ(StatusCode::OK, getDataResultAfter.status);

  // Should have 1000 keys.
  EXPECT_EQ(1000, getDataResultAfter.keys.size());

  // Each key should have TimeSeriesBlock
  // since we put data points in the time window.
  EXPECT_EQ(1000, getDataResultAfter.data.size());

  // Each key should have recentRead info associated.
  EXPECT_EQ(1000, getDataResultAfter.recentRead.size());

  for (bool recentRead : getDataResultAfter.recentRead) {
    // The associated BucketedTimeSeries of key are recently queried.
    EXPECT_TRUE(recentRead);
  }
}

TEST_F(BeringeiServiceHandlerTest, GetShardDataTimeElapse) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();
  FLAGS_allowed_timestamp_ahead = kGorillaSecondsPerHour * 30;

  BeringeiServiceHandlerForTest handler;

  // FLAGS_bucket_size datapoints, shift one bucket back.
  int64_t oneBucketBack = time(nullptr) - FLAGS_bucket_size;
  int64_t startTime = oneBucketBack - oneBucketBack % FLAGS_bucket_size;
  int64_t endTime = startTime + FLAGS_bucket_size;

  int64_t shardId = 0;
  string keyPrefix = "key";
  // 1000 keys, FLAGS_bucket_size datapoints.
  auto putRequest =
      generatePutRequest(1000, startTime, endTime, keyPrefix, shardId);
  putDataPoints(handler, std::move(putRequest));

  GetDataResult result;
  auto getRequest =
      generateGetRequest(1000, startTime, endTime, keyPrefix, shardId);
  // Data is queried, so it should be recent.
  handler.getData(result, std::move(getRequest));
  checkGeneratedGetResults(result, 1000, startTime, endTime);

  handler.finalizeBucket(endTime);
  GetShardDataBucketResult getDataResult;
  handler.getShardDataBucket(
      getDataResult, endTime, endTime, shardId, 0, 10000);

  EXPECT_EQ(StatusCode::OK, getDataResult.status);

  // Should have 1000 keys.
  EXPECT_EQ(1000, getDataResult.keys.size());

  // Each key should have TimeSeriesBlock
  // since we put data points in the time window.
  EXPECT_EQ(1000, getDataResult.data.size());

  // Each key should have recentRead info associated.
  EXPECT_EQ(1000, getDataResult.recentRead.size());

  for (bool recentRead : getDataResult.recentRead) {
    EXPECT_TRUE(recentRead);
  }

  // Push some new buckets with same keys so the
  // timeseries won't be recent.
  startTime = endTime;
  endTime = endTime + kGorillaSecondsPerDay;

  for (int64_t i = startTime; i < endTime; i += FLAGS_bucket_size) {
    putRequest =
        generatePutRequest(1000, i, i + FLAGS_bucket_size, keyPrefix, shardId);
    putDataPoints(handler, std::move(putRequest));
    handler.finalizeBucket(i + FLAGS_bucket_size);
  }

  GetShardDataBucketResult getDataResultAfter;
  handler.getShardDataBucket(
      getDataResultAfter, endTime, endTime, shardId, 0, 10000);

  EXPECT_EQ(StatusCode::OK, getDataResultAfter.status);

  // Should have 1000 keys.
  EXPECT_EQ(1000, getDataResultAfter.keys.size());

  // Each key should have TimeSeriesBlock
  // since we put data points in the time window.
  EXPECT_EQ(1000, getDataResultAfter.data.size());

  // Each key should have recentRead info associated.
  EXPECT_EQ(1000, getDataResultAfter.recentRead.size());

  for (bool recentRead : getDataResultAfter.recentRead) {
    EXPECT_FALSE(recentRead);
  }
}

TEST_F(BeringeiServiceHandlerTest, GetShardDataBucket) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();
  FLAGS_allowed_timestamp_ahead = kGorillaSecondsPerHour * 30;

  BeringeiServiceHandlerForTest handler;

  // FLAGS_bucket_size datapoints shift one bucket back.
  int64_t oneBucketBack = time(nullptr) - FLAGS_bucket_size;
  int64_t startTime = oneBucketBack - oneBucketBack % FLAGS_bucket_size;
  int64_t endTime = startTime + FLAGS_bucket_size;

  int64_t shardId = 0;
  string keyPrefix = "key";
  // 1000 keys, FLAGS_bucket_size datapoints.
  auto putRequest =
      generatePutRequest(1000, startTime, endTime, keyPrefix, shardId);
  putDataPoints(handler, std::move(putRequest));
  handler.finalizeBucket(endTime);

  GetShardDataBucketResult getDataResult;
  handler.getShardDataBucket(getDataResult, endTime, endTime, shardId, 0, 600);

  EXPECT_EQ(StatusCode::OK, getDataResult.status);
  EXPECT_TRUE(getDataResult.moreEntries);

  // Should have 600 keys.
  EXPECT_EQ(600, getDataResult.keys.size());

  // Each key should have TimeSeriesBlock
  // since we put data points in the time window.
  EXPECT_EQ(600, getDataResult.data.size());

  // Each key should have recentRead info associated.
  EXPECT_EQ(600, getDataResult.recentRead.size());

  for (bool recentRead : getDataResult.recentRead) {
    // The associated BucketedTimeSeries of key are not recently queried.
    EXPECT_FALSE(recentRead);
  }

  GetShardDataBucketResult getDataResultAfter;
  handler.getShardDataBucket(
      getDataResultAfter, endTime, endTime, shardId, 600, 500);

  EXPECT_EQ(StatusCode::OK, getDataResultAfter.status);
  EXPECT_FALSE(getDataResultAfter.moreEntries);

  // Should have 400 keys (since we got 600 last time)
  EXPECT_EQ(400, getDataResultAfter.keys.size());

  // Each key should have TimeSeriesBlock
  // since we put data points in the time window.
  EXPECT_EQ(400, getDataResultAfter.data.size());

  // Each key should have recentRead info associated.
  EXPECT_EQ(400, getDataResultAfter.recentRead.size());

  for (bool recentRead : getDataResultAfter.recentRead) {
    // The associated BucketedTimeSeries of key are not recently queried.
    EXPECT_FALSE(recentRead);
  }
}

TEST_F(BeringeiServiceHandlerTest, OneHourOfOneMinuteData) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandlerForTest handler;

  int64_t startTime = time(nullptr) - kGorillaSecondsPerHour;
  int64_t endTime = startTime + kGorillaSecondsPerHour;

  string keyPrefix = "key";
  int shardId = 14;

  auto putRequest =
      generatePutRequest(1000, startTime, endTime, keyPrefix, shardId);
  putDataPoints(handler, std::move(putRequest));

  GetDataResult result;
  auto getRequest =
      generateGetRequest(1000, startTime, endTime, keyPrefix, shardId);
  handler.getData(result, std::move(getRequest));

  checkGeneratedGetResults(result, 1000, startTime, endTime);
}

TEST_F(BeringeiServiceHandlerTest, AllShards) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();
  FLAGS_bucket_size = kGorillaSecondsPerHour;

  BeringeiServiceHandlerForTest handler;

  int64_t startTime = time(nullptr) - kGorillaSecondsPerMinute * 5;
  int64_t endTime = startTime + kGorillaSecondsPerMinute * 5;

  string keyPrefix = "key";

  for (int shardId = 0; shardId < 100; shardId++) {
    auto putRequest =
        generatePutRequest(5, startTime, endTime, keyPrefix, shardId);
    putDataPoints(handler, std::move(putRequest));

    GetDataResult result;
    auto getRequest =
        generateGetRequest(5, startTime, endTime, keyPrefix, shardId);
    handler.getData(result, std::move(getRequest));

    checkGeneratedGetResults(result, 5, startTime, endTime);
  }
}

TEST_F(BeringeiServiceHandlerTest, OldData) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandlerForTest handler(true);

  int64_t now = time(nullptr);
  int64_t startTime = now - kGorillaSecondsPerHour * 2;
  int64_t endTime = startTime + kGorillaSecondsPerMinute;

  auto putRequest = generatePutRequest(10, startTime, endTime);
  putDataPoints(handler, std::move(putRequest));

  GetDataResult result;
  auto getRequest = generateGetRequest(10, now, now + kGorillaSecondsPerMinute);
  handler.getData(result, std::move(getRequest));

  checkGeneratedGetResults(result, 10, 1);
}

TEST_F(BeringeiServiceHandlerTest, DataInTheFuture) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandlerForTest handler(true);

  int64_t startTime = time(nullptr) + FLAGS_allowed_timestamp_ahead;
  int64_t endTime = startTime + kGorillaSecondsPerMinute * 5;

  auto putRequest = generatePutRequest(10, startTime, endTime);
  putDataPoints(handler, std::move(putRequest));

  GetDataResult result;
  auto getRequest = generateGetRequest(10, startTime, endTime);
  handler.getData(result, std::move(getRequest));

  checkGeneratedGetResults(result, 10, 1);
}

TEST_F(BeringeiServiceHandlerTest, DataAtTimeZero) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandlerForTest handler(true);

  int64_t startTime = time(nullptr);
  int64_t endTime = startTime;

  auto putRequest = generatePutRequest(10, 0, 0);
  putDataPoints(handler, std::move(putRequest));

  GetDataResult result;
  auto getRequest = generateGetRequest(10, startTime, endTime);
  handler.getData(result, std::move(getRequest));

  checkGeneratedGetResults(result, 10, 1);
}

TEST_F(BeringeiServiceHandlerTest, TooLongKey) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  string keyPrefix(500, '?');

  BeringeiServiceHandlerForTest handler;

  int64_t startTime = time(nullptr) - 1800;
  int64_t endTime = time(nullptr);

  auto putRequest = generatePutRequest(10, startTime, endTime, keyPrefix);
  putDataPoints(handler, std::move(putRequest));

  GetDataResult result;
  auto getRequest = generateGetRequest(10, startTime, endTime, keyPrefix);
  handler.getData(result, std::move(getRequest));

  checkBadGeneratedGetResults(result, 10, StatusCode::KEY_MISSING);
}

TEST_F(BeringeiServiceHandlerTest, DropShardWithData) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandlerForTest handler;

  int64_t startTime = time(nullptr) - 1800;
  int64_t endTime = startTime + 1800;
  int64_t shardId = 14;
  string keyPrefix = "key";

  auto putRequest =
      generatePutRequest(1000, startTime, endTime, keyPrefix, shardId);
  putDataPoints(handler, std::move(putRequest));

  dropShardAndWait(&handler, shardId);

  GetDataResult result;
  auto getRequest =
      generateGetRequest(1000, startTime, endTime, keyPrefix, shardId);
  handler.getData(result, std::move(getRequest));

  checkBadGeneratedGetResults(result, 1000, StatusCode::DONT_OWN_SHARD);
}

TEST_F(BeringeiServiceHandlerTest, PutDataPointsToDroppedShard) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandlerForTest handler;
  int64_t shardId = 14;
  dropShardAndWait(&handler, shardId);

  int64_t startTime = time(nullptr) - 1800;
  int64_t endTime = startTime + 1800;
  string keyPrefix = "key";

  auto putRequest =
      generatePutRequest(10, startTime, endTime, keyPrefix, shardId);
  PutDataResult putDataResult;
  int putRequestSize = putRequest->data.size();
  handler.putDataPoints(putDataResult, std::move(putRequest));
  ASSERT_EQ(putRequestSize, putDataResult.data.size());
}

TEST_F(BeringeiServiceHandlerTest, AddDataToDroppedShard) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandlerForTest handler;
  int64_t shardId = 14;
  dropShardAndWait(&handler, shardId);
  shardTest(&handler, shardId, false);
}

TEST_F(BeringeiServiceHandlerTest, AddDroppedShardBack) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  // Fatals if adjustTimestamps is off.
  BeringeiServiceHandlerForTest handler(true);

  int64_t startTime = time(nullptr) - 1800;
  int64_t endTime = startTime + 1800;
  int64_t shardId = 14;
  string keyPrefix = "key";

  auto putRequest =
      generatePutRequest(1000, startTime, endTime, keyPrefix, shardId);
  putDataPoints(handler, std::move(putRequest));

  dropShardAndWait(&handler, shardId);
  addShardAndWait(&handler, shardId);
  shardTest(&handler, shardId, true, false);
}

TEST_F(BeringeiServiceHandlerTest, SetShardsToOwnSingleShard) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandlerForTest handler;
  setShardsAndWait(&handler, {14});
  shardTest(&handler, 13, false);
  shardTest(&handler, 14, true);
}

TEST_F(BeringeiServiceHandlerTest, SetShardsToDifferentSet) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandlerForTest handler;
  setShardsAndWait(&handler, {14});
  setShardsAndWait(&handler, {13});
  shardTest(&handler, 14, false);
  shardTest(&handler, 13, true);
}

TEST_F(BeringeiServiceHandlerTest, ShuffleShards) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  // Fatals if adjustTimestamps is off.
  BeringeiServiceHandlerForTest handler(true);
  std::set<int64_t> shards = {11, 12, 13};

  setShardsAndWait(&handler, shards);
  EXPECT_EQ(shards, getShards(&handler));
  shardTest(&handler, 11, true, false);
  shardTest(&handler, 12, true, false);
  shardTest(&handler, 13, true, false);
  shardTest(&handler, 14, false);

  shards = {12, 13, 14};
  setShardsAndWait(&handler, shards);
  EXPECT_EQ(shards, getShards(&handler));
  shardTest(&handler, 11, false);
  shardTest(&handler, 12, true, false);
  shardTest(&handler, 13, true, false);
  shardTest(&handler, 14, true, false);

  shards = {11, 14};
  setShardsAndWait(&handler, shards);
  EXPECT_EQ(shards, getShards(&handler));
  shardTest(&handler, 11, true, false);
  shardTest(&handler, 12, false);
  shardTest(&handler, 13, false);
  shardTest(&handler, 14, true, false);

  shards = {};
  setShardsAndWait(&handler, shards);
  EXPECT_EQ(shards, getShards(&handler));
  shardTest(&handler, 11, false);
  shardTest(&handler, 12, false);
  shardTest(&handler, 13, false);
  shardTest(&handler, 14, false);
}

TEST_F(BeringeiServiceHandlerTest, AddDropSetShards) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandlerForTest handler;
  std::set<int64_t> expected = {12, 13};
  addShardAndWait(&handler, 11);
  setShardsAndWait(&handler, {12, 13});
  shardTest(&handler, 11, false);
  shardTest(&handler, 12, true);
  shardTest(&handler, 13, true);
  EXPECT_EQ(expected, getShards(&handler));

  expected = {13};
  dropShardAndWait(&handler, 12);
  shardTest(&handler, 11, false);
  shardTest(&handler, 12, false);
  shardTest(&handler, 13, true);
  EXPECT_EQ(expected, getShards(&handler));

  expected = {11};
  setShardsAndWait(&handler, {11});
  shardTest(&handler, 11, true);
  shardTest(&handler, 12, false);
  shardTest(&handler, 13, false);
  EXPECT_EQ(expected, getShards(&handler));
}

TEST_F(BeringeiServiceHandlerTest, AddShardAsync) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandlerForTest handler;
  auto status = addShardAsync(&handler, 11);
  EXPECT_EQ(ShardData::BeringeiShardState::SUCCESS, status);

  /* sleep override */ usleep(200000);
  status = addShardAsync(&handler, 11);
  EXPECT_EQ(ShardData::BeringeiShardState::SUCCESS, status);
  shardTest(&handler, 11, true);
}

TEST_F(BeringeiServiceHandlerTest, GetLastUpdateTimes) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandlerForTest handler;

  int64_t startTime = time(nullptr) - 300;
  int64_t endTime = startTime + 300;

  auto putRequest = generatePutRequest(100, startTime, endTime);
  putDataPoints(handler, std::move(putRequest));

  for (int i = 0; i < 110; i += 10) {
    std::unique_ptr<GetLastUpdateTimesRequest> req(
        new GetLastUpdateTimesRequest);
    req->shardId = 0;
    req->minLastUpdateTime = endTime;
    req->offset = i;
    req->limit = 10;

    GetLastUpdateTimesResult result;
    handler.getLastUpdateTimes(result, std::move(req));

    if (i < 100) {
      EXPECT_EQ(i < 90, result.moreResults);
      EXPECT_EQ(10, result.keys.size());
      for (auto& key : result.keys) {
        EXPECT_EQ(endTime, key.updateTime);
      }
    } else {
      EXPECT_FALSE(result.moreResults);
      EXPECT_EQ(0, result.keys.size());
    }
  }
}

TEST_F(BeringeiServiceHandlerTest, GetLastUpdateTimesWithNoMatches) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandlerForTest handler;

  int64_t startTime = time(nullptr) - 300;
  int64_t endTime = startTime + 300;

  auto putRequest = generatePutRequest(100, startTime, endTime);
  putDataPoints(handler, std::move(putRequest));

  std::unique_ptr<GetLastUpdateTimesRequest> req(new GetLastUpdateTimesRequest);
  req->shardId = 0;
  req->minLastUpdateTime = endTime + 1;
  req->offset = 0;
  req->limit = 100;

  GetLastUpdateTimesResult result;
  handler.getLastUpdateTimes(result, std::move(req));

  EXPECT_EQ(0, result.keys.size());
}
