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
DECLARE_int32(shards);
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
  virtual void SetUp() override {
    FLAGS_add_shard_threads = 1;
    FLAGS_create_directories = true;
    FLAGS_buckets = 6;
    FLAGS_bucket_size = 4 * kGorillaSecondsPerHour;
    fLI::FLAGS_allowed_timestamp_ahead = kGorillaSecondsPerDay;
    FLAGS_disable_shard_refresh = true;
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

  bool waitTillShardProcessed(
      BeringeiServiceHandler* handler,
      int64_t shardId,
      bool expectData) {
    bool success = false;
    for (int retries = 0; !success && retries < 50; retries++) {
      auto map = handler->getShardMap(shardId);
      if (map) {
        auto state = map->getState();
        if ((!expectData && state == BucketMap::UNOWNED) ||
            (expectData && state == BucketMap::OWNED)) {
          success = true;
        } else {
          /* sleep override */ usleep(20000);
        }
      }
    }

    return success;
  }

  void dropShardAndWait(
      BeringeiServiceHandler* handler,
      int64_t shardId,
      int64_t delay = BeringeiServiceHandler::kAsyncDropShardsDelaySecs) {
    handler->dropShardAsync(shardId, delay);
    while (!waitTillShardProcessed(handler, shardId, false)) {
      // do nothing
    }
  }

  void addShardAndWait(BeringeiServiceHandler* handler, int64_t shardId) {
    handler->addShardAsync(shardId);
    while (!waitTillShardProcessed(handler, shardId, true)) {
      // do nothing
    }
  }

  void setShardsAndWait(
      BeringeiServiceHandler* handler,
      std::set<int64_t> shards,
      int64_t delay = BeringeiServiceHandler::kAsyncDropShardsDelaySecs) {
    bool retrySetShard = true;

    auto shardsOwned = handler->getShards();
    // Shard Manager will retry set shards every few minutes.
    std::vector<int64_t> shardsToBeDropped;
    std::set_difference(
        shardsOwned.begin(),
        shardsOwned.end(),
        shards.begin(),
        shards.end(),
        std::back_inserter(shardsToBeDropped));

    while (retrySetShard) {
      retrySetShard = false;
      handler->setShards(shards, delay);
      for (auto& shard : shards) {
        if (!waitTillShardProcessed(handler, shard, true)) {
          retrySetShard = true;
        }
      }

      for (auto& shardToDrop : shardsToBeDropped) {
        if (!waitTillShardProcessed(handler, shardToDrop, false)) {
          retrySetShard = true;
        }
      }
    }
  }

  void
  shardTest(BeringeiServiceHandler* handler, int64_t shardId, bool expectData) {
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

    ASSERT_EQ(1000, result.results.size());
    for (int i = 0; i < 1000; i++) {
      if (expectData) {
        EXPECT_NE(0, result.results[i].data.size());
        EXPECT_EQ(StatusCode::OK, result.results[i].status);
      } else {
        EXPECT_EQ(0, result.results[i].data.size());
        EXPECT_EQ(StatusCode::DONT_OWN_SHARD, result.results[i].status);
      }
    }
  }
};

TEST_F(BeringeiServiceHandlerTest, EmptyTimeSeries) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);

  GetDataResult result;

  auto request = generateGetRequest(1000, 1000, 9999);
  handler.getData(result, std::move(request));
  ASSERT_EQ(1000, result.results.size());
  for (int i = 0; i < 1000; i++) {
    EXPECT_EQ(0, result.results[i].data.size());
    EXPECT_EQ(StatusCode::KEY_MISSING, result.results[i].status);
  }

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

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);

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

  handler.finalizeBucket(endTime / FLAGS_bucket_size);
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

  EXPECT_EQ(1000, result.results.size());
  EXPECT_EQ(1, result.results[0].data.size());
  EXPECT_NE(0, result.results[0].data[0].count);

  handler.finalizeBucket(endTime / FLAGS_bucket_size);
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

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);

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

  EXPECT_EQ(1000, result.results.size());
  EXPECT_EQ(1, result.results[0].data.size());
  EXPECT_NE(0, result.results[0].data[0].count);

  handler.finalizeBucket(endTime / FLAGS_bucket_size);
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
    handler.finalizeBucket((i + FLAGS_bucket_size) / FLAGS_bucket_size);
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

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);

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
  handler.finalizeBucket(endTime / FLAGS_bucket_size);

  GetShardDataBucketResult getDataResult;
  handler.getShardDataBucket(getDataResult, endTime, endTime, shardId, 0, 600);

  EXPECT_EQ(StatusCode::OK, getDataResult.status);
  EXPECT_EQ(true, getDataResult.moreEntries);

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

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);

  int64_t startTime = time(nullptr) - 1800;
  int64_t endTime = startTime + 1800;

  auto putRequest = generatePutRequest(1000, startTime, endTime);
  putDataPoints(handler, std::move(putRequest));

  GetDataResult result;
  auto getRequest = generateGetRequest(1000, startTime, endTime);
  handler.getData(result, std::move(getRequest));

  getRequest = generateGetRequest(1000, startTime, endTime);
  handler.getData(result, std::move(getRequest));

  ASSERT_EQ(1000, result.results.size());
  for (int i = 0; i < 1000; i++) {
    EXPECT_NE(0, result.results[i].data.size());
    EXPECT_NE(0, result.results[i].data[0].count);
    EXPECT_EQ(StatusCode::OK, result.results[i].status);
  }
}

TEST_F(BeringeiServiceHandlerTest, OldData) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);

  int64_t now = time(nullptr);
  int64_t startTime = now - kGorillaSecondsPerHour * 2;
  int64_t endTime = startTime + kGorillaSecondsPerMinute;

  auto putRequest = generatePutRequest(10, startTime, endTime);
  putDataPoints(handler, std::move(putRequest));

  GetDataResult result;
  auto getRequest = generateGetRequest(10, now, now + kGorillaSecondsPerMinute);
  handler.getData(result, std::move(getRequest));

  ASSERT_EQ(10, result.results.size());
  for (int i = 0; i < 10; i++) {
    EXPECT_NE(0, result.results[i].data.size());
    EXPECT_NE(0, result.results[i].data[0].count);
    EXPECT_EQ(StatusCode::OK, result.results[i].status);
  }
}

TEST_F(BeringeiServiceHandlerTest, DataInTheFuture) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);

  int64_t startTime = time(nullptr) + FLAGS_allowed_timestamp_ahead;
  int64_t endTime = startTime + kGorillaSecondsPerMinute * 5;

  auto putRequest = generatePutRequest(10, startTime, endTime);
  putDataPoints(handler, std::move(putRequest));

  GetDataResult result;
  auto getRequest = generateGetRequest(10, startTime, endTime);
  handler.getData(result, std::move(getRequest));

  ASSERT_EQ(10, result.results.size());
  for (int i = 0; i < 10; i++) {
    EXPECT_NE(0, result.results[i].data.size());
    EXPECT_EQ(1, result.results[i].data[0].count);
    EXPECT_EQ(StatusCode::OK, result.results[i].status);
  }
}

TEST_F(BeringeiServiceHandlerTest, DataAtTimeZero) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);

  int64_t startTime = time(nullptr);
  int64_t endTime = startTime;

  auto putRequest = generatePutRequest(10, 0, 0);
  putDataPoints(handler, std::move(putRequest));

  GetDataResult result;
  auto getRequest = generateGetRequest(10, startTime, endTime);
  handler.getData(result, std::move(getRequest));

  ASSERT_EQ(10, result.results.size());
  for (int i = 0; i < 10; i++) {
    EXPECT_NE(0, result.results[i].data.size());
    EXPECT_EQ(1, result.results[i].data[0].count);
    EXPECT_EQ(StatusCode::OK, result.results[i].status);
  }
}

TEST_F(BeringeiServiceHandlerTest, TooLongKey) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  string keyPrefix(500, '?');

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);
  int64_t startTime = time(nullptr) - 1800;
  int64_t endTime = time(nullptr);

  auto putRequest = generatePutRequest(10, startTime, endTime, keyPrefix);
  putDataPoints(handler, std::move(putRequest));

  GetDataResult result;
  auto getRequest = generateGetRequest(10, startTime, endTime, keyPrefix);
  handler.getData(result, std::move(getRequest));

  ASSERT_EQ(10, result.results.size());
  for (int i = 0; i < 10; i++) {
    EXPECT_EQ(0, result.results[i].data.size());
    EXPECT_EQ(StatusCode::KEY_MISSING, result.results[i].status);
  }
}

TEST_F(BeringeiServiceHandlerTest, DropShardWithData) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);

  int64_t startTime = time(nullptr) - 1800;
  int64_t endTime = startTime + 1800;
  int64_t shardId = 14;
  string keyPrefix = "key";

  auto putRequest =
      generatePutRequest(1000, startTime, endTime, keyPrefix, shardId);
  putDataPoints(handler, std::move(putRequest));

  dropShardAndWait(&handler, shardId, 0);

  GetDataResult result;
  auto getRequest =
      generateGetRequest(1000, startTime, endTime, keyPrefix, shardId);
  handler.getData(result, std::move(getRequest));

  ASSERT_EQ(1000, result.results.size());
  for (int i = 0; i < 1000; i++) {
    EXPECT_EQ(0, result.results[i].data.size());
    EXPECT_EQ(StatusCode::DONT_OWN_SHARD, result.results[i].status);
  }
}

TEST_F(BeringeiServiceHandlerTest, PutDataPointsToDroppedShard) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);
  int64_t shardId = 14;
  dropShardAndWait(&handler, shardId, 0);

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

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);
  int64_t shardId = 14;
  dropShardAndWait(&handler, shardId, 0);
  shardTest(&handler, shardId, false);
}

TEST_F(BeringeiServiceHandlerTest, AddDroppedShardBack) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);

  int64_t startTime = time(nullptr) - 1800;
  int64_t endTime = startTime + 1800;
  int64_t shardId = 14;
  string keyPrefix = "key";

  auto putRequest =
      generatePutRequest(1000, startTime, endTime, keyPrefix, shardId);
  putDataPoints(handler, std::move(putRequest));

  dropShardAndWait(&handler, shardId, 0);
  addShardAndWait(&handler, shardId);
  shardTest(&handler, shardId, true);
}

TEST_F(BeringeiServiceHandlerTest, SetShardsToOwnSingleShard) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);
  setShardsAndWait(&handler, {14}, 0);
  shardTest(&handler, 13, false);
  shardTest(&handler, 14, true);
}

TEST_F(BeringeiServiceHandlerTest, SetShardsToDifferentSet) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);
  setShardsAndWait(&handler, {14}, 0);
  setShardsAndWait(&handler, {13}, 0);
  shardTest(&handler, 14, false);
  shardTest(&handler, 13, true);
}

TEST_F(BeringeiServiceHandlerTest, ShuffleShards) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);
  std::set<int64_t> shards = {11, 12, 13};

  setShardsAndWait(&handler, shards, 0);
  EXPECT_EQ(shards, handler.getShards());
  shardTest(&handler, 11, true);
  shardTest(&handler, 12, true);
  shardTest(&handler, 13, true);
  shardTest(&handler, 14, false);

  shards = {12, 13, 14};
  setShardsAndWait(&handler, shards, 0);
  EXPECT_EQ(shards, handler.getShards());
  shardTest(&handler, 11, false);
  shardTest(&handler, 12, true);
  shardTest(&handler, 13, true);
  shardTest(&handler, 14, true);

  shards = {11, 14};
  setShardsAndWait(&handler, shards, 0);
  EXPECT_EQ(shards, handler.getShards());
  shardTest(&handler, 11, true);
  shardTest(&handler, 12, false);
  shardTest(&handler, 13, false);
  shardTest(&handler, 14, true);

  shards = {};
  setShardsAndWait(&handler, shards, 0);
  EXPECT_EQ(shards, handler.getShards());
  shardTest(&handler, 11, false);
  shardTest(&handler, 12, false);
  shardTest(&handler, 13, false);
  shardTest(&handler, 14, false);
}

TEST_F(BeringeiServiceHandlerTest, AddDropSetShards) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);

  std::set<int64_t> expected = {12, 13};
  addShardAndWait(&handler, 11);
  setShardsAndWait(&handler, {12, 13}, 0);
  shardTest(&handler, 11, false);
  shardTest(&handler, 12, true);
  shardTest(&handler, 13, true);
  EXPECT_EQ(expected, handler.getShards());

  expected = {13};
  dropShardAndWait(&handler, 12, 0);
  shardTest(&handler, 11, false);
  shardTest(&handler, 12, false);
  shardTest(&handler, 13, true);
  EXPECT_EQ(expected, handler.getShards());

  expected = {11};
  setShardsAndWait(&handler, {11}, 0);
  shardTest(&handler, 11, true);
  shardTest(&handler, 12, false);
  shardTest(&handler, 13, false);
  EXPECT_EQ(expected, handler.getShards());
}

TEST_F(BeringeiServiceHandlerTest, AddShardAsync) {
  TemporaryDirectory dir("beringei_data_block");
  FLAGS_data_directory = dir.dirname();

  BeringeiServiceHandler handler(
      std::make_shared<MockConfigurationAdapter>(),
      std::make_shared<MockMemoryUsageGuard>(),
      "mock_beringei_service",
      9999);
  auto status = handler.addShardAsync(11);
  EXPECT_EQ(BeringeiServiceHandler::BeringeiShardState::SUCCESS, status);

  /* sleep override */ usleep(200000);
  status = handler.addShardAsync(11);
  EXPECT_EQ(BeringeiServiceHandler::BeringeiShardState::SUCCESS, status);
  shardTest(&handler, 11, true);
}
