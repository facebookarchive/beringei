/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <folly/futures/Future.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "beringei/client/BeringeiClientImpl.h"
#include "beringei/client/BeringeiConfigurationAdapterIf.h"
#include "beringei/client/BeringeiNetworkClient.h"
#include "beringei/client/tests/MockConfigurationAdapter.h"
#include "beringei/if/gen-cpp2/BeringeiService.h"
#include "beringei/lib/BucketedTimeSeries.h"

using namespace ::testing;
using namespace facebook::gorilla;
using namespace folly;
using namespace std;

namespace facebook {
namespace gorilla {
DECLARE_int32(gorilla_retry_delay_secs);
}
}

class BeringeiClientMock : public BeringeiNetworkClient {
 public:
  explicit BeringeiClientMock(int numShards)
      : BeringeiNetworkClient(), numShards_(numShards) {}

  MOCK_METHOD1(performGet, void(BeringeiNetworkClient::GetRequestMap&));

  MOCK_METHOD1(invalidateCache, void(const std::unordered_set<int64_t>&));

  MOCK_METHOD1(
      performPut,
      vector<DataPoint>(BeringeiNetworkClient::PutRequestMap& requests));

  bool addDataPointToRequest(
      DataPoint& dp,
      BeringeiNetworkClient::PutRequestMap& requests,
      bool& dropped) override {
    auto& request = requests[make_pair("", dp.key.shardId)];
    request.data.push_back(dp);
    return true;
  }

  void addKeyToGetRequest(
      const facebook::gorilla::Key& key,
      BeringeiNetworkClient::GetRequestMap& requests) override {
    auto& request = requests[make_pair("", key.shardId)];
    request.first.keys.push_back(key);
  }

  // Assume shards are evenly distributed across 2 hosts.
  bool getHostForShard(int64_t shard, std::pair<std::string, int>& hostInfo)
      override {
    hostInfo.first = "some_host";
    hostInfo.second = shard % 2;
    return true;
  }

  int64_t getNumShards() override {
    return numShards_;
  }

  string getServiceName() override {
    return "";
  }

  MOCK_METHOD2(
      mockPerformGet,
      std::pair<StatusCode, int>(int, const std::vector<int64_t>&));

  Future<GetDataResult> performGet(
      const std::pair<std::string, int>& hostInfo,
      const GetDataRequest& request,
      folly::EventBase*) override {
    std::vector<int64_t> shards;
    for (auto& k : request.keys) {
      shards.push_back(k.shardId);
    }

    auto r = mockPerformGet(hostInfo.second, shards);

    TimeSeriesData ts;
    ts.status = r.first;
    GetDataResult result;
    for (int i = 0; i < request.keys.size(); i++) {
      result.results.push_back(ts);
    }
    if (r.second == 0) {
      return makeFuture<GetDataResult>(std::move(result));
    }
    return futures::sleep(
               std::chrono::milliseconds(folly::Random::rand64(r.second)))
        .then([result]() { return result; });
  }

 private:
  int64_t numShards_;
};

class BeringeiClientTest : public testing::Test {
 protected:
  void SetUp() override {
    DataPoint dp;
    dp.key.key = "one";
    dp.key.shardId = 1;
    dp.value.unixTime = 1;
    dps.push_back(dp);

    dp.key.shardId = 2;
    dps.push_back(dp);

    k1 = buildKey("eleven", 3);
    k2 = buildKey("twelve", 5);

    // Results should come back sorted by shard.
    getReq.keys = {k2, k1};

    resultVec.resize(2);
    resultVec[0].first = k1;
    resultVec[1].first = k2;

    BucketedTimeSeries b1, b2;
    b1.reset(5);
    b2.reset(5);
    BucketStorage storage(5, 0, "");

    for (int i = 19; i < 22; i++) {
      TimeValuePair tv;
      tv.unixTime = i * minTimestampDelta;
      tv.value = i;
      b1.put(i, tv, &storage, 0, nullptr);
      resultVec[0].second.push_back(tv);
    }

    for (int i = 19; i < 22; i++) {
      TimeValuePair tv;
      tv.unixTime = (i + 7) * minTimestampDelta;
      tv.value = i;
      b2.put(i + 7, tv, &storage, 0, nullptr);
      resultVec[1].second.push_back(tv);
    }

    getRes1.results.resize(1);
    getRes2.results.resize(1);
    b1.get(0, 50, getRes1.results[0].data, &storage);
    b2.get(0, 50, getRes2.results[0].data, &storage);

    getResult.results.resize(2);
    b1.get(0, 50, getResult.results[0].data, &storage);
    b2.get(0, 50, getResult.results[1].data, &storage);

    shardUnownedRes.results.resize(1);
    rpcFailRes.results.resize(1);
    inProgressRes.results.resize(1);

    inProgressPartialRes.results.resize(1);
    b1.get(0, 50, inProgressPartialRes.results[0].data, &storage);

    resWithHoles.results.resize(1);
    b1.get(0, 50, resWithHoles.results[0].data, &storage);

    shardUnownedRes.results[0].status = StatusCode::DONT_OWN_SHARD;
    rpcFailRes.results[0].status = StatusCode::RPC_FAIL;
    inProgressRes.results[0].status = StatusCode::SHARD_IN_PROGRESS;
    inProgressPartialRes.results[0].status = StatusCode::SHARD_IN_PROGRESS;
    resWithHoles.results[0].status = StatusCode::MISSING_TOO_MUCH_DATA;
  }

  facebook::gorilla::Key buildKey(const std::string& key, int shard) {
    facebook::gorilla::Key k;
    k.key = key;
    k.shardId = shard;
    return k;
  }

  std::unique_ptr<BeringeiClientImpl> createBeringeiClient(
      std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
      int queueCapacity,
      int writerThreads,
      bool throwExceptionOnPartialRead,
      BeringeiNetworkClient* testClient,
      BeringeiNetworkClient* shadowClient = nullptr) {
    auto client = std::make_unique<BeringeiClientImpl>(
        configurationAdapter, throwExceptionOnPartialRead);

    client->initializeTestClients(
        queueCapacity, writerThreads, testClient, shadowClient);
    return client;
  }

  vector<DataPoint> dps;

  GetDataRequest getReq;
  GetDataResult getRes1, getRes2;
  GetDataResult shardUnownedRes;
  GetDataResult rpcFailRes;
  GetDataResult inProgressRes;
  GetDataResult inProgressPartialRes;
  GetDataResult resWithHoles;

  GetDataResult getResult;
  GorillaResultVector resultVec;

  facebook::gorilla::Key k1;
  facebook::gorilla::Key k2;

  // This must match default value for FLAGS_mintimestampdelta.
  // It ensures we do not drop any pairs inserted via put() in Setup()
  const uint32_t minTimestampDelta = 30;
};

// PUTS

TEST_F(BeringeiClientTest, Put) {
  auto client = new StrictMock<BeringeiClientMock>(3);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient = createBeringeiClient(adapterMock, 7, 1, false, client);

  PutDataRequest req1;
  req1.data.push_back(dps[0]);
  PutDataRequest req2;
  req2.data.push_back(dps[1]);
  vector<DataPoint> dps2 = dps;

  BeringeiNetworkClient::PutRequestMap expected;
  expected[make_pair("", 1)] = req1;
  expected[make_pair("", 2)] = req2;

  EXPECT_CALL(*client, performPut(expected))
      .WillOnce(Return(std::vector<DataPoint>{}));

  EXPECT_TRUE(beringeiClient->putDataPoints(dps));

  beringeiClient->flushQueue();

  EXPECT_CALL(*client, performPut(expected))
      .WillOnce(Return(std::vector<DataPoint>{}));
  EXPECT_TRUE(beringeiClient->putDataPoints(dps2));
}

TEST_F(BeringeiClientTest, PutRetryAll) {
  FLAGS_gorilla_retry_delay_secs = 0;

  auto client = new StrictMock<BeringeiClientMock>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient = createBeringeiClient(adapterMock, 12, 1, false, client);

  {
    ::testing::InSequence dummy;
    EXPECT_CALL(*client, performPut(_)).WillOnce(Return(dps));
    EXPECT_CALL(*client, performPut(_))
        .WillOnce(Return(std::vector<DataPoint>{}));
  }

  EXPECT_TRUE(beringeiClient->putDataPoints(dps));
  beringeiClient->flushQueue();
}

TEST_F(BeringeiClientTest, PutRetryOne) {
  FLAGS_gorilla_retry_delay_secs = 0;

  auto client = new StrictMock<BeringeiClientMock>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient = createBeringeiClient(adapterMock, 12, 1, false, client);

  PutDataRequest req1;
  req1.data.push_back(dps[0]);
  BeringeiNetworkClient::PutRequestMap retryExpected;
  retryExpected[make_pair("", 1)] = req1;

  {
    ::testing::InSequence dummy;
    EXPECT_CALL(*client, performPut(_))
        .WillOnce(Return(std::vector<DataPoint>{dps[0]}));
    EXPECT_CALL(*client, performPut(retryExpected))
        .WillOnce(Return(std::vector<DataPoint>{}));
  }

  EXPECT_TRUE(beringeiClient->putDataPoints(dps));
  beringeiClient->flushQueue();
}

TEST_F(BeringeiClientTest, PutRetryShadow) {
  FLAGS_gorilla_retry_delay_secs = 0;

  auto client = new StrictMock<BeringeiClientMock>(8);
  auto shadowClient = new StrictMock<BeringeiClientMock>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient =
      createBeringeiClient(adapterMock, 7, 1, false, client, shadowClient);

  EXPECT_CALL(*client, performPut(_))
      .WillOnce(Return(std::vector<DataPoint>{}));

  {
    ::testing::InSequence dummy;
    EXPECT_CALL(*shadowClient, performPut(_)).WillOnce(Return(dps));
    EXPECT_CALL(*shadowClient, performPut(_))
        .WillOnce(Return(std::vector<DataPoint>{}));
  }

  EXPECT_TRUE(beringeiClient->putDataPoints(dps));
  beringeiClient->flushQueue();
}

// GETS

TEST_F(BeringeiClientTest, Get) {
  auto client = new StrictMock<BeringeiClientMock>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient = createBeringeiClient(
      adapterMock, 1, BeringeiClientImpl::kNoWriterThreads, false, client);

  BeringeiNetworkClient::GetRequestMap expected;
  expected[make_pair("", 3)].first.keys.push_back(k1);
  expected[make_pair("", 3)].second = getRes1;
  expected[make_pair("", 5)].first.keys.push_back(k2);
  expected[make_pair("", 5)].second = getRes2;

  EXPECT_CALL(*client, performGet(_)).WillOnce(SetArgReferee<0>(expected));

  GetDataResult result;
  beringeiClient->get(getReq, result);

  ASSERT_EQ(getResult.results.size(), result.results.size());
  for (int i = 0; i < result.results.size(); i++) {
    ASSERT_EQ(getResult.results[i].data.size(), result.results[i].data.size());
    for (int j = 0; j < result.results[i].data.size(); j++) {
      EXPECT_EQ(
          getResult.results[i].data[j].count, result.results[i].data[j].count);
    }
  }
}

TEST_F(BeringeiClientTest, GetPoints) {
  auto client = new StrictMock<BeringeiClientMock>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient = createBeringeiClient(
      adapterMock, 1, BeringeiClientImpl::kNoWriterThreads, false, client);

  BeringeiNetworkClient::GetRequestMap expected;
  expected[make_pair("", 3)].first.keys.push_back(k1);
  expected[make_pair("", 3)].second = getRes1;
  expected[make_pair("", 5)].first.keys.push_back(k2);
  expected[make_pair("", 5)].second = getRes2;

  EXPECT_CALL(*client, performGet(_))
      .WillRepeatedly(SetArgReferee<0>(expected));

  GorillaResultVector result;
  beringeiClient->get(getReq, result);

  ASSERT_EQ(resultVec.size(), result.size());
  for (int i = 0; i < result.size(); i++) {
    ASSERT_EQ(0, result[i].second.size());
  }

  auto fullReq = getReq;
  // range below must match that used in BeringeiClientTest::SetUp
  fullReq.begin = 19 * minTimestampDelta;
  fullReq.end = 29 * minTimestampDelta;
  result.clear();
  beringeiClient->get(fullReq, result);
  ASSERT_EQ(resultVec.size(), result.size());

  auto sortFn = [](
      const std::pair<facebook::gorilla::Key, std::vector<TimeValuePair>>& a,
      const std::pair<facebook::gorilla::Key, std::vector<TimeValuePair>>& b) {
    return a.first.key < b.first.key;
  };

  sort(resultVec.begin(), resultVec.end(), sortFn);
  sort(result.begin(), result.end(), sortFn);

  for (int i = 0; i < result.size(); i++) {
    EXPECT_EQ(resultVec[i].first.key, result[i].first.key);
    ASSERT_EQ(resultVec[i].second.size(), result[i].second.size());
    for (int j = 0; j < result[i].second.size(); j++) {
      EXPECT_EQ(resultVec[i].second[j].unixTime, result[i].second[j].unixTime);
    }
  }
}

// FAILOVER READ GETS

TEST_F(BeringeiClientTest, ReadClientFailoverOtherScope) {
  auto client = new StrictMock<BeringeiClientMock>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient = createBeringeiClient(
      adapterMock, 1, BeringeiClientImpl::kNoWriterThreads, false, client);

  BeringeiNetworkClient::GetRequestMap shardUnowned;
  shardUnowned[make_pair("", 3)].first.keys.push_back(k1);
  shardUnowned[make_pair("", 3)].second = shardUnownedRes;
  shardUnowned[make_pair("", 5)].first.keys.push_back(k2);
  shardUnowned[make_pair("", 5)].second = shardUnownedRes;
  BeringeiNetworkClient::GetRequestMap expected;
  expected[make_pair("", 3)].first.keys.push_back(k1);
  expected[make_pair("", 3)].second = getRes1;
  expected[make_pair("", 5)].first.keys.push_back(k2);
  expected[make_pair("", 5)].second = getRes2;

  // beringeiClient should retry with same BeringeiNetworkClient in same scope
  // after invalidating shardCache, then if that fails, use a failover
  // BeringeiNetworkClient in a different read scope
  EXPECT_CALL(*client, performGet(_))
      .WillOnce(SetArgReferee<0>(shardUnowned))
      .WillOnce(SetArgReferee<0>(shardUnowned))
      .WillOnce(SetArgReferee<0>(expected));

  EXPECT_CALL(
      *client,
      invalidateCache(std::unordered_set<int64_t>({k1.shardId, k2.shardId})))
      .Times(1);

  GetDataResult result;
  beringeiClient->get(getReq, result);

  ASSERT_EQ(getResult.results.size(), result.results.size());
  for (int i = 0; i < result.results.size(); i++) {
    ASSERT_EQ(getResult.results[i].data.size(), result.results[i].data.size());
    for (int j = 0; j < result.results[i].data.size(); j++) {
      EXPECT_EQ(
          getResult.results[i].data[j].count, result.results[i].data[j].count);
    }
  }
}

TEST_F(BeringeiClientTest, ReadClientRetrySameScope) {
  auto client = new StrictMock<BeringeiClientMock>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient = createBeringeiClient(
      adapterMock, 1, BeringeiClientImpl::kNoWriterThreads, false, client);

  BeringeiNetworkClient::GetRequestMap partialShardUnowned;
  // Have data for first key, but not for second
  partialShardUnowned[make_pair("", 3)].first.keys.push_back(k1);
  partialShardUnowned[make_pair("", 3)].second = getRes1;
  partialShardUnowned[make_pair("", 5)].first.keys.push_back(k2);
  partialShardUnowned[make_pair("", 5)].second = shardUnownedRes;
  BeringeiNetworkClient::GetRequestMap secondKeyRes;
  secondKeyRes[make_pair("", 5)].first.keys.push_back(k2);
  secondKeyRes[make_pair("", 5)].second = getRes2;

  // beringeiClient should retry with same BeringeiNetworkClient in same scope
  // after invalidating shardCache, we will succeed on that retry
  EXPECT_CALL(*client, performGet(_))
      .WillOnce(SetArgReferee<0>(partialShardUnowned))
      .WillOnce(SetArgReferee<0>(secondKeyRes));

  EXPECT_CALL(
      *client, invalidateCache(std::unordered_set<int64_t>({k2.shardId})))
      .Times(1);

  GetDataResult result;
  beringeiClient->get(getReq, result);

  ASSERT_EQ(getResult.results.size(), result.results.size());
  for (int i = 0; i < result.results.size(); i++) {
    ASSERT_EQ(getResult.results[i].data.size(), result.results[i].data.size());
    for (int j = 0; j < result.results[i].data.size(); j++) {
      EXPECT_EQ(
          getResult.results[i].data[j].count, result.results[i].data[j].count);
    }
  }
}

TEST_F(BeringeiClientTest, ReadClientSameHostFailoverOtherScope) {
  auto client = new StrictMock<BeringeiClientMock>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient = createBeringeiClient(
      adapterMock, 1, BeringeiClientImpl::kNoWriterThreads, false, client);

  BeringeiNetworkClient::GetRequestMap partialShardUnowned;
  // Have data for first key, but not for second talking to same host
  partialShardUnowned[make_pair("", 3)].first.keys.push_back(k1);
  partialShardUnowned[make_pair("", 3)].first.keys.push_back(k2);
  GetDataResult sameHostUnownedRes;
  sameHostUnownedRes.results.insert(
      sameHostUnownedRes.results.end(),
      getRes1.results.begin(),
      getRes1.results.end());
  sameHostUnownedRes.results.insert(
      sameHostUnownedRes.results.end(),
      shardUnownedRes.results.begin(),
      shardUnownedRes.results.end());
  partialShardUnowned[make_pair("", 3)].second = sameHostUnownedRes;
  BeringeiNetworkClient::GetRequestMap secondKeyShardUnowned;
  secondKeyShardUnowned[make_pair("", 5)].first.keys.push_back(k2);
  secondKeyShardUnowned[make_pair("", 5)].second = shardUnownedRes;
  BeringeiNetworkClient::GetRequestMap secondKeyRes;
  secondKeyRes[make_pair("", 3)].first.keys.push_back(k2);
  secondKeyRes[make_pair("", 3)].second = getRes2;

  // beringeiClient should retry with same BeringeiNetworkClient in same scope
  // after invalidating shardCache, then if that fails, use a failover
  // BeringeiNetworkClient in a different read scope
  EXPECT_CALL(*client, performGet(_))
      // Get first key, second key unowned
      .WillOnce(SetArgReferee<0>(partialShardUnowned))
      // Retry for second key on same client fails
      .WillOnce(SetArgReferee<0>(secondKeyShardUnowned))
      // Succeed in other read scope
      .WillOnce(SetArgReferee<0>(secondKeyRes));

  EXPECT_CALL(
      *client, invalidateCache(std::unordered_set<int64_t>({k2.shardId})))
      .Times(1);

  GetDataResult result;
  beringeiClient->get(getReq, result);

  ASSERT_EQ(getResult.results.size(), result.results.size());
  for (int i = 0; i < result.results.size(); i++) {
    ASSERT_EQ(getResult.results[i].data.size(), result.results[i].data.size());
    for (int j = 0; j < result.results[i].data.size(); j++) {
      EXPECT_EQ(
          getResult.results[i].data[j].count, result.results[i].data[j].count);
    }
  }
}

TEST_F(BeringeiClientTest, MultiMasterGet) {
  auto client = new StrictMock<BeringeiClientMock>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient = createBeringeiClient(
      adapterMock, 1, BeringeiClientImpl::kNoWriterThreads, false, client);

  GetDataRequest req;
  req.keys = {buildKey("zero", 0),
              buildKey("one", 1),
              buildKey("two", 2),
              buildKey("three", 3)};

  // Client should issue 4 requests for 2 different shards.
  // In this test environment, all requests go to the same client object.
  // We respond with immediate and delayed success (for different shards),
  // incomplete data, and a timeout (1h delayed response).
  EXPECT_CALL(*client, mockPerformGet(0, std::vector<int64_t>{0, 2}))
      .WillOnce(Return(std::pair<StatusCode, int>{StatusCode::OK, 200}))
      .WillOnce(Return(std::pair<StatusCode, int>{StatusCode::OK, 3600000}));

  EXPECT_CALL(*client, mockPerformGet(1, std::vector<int64_t>{1, 3}))
      .WillOnce(
          Return(std::pair<StatusCode, int>{StatusCode::SHARD_IN_PROGRESS, 0}))
      .WillOnce(Return(std::pair<StatusCode, int>{StatusCode::OK, 0}));

  auto result = beringeiClient->get(req);

  // Client should not block forever and should claim to have succeeded.
  EXPECT_EQ(4, result.results.size());
  EXPECT_TRUE(result.allSuccess);
  EXPECT_GT(result.memoryEstimate, 0);
}

TEST_F(BeringeiClientTest, NetworkClientHandleException) {
  auto client = new StrictMock<BeringeiClientMock>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient = createBeringeiClient(
      adapterMock, 1, BeringeiClientImpl::kNoWriterThreads, false, client);

  BeringeiNetworkClient::GetRequestMap rpcFail;
  rpcFail[make_pair("", 3)].first.keys.push_back(k1);
  rpcFail[make_pair("", 3)].second = rpcFailRes;
  rpcFail[make_pair("", 5)].first.keys.push_back(k2);
  rpcFail[make_pair("", 5)].second = rpcFailRes;
  BeringeiNetworkClient::GetRequestMap expected;
  expected[make_pair("", 3)].first.keys.push_back(k1);
  expected[make_pair("", 3)].second = getRes1;
  expected[make_pair("", 5)].first.keys.push_back(k2);
  expected[make_pair("", 5)].second = getRes2;

  // beringeiClient should retry with same BeringeiNetworkClient in same scope
  // after invalidating shardCache, then if that fails, use a failover
  // BeringeiNetworkClient in a different read scope
  EXPECT_CALL(*client, performGet(_))
      .WillOnce(SetArgReferee<0>(rpcFail))
      .WillOnce(SetArgReferee<0>(expected));

  EXPECT_CALL(
      *client,
      invalidateCache(std::unordered_set<int64_t>({k1.shardId, k2.shardId})))
      .Times(1);

  GetDataResult result;
  beringeiClient->get(getReq, result);

  ASSERT_EQ(getResult.results.size(), result.results.size());
  for (int i = 0; i < result.results.size(); i++) {
    ASSERT_EQ(getResult.results[i].data.size(), result.results[i].data.size());
    for (int j = 0; j < result.results[i].data.size(); j++) {
      EXPECT_EQ(
          getResult.results[i].data[j].count, result.results[i].data[j].count);
    }
  }
}

TEST_F(BeringeiClientTest, NetworkClientThrowException) {
  for (bool throwOnError : {false, true}) {
    auto client = new StrictMock<BeringeiClientMock>(8);
    auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
    auto beringeiClient = createBeringeiClient(
        adapterMock,
        1,
        BeringeiClientImpl::kNoWriterThreads,
        throwOnError,
        client);

    BeringeiNetworkClient::GetRequestMap rpcFirstRequest, rpcSecondRequest;
    BeringeiNetworkClient::GetRequestMap rpcFirstFail, rpcSecondFail;
    rpcFirstRequest[make_pair("", 3)].first.keys.push_back(k1);
    rpcFirstFail[make_pair("", 3)].first.keys.push_back(k1);
    rpcFirstFail[make_pair("", 3)].second = getRes1;
    rpcFirstRequest[make_pair("", 5)].first.keys.push_back(k2);
    rpcFirstFail[make_pair("", 5)].first.keys.push_back(k2);
    rpcFirstFail[make_pair("", 5)].second = rpcFailRes;

    rpcSecondRequest[make_pair("", 5)].first.keys.push_back(k2);
    rpcSecondFail[make_pair("", 5)].first.keys.push_back(k2);
    rpcSecondFail[make_pair("", 5)].second = rpcFailRes;

    EXPECT_CALL(*client, performGet(rpcFirstRequest))
        .WillOnce(SetArgReferee<0>(rpcFirstFail));
    EXPECT_CALL(*client, performGet(rpcSecondRequest))
        .WillRepeatedly(SetArgReferee<0>(rpcSecondFail));

    EXPECT_CALL(
        *client, invalidateCache(std::unordered_set<int64_t>({k2.shardId})))
        .Times(2);

    if (throwOnError) {
      GetDataResult result;
      auto tmpReq = getReq;
      ASSERT_THROW(beringeiClient->get(tmpReq, result), std::runtime_error);
    } else {
      GetDataResult result;
      auto tmpReq = getReq;
      beringeiClient->get(tmpReq, result);

      ASSERT_EQ(getRes2.results.size(), result.results.size());
      for (int i = 0; i < result.results.size(); i++) {
        ASSERT_EQ(
            getRes2.results[i].data.size(), result.results[i].data.size());
        for (int j = 0; j < result.results[i].data.size(); j++) {
          EXPECT_EQ(
              getRes2.results[i].data[j].count,
              result.results[i].data[j].count);
        }
      }
    }
    Mock::VerifyAndClearExpectations(client);
  }
}

TEST_F(BeringeiClientTest, ReadClientFailoverShardInProgress) {
  auto client = new StrictMock<BeringeiClientMock>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient = createBeringeiClient(
      adapterMock, 1, BeringeiClientImpl::kNoWriterThreads, false, client);

  // Should retry both SHARD_IN_PROGRESS and MISSING_TOO_MUCH_DATA.
  BeringeiNetworkClient::GetRequestMap shardInProgress;
  shardInProgress[make_pair("", 3)].first.keys.push_back(k1);
  shardInProgress[make_pair("", 3)].second = inProgressRes;
  shardInProgress[make_pair("", 5)].first.keys.push_back(k2);
  shardInProgress[make_pair("", 5)].second = resWithHoles;

  BeringeiNetworkClient::GetRequestMap expected;
  expected[make_pair("", 3)].first.keys.push_back(k1);
  expected[make_pair("", 3)].second = getRes1;
  expected[make_pair("", 5)].first.keys.push_back(k2);
  expected[make_pair("", 5)].second = getRes2;

  // beringeiClient should retry with different BeringeiNetworkClient in
  // different scope without invalidating shardCache.
  EXPECT_CALL(*client, performGet(_))
      .WillOnce(SetArgReferee<0>(shardInProgress))
      .WillOnce(SetArgReferee<0>(expected));

  GetDataResult result;
  beringeiClient->get(getReq, result);

  ASSERT_EQ(getResult.results.size(), result.results.size());
  for (int i = 0; i < result.results.size(); i++) {
    ASSERT_EQ(getResult.results[i].data.size(), result.results[i].data.size());
    for (int j = 0; j < result.results[i].data.size(); j++) {
      EXPECT_EQ(
          getResult.results[i].data[j].count, result.results[i].data[j].count);
    }
  }
}

TEST_F(BeringeiClientTest, ReadClientShardInProgressPartialResults) {
  auto client = new StrictMock<BeringeiClientMock>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient = createBeringeiClient(
      adapterMock, 1, BeringeiClientImpl::kNoWriterThreads, false, client);

  BeringeiNetworkClient::GetRequestMap shardInProgress;
  shardInProgress[make_pair("", 3)].first.keys.push_back(k1);
  shardInProgress[make_pair("", 3)].second = inProgressRes;
  shardInProgress[make_pair("", 5)].first.keys.push_back(k2);
  shardInProgress[make_pair("", 5)].second = inProgressRes;

  BeringeiNetworkClient::GetRequestMap expected;
  expected[make_pair("", 3)].first.keys.push_back(k1);
  expected[make_pair("", 3)].second = inProgressRes;
  expected[make_pair("", 5)].first.keys.push_back(k2);
  expected[make_pair("", 5)].second = inProgressPartialRes;

  // beringeiClient should retry with different BeringeiNetworkClient in
  // different scope without invalidating shardCache.
  EXPECT_CALL(*client, performGet(_))
      .WillOnce(SetArgReferee<0>(shardInProgress))
      .WillOnce(SetArgReferee<0>(expected));

  GetDataResult result;
  beringeiClient->get(getReq, result);

  // Shards are in progress on both clients. k1 had no data and k2 had some,
  // so we expect to get just k2 back.
  ASSERT_EQ(1, result.results.size());
  ASSERT_EQ(1, getReq.keys.size());
  ASSERT_EQ(k2, getReq.keys[0]);
}

TEST_F(BeringeiClientTest, ReadClientThrowsExceptions) {
  auto client = new StrictMock<BeringeiClientMock>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();

  // Configured to throw exceptions.
  auto beringeiClient = createBeringeiClient(
      adapterMock, 1, BeringeiClientImpl::kNoWriterThreads, true, client);

  BeringeiNetworkClient::GetRequestMap shardInProgress;
  shardInProgress[make_pair("", 3)].first.keys.push_back(k1);
  shardInProgress[make_pair("", 3)].second = inProgressRes;
  shardInProgress[make_pair("", 5)].first.keys.push_back(k2);
  shardInProgress[make_pair("", 5)].second = resWithHoles;

  // Nothing happens if the calls eventually succeed.
  BeringeiNetworkClient::GetRequestMap expected;
  expected[make_pair("", 3)].first.keys.push_back(k1);
  expected[make_pair("", 3)].second = getRes1;
  expected[make_pair("", 5)].first.keys.push_back(k2);
  expected[make_pair("", 5)].second = getRes2;

  // beringeiClient should retry with different BeringeiNetworkClient in
  // different scope without invalidating shardCache.
  EXPECT_CALL(*client, performGet(_))
      .WillOnce(SetArgReferee<0>(shardInProgress))
      .WillOnce(SetArgReferee<0>(expected));

  GetDataResult result;
  beringeiClient->get(getReq, result);

  ASSERT_EQ(getResult.results.size(), result.results.size());
  for (int i = 0; i < result.results.size(); i++) {
    ASSERT_EQ(getResult.results[i].data.size(), result.results[i].data.size());
    for (int j = 0; j < result.results[i].data.size(); j++) {
      EXPECT_EQ(
          getResult.results[i].data[j].count, result.results[i].data[j].count);
    }
  }

  // Verify everything above is good.
  Mock::VerifyAndClear(client);

  // Try again, but MISSING_TOO_MUCH_DATA persists.
  // No exception is thrown, as this is not a transient failure.
  expected.begin()->second.second = resWithHoles;
  EXPECT_CALL(*client, performGet(_))
      .WillOnce(SetArgReferee<0>(shardInProgress))
      .WillOnce(SetArgReferee<0>(expected));

  result.results.clear();
  beringeiClient->get(getReq, result);

  ASSERT_EQ(getResult.results.size(), result.results.size());
  for (int i = 0; i < result.results.size(); i++) {
    ASSERT_EQ(getResult.results[i].data.size(), result.results[i].data.size());
    for (int j = 0; j < result.results[i].data.size(); j++) {
      EXPECT_EQ(
          getResult.results[i].data[j].count, result.results[i].data[j].count);
    }
  }

  // Verify everything above is good.
  Mock::VerifyAndClear(client);

  // Try again, but SHARD_IN_PROGRESS persists.
  // Exception is thrown.
  expected.begin()->second.second = inProgressRes;
  EXPECT_CALL(*client, performGet(_))
      .WillOnce(SetArgReferee<0>(shardInProgress))
      .WillOnce(SetArgReferee<0>(expected));

  ASSERT_ANY_THROW(beringeiClient->get(getReq, result));
}
