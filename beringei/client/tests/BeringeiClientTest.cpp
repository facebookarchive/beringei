/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <limits>

#include <folly/Random.h>
#include <folly/container/Enumerate.h>
#include <folly/futures/Future.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "beringei/client/BeringeiClientImpl.h"
#include "beringei/client/BeringeiConfigurationAdapterIf.h"
#include "beringei/client/BeringeiNetworkClient.h"
#include "beringei/client/tests/MockConfigurationAdapter.h"
#include "beringei/if/gen-cpp2/BeringeiService.h"
#include "beringei/lib/BucketedTimeSeries.h"
#include "beringei/lib/TimeSeries.h"

using namespace ::testing;
using namespace facebook::gorilla;
using namespace folly;
using namespace std;

DECLARE_int32(mintimestampdelta);

namespace {
using HostInfo = std::pair<std::string, int>;

facebook::gorilla::Key buildKey(const std::string& key, int shard) {
  facebook::gorilla::Key k;
  k.key = key;
  k.shardId = shard;
  return k;
}

TimeValuePair tvp(int64_t unixTime, double value) {
  TimeValuePair tv;
  tv.unixTime = unixTime;
  tv.value = value;
  return tv;
}

std::bitset<32> allServices(int services) {
  return (1 << services) - 1;
}

std::unique_ptr<BeringeiClientImpl> createBeringeiClient(
    std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
    int queueCapacity,
    bool throwExceptionOnPartialRead,
    const std::vector<std::shared_ptr<BeringeiNetworkClient>>& readers,
    const std::vector<BeringeiNetworkClient*>& writers = {}) {
  auto client = std::make_unique<BeringeiClientImpl>(
      configurationAdapter, throwExceptionOnPartialRead);

  client->initializeTestClients(queueCapacity, readers, writers);
  return client;
}

void expectEq(const BeringeiScanStats& lhs, const BeringeiScanStats& rhs) {
  EXPECT_EQ(lhs.getNumServices(), rhs.getNumServices());
  EXPECT_EQ(lhs.getTotal(), rhs.getTotal());

  for (size_t service = 0; service < lhs.getNumServices(); ++service) {
    bool lhsMissing;
    int64_t lhsCount;
    bool rhsMissing;
    int64_t rhsCount;

    lhsMissing = lhs.getMissingByService(service, &lhsCount);
    rhsMissing = lhs.getMissingByService(service, &rhsCount);
    EXPECT_TRUE(
        (!lhsMissing && !rhsMissing) ||
        (lhsMissing && rhsMissing && lhsCount == rhsCount));

    lhsMissing = lhs.getMissingByNumServices(service, &lhsCount);
    rhsMissing = lhs.getMissingByNumServices(service, &rhsCount);
    EXPECT_TRUE(
        (!lhsMissing && !rhsMissing) ||
        (lhsMissing && rhsMissing && lhsCount == rhsCount));
  }
}

void expectEq(
    const BeringeiScanShardResult& lhs,
    const BeringeiScanShardResult& rhs) {
  // Fail predictably on lhs and rhs class invariant violations
  CHECK_EQ(lhs.data.size(), lhs.keys.size());
  CHECK_EQ(lhs.queriedRecently.size(), lhs.keys.size());
  CHECK_EQ(rhs.data.size(), rhs.keys.size());
  CHECK_EQ(rhs.queriedRecently.size(), rhs.keys.size());

  EXPECT_EQ(lhs.status, rhs.status);
  EXPECT_EQ(lhs.keys.size(), rhs.keys.size());
  EXPECT_EQ(lhs.beginTime, rhs.beginTime);
  EXPECT_EQ(lhs.endTime, rhs.endTime);

  std::map<std::string, size_t> lhsToIndex;
  for (const auto& i : folly::enumerate(lhs.keys)) {
    const auto insertResult = lhsToIndex.insert(std::make_pair(*i, i.index));
    CHECK(insertResult.second);
  }

  std::map<unsigned, unsigned> lhsToRhs;
  for (size_t i = 0; i < rhs.keys.size(); ++i) {
    const auto lhsIndex = lhsToIndex.find(rhs.keys[i]);
    EXPECT_NE(lhsIndex, lhsToIndex.end());
    const auto insertResult =
        lhsToRhs.insert(std::make_pair(lhsIndex->second, i));
    CHECK(insertResult.second);
  }

  for (size_t lhsIndex = 0; lhsIndex < lhs.keys.size(); ++lhsIndex) {
    const unsigned rhsIndex = lhsToRhs[lhsIndex];
    const auto lhsData = lhs.getUncompressedData(lhsIndex);
    const auto rhsData = rhs.getUncompressedData(rhsIndex);
    EXPECT_EQ(lhsData, rhsData);
    EXPECT_EQ(lhs.queriedRecently[lhsIndex], rhs.queriedRecently[rhsIndex]);
  }

  expectEq(lhs.keyStats, rhs.keyStats);
}
} // namespace

namespace facebook {
namespace gorilla {
DECLARE_bool(gorilla_parallel_scan_shard);
} // namespace gorilla
} // namespace facebook

class BeringeiClientMock : public BeringeiNetworkClient {
 public:
  explicit BeringeiClientMock(int numShards)
      : BeringeiNetworkClient(), numShards_(numShards) {}

  MOCK_METHOD1(performGet, void(BeringeiNetworkClient::GetRequestMap&));

  MOCK_METHOD1(invalidateCache, void(const std::unordered_set<int64_t>&));

  MOCK_METHOD1(
      performPut,
      vector<DataPoint>(BeringeiNetworkClient::PutRequestMap& requests));

  Future<std::vector<DataPoint>> futurePerformPut(
      PutDataRequest& request,
      const std::pair<std::string, int>& hostInfo) override {
    BeringeiNetworkClient::PutRequestMap requests;
    requests[hostInfo] = request;
    LOG(INFO) << "################ host info: " << hostInfo.second;
    return makeFuture(performPut(requests));
  }
  void addKeyToGetRequest(
      const facebook::gorilla::Key& key,
      BeringeiNetworkClient::GetRequestMap& requests) override {
    auto& request = requests[make_pair("", key.shardId)];
    request.first.keys.push_back(key);
  }

  bool getHostForShard(int64_t shard, HostInfo& hostInfo) override {
    hostInfo.first = "some_host";
    hostInfo.second = shard % getNumShards();
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

  MOCK_METHOD2(
      mockPerformScanShard,
      ScanShardResult(
          const HostInfo& hostInfo,
          const ScanShardRequest& request));

  virtual folly::Future<ScanShardResult> performScanShard(
      const std::pair<std::string, int>& hostInfo,
      const ScanShardRequest& request,
      folly::EventBase*) override {
    ScanShardResult result = mockPerformScanShard(hostInfo, request);
    return makeFuture<ScanShardResult>(std::move(result));
  }

  virtual void performScanShard(
      const ScanShardRequest& request,
      ScanShardResult& result) override {
    std::pair<std::string, int> hostInfo;
    bool success = getHostForShard(request.shardId, hostInfo);

    if (!success) {
      result = {};
      result.status = StatusCode::RPC_FAIL;
    } else {
      result = mockPerformScanShard(hostInfo, request);
    }
  }

 private:
  int64_t numShards_;

  static folly::EventBase* getEventBase() {
    return folly::EventBaseManager::get()->getEventBase();
  }
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
    b1.reset(5, 0, 0);
    b2.reset(5, 0, 0);
    BucketStorageSingle storage(5, 0, "");

    for (int i = 19; i < 22; i++) {
      TimeValuePair tv;
      tv.unixTime = i * FLAGS_mintimestampdelta;
      tv.value = i;
      b1.put(i, tv, &storage, 0, nullptr);
      resultVec[0].second.push_back(tv);
    }

    for (int i = 19; i < 22; i++) {
      TimeValuePair tv;
      tv.unixTime = (i + 7) * FLAGS_mintimestampdelta;
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
};

class BeringeiClientScanShardTest : public testing::Test {
 public:
  BeringeiClientScanShardTest()
      : parallelFlag_(FLAGS_gorilla_parallel_scan_shard),
        forever_(std::numeric_limits<decltype(forever_)>::max()),
        k1_(buildKey("k1", shard_)),
        k2_(buildKey("k2", shard_)) {
    scanShardReq_.shardId = shard_;
    scanShardReq_.begin = 0;
    scanShardReq_.end = forever_;
  }

  ~BeringeiClientScanShardTest() {
    FLAGS_gorilla_parallel_scan_shard = parallelFlag_;
  }

  ScanShardRequest scanShardReq_;
  using UnixTime = decltype(scanShardReq_.end);

  static const int shard_ = 1;

  const bool parallelFlag_;

  const UnixTime forever_;

  const facebook::gorilla::Key k1_;
  const facebook::gorilla::Key k2_;
};

struct ShardParam {
  ShardParam(int count, bool enable) : count_(count), enable_(enable) {}
  const int count_;
  const bool enable_;
};

// template<class S> operator(S& lhs) is ambiguous
template <typename T>
auto& operator<<(std::basic_ostream<T>& lhs, const ShardParam& rhs) {
  lhs << '(' << rhs.count_ << ", " << (rhs.enable_ ? "true" : "false") << ')';
  return lhs;
}

class BeringeiClientScanShardNwayTest
    : public BeringeiClientScanShardTest,
      public ::testing::WithParamInterface<ShardParam> {};

// PUTS

TEST_F(BeringeiClientTest, Put) {
  auto client = new StrictMock<BeringeiClientMock>(3);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient =
      createBeringeiClient(adapterMock, 7, false, {}, {client});

  PutDataRequest req1;
  req1.data.push_back(dps[0]);
  PutDataRequest req2;
  req2.data.push_back(dps[1]);
  vector<DataPoint> dps2 = dps;

  BeringeiNetworkClient::PutRequestMap expected1;
  expected1[make_pair("some_host", 1)] = req1;

  EXPECT_CALL(*client, performPut(expected1))
      .WillOnce(Return(std::vector<DataPoint>{}));

  BeringeiNetworkClient::PutRequestMap expected2;
  expected2[make_pair("some_host", 2)] = req2;
  EXPECT_CALL(*client, performPut(expected2))
      .WillOnce(Return(std::vector<DataPoint>{}));

  EXPECT_TRUE(beringeiClient->putDataPoints(dps));

  beringeiClient->flushQueue();

  EXPECT_CALL(*client, performPut(expected1))
      .WillOnce(Return(std::vector<DataPoint>{}));
  EXPECT_CALL(*client, performPut(expected2))
      .WillOnce(Return(std::vector<DataPoint>{}));
  EXPECT_TRUE(beringeiClient->putDataPoints(dps2));
}

TEST_F(BeringeiClientTest, PutRetryAll) {
  FLAGS_gorilla_retry_delay_secs = 0;

  auto client = new StrictMock<BeringeiClientMock>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient =
      createBeringeiClient(adapterMock, 12, false, {}, {client});

  {
    ::testing::InSequence dummy;
    EXPECT_CALL(*client, performPut(_)).WillOnce(Return(dps));
    EXPECT_CALL(*client, performPut(_))
        .WillRepeatedly(Return(std::vector<DataPoint>{}));
  }

  EXPECT_TRUE(beringeiClient->putDataPoints(dps));
  beringeiClient->flushQueue();
}

TEST_F(BeringeiClientTest, PutRetryOne) {
  FLAGS_gorilla_retry_delay_secs = 0;

  auto client = new StrictMock<BeringeiClientMock>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient =
      createBeringeiClient(adapterMock, 12, false, {}, {client});

  PutDataRequest req1;
  req1.data.push_back(dps[0]);
  BeringeiNetworkClient::PutRequestMap retryExpected;
  retryExpected[make_pair("some_host", 1)] = req1;

  {
    ::testing::InSequence dummy;
    EXPECT_CALL(*client, performPut(_))
        .WillOnce(Return(std::vector<DataPoint>{dps[0]}))
        .RetiresOnSaturation();
    EXPECT_CALL(*client, performPut(_))
        .WillOnce(Return(std::vector<DataPoint>{}))
        .RetiresOnSaturation();

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
      createBeringeiClient(adapterMock, 7, false, {}, {client, shadowClient});

  EXPECT_CALL(*client, performPut(_))
      .Times(2)
      .WillRepeatedly(Return(std::vector<DataPoint>{}));

  {
    ::testing::InSequence dummy;
    EXPECT_CALL(*shadowClient, performPut(_))
        .WillOnce(Return(dps))
        .RetiresOnSaturation();
    EXPECT_CALL(*shadowClient, performPut(_))
        .WillRepeatedly(Return(std::vector<DataPoint>{}));
  }

  EXPECT_TRUE(beringeiClient->putDataPoints(dps));
  beringeiClient->flushQueue();
}

// GETS

TEST_F(BeringeiClientTest, Get) {
  auto client = std::make_shared<StrictMock<BeringeiClientMock>>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient =
      createBeringeiClient(adapterMock, 1, false, {client, client}, {});

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
  auto client = std::make_shared<StrictMock<BeringeiClientMock>>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient =
      createBeringeiClient(adapterMock, 1, false, {client, client}, {});

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
  fullReq.begin = 19 * FLAGS_mintimestampdelta;
  fullReq.end = 29 * FLAGS_mintimestampdelta;
  result.clear();
  beringeiClient->get(fullReq, result);
  ASSERT_EQ(resultVec.size(), result.size());

  auto sortFn =
      [](const std::pair<facebook::gorilla::Key, std::vector<TimeValuePair>>& a,
         const std::pair<facebook::gorilla::Key, std::vector<TimeValuePair>>&
             b) { return a.first.key < b.first.key; };

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
  auto client = std::make_shared<StrictMock<BeringeiClientMock>>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient =
      createBeringeiClient(adapterMock, 1, false, {client, client}, {});

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
  auto client = std::make_shared<StrictMock<BeringeiClientMock>>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient =
      createBeringeiClient(adapterMock, 1, false, {client, client}, {});

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
  auto client = std::make_shared<StrictMock<BeringeiClientMock>>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient =
      createBeringeiClient(adapterMock, 1, false, {client, client}, {});

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
  auto client = std::make_shared<StrictMock<BeringeiClientMock>>(2);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient =
      createBeringeiClient(adapterMock, 1, false, {client, client}, {});

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
  EXPECT_GT(result.stats.memoryEstimate, 0);
}

TEST_F(BeringeiClientTest, NetworkClientHandleException) {
  auto client = std::make_shared<StrictMock<BeringeiClientMock>>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient =
      createBeringeiClient(adapterMock, 1, false, {client, client}, {});

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
    auto client = std::make_shared<StrictMock<BeringeiClientMock>>(8);
    auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
    auto beringeiClient = createBeringeiClient(
        adapterMock, 1, throwOnError, {client, client}, {});

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
    Mock::VerifyAndClearExpectations(client.get());
  }
}

TEST_F(BeringeiClientTest, ReadClientFailoverShardInProgress) {
  auto client = std::make_shared<StrictMock<BeringeiClientMock>>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient =
      createBeringeiClient(adapterMock, 1, false, {client, client}, {});

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
  auto client = std::make_shared<StrictMock<BeringeiClientMock>>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient =
      createBeringeiClient(adapterMock, 1, false, {client, client}, {});

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
  auto client = std::make_shared<StrictMock<BeringeiClientMock>>(8);
  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();

  // Configured to throw exceptions.
  auto beringeiClient =
      createBeringeiClient(adapterMock, 1, true, {client, client}, {});

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
  Mock::VerifyAndClear(client.get());

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
  Mock::VerifyAndClear(client.get());

  // Try again, but SHARD_IN_PROGRESS persists.
  // Exception is thrown.
  expected.begin()->second.second = inProgressRes;
  EXPECT_CALL(*client, performGet(_))
      .WillOnce(SetArgReferee<0>(shardInProgress))
      .WillOnce(SetArgReferee<0>(expected));

  ASSERT_ANY_THROW(beringeiClient->get(getReq, result));
}

TEST_P(BeringeiClientScanShardNwayTest, Basic) {
  FLAGS_gorilla_parallel_scan_shard = GetParam().enable_;

  const int services = GetParam().enable_ ? GetParam().count_ : 1;
  const std::bitset<32> serviceDataValid = allServices(services);

  ScanShardResult provideNetwork;
  BeringeiScanShardResult expectBeringei;

  expectBeringei.beginTime = scanShardReq_.begin;
  expectBeringei.endTime = scanShardReq_.end;
  expectBeringei.status = provideNetwork.status = StatusCode::OK;
  expectBeringei.keys = provideNetwork.keys = {k1_.key};
  expectBeringei.data.push_back({});
  expectBeringei.queriedRecently = provideNetwork.queriedRecently = {false};

  expectBeringei.numServices = services;
  expectBeringei.serviceNames = std::vector<std::string>(services);
  expectBeringei.serviceDataValid = serviceDataValid;
  expectBeringei.keyStats = BeringeiScanStats(services, serviceDataValid);
  expectBeringei.keyStats.setTotal(expectBeringei.keys.size());
  expectBeringei.keyStats.setMissingByNumServices(0, 1);

  std::vector<TimeValuePair> expectData;
  for (unsigned i = 0; i < 3; ++i) {
    expectData.push_back(tvp(19 + i * FLAGS_mintimestampdelta, i));
  }
  TimeSeriesBlock block;
  TimeSeries::writeValues(expectData, block);
  expectBeringei.data[0] = provideNetwork.data = {{block}};

  BeringeiScanStats expectStats(services, serviceDataValid);
  expectStats.setTotal(expectData.size());
  if (services < 3) {
    expectStats.setMissingByNumServices(0, expectData.size());
  } else {
    expectStats.setMissingByNumServicesInvalid();
  }

  std::vector<std::shared_ptr<BeringeiNetworkClient>> readers;

  while (readers.size() < GetParam().count_) {
    auto client = std::make_shared<StrictMock<BeringeiClientMock>>(8);

    HostInfo hostInfo;
    EXPECT_TRUE(client->getHostForShard(scanShardReq_.shardId, hostInfo));

    if (readers.empty() || GetParam().enable_) {
      EXPECT_CALL(*client, mockPerformScanShard(hostInfo, scanShardReq_))
          .WillOnce(Return(provideNetwork));
    }

    readers.push_back(client);
  }

  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient = createBeringeiClient(adapterMock, 1, false, readers);

  BeringeiScanShardResult result = beringeiClient->scanShard(scanShardReq_);

  expectEq(result, expectBeringei);

  BeringeiScanStats gotStats;
  std::vector<TimeValuePair> gotData =
      result.takeUncompressedData(0, &gotStats);
  EXPECT_EQ(gotData, expectData);
  expectEq(gotStats, expectStats);
}

INSTANTIATE_TEST_CASE_P(
    Count,
    BeringeiClientScanShardNwayTest,
    ::testing::Values(
        ShardParam(1, true),
        ShardParam(2, true),
        ShardParam(2, false),
        ShardParam(3, true)));

TEST_F(BeringeiClientScanShardTest, TimeoutAndMerge) {
  FLAGS_gorilla_parallel_scan_shard = true;

  const int services = 3;
  const std::bitset<32> serviceDataValid = allServices(2);

  std::vector<std::string> keys = {k1_.key, k2_.key};
  // [0], [1] alternate in same series; [2] is [0] and [1] together;
  // [3] is different
  std::array<std::vector<TimeValuePair>, 4> data;
  std::array<TimeSeriesBlock, 4> dataCompressed;

  std::array<ScanShardResult, 3> networkResult;
  BeringeiScanShardResult expectBeringei(
      scanShardReq_, 2, std::vector<std::string>(3), serviceDataValid);

  networkResult[0].status = networkResult[1].status = StatusCode::OK;
  networkResult[2].status = StatusCode::RPC_FAIL;
  expectBeringei.keys = keys;
  networkResult[0].keys = {keys[0]};
  networkResult[1].keys = {keys[0], keys[1]};

  for (unsigned i = 0; i < 6; ++i) {
    const auto pair = tvp(19 + i * FLAGS_mintimestampdelta, i);
    data[i % 2].push_back(pair);
    data[2].push_back(pair);
  }
  for (unsigned i = 6; i > 9; ++i) {
    data[3].push_back(tvp(19 + i * FLAGS_mintimestampdelta, i));
  }
  for (unsigned i = 0; i < data.size(); ++i) {
    TimeSeries::writeValues(data[i], dataCompressed[i]);
  }
  std::map<std::string, std::vector<TimeValuePair>> expectData = {
      {keys[0], data[2]}, {keys[1], data[3]}};

  networkResult[0].data = {{dataCompressed[0]}};
  networkResult[1].data = {{dataCompressed[1]}, {dataCompressed[3]}};

  expectBeringei.data = {{networkResult[0].data[0], networkResult[1].data[0]},
                         {networkResult[1].data[1]}};

  networkResult[0].queriedRecently = {false};
  expectBeringei.queriedRecently =
      networkResult[1].queriedRecently = {false, false};

  expectBeringei.numServices = services;
  expectBeringei.serviceNames = std::vector<std::string>(services);
  expectBeringei.serviceDataValid = serviceDataValid;
  expectBeringei.keyStats = BeringeiScanStats(services, serviceDataValid);

  expectBeringei.keyStats.setTotal(expectBeringei.keys.size());
  expectBeringei.keyStats.setMissingByService(
      {(int64_t)(keys.size() - networkResult[0].keys.size()),
       (int64_t)(keys.size() - networkResult[1].keys.size()),
       0});

  BeringeiScanStats expectStats(services, serviceDataValid);
  expectStats.setTotal(expectData.size());
  expectStats.setMissingByService(
      {3 /* half of k1 */, 6 /* half of k1 and all k2 */});

  std::vector<std::shared_ptr<BeringeiNetworkClient>> readers;

  std::function<void()> doSleep = []() {
    /* sleep override */ std::this_thread::sleep_for(
        2 * std::chrono::milliseconds(BeringeiNetworkClient::getTimeoutMs()));
  };
  for (unsigned i = 0; i < 3; ++i) {
    auto client = std::make_shared<StrictMock<BeringeiClientMock>>(8);
    HostInfo hostInfo;
    EXPECT_TRUE(client->getHostForShard(scanShardReq_.shardId, hostInfo));
    if (i < 2) {
      EXPECT_CALL(*client, mockPerformScanShard(hostInfo, scanShardReq_))
          .WillOnce(Return(networkResult[i]));
    } else {
      EXPECT_CALL(*client, mockPerformScanShard(hostInfo, scanShardReq_))
          .WillOnce(
              DoAll(InvokeWithoutArgs(doSleep), Return(networkResult[i])));
    }
    readers.push_back(client);
  }

  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient = createBeringeiClient(adapterMock, 1, false, readers);

  BeringeiScanShardResult result = beringeiClient->scanShard(scanShardReq_);

  expectEq(result, expectBeringei);

  for (const auto& key : folly::enumerate(result.keys)) {
    auto const& expect = expectData[*key];

    auto got = result.getUncompressedData(key.index);
    EXPECT_EQ(got, expect);

    got = result.takeUncompressedData(key.index);
    EXPECT_EQ(got, expect);
  }

  // Block until after delayed third BeringeiNetworkClient::performScanShard
  // invocation completes so its code runs before test termination, instead
  // of being cancelled during program shutdown.
  doSleep();
}

TEST_F(BeringeiClientScanShardNwayTest, StatsMissingNumByServices) {
  FLAGS_gorilla_parallel_scan_shard = true;

  const int services = 2;
  const std::bitset<32> serviceDataValid = allServices(services);

  std::string key = k1_.key;
  // [2] is combined result
  std::array<std::vector<TimeValuePair>, 2> data;
  std::array<TimeSeriesBlock, 2> dataCompressed;
  std::array<ScanShardResult, 2> networkResult;

  BeringeiScanShardResult expectBeringei(
      scanShardReq_,
      1 /* keys */,
      std::vector<std::string>(2),
      serviceDataValid);

  networkResult[0].status = networkResult[1].status = StatusCode::OK;
  expectBeringei.keys = networkResult[0].keys = networkResult[1].keys = {key};
  expectBeringei.keyStats.setMissingByNumServices(0, 0);

  std::vector<TimeValuePair> expectData;
  for (unsigned i = 0; i < 3; ++i) {
    const auto pair = tvp(19 + i * FLAGS_mintimestampdelta, i);
    expectData.push_back(pair);
  }
  data[0] = {expectData[0], expectData[1]};
  data[1] = {expectData[1], expectData[2]};
  for (unsigned i = 0; i < data.size(); ++i) {
    TimeSeries::writeValues(data[i], dataCompressed[i]);
  }

  networkResult[0].data = {{dataCompressed[0]}};
  networkResult[1].data = {{dataCompressed[1]}};

  expectBeringei.data = {{networkResult[0].data[0], networkResult[1].data[0]}};

  networkResult[0].queriedRecently = networkResult[1].queriedRecently = {false};

  expectBeringei.numServices = services;
  expectBeringei.serviceNames = std::vector<std::string>(services);
  expectBeringei.serviceDataValid = serviceDataValid;
  expectBeringei.keyStats = BeringeiScanStats(services, serviceDataValid);

  expectBeringei.keyStats.setTotal(expectBeringei.keys.size());
  expectBeringei.keyStats.setMissingByService({0, 0});

  BeringeiScanStats expectStats(services, serviceDataValid);
  expectStats.setTotal(expectData.size());
  expectStats.setMissingByService({1, 1});
  // data[1] is common, data[0] and data[1] on one region only
  expectStats.setMissingByNumServices({1, 2});

  std::vector<std::shared_ptr<BeringeiNetworkClient>> readers;
  for (unsigned i = 0; i < networkResult.size(); ++i) {
    auto client = std::make_shared<StrictMock<BeringeiClientMock>>(8);
    HostInfo hostInfo;
    EXPECT_TRUE(client->getHostForShard(scanShardReq_.shardId, hostInfo));
    EXPECT_CALL(*client, mockPerformScanShard(hostInfo, scanShardReq_))
        .WillOnce(Return(networkResult[i]));
    readers.push_back(client);
  }

  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient = createBeringeiClient(adapterMock, 1, false, readers);

  BeringeiScanShardResult result = beringeiClient->scanShard(scanShardReq_);

  expectEq(result, expectBeringei);

  BeringeiScanStats gotStats;
  std::vector<TimeValuePair> gotData =
      result.takeUncompressedData(0, &gotStats);
  EXPECT_EQ(gotData, expectData);
  expectEq(gotStats, expectStats);
}

TEST_F(BeringeiClientScanShardTest, BucketNotFinalized) {
  FLAGS_gorilla_parallel_scan_shard = true;

  const int services = 2;
  const std::bitset<32> serviceDataValid = 0b10;

  std::vector<std::string> keys = {k1_.key};
  std::vector<TimeValuePair> data;
  TimeSeriesBlock dataCompressed;

  std::array<ScanShardResult, 2> networkResult;
  BeringeiScanShardResult expectBeringei(
      scanShardReq_, 1, std::vector<std::string>(2), serviceDataValid);

  networkResult[0].status = StatusCode::BUCKET_NOT_FINALIZED;
  networkResult[1].status = StatusCode::OK;
  expectBeringei.keys = networkResult[1].keys = keys;

  for (unsigned i = 0; i < 3; ++i) {
    data.push_back(tvp(19 + i * FLAGS_mintimestampdelta, i));
  }
  TimeSeries::writeValues(data, dataCompressed);
  std::map<std::string, std::vector<TimeValuePair>> expectData = {
      {k1_.key, data}};

  networkResult[1].data = {{dataCompressed}};
  expectBeringei.data = {{{}, {networkResult[1].data[0]}}};
  expectBeringei.queriedRecently = networkResult[1].queriedRecently = {false};
  expectBeringei.keyStats = BeringeiScanStats(services, serviceDataValid);

  expectBeringei.keyStats.setTotal(expectBeringei.keys.size());
  expectBeringei.keyStats.setMissingByService({(int64_t)keys.size(), 0});

  BeringeiScanStats expectStats(services, serviceDataValid);
  expectStats.setTotal(expectData.size());
  expectStats.setMissingByService(0, data.size());

  std::vector<std::shared_ptr<BeringeiNetworkClient>> readers;

  const int delay = 1000;
  CHECK_LT(delay, BeringeiNetworkClient::getTimeoutMs());
  std::function<void()> doSleep = [delay]() {
    /* sleep override */ std::this_thread::sleep_for(
        std::chrono::milliseconds(delay));
  };
  for (unsigned i = 0; i < 2; ++i) {
    auto client = std::make_shared<StrictMock<BeringeiClientMock>>(8);
    HostInfo hostInfo;
    EXPECT_TRUE(client->getHostForShard(scanShardReq_.shardId, hostInfo));
    if (i < 1) {
      EXPECT_CALL(*client, mockPerformScanShard(hostInfo, scanShardReq_))
          .WillOnce(Return(networkResult[i]));
    } else {
      EXPECT_CALL(*client, mockPerformScanShard(hostInfo, scanShardReq_))
          .WillOnce(
              DoAll(InvokeWithoutArgs(doSleep), Return(networkResult[i])));
    }
    readers.push_back(client);
  }

  auto adapterMock = std::make_shared<StrictMock<MockConfigurationAdapter>>();
  auto beringeiClient = createBeringeiClient(adapterMock, 1, false, readers);

  BeringeiScanShardResult result = beringeiClient->scanShard(scanShardReq_);

  expectEq(result, expectBeringei);

  for (const auto& key : folly::enumerate(result.keys)) {
    auto const& expect = expectData[*key];

    auto got = result.getUncompressedData(key.index);
    EXPECT_EQ(got, expect);

    got = result.takeUncompressedData(key.index);
    EXPECT_EQ(got, expect);
  }
}
