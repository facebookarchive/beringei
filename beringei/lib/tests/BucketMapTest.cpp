/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <gtest/gtest.h>
#include <atomic>

#include "beringei/lib/BucketMap.h"
#include "beringei/lib/BucketedTimeSeries.h"
#include "beringei/lib/GorillaTimeConstants.h"
#include "beringei/lib/TimeSeries.h"
#include "beringei/lib/Timer.h"
#include "beringei/lib/tests/TestDataLoader.h"
#include "beringei/lib/tests/TestKeyList.h"

using namespace ::testing;
using namespace facebook;
using namespace facebook::gorilla;
using namespace google;

using std::string;
using std::vector;

const int kKeys = 200000;
const int kKeyListSize = 5000;
const int kLoadTestRuns = 1;
const string kDefaultKey = "key";

// Keylist writer changed from blockingWrite to best-effort write when trying
// to add key. This means we need a large enough queue for this keylist.
const int kKeyListQueueSize = kKeys;

DECLARE_int32(zippydb_batch_queue_element_size);

class BucketMapTest : public testing::Test {
 public:
  BucketMapTest() : keyList_(kKeyListSize) {}

 protected:
  void SetUp() override {
    FLAGS_gorilla_async_file_close = false;
    keyReaderFactory_ = std::make_shared<LocalKeyListReaderFactory>();
  }

  void test(BucketMap& map) {
    TimeValuePair tv;
    tv.value = 100.0;
    tv.unixTime = map.timestamp(1);

    // First insertion is 3x as long as bumping counters.
    gorilla::Timer timer(true);
    for (int i = 0; i < kKeys; i++) {
      map.put(keyList_.testStr(i), tv, 0);
    }
    LOG(INFO) << "INSERT 1 : " << timer.get();

    // Bump all the counters.
    timer.reset();
    tv.unixTime += 60;
    for (int i = 0; i < kKeys; i++) {
      map.put(keyList_.testStr(i), tv, 0);
    }
    LOG(INFO) << "INSERT 2 : " << timer.get();

    map.finalizeBuckets(1);

    testReads(map);
  }

  void testReads(BucketMap& map) {
    // Reads are on par with writes.
    typename BucketedTimeSeries::Output out;
    out.reserve(kKeys);
    gorilla::Timer timer(true);
    for (int i = 0; i < kKeys; i++) {
      auto row = map.get(keyList_.testStr(i));
      ASSERT_NE(nullptr, row.get());
      row->second.get(0, 0, out, map.getStorage());
    }
    LOG(INFO) << "GET : " << timer.get();

    // Verify everything got bumped twice.
    for (auto& bucket : out) {
      ASSERT_EQ(2, bucket.count);
    }

    // Copy all the shared_ptrs out.
    vector<typename BucketMap::Item> ptrs;
    timer.reset();
    map.getEverything(ptrs);
    LOG(INFO) << "READ PTRS : " << timer.get();
  }

  int insert(BucketMap& map, vector<vector<TimeValuePair>>& samples) {
    gorilla::Timer timer(true);
    int inserted = 0;
    int added = 0;
    std::set<std::pair<int, int>> seen;

    for (int hour = 0; hour < 24; hour++) {
      for (int i = 0; i < samples.size(); i++) {
        std::string key = keyList_.testStr(i);
        for (auto& tv : samples[i]) {
          auto ret = map.put(key, tv, 0);
          added += ret.first;
          inserted += ret.second;
          seen.insert({i, map.bucket(tv.unixTime)});
          tv.unixTime += kGorillaSecondsPerHour;
        }
      }
    }
    LOG(INFO) << "PUT 24H (" << inserted << " dp) : " << timer.get();
    LOG(INFO) << "ROWS ADDED : " << added;
    LOG(INFO) << "UNIQUE BUCKETS: " << seen.size();
    return seen.size();
  }

  TestKeyList keyList_;
  std::shared_ptr<KeyListReaderFactory> keyReaderFactory_;
};

TEST_F(BucketMapTest, TimeSeries) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "10"));

  auto keyWriter =
      std::make_shared<KeyListWriter>(dir.dirname(), kKeyListQueueSize);
  auto bucketLogWriter = std::make_shared<BucketLogWriter>(
      4 * kGorillaSecondsPerHour, dir.dirname(), 100, 0);
  keyWriter->startShard(10);
  bucketLogWriter->startShard(10);

  BucketMap map(
      6,
      4 * kGorillaSecondsPerHour,
      10,
      dir.dirname(),
      keyWriter,
      bucketLogWriter,
      BucketMap::OWNED,
      std::make_shared<LocalLogReaderFactory>(dir.dirname()),
      keyReaderFactory_);
  test(map);
}

TEST_F(BucketMapTest, Reload) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "10"));

  auto bucketLogWriter = std::make_shared<BucketLogWriter>(
      4 * kGorillaSecondsPerHour, dir.dirname(), 100, 0);
  bucketLogWriter->startShard(10);

  int32_t ts2, ts3;

  {
    // Fill, then close the BucketMap.
    auto keyWriter =
        std::make_shared<KeyListWriter>(dir.dirname(), kKeyListQueueSize);
    keyWriter->startShard(10);
    BucketMap map(
        6,
        4 * kGorillaSecondsPerHour,
        10,
        dir.dirname(),
        keyWriter,
        bucketLogWriter,
        BucketMap::OWNED,
        std::make_shared<LocalLogReaderFactory>(dir.dirname()),
        keyReaderFactory_);
    test(map);

    ts2 = map.timestamp(2);
    ts3 = map.timestamp(3);
  }

  auto keyWriter =
      std::make_shared<KeyListWriter>(dir.dirname(), kKeyListQueueSize);
  keyWriter->startShard(10);
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(2));

  {
    // Create a new one, reading the keys from disk.
    BucketMap map(
        6,
        4 * kGorillaSecondsPerHour,
        10,
        dir.dirname(),
        keyWriter,
        bucketLogWriter,
        BucketMap::OWNED,
        std::make_shared<LocalLogReaderFactory>(dir.dirname()),
        keyReaderFactory_);
    map.setState(BucketMap::PRE_UNOWNED);
    map.setState(BucketMap::UNOWNED);

    gorilla::Timer timer(true);
    map.setState(BucketMap::PRE_OWNED);
    map.readKeyList();
    map.readData();
    while (map.readBlockFiles()) {
    }
    LOG(INFO) << "READ FROM DISK : " << timer.get();

    std::vector<BucketMap::Item> items;
    map.getEverything(items);

    std::set<std::string> keySet;
    for (auto& item : items) {
      if (item) {
        keySet.insert(item->first);
      }
    }

    for (int i = 0; i < kKeys; i++) {
      EXPECT_GT(keySet.count(keyList_.testStr(i)), 0)
          << "testStr(" << i << ") = " << keyList_.testStr(i);
    }

    testReads(map);
  }

  // We need to enable it again.
  keyWriter->startShard(10, true);

  // Now wipe the key_list file and reload the data yet again.
  // Create a key with a timestamp that post-dates the data on disk so we can
  // verify it doesn't get loaded.
  bool one = false;
  keyWriter->compact(10, [&one, ts2]() {
    if (!one) {
      one = true;
      return std::tuple<uint32_t, const char*, uint16_t, int32_t>{
          0, "a_key", 0, ts2};
    }
    return std::tuple<uint32_t, const char*, uint16_t, int32_t>{
        0, nullptr, 0, 0};
  });

  // Read it all again.
  // This time, insert a point before reading blocks. This point should not have
  // older data.
  BucketMap map(
      6,
      4 * kGorillaSecondsPerHour,
      10,
      dir.dirname(),
      keyWriter,
      bucketLogWriter,
      BucketMap::OWNED,
      std::make_shared<LocalLogReaderFactory>(dir.dirname()),
      keyReaderFactory_);
  map.setState(BucketMap::PRE_UNOWNED);
  map.setState(BucketMap::UNOWNED);

  gorilla::Timer timer(true);
  map.setState(BucketMap::PRE_OWNED);
  map.readKeyList();
  map.readData();

  // Add a point. This will get assigned an ID that still has block
  // data on disk.
  TimeValuePair tv;
  tv.value = 100.0;
  tv.unixTime = ts2;
  map.put("another_key", tv, 0);

  while (map.readBlockFiles()) {
  }
  LOG(INFO) << "READ FROM DISK : " << timer.get();

  // Make sure we have only the keys we expect.
  std::vector<BucketMap::Item> everything;
  map.getEverything(everything);
  int have = 0;
  for (auto& thing : everything) {
    if (thing.get()) {
      have++;
    }
  }
  ASSERT_EQ(2, have); // "a_key" and "another_key".

  // Neither key should be associated with old data.
  BucketedTimeSeries::Output o;
  map.get("a_key")->second.get(0, ts3, o, map.getStorage());
  ASSERT_EQ(1, o.size());
  o.clear();
  map.get("another_key")->second.get(0, ts3, o, map.getStorage());
  ASSERT_EQ(1, o.size());
}

TEST_F(BucketMapTest, Load) {
  // Repeatedly insert points for 5k timeseries 24 times.
  // Timestamps are in the range [1377721380, 1377730980], which conveniently
  // all falls in bucket 6.
  // Keep 2 extra buckets to guarantee we can always read out 24 hr data.

  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "10"));

  auto keyWriter = std::make_shared<KeyListWriter>(dir.dirname(), 100);
  auto bucketLogWriter = std::make_shared<BucketLogWriter>(
      4 * kGorillaSecondsPerHour, dir.dirname(), 100, 0);
  keyWriter->startShard(10);
  bucketLogWriter->startShard(10);

  BucketMap map(
      8,
      4 * kGorillaSecondsPerHour,
      10,
      dir.dirname(),
      keyWriter,
      bucketLogWriter,
      BucketMap::OWNED,
      std::make_shared<LocalLogReaderFactory>(dir.dirname()),
      keyReaderFactory_);
  vector<vector<TimeValuePair>> samples;
  for (int i = 0; i < kLoadTestRuns; i++) {
    loadData(samples);
  }

  int buckets = insert(map, samples);

  BucketedTimeSeries::Output out;
  out.reserve(buckets);
  uint64_t begin = 1377721380;
  uint64_t end = 1377730980 + 23 * kGorillaSecondsPerHour;

  gorilla::Timer timer(true);
  for (int i = 0; i < samples.size(); i++) {
    auto row = map.get(keyList_.testStr(i));
    ASSERT_NE(nullptr, row.get());
    row->second.get(map.bucket(begin), map.bucket(end), out, map.getStorage());
  }
  LOG(INFO) << "GET : " << timer.get();

  // Verify we got everything back out.
  EXPECT_EQ(buckets, out.size());
}

TEST_F(BucketMapTest, ShardTransitions) {
  // Going to a bigger state is always allowed.
  for (int i = (int)BucketMap::PRE_UNOWNED; i <= (int)BucketMap::OWNED; i++) {
    for (int j = i + 1; j <= (int)BucketMap::OWNED; j++) {
      ASSERT_TRUE(BucketMap::isAllowedStateTransition(
          (BucketMap::State)i, (BucketMap::State)j));
    }
  }

  // Going to the same or smaller state is not allowed with one exception.
  for (int i = (int)BucketMap::PRE_UNOWNED; i <= (int)BucketMap::OWNED; i++) {
    for (int j = (int)BucketMap::PRE_UNOWNED; j <= i; j++) {
      if (i == (int)BucketMap::OWNED && j == (int)BucketMap::PRE_UNOWNED) {
        // Only allowed transition going to a smaller state.
        ASSERT_TRUE(BucketMap::isAllowedStateTransition(
            (BucketMap::State)i, (BucketMap::State)j));
      } else {
        ASSERT_FALSE(BucketMap::isAllowedStateTransition(
            (BucketMap::State)i, (BucketMap::State)j));
      }
    }
  }
}

TEST_F(BucketMapTest, QueuedPutNewKey) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "10"));

  auto keyWriter = std::make_shared<KeyListWriter>(dir.dirname(), 100);
  auto bucketLogWriter = std::make_shared<BucketLogWriter>(
      4 * kGorillaSecondsPerHour, dir.dirname(), 100, 0);

  BucketMap map(
      8,
      4 * kGorillaSecondsPerHour,
      10,
      dir.dirname(),
      keyWriter,
      bucketLogWriter,
      BucketMap::UNOWNED,
      std::make_shared<LocalLogReaderFactory>(dir.dirname()),
      keyReaderFactory_);

  TimeValuePair value;
  value.unixTime = time(nullptr);
  value.value = 100;

  // Dropped because shard is not owned.
  auto ret = map.put(kDefaultKey, value, 0);
  ASSERT_EQ(BucketMap::kNotOwned, ret.first);
  ASSERT_EQ(BucketMap::kNotOwned, ret.second);

  map.setState(BucketMap::PRE_OWNED);
  ret = map.put(kDefaultKey, value, 0);
  ASSERT_NE(BucketMap::kNotOwned, ret.first);
  ASSERT_NE(BucketMap::kNotOwned, ret.second);

  // Currently queued
  auto item = map.get(kDefaultKey);
  ASSERT_EQ(nullptr, item);

  map.readKeyList();
  // Still queued
  item = map.get(kDefaultKey);
  ASSERT_EQ(nullptr, item);

  map.readData();
  // Now available.
  item = map.get(kDefaultKey);
  ASSERT_NE(nullptr, item);
}

TEST_F(BucketMapTest, QueuedPutExistingKey) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "10"));

  auto bucketLogWriter = std::make_shared<BucketLogWriter>(
      4 * kGorillaSecondsPerHour, dir.dirname(), 100, 0);
  bucketLogWriter->startShard(10);

  TimeValuePair dp1, dp2, dp3;
  dp1.value = 100.0;
  dp1.unixTime = time(nullptr);
  dp2.value = 110.0;
  dp2.unixTime = dp1.unixTime + 60;
  dp3.value = 120.0;
  dp3.unixTime = dp2.unixTime + 60;

  int windowSize = 4 * kGorillaSecondsPerHour;

  {
    // Fill, then close the BucketMap.
    auto keyWriter = std::make_shared<KeyListWriter>(dir.dirname(), 100);
    keyWriter->startShard(10);

    BucketMap map(
        6,
        windowSize,
        10,
        dir.dirname(),
        keyWriter,
        bucketLogWriter,
        BucketMap::OWNED,
        std::make_shared<LocalLogReaderFactory>(dir.dirname()),
        keyReaderFactory_);

    map.put(kDefaultKey, dp1, 0);
    auto item = map.get(kDefaultKey);
    ASSERT_NE(nullptr, item);

    // To flush
    bucketLogWriter->stopShard(10);
  }

  bucketLogWriter->flushQueue();
  bucketLogWriter->startShard(10);

  auto keyWriter = std::make_shared<KeyListWriter>(dir.dirname(), 100);
  keyWriter->startShard(10);

  // Create a new one, reading the keys from disk.
  BucketMap map(
      6,
      windowSize,
      10,
      dir.dirname(),
      keyWriter,
      bucketLogWriter,
      BucketMap::UNOWNED,
      std::make_shared<LocalLogReaderFactory>(dir.dirname()),
      keyReaderFactory_);

  map.setState(BucketMap::PRE_OWNED);

  // Queued with string because key list is not read
  auto ret = map.put(kDefaultKey, dp2, 0);
  ASSERT_NE(BucketMap::kNotOwned, ret.first);
  ASSERT_NE(BucketMap::kNotOwned, ret.second);
  map.readKeyList();

  // Queued with time series id
  ret = map.put(kDefaultKey, dp3, 0);
  ASSERT_NE(BucketMap::kNotOwned, ret.first);
  ASSERT_NE(BucketMap::kNotOwned, ret.second);
  map.readData();

  auto item = map.get(kDefaultKey);
  ASSERT_NE(nullptr, item);

  BucketedTimeSeries::Output output;
  item->second.get(
      map.bucket(dp1.unixTime),
      map.bucket(dp3.unixTime),
      output,
      map.getStorage());

  std::vector<TimeValuePair> values;
  for (auto& out : output) {
    TimeSeries::getValues(out, values, dp1.unixTime, dp3.unixTime);
  }

  vector<TimeValuePair> expected = {dp1, dp2, dp3};
  ASSERT_EQ(expected, values);
}

TEST_F(BucketMapTest, CorruptKeys) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "10"));

  auto bucketLogWriter = std::make_shared<BucketLogWriter>(
      4 * kGorillaSecondsPerHour, dir.dirname(), 100, 0);
  bucketLogWriter->startShard(10);
  auto keyWriter = std::make_shared<KeyListWriter>(dir.dirname(), 100);
  keyWriter->startShard(10);

  BucketMap map(
      6,
      4 * kGorillaSecondsPerHour,
      10,
      dir.dirname(),
      keyWriter,
      bucketLogWriter,
      BucketMap::UNOWNED,
      std::make_shared<LocalLogReaderFactory>(dir.dirname()),
      keyReaderFactory_);

  map.setState(BucketMap::PRE_OWNED);

  keyWriter->addKey(10, 0, "key with valid id", 16, 0);
  keyWriter->addKey(10, 0xDEADBEEF, "key with too large id", 0, 0);
  keyWriter->stopShard(10);
  keyWriter->flushQueue();

  map.readKeyList();
  map.readData();

  auto item = map.get("key with valid id");
  ASSERT_NE(nullptr, item);
  ASSERT_EQ(16, item->second.getCategory());
  item = map.get("key with too large id");
  ASSERT_EQ(nullptr, item);
}

TEST_F(BucketMapTest, DuplicateKeys) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "10"));

  auto bucketLogWriter = std::make_shared<BucketLogWriter>(
      4 * kGorillaSecondsPerHour, dir.dirname(), 100, 0);
  bucketLogWriter->startShard(10);
  auto keyWriter = std::make_shared<KeyListWriter>(dir.dirname(), 100);
  keyWriter->startShard(10);

  BucketMap map(
      6,
      4 * kGorillaSecondsPerHour,
      10,
      dir.dirname(),
      keyWriter,
      bucketLogWriter,
      BucketMap::UNOWNED,
      std::make_shared<LocalLogReaderFactory>(dir.dirname()),
      keyReaderFactory_);

  map.setState(BucketMap::PRE_OWNED);

  keyWriter->addKey(10, 0, "duplicate key", 16, 0);
  keyWriter->addKey(10, 1, "duplicate key", 0, 0);
  keyWriter->stopShard(10);
  keyWriter->flushQueue();

  map.readKeyList();
  map.readData();

  BucketMap::Item key = map.get("duplicate key");
  EXPECT_NE(nullptr, key);

  std::vector<BucketMap::Item> everything;
  map.getEverything(everything);
  int total = 0;
  for (auto& item : everything) {
    if (item) {
      EXPECT_EQ(key, item);
      total++;
    }
  }
  ASSERT_EQ(1, total);
}

static std::unique_ptr<BucketMap> buildBucketMap(
    const char* tempDir,
    uint32_t bucketSize = 4 * kGorillaSecondsPerHour,
    bool usePrimaryTopology = false) {
  VLOG(1) << "Building bucketmap with usePrimaryTopology="
          << usePrimaryTopology;
  auto bucketLogWriter =
      std::make_shared<BucketLogWriter>(bucketSize, tempDir, 100, 0);
  auto keyWriter = std::make_shared<KeyListWriter>(tempDir, 100);
  auto keyReaderFactory = std::make_shared<LocalKeyListReaderFactory>();

  std::unique_ptr<BucketMap> map(new BucketMap(
      6,
      bucketSize,
      10,
      tempDir,
      keyWriter,
      bucketLogWriter,
      BucketMap::UNOWNED,
      std::make_shared<LocalLogReaderFactory>(tempDir),
      keyReaderFactory,
      usePrimaryTopology));

  map->setState(BucketMap::PRE_OWNED);
  map->setState(BucketMap::OWNED);
  return map;
}

static uint32_t kTestDataSize = 10;

static void addTestData(
    std::unique_ptr<BucketMap>& map,
    uint32_t seriesBeginning) {
  for (int i = 0; i < kTestDataSize; i++) {
    for (int j = 0; j < kTestDataSize; j++) {
      TimeValuePair value;
      value.unixTime = j * 60 + seriesBeginning;
      value.value = i == j ? kTestDataSize : 0;
      string key = kDefaultKey + std::to_string(i);
      auto ret = map->put(key, value, 0);
      ASSERT_EQ(1, ret.second);
    }
  }
}

TEST_F(BucketMapTest, SingleTimeSeriesWithOneDeviation) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "10"));

  auto map = buildBucketMap(dir.dirname().c_str());
  int start = map->timestamp(0);

  int kDeviatingPoint = 5;
  for (int i = 0; i < 10; i++) {
    TimeValuePair value;
    value.unixTime = start + i * 60;
    value.value = i == kDeviatingPoint ? 10 : 1;
    auto ret = map->put(kDefaultKey, value, 0);
    ASSERT_EQ(1, ret.second);
  }

  // The deviating value will be more than 2.0 standard deviations
  // away from the mean.
  ASSERT_EQ(
      1, map->indexDeviatingTimeSeries(start, start, start + 10 * 60, 2.0));
  for (int i = 0; i < 10; i++) {
    auto deviations = map->getDeviatingTimeSeries(start + i * 60);
    ASSERT_EQ(i == kDeviatingPoint ? 1 : 0, deviations.size());
  }
}

TEST_F(BucketMapTest, SingleTimeSeriesWhereEverythingDeviates) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "10"));

  auto map = buildBucketMap(dir.dirname().c_str());
  int start = map->timestamp(0);

  for (int i = 0; i < 10; i++) {
    TimeValuePair value;
    value.unixTime = start + i * 60;
    value.value = i % 2 == 0 ? 0 : 100;
    auto ret = map->put(kDefaultKey, value, 0);
    ASSERT_EQ(1, ret.second);
  }

  // Every value will be more than 0.1 standard deviations away from
  // the mean.
  ASSERT_EQ(
      10, map->indexDeviatingTimeSeries(start, start, start + 10 * 60, 0.1));
  for (int i = 0; i < 10; i++) {
    auto deviations = map->getDeviatingTimeSeries(start + i * 60);
    ASSERT_EQ(1, deviations.size());
  }
}

TEST_F(BucketMapTest, MultipleTimeSeriesWithDifferentDeviations) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "10"));

  auto map = buildBucketMap(dir.dirname().c_str());

  int start = map->timestamp(0);
  addTestData(map, start);

  ASSERT_EQ(
      10, map->indexDeviatingTimeSeries(start, start, start + 10 * 60, 2.0));
  for (int i = 0; i < 10; i++) {
    auto deviations = map->getDeviatingTimeSeries(start + i * 60);
    ASSERT_EQ(1, deviations.size());

    string expectedKey = kDefaultKey + std::to_string(i);
    ASSERT_EQ(expectedKey, deviations[0]->first);
  }
}

TEST_F(BucketMapTest, DoubleErase) {
  // This can happen due to a race condition in purging and blacklisting.
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "10"));
  TimeValuePair value;
  value.unixTime = 1000;
  value.value = 100;

  auto map = buildBucketMap(dir.dirname().c_str());

  // Insert.
  map->put(kDefaultKey, value, 0);
  ASSERT_NE(nullptr, map->get(kDefaultKey));

  std::vector<BucketMap::Item> everything;
  map->getEverything(everything);
  ASSERT_EQ(1, everything.size());

  // Remove.
  map->erase(
      0,
      everything.front()->first.c_str(),
      everything.front()->second.getCategory());
  ASSERT_EQ(nullptr, map->get(kDefaultKey));

  // Insert again, then remove the old reference a second time.
  map->put(kDefaultKey, value, 0);
  everything.clear();
  map->getEverything(everything);
  size_t idx = 0;
  for (size_t i = 0; i < everything.size(); ++i) {
    if (everything[i]) {
      idx = i;
      break;
    }
  }

  map->erase(
      idx,
      everything[idx]->first.c_str(),
      everything[idx]->second.getCategory());

  ASSERT_EQ(nullptr, map->get(kDefaultKey));

  everything.clear();
  map->getEverything(everything);
  for (auto row : everything) {
    ASSERT_EQ(nullptr, row);
  }
  ASSERT_TRUE(map->consistencyCheck());
}

TEST_F(BucketMapTest, RoleTest) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "10"));

  auto map =
      buildBucketMap(dir.dirname().c_str(), 4 * kGorillaSecondsPerHour, true);
  ASSERT_TRUE(map->setRole(true));
  ASSERT_FALSE(map->setRole(true));
  ASSERT_TRUE(map->setRole(false));
}
