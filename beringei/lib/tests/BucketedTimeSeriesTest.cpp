/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <gflags/gflags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <tuple>

#include "beringei/if/gen-cpp2/beringei_data_types.h"
#include "beringei/lib/BucketStorage.h"
#include "beringei/lib/BucketedTimeSeries.h"
#include "beringei/lib/TimeSeries.h"

// Values coming in faster than this are considered spam
DECLARE_int32(mintimestampdelta);
// Time series queried more buckets ago are considered cold
DECLARE_uint32(cold_bucket_threshold);
// Enable persistently flagging cold data buckets
DECLARE_bool(enable_cold_writes);

using namespace ::testing;
using namespace facebook::gorilla;
using namespace std;

typedef vector<pair<uint8_t, vector<TimeValuePair>>> In;

template <class T>
using Out = vector<tuple<uint8_t, uint8_t, T>>;

typedef vector<TimeSeriesBlock> Block;

template <class Bucket>
void test(
    Bucket& buckets,
    BucketStorage* storage,
    const In& in,
    const Out<typename Bucket::Output>& out) {
  // Insert data.
  for (auto& bucket : in) {
    for (auto& value : bucket.second) {
      buckets.put(bucket.first, value, storage, 0, nullptr);
    }
  }

  // Read it back out.
  for (auto& query : out) {
    typename Bucket::Output output;
    buckets.get(get<0>(query), get<1>(query), output, storage);
    auto& base = get<2>(query);
    ASSERT_EQ(base.size(), output.size());
    for (int i = 0; i < base.size(); i++) {
      EXPECT_EQ(base[i].count, output[i].count);
      EXPECT_EQ(base[i], output[i]);
    }
  }
}

static TimeValuePair makeTV(double v, int64_t t) {
  TimeValuePair tv;
  tv.value = v;
  tv.unixTime = t;
  return tv;
}

class BucketedTimeSeriesTest2 : public testing::Test {
 public:
  BucketedTimeSeriesTest2()
      : flagMinTimestampDelta_(FLAGS_mintimestampdelta),
        flagColdBucketThreshold_(FLAGS_cold_bucket_threshold),
        flagEnableColdWrites_(FLAGS_enable_cold_writes) {}
  ~BucketedTimeSeriesTest2() {
    FLAGS_mintimestampdelta = flagMinTimestampDelta_;
    FLAGS_cold_bucket_threshold = flagColdBucketThreshold_;
    FLAGS_enable_cold_writes = flagEnableColdWrites_;
  }

 private:
  const decltype(FLAGS_mintimestampdelta) flagMinTimestampDelta_;
  const decltype(FLAGS_cold_bucket_threshold) flagColdBucketThreshold_;
  const decltype(FLAGS_enable_cold_writes) flagEnableColdWrites_;
};

class BucketedTimeSeriesColdTest : public BucketedTimeSeriesTest2,
                                   public ::testing::WithParamInterface<bool> {
};

class BucketedTimeSeriesTest : public BucketedTimeSeriesTest2 {
 protected:
  void SetUp() override {
    tv[0] = makeTV(0.0, 60);
    tv[1] = makeTV(2.5, 120);
    tv[2] = makeTV(5.0, 180);
    tv[3] = makeTV(7.5, 240);
    tv[4] = makeTV(10.0, 300);

    blocks.emplace_back();
    TimeSeries::writeValues({tv[0], tv[1], tv[2]}, blocks.back());

    blocks.emplace_back();
    TimeSeries::writeValues({tv[3], tv[4]}, blocks.back());
  }

  In source0() {
    return {
        {7, {tv[0], tv[1], tv[2]}}, {8, {tv[3], tv[4]}},
    };
  }

  Out<vector<TimeSeriesBlock>> ts0() {
    return {
        make_tuple<uint32_t, uint32_t, Block>(3, 4, {}),
        make_tuple<uint32_t, uint32_t, Block>(0, 100, {blocks[0], blocks[1]}),
        make_tuple<uint32_t, uint32_t, Block>(8, 8, {blocks[1]})};
  }

  TimeValuePair tv[5];
  vector<TimeSeriesBlock> blocks;
};

TEST_F(BucketedTimeSeriesTest, TimeSeries) {
  BucketedTimeSeries bucket;
  bucket.reset(5, 0, 0);
  BucketStorageSingle storage(5, 0, "");
  test<BucketedTimeSeries>(bucket, &storage, source0(), ts0());
}

TEST_P(BucketedTimeSeriesColdTest, QueriedBucketsAgo) {
  FLAGS_cold_bucket_threshold = 0;
  FLAGS_enable_cold_writes = GetParam();
  BucketedTimeSeries bucket;
  bucket.reset(5, 0, 0);
  BucketStorageSingle storage(5, 0, "");

  // No queries yet.
  ASSERT_EQ(255, bucket.getQueriedBucketsAgo());

  TimeValuePair value = makeTV(10, 10);

  bucket.put(1, value, &storage, 12, nullptr);

  // Still no queries.
  ASSERT_EQ(255, bucket.getQueriedBucketsAgo());
  // --enable_cold_writes only impacts persistence, not hot/cold determination
  ASSERT_TRUE(bucket.getCold());

  bucket.setQueried();

  // Was just queried.
  ASSERT_EQ(0, bucket.getQueriedBucketsAgo());
  ASSERT_FALSE(bucket.getCold());
  bucket.put(2, value, &storage, 12, nullptr);

  // New bucket started after last get
  ASSERT_EQ(1, bucket.getQueriedBucketsAgo());
  ASSERT_TRUE(bucket.getCold());
}

INSTANTIATE_TEST_CASE_P(
    EnableColdWrites,
    BucketedTimeSeriesColdTest,
    ::testing::Values(false, true));

TEST_F(BucketedTimeSeriesTest2, MinTimestampDeltaCheck) {
  BucketedTimeSeries bucket;
  bucket.reset(5, 0, 0);
  BucketStorageSingle storage(5, 0, "");

  uint32_t timeSeriesId = 0;
  uint16_t* timeSeriesCategory = nullptr;
  uint32_t defaultMinTimestampDelta = FLAGS_mintimestampdelta;
  uint32_t bucketId;
  TimeValuePair tvPair;

  // Attempt to insert first pair in first bucket must always pass
  bucketId = 1;
  tvPair.unixTime = 5;
  ASSERT_EQ(
      true,
      bucket.put(bucketId, tvPair, &storage, timeSeriesId, timeSeriesCategory));
  // Attempt to insert a pair in same bucket with zero delta should fail
  tvPair.unixTime = 5;
  ASSERT_EQ(
      false,
      bucket.put(bucketId, tvPair, &storage, timeSeriesId, timeSeriesCategory));
  // Attempt to insert a pair in same bucket with insufficient delta should fail
  tvPair.unixTime += defaultMinTimestampDelta - 1;
  ASSERT_EQ(
      false,
      bucket.put(bucketId, tvPair, &storage, timeSeriesId, timeSeriesCategory));
  // Attempt to insert a pair in same bucket with sufficient delta should pass
  tvPair.unixTime += 1;
  ASSERT_EQ(
      true,
      bucket.put(bucketId, tvPair, &storage, timeSeriesId, timeSeriesCategory));
  // Attempt to insert a pair in same bucket with > sufficient delta should pass
  tvPair.unixTime += defaultMinTimestampDelta + 1;
  ASSERT_EQ(
      true,
      bucket.put(bucketId, tvPair, &storage, timeSeriesId, timeSeriesCategory));

  bucketId = 2;
  // Attempt to insert a pair in next bucket with zero delta should fail
  tvPair.unixTime += 0;
  ASSERT_EQ(
      false,
      bucket.put(bucketId, tvPair, &storage, timeSeriesId, timeSeriesCategory));
  // Attempt to insert a pair in next bucket with insufficient delta should fail
  tvPair.unixTime += defaultMinTimestampDelta - 1;
  ASSERT_EQ(
      false,
      bucket.put(bucketId, tvPair, &storage, timeSeriesId, timeSeriesCategory));
  // Attempt to insert a pair in next bucket with sufficient delta should pass
  tvPair.unixTime += 1;
  ASSERT_EQ(
      true,
      bucket.put(bucketId, tvPair, &storage, timeSeriesId, timeSeriesCategory));
  // Attempt to insert a pair in same bucket with insufficient delta should fail
  tvPair.unixTime += 1;
  ASSERT_EQ(
      false,
      bucket.put(bucketId, tvPair, &storage, timeSeriesId, timeSeriesCategory));
  // Attempt to insert a pair in same bucket with sufficient delta should pass
  tvPair.unixTime += defaultMinTimestampDelta;
  ASSERT_EQ(
      true,
      bucket.put(bucketId, tvPair, &storage, timeSeriesId, timeSeriesCategory));
}

TEST_F(BucketedTimeSeriesTest2, MinimumBucketAndTimestamp) {
  BucketedTimeSeries b;
  BucketStorageSingle s(5, 0, "");

  auto block1 = s.store(8, "a", 1, 22, 7, false);
  auto block2 = s.store(9, "b", 1, 23, 7, false);

  ASSERT_NE(BucketStorage::kInvalidId, block1);
  ASSERT_NE(BucketStorage::kInvalidId, block2);

  auto tv1 = makeTV(0.0, 72060);
  auto tv2 = makeTV(1.0, 72120);

  auto test = [&]() {
    b.put(10, tv1, &s, 7, nullptr);
    b.setDataBlock(8, &s, block1);
    b.setDataBlock(9, &s, block2);
    b.put(10, tv2, &s, 7, nullptr);

    vector<TimeSeriesBlock> data;
    b.get(0, 100, data, &s);

    uint32_t r = 0;
    for (auto& d : data) {
      switch (d.count) {
        case 22:
          r |= 0b1000;
          break;
        case 23:
          r |= 0b0100;
          break;
        case 1:
          r |= 0b0010;
          break;
        case 2:
          r |= 0b0001;
          break;
      }
    }
    return r;
  };

  // All of these should return both blocks and both points.
  b.reset(5, 0, 0);
  EXPECT_EQ(0b1101, test());
  b.reset(5, 7, 0);
  EXPECT_EQ(0b1101, test());
  b.reset(5, 8, 0);
  EXPECT_EQ(0b1101, test());
  b.reset(5, 8, 7200);
  EXPECT_EQ(0b1101, test());
  b.reset(5, 8, 7260);
  EXPECT_EQ(0b1101, test());

  // Block 8 should be ignored.
  b.reset(5, 9, 0);
  EXPECT_EQ(0b0101, test());

  // No blocks at all.
  b.reset(5, 10, 0);
  EXPECT_EQ(0b0001, test());

  // Accept both blocks but only one point.
  b.reset(5, 0, 72061);
  EXPECT_EQ(0b1110, test());

  // Both blocks, no points.
  b.reset(5, 0, 72121);
  EXPECT_EQ(0b1100, test());

  // No data at all.
  b.reset(5, 10, 72121);
  EXPECT_EQ(0b0000, test());
}
