/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "beringei/client/BeringeiGetResult.h"
#include "beringei/lib/TimeSeries.h"

using namespace ::testing;
using namespace facebook::gorilla;
using namespace folly;
using namespace std;

class BeringeiGetResultTest : public testing::Test {
 protected:
  void SetUp() override {
    ok = result({{}}, StatusCode::OK);
    missing = result({{}}, StatusCode::KEY_MISSING);
    unowned = result({{}}, StatusCode::DONT_OWN_SHARD);
  }

  TimeValuePair tvp(int64_t unixTime, double value) {
    TimeValuePair tv;
    tv.unixTime = unixTime;
    tv.value = value;
    return tv;
  }

  // Put data points into a TimeSeriesData object.
  // Allow no more than 3 points per TimeSeriesBlock to make sure we exercise
  // all the deserialization logic.
  TimeSeriesData timeSeriesData(const vector<pair<int, int>>& data) {
    const int perBlock = 3;

    TimeSeriesData out;

    std::vector<TimeValuePair> tvs;
    for (int i = 0; i < data.size(); i++) {
      tvs.push_back(tvp(data[i].first, data[i].second));
      if (tvs.size() >= perBlock) {
        out.data.emplace_back();
        TimeSeries::writeValues(tvs, out.data.back());
        tvs.clear();
      }
    }
    // Add the remainder (doesn't really matter if it's empty).
    out.data.emplace_back();
    TimeSeries::writeValues(tvs, out.data.back());

    return out;
  }

  GetDataResult result(
      const vector<vector<pair<int, int>>>& data,
      StatusCode status) {
    GetDataResult r;
    r.results.reserve(data.size());

    for (int i = 0; i < data.size(); i++) {
      r.results.push_back(timeSeriesData(data[i]));
      r.results.back().status = status;
    }

    return r;
  }

  GetDataResult ok, missing, unowned;
};

TEST_F(BeringeiGetResultTest, Merge) {
  BeringeiGetResultCollector collector(3, 2, 60, 240);

  collector.addResults(
      result(
          {{{0, 0}, {60, 1}, {120, 2}},
           {{60, 1}, {180, 3}, {240, 4}},
           {{60, 1}, {120, 2}, {180, 3}, {240, 4}}},
          StatusCode::OK),
      {0, 1, 2},
      0);

  collector.addResults(
      result(
          {{{62, 1}, {118, 2}, {181, 3}, {239, 4}},
           {{60, 1}, {120, 2}, {180, 3}, {240, 4}},
           {{180, 3}, {240, 4}, {300, 5}}},
          StatusCode::OK),
      {2, 1, 0}, // This set of timeseries is in a reversed order.
      1);

  auto result = collector.finalize(true, {"", ""});

  vector<vector<TimeValuePair>> expected = {
      {tvp(60, 1), tvp(120, 2), tvp(180, 3), tvp(240, 4)},
      {tvp(60, 1), tvp(120, 2), tvp(180, 3), tvp(240, 4)},
      {tvp(60, 1), tvp(118, 2), tvp(180, 3), tvp(239, 4)}};

  EXPECT_THAT(result.results, ContainerEq(expected));
}

TEST_F(BeringeiGetResultTest, Complete) {
  BeringeiGetResultCollector collector(2, 2, 60, 240);

  EXPECT_FALSE(collector.addResults(missing, {0}, 0));
  EXPECT_FALSE(collector.addResults(missing, {0}, 1));
  EXPECT_TRUE(collector.addResults(ok, {1}, 1));
  EXPECT_FALSE(collector.addResults(ok, {1}, 0));
  EXPECT_TRUE(collector.finalize(false, {"", ""}).allSuccess);
}

TEST_F(BeringeiGetResultTest, ShardsMissing) {
  BeringeiGetResultCollector collector(2, 2, 60, 240);

  EXPECT_FALSE(collector.addResults(ok, {0}, 0));
  EXPECT_FALSE(collector.addResults(unowned, {0}, 1));
  EXPECT_TRUE(collector.addResults(ok, {1}, 1));
  EXPECT_FALSE(collector.addResults(unowned, {1}, 0));
  EXPECT_TRUE(collector.finalize(false, {"", ""}).allSuccess);
}

TEST_F(BeringeiGetResultTest, Timeout) {
  BeringeiGetResultCollector collector(2, 2, 60, 240);

  EXPECT_FALSE(collector.addResults(ok, {0}, 0));
  EXPECT_FALSE(collector.addResults(ok, {0}, 1));
  EXPECT_TRUE(collector.addResults(ok, {1}, 1));
  EXPECT_TRUE(collector.finalize(false, {"", ""}).allSuccess);
}

TEST_F(BeringeiGetResultTest, Incomplete) {
  BeringeiGetResultCollector collector(2, 2, 60, 240);

  EXPECT_FALSE(collector.addResults(ok, {0}, 0));
  EXPECT_FALSE(collector.addResults(unowned, {1}, 0));
  EXPECT_FALSE(collector.addResults(ok, {0}, 1));
  EXPECT_FALSE(collector.addResults(unowned, {1}, 1));
  EXPECT_FALSE(collector.finalize(false, {"", ""}).allSuccess);
}
