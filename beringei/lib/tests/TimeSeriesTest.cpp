/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <gtest/gtest.h>

#include "beringei/lib/TimeSeries.h"

using namespace ::testing;
using namespace facebook::gorilla;
using namespace std;

class TimeSeriesTest : public testing::Test {
 protected:
  void SetUp() override {
    t1_.unixTime = 5;
    t1_.value = 7.0;
    t2_.unixTime = 6;
    t2_.value = 8.0;
    t3_.unixTime = 7;
    t3_.value = 9.0;
    t4_.unixTime = 8;
    t4_.value = 10.0;
  }

  void fill(TimeSeriesBlock& block) {
    TimeSeries::writeValues({t1_, t2_, t3_, t4_}, block);
  }

  TimeValuePair t1_;
  TimeValuePair t2_;
  TimeValuePair t3_;
  TimeValuePair t4_;
};

TEST_F(TimeSeriesTest, FillAndVerifyAllDataPoints) {
  TimeSeriesBlock block;

  fill(block);

  vector<TimeValuePair> values;
  TimeSeries::getValues(block, values, 0, 10);

  ASSERT_EQ(4, values.size());
  EXPECT_EQ(t1_, values[0]);
  EXPECT_EQ(t2_, values[1]);
  EXPECT_EQ(t3_, values[2]);
  EXPECT_EQ(t4_, values[3]);
}

TEST_F(TimeSeriesTest, FillAndVerifyFirstHalf) {
  TimeSeriesBlock block;

  fill(block);

  vector<TimeValuePair> values;
  TimeSeries::getValues(block, values, 0, 6);

  ASSERT_EQ(2, values.size());
  EXPECT_EQ(t1_, values[0]);
  EXPECT_EQ(t2_, values[1]);
}

TEST_F(TimeSeriesTest, FillAndVerifySecondHalf) {
  TimeSeriesBlock block;

  fill(block);

  vector<TimeValuePair> values;
  TimeSeries::getValues(block, values, 7, 10);

  ASSERT_EQ(2, values.size());
  EXPECT_EQ(t3_, values[0]);
  EXPECT_EQ(t4_, values[1]);
}

TEST_F(TimeSeriesTest, FillAndVerifyNoData) {
  TimeSeriesBlock block;

  fill(block);

  vector<TimeValuePair> values;
  TimeSeries::getValues(block, values, 10, 12);

  ASSERT_EQ(0, values.size());
}
