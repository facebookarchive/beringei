/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <gtest/gtest.h>

#include "TestDataLoader.h"
#include "beringei/lib/GorillaTimeConstants.h"
#include "beringei/lib/TimeSeriesStream.h"

#include <time.h>
#include <unordered_map>

using namespace ::testing;
using namespace facebook;
using namespace facebook::gorilla;
using namespace std;

bool append(
    TimeSeriesStream& stream,
    int64_t unixTime,
    double value,
    int64_t minDelta = 60) {
  TimeValuePair timeValue;
  timeValue.unixTime = unixTime;
  timeValue.value = value;
  return stream.append(timeValue, minDelta);
}

TEST(TimeSeriesStreamTest, InsertAndRead) {
  TimeSeriesStream stream;

  append(stream, 0.0, 5.0, 0);
  append(stream, 60.0, 6.0, 0);
  append(stream, 120.0, 7.0, 0);
  append(stream, 180.0, 7.0, 0);
  append(stream, 240.0, 8.0, 0);
  append(stream, 300.0, 0.3333, 0);

  vector<TimeValuePair> out;

  string data;
  stream.readData(data);
  TimeSeriesStream::readValues(out, data, 6);
  ASSERT_EQ(0.0, out[0].unixTime);
  ASSERT_EQ(5.0, out[0].value);
  ASSERT_EQ(60.0, out[1].unixTime);
  ASSERT_EQ(6.0, out[1].value);
  ASSERT_EQ(120.0, out[2].unixTime);
  ASSERT_EQ(7.0, out[2].value);
  ASSERT_EQ(180.0, out[3].unixTime);
  ASSERT_EQ(7.0, out[3].value);
  ASSERT_EQ(240.0, out[4].unixTime);
  ASSERT_EQ(8.0, out[4].value);
  ASSERT_EQ(300.0, out[5].unixTime);
  ASSERT_EQ(0.3333, out[5].value);
}

TEST(TimeSeriesStreamTest, InsertAndReadConsecutiveTimestampValues) {
  TimeSeriesStream stream;

  append(stream, 5, 1, 1);
  append(stream, 6, 1, 1);
  append(stream, 7, 1, 1);

  vector<TimeValuePair> out;

  string data;
  stream.readData(data);
  TimeSeriesStream::readValues(out, data, 3);
  ASSERT_EQ(5, out[0].unixTime);
  ASSERT_EQ(6, out[1].unixTime);
  ASSERT_EQ(7, out[2].unixTime);
}

TEST(TimeSeriesStreamTest, InsertAndReadHugeAndSmallDifferencesInTimestamps) {
  TimeSeriesStream stream;

  append(stream, 5, 1, 1);
  append(stream, 60000000, 1, 1);
  append(stream, 60000007, 1, 1);
  append(stream, 1234567890, 1, 1);
  append(stream, 1234567891, 1, 1);
  append(stream, 2000000000, 1, 1);

  vector<TimeValuePair> out;

  string data;
  stream.readData(data);
  TimeSeriesStream::readValues(out, data, 6);
  ASSERT_EQ(5, out[0].unixTime);
  ASSERT_EQ(60000000, out[1].unixTime);
  ASSERT_EQ(60000007, out[2].unixTime);
  ASSERT_EQ(1234567890, out[3].unixTime);
  ASSERT_EQ(1234567891, out[4].unixTime);
  ASSERT_EQ(2000000000, out[5].unixTime);
}

TEST(TimeSeriesStreamTest, SpammyTimeStream) {
  TimeSeriesStream stream;

  ASSERT_TRUE(append(stream, 100, 1, 60));
  ASSERT_TRUE(append(stream, 160, 1, 60));
  ASSERT_FALSE(append(stream, 190, 1, 60));
  ASSERT_TRUE(append(stream, 220, 1, 60));
  ASSERT_FALSE(append(stream, 160, 1, 60));

  vector<TimeValuePair> out;
  string data;
  stream.readData(data);
  TimeSeriesStream::readValues(out, data, 3);
  ASSERT_EQ(100, out[0].unixTime);
  ASSERT_EQ(160, out[1].unixTime);
  ASSERT_EQ(220, out[2].unixTime);
}

TEST(TimeSeriesStreamTest, RealSamples) {
  vector<vector<TimeValuePair>> samples;
  loadData(samples);

  uint32_t totalLength = 0;
  uint32_t packedLength = 0;
  uint32_t dataPoints = 0;

  // WallClock uses the CLOCK_REALTIME
  struct timespec encodingTimeStart;
  struct timespec encodingTimeStop;
  struct timespec decodingTimeStart;
  struct timespec decodingTimeStop;
  int64_t encodingTime = 0;
  int64_t decodingTime = 0;

  for (auto& timeSeries : samples) {
    clock_gettime(CLOCK_REALTIME, &encodingTimeStart);
    TimeSeriesStream stream;
    dataPoints += timeSeries.size();
    for (auto& value : timeSeries) {
      append(stream, value.unixTime, value.value, 0);
    }

    clock_gettime(CLOCK_REALTIME, &encodingTimeStop);

    vector<TimeValuePair> out;
    clock_gettime(CLOCK_REALTIME, &decodingTimeStart);
    encodingTime += timespecDiff(encodingTimeStop, encodingTimeStart);
    decodingTime += timespecDiff(decodingTimeStop, decodingTimeStart);

    string data;
    stream.readData(data);
    TimeSeriesStream::readValues(out, data, timeSeries.size());
    clock_gettime(CLOCK_REALTIME, &decodingTimeStop);
    for (int i = 0; i < timeSeries.size(); i++) {
      ASSERT_EQ(timeSeries[i].unixTime, out[i].unixTime);
      ASSERT_EQ(timeSeries[i].value, out[i].value);
    }
    packedLength += stream.size();
    totalLength += timeSeries.size() * (sizeof(double) + sizeof(int64_t));
  }

  LOG(INFO) << "ENCODING TOOK : " << encodingTime;
  LOG(INFO) << "DECODING TOOK : " << decodingTime;
  LOG(INFO) << "ENCODED SIZE : " << packedLength;
  LOG(INFO) << "DECODED SIZE : " << totalLength;

  LOG(INFO) << "DATA POINTS : " << dataPoints;
  LOG(INFO) << "BYTES PER DATA POINT : " << ((double)packedLength) / dataPoints;
}

double doubleRand(double min, double max) {
  double f = (double)random() / RAND_MAX;
  return min + f * (max - min);
}

TEST(TimeSeriesStreamTest, FuzzyTest) {
  srandom(2);

  TimeSeriesStream stream;
  vector<pair<int64_t, double>> values;
  int64_t t = 0;
  for (int i = 0; i < 100000; i++) {
    double value = doubleRand(
        std::numeric_limits<int16_t>::min(),
        std::numeric_limits<int16_t>::max());

    t += random() % 100 + 30;
    values.push_back(make_pair(t, value));
    append(stream, t, value, 30);
  }

  vector<TimeValuePair> out;
  string data;
  stream.readData(data);
  TimeSeriesStream::readValues(out, data, values.size());

  for (int i = 0; i < values.size(); i++) {
    ASSERT_EQ(values[i].first, out[i].unixTime);
    ASSERT_EQ(values[i].second, out[i].value);
  }
}

TEST(TimeSeriesStreamTest, InsertAndReadMap) {
  TimeSeriesStream stream;

  append(stream, 0.0, 5.0, 0);
  append(stream, 60.0, 6.0, 0);
  append(stream, 120.0, 7.0, 0);
  append(stream, 180.0, 7.0, 0);
  append(stream, 240.0, 8.0, 0);
  append(stream, 300.0, 0.3333, 0);

  vector<TimeValuePair> out;

  string data;
  stream.readData(data);
  int ret = TimeSeriesStream::readValues(out, data, 6);
  ASSERT_EQ(6, ret);

  std::unordered_map<int64_t, double> values;
  TimeSeriesStream::readValues(values, data, 6);
  ASSERT_EQ(6, values.size());

  for (auto& dp : out) {
    ASSERT_EQ(dp.value, values[dp.unixTime]);
  }
}

TEST(TimeSeriesStreamTest, GetFirstTimeStamp) {
  TimeSeriesStream stream;

  append(stream, 100, 5.0, 0);
  append(stream, 200, 6.0, 0);
  append(stream, 300, 7.0, 0);

  ASSERT_EQ(100, stream.getFirstTimeStamp());
}
