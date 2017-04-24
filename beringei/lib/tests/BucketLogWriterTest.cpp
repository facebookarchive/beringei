/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <gtest/gtest.h>

#include "beringei/lib/BucketLogWriter.h"
#include "beringei/lib/BucketUtils.h"
#include "beringei/lib/DataLog.h"
#include "beringei/lib/FileUtils.h"

using namespace ::testing;
using namespace facebook;
using namespace facebook::gorilla;
using namespace std;

class BucketLogWriterTest : public testing::Test {
 protected:
  void SetUp() override {
    FLAGS_gorilla_async_file_close = false;
  }
};

void readSingleValueFromLog(
    FileUtils& fileUtils,
    int shardId,
    uint32_t expectedId,
    uint32_t expectedUnixTime,
    double expectValue,
    uint32_t windowSize) {
  uint32_t baseTime = expectedUnixTime;
  auto f = fileUtils.open(expectedUnixTime, "rb", 0);
  if (f.file == nullptr) {
    // The file has been opened in advanced with the bucket starting
    // time file name.
    baseTime =
        BucketUtils::floorTimestamp(expectedUnixTime, windowSize, shardId);
    f = fileUtils.open(baseTime, "rb", 0);
  }
  ASSERT_NE(nullptr, f.file);

  uint32_t id;
  uint32_t unixTime;
  double value;

  int points = DataLogReader::readLog(
      f, baseTime, [&](uint32_t _id, uint32_t _unixTime, double _value) {
        id = _id;
        unixTime = _unixTime;
        value = _value;
        return true;
      });

  ASSERT_EQ(expectedId, id);
  ASSERT_EQ(expectedUnixTime, unixTime);
  ASSERT_EQ(expectValue, value);

  ASSERT_EQ(1, points);
  fclose(f.file);
}

TEST_F(BucketLogWriterTest, WriteSingleValue) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "54"));

  int shardId = 54;
  int windowSize = 100;
  int unixTime = 6480;

  // This test uses internal information from BucketLogWriter...
  FileUtils fileUtils(shardId, "log", dir.dirname());
  fileUtils.clearAll();

  BucketLogWriter writer(windowSize, dir.dirname(), 10, 0);
  writer.startShard(shardId);
  writer.logData(shardId, 37, unixTime, 38.0);
  writer.stopShard(shardId);
  writer.flushQueue();

  readSingleValueFromLog(fileUtils, shardId, 37, unixTime, 38.0, windowSize);
}

TEST_F(BucketLogWriterTest, ThreadedWrite) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "23"));

  int shardId = 23;
  int windowSize = 100;
  int unixTime = BucketUtils::floorTimestamp(5000, windowSize, shardId);
  int ts1 = unixTime + 1;
  int ts2 = unixTime + windowSize - 1;
  int ts3 = unixTime + windowSize;
  int ts4 = unixTime + 5 * windowSize / 2;

  FileUtils fileUtils(shardId, "log", dir.dirname());
  fileUtils.clearAll();

  BucketLogWriter writer(windowSize, dir.dirname(), 10, 0);

  writer.startShard(shardId);
  writer.logData(shardId, 37, ts1, 1.0);
  writer.logData(shardId, 38, ts2, 2.0);
  writer.logData(shardId, 39, ts3, 3.0);
  writer.logData(shardId, 40, ts4, 4.0);
  writer.stopShard(shardId);

  // Sleep a while to let the writer thread do its job.
  writer.flushQueue();

  auto f = fileUtils.open(ts1, "rb", 0);
  ASSERT_NE(nullptr, f.file);

  vector<uint32_t> ids;
  vector<int64_t> times;
  vector<double> values;
  int points = DataLogReader::readLog(
      f, ts1, [&](uint32_t id, uint32_t timestamp, double value) {
        ids.push_back(id);
        times.push_back(timestamp);
        values.push_back(value);
        return true;
      });

  vector<uint32_t> expectedIds = {37, 38};
  vector<int64_t> expectedTimes = {ts1, ts2};
  vector<double> expectedValues = {1.0, 2.0};

  EXPECT_EQ(expectedIds, ids);
  EXPECT_EQ(expectedTimes, times);
  EXPECT_EQ(expectedValues, values);

  EXPECT_EQ(2, points);
  fclose(f.file);

  readSingleValueFromLog(fileUtils, shardId, 39, ts3, 3.0, windowSize);
  readSingleValueFromLog(fileUtils, shardId, 40, ts4, 4.0, windowSize);
}

TEST_F(BucketLogWriterTest, MultipleShards) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "23"));
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "24"));

  int windowSize = 100;
  BucketLogWriter writer(windowSize, dir.dirname(), 10, 0);
  writer.logData(23, 37, 5001, 1.0); // Blocked and dropped
  writer.startShard(23);
  writer.logData(23, 38, 5002, 2.0);
  writer.logData(24, 39, 5003, 3.0); // Blocked and dropped
  writer.startShard(24);
  writer.logData(24, 41, 5005, 5.0);
  writer.stopShard(23);
  writer.logData(23, 42, 5006, 6.0); // Blocked and dropped
  writer.stopShard(24);
  writer.logData(24, 44, 5008, 8.0); // Blocked and dropped

  writer.flushQueue();

  FileUtils fileUtils23(23, "log", dir.dirname());
  readSingleValueFromLog(fileUtils23, 23, 38, 5002, 2.0, windowSize);

  FileUtils fileUtils24(24, "log", dir.dirname());
  readSingleValueFromLog(fileUtils24, 24, 41, 5005, 5.0, windowSize);
}
