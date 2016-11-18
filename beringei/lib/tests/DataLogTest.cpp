/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <folly/File.h>
#include <folly/Random.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include "beringei/if/gen-cpp2/beringei_data_types.h"
#include "beringei/lib/DataLog.h"
#include "beringei/lib/FileUtils.h"

#include "TestDataLoader.h"

using namespace ::testing;
using namespace facebook;
using namespace facebook::gorilla;
using namespace std;

TEST(DataLogTest, writeAndRead) {
  FLAGS_gorilla_async_file_close = false;

  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "10"));

  FileUtils files(10, "test_log", dir.dirname());
  files.clearAll();

  {
    auto testFile = files.open(1, "ab", 100);
    FILE* f = testFile.file;
    DataLogWriter writer(std::move(testFile), 0);
    writer.append(0, 101, 2);
    writer.append(3, 104, 5);
    writer.append(1, 100, 0);
  }

  vector<uint32_t> ids;
  vector<int64_t> times;
  vector<double> values;

  auto testFile = files.open(1, "rb", 0);
  int read = DataLogReader::readLog(
      testFile, 0, [&](uint32_t id, int64_t time, double value) {
        ids.push_back(id);
        times.push_back(time);
        values.push_back(value);
        return true;
      });

  vector<uint32_t> expectedIds = {0, 3, 1};
  vector<int64_t> expectedTimes = {101, 104, 100};
  vector<double> expectedValues = {2.0, 5.0, 0.0};

  ASSERT_EQ(expectedIds, ids);
  ASSERT_EQ(expectedTimes, times);
  ASSERT_EQ(expectedValues, values);
}

TEST(DataLogTest, Compression) {
  FLAGS_gorilla_async_file_close = false;

  vector<vector<TimeValuePair>> samples;
  loadData(samples);

  auto tmpFile = folly::File::temporary();

  vector<pair<uint32_t, TimeValuePair>> dataPoints;

  {
    FILE* f = fdopen(dup(tmpFile.fd()), "wb");
    DataLogWriter writer(FileUtils::File{f, "tmpFile"}, samples[0][0].unixTime);

    int datapoints = 0;

    for (int i = 0; i < samples.size(); i++) {
      int timeSeriesId = i + 200000;
      for (auto& v : samples[i]) {
        dataPoints.push_back({timeSeriesId, v});
      }
    }

    sort(
        dataPoints.begin(),
        dataPoints.end(),
        [](const pair<uint32_t, TimeValuePair>& a,
           const pair<uint32_t, TimeValuePair>& b) {
          return a.second.unixTime < b.second.unixTime;
        });

    for (auto& v : dataPoints) {
      writer.append(v.first, v.second.unixTime, v.second.value);
    }

    writer.flushBuffer();
    fseek(f, 0, SEEK_END);
    size_t len = ftell(f);
    LOG(INFO) << len;
    LOG(INFO) << dataPoints.size();
    LOG(INFO) << (double)len / dataPoints.size();
  }

  vector<pair<uint32_t, TimeValuePair>> readValues;

  FILE* f = fdopen(dup(tmpFile.fd()), "rb");
  int points = DataLogReader::readLog(
      FileUtils::File{f, "tmpFile"},
      samples[0][0].unixTime,
      [&](uint32_t id, int64_t unixTime, double value) {
        TimeValuePair timeValue;
        timeValue.unixTime = unixTime;
        timeValue.value = value;
        readValues.push_back(make_pair(id, timeValue));
        return true;
      });
  fclose(f);

  ASSERT_EQ(dataPoints, readValues);
}

TEST(DataLogTest, DISABLED_Fuzz) {
  FLAGS_gorilla_async_file_close = false;

  vector<pair<uint32_t, TimeValuePair>> dataPoints;
  auto tmpFile = folly::File::temporary();

  FILE* f1 = fdopen(dup(tmpFile.fd()), "wb");
  {
    DataLogWriter writer(FileUtils::File{f1, "tmpFile"}, 1000);

    int64_t prevTime = 1300000000;

    // 200,000,000 is enough to exceed 2**32 bits in the file and excercise
    // overflow bugs.
    for (int i = 0; i < 200000000; i++) {
      uint32_t id = (folly::Random::rand32() % 1000) * 10000;
      TimeValuePair timeValue;

      if (folly::Random::rand32() % 2 == 0) {
        timeValue.unixTime = prevTime + folly::Random::rand32() % 40 - 20;
      } else {
        timeValue.unixTime = prevTime + folly::Random::rand32() % 40000 - 20000;
      }
      prevTime = timeValue.unixTime;

      if (folly::Random::rand32() % 10 == 0) {
        timeValue.value = 0;
      } else {
        timeValue.value =
            folly::Random::rand32() / (folly::Random::rand32() % 10 + 1);
      }

      dataPoints.push_back(make_pair(id, timeValue));
    }

    for (auto& v : dataPoints) {
      writer.append(v.first, v.second.unixTime, v.second.value);
    }
  }

  vector<pair<uint32_t, TimeValuePair>> readValues;
  FILE* f2 = fdopen(dup(tmpFile.fd()), "rb");
  int points = DataLogReader::readLog(
      FileUtils::File{f2, "tmpFile"},
      1000,
      [&](uint32_t id, int64_t unixTime, double value) {
        TimeValuePair timeValue;
        timeValue.unixTime = unixTime;
        timeValue.value = value;
        readValues.push_back(make_pair(id, timeValue));
        return true;
      });
  fclose(f2);

  ASSERT_EQ(dataPoints, readValues);
}
