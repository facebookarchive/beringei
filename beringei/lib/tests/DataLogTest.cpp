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
#include <gmock/gmock.h>
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

class PartialDataLogWriter : public DataLogWriter {
 public:
  // Pass the expected partial writes as a first param.
  PartialDataLogWriter(
      std::vector<size_t>&& partialWrites,
      FileUtils::File&& out,
      int64_t baseTime)
      : DataLogWriter(std::move(out), baseTime),
        mockedPartialWrites_(partialWrites) {}
  virtual size_t writeToFile(char* const buffer, const size_t bufferSize)
      override {
    if (bufferSize == 0 || mockedPartialWrites_.size() < counter_) {
      return 0;
    }

    // Store received buffer sizes to check later.
    receivedWrites_.push_back(bufferSize);

    if (lastBuffer_ == nullptr) {
      lastBuffer_ = buffer;
    }
    bufferOffsets_.push_back(buffer - lastBuffer_);
    lastBuffer_ = buffer;

    if (mockedPartialWrites_.size() == counter_) {
      ++counter_;
      return bufferSize; // Last write should write the whole thing.
    }

    return mockedPartialWrites_.at(counter_++);
  }

  std::vector<size_t> receivedWrites_;
  std::vector<int64_t> bufferOffsets_;
  char* lastBuffer_ = nullptr;

 private:
  std::vector<size_t> mockedPartialWrites_;
  int counter_ = 0;
};

TEST(DataLogTest, partialWrites) {
  FLAGS_gorilla_async_file_close = false;

  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "10"));

  FileUtils files(10, "test_log", dir.dirname());
  files.clearAll();

  auto testFile = files.open(1, "ab", 100);
  PartialDataLogWriter writer({10, 8, 12, 13, 7}, std::move(testFile), 0);
  writer.append(0, 101, 2);
  writer.append(3, 104, 5);
  writer.append(1, 100, 0);
  writer.append(3, 21, 4);
  writer.append(5, 101, 2);
  writer.append(2, 104, 5);
  writer.append(4, 100, 0);
  writer.append(1, 21, 4);
  // The above writes result in a 51-byte initial write. This test is a bit
  // fragile and will break if the compression changes.
  EXPECT_TRUE(writer.flushBuffer());
  EXPECT_THAT(writer.receivedWrites_, ElementsAre(51, 41, 33, 21, 8, 1));
  EXPECT_THAT(writer.bufferOffsets_, ElementsAre(0, 10, 8, 12, 13, 7));
}

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
