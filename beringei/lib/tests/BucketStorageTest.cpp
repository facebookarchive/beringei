/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <gtest/gtest.h>
#include <string>

#include "beringei/lib/BucketStorage.h"
#include "beringei/lib/DataBlockReader.h"

using namespace ::testing;
using namespace facebook;
using namespace facebook::gorilla;
using namespace google;
using namespace std;

TEST(BucketStorageTest, SmallStoreAndFetch) {
  BucketStorage storage(5, 0, "");

  auto id = storage.store(11, "test", 4, 100);
  ASSERT_NE(BucketStorage::kInvalidId, id);

  string str;
  uint16_t itemCount;

  ASSERT_EQ(
      BucketStorage::FetchStatus::SUCCESS,
      storage.fetch(11, id, str, itemCount));
  ASSERT_EQ("test", str);
  ASSERT_EQ(100, itemCount);
}

TEST(BucketStorageTest, DedupData) {
  BucketStorage storage(5, 0, "");

  auto id1 = storage.store(11, "test1", 5, 100);
  auto id2 = storage.store(11, "test2", 5, 100);
  auto id3 = storage.store(11, "test1", 5, 100);
  auto id4 = storage.store(11, "test1", 5, 101);
  ASSERT_NE(BucketStorage::kInvalidId, id1);
  ASSERT_NE(BucketStorage::kInvalidId, id2);
  ASSERT_NE(BucketStorage::kInvalidId, id3);
  ASSERT_NE(BucketStorage::kInvalidId, id4);
  ASSERT_EQ(id1, id3);

  string str;
  uint16_t itemCount;

  ASSERT_EQ(
      BucketStorage::FetchStatus::SUCCESS,
      storage.fetch(11, id1, str, itemCount));
  ASSERT_EQ("test1", str);
  ASSERT_EQ(100, itemCount);
  ASSERT_EQ(
      BucketStorage::FetchStatus::SUCCESS,
      storage.fetch(11, id2, str, itemCount));
  ASSERT_EQ("test2", str);
  ASSERT_EQ(100, itemCount);
  ASSERT_EQ(
      BucketStorage::FetchStatus::SUCCESS,
      storage.fetch(11, id3, str, itemCount));
  ASSERT_EQ("test1", str);
  ASSERT_EQ(100, itemCount);
  ASSERT_EQ(
      BucketStorage::FetchStatus::SUCCESS,
      storage.fetch(11, id4, str, itemCount));
  ASSERT_EQ("test1", str);
  ASSERT_EQ(101, itemCount);
}

TEST(BucketStorageTest, StoringOldData) {
  BucketStorage storage(4, 0, "");

  auto firstId = storage.store(11, "test1", 5, 101);
  ASSERT_NE(BucketStorage::kInvalidId, firstId);

  auto secondId = storage.store(12, "test2", 5, 102);
  ASSERT_NE(BucketStorage::kInvalidId, secondId);

  auto thirdId = storage.store(11, "test3", 5, 103);
  ASSERT_NE(BucketStorage::kInvalidId, thirdId);

  string str;
  uint16_t itemCount;

  ASSERT_EQ(
      BucketStorage::FetchStatus::SUCCESS,
      storage.fetch(11, firstId, str, itemCount));
  ASSERT_EQ("test1", str);
  ASSERT_EQ(101, itemCount);

  ASSERT_EQ(
      BucketStorage::FetchStatus::SUCCESS,
      storage.fetch(12, secondId, str, itemCount));
  ASSERT_EQ("test2", str);
  ASSERT_EQ(102, itemCount);

  ASSERT_EQ(
      BucketStorage::FetchStatus::SUCCESS,
      storage.fetch(11, thirdId, str, itemCount));
  ASSERT_EQ("test3", str);
  ASSERT_EQ(103, itemCount);
}

TEST(BucketStorageTest, SingleBucket) {
  BucketStorage storage(1, 0, "");

  vector<BucketStorage::BucketStorageId> ids(10);
  for (int i = 1; i < 10; i++) {
    auto id = storage.store(i, "test1", 5, 100);
    ASSERT_NE(BucketStorage::kInvalidId, id);
    ids[i] = id;
  }

  string str;
  uint16_t itemCount;
  for (int i = 1; i < 9; i++) {
    ASSERT_EQ(
        BucketStorage::FetchStatus::FAILURE,
        storage.fetch(i, ids[i], str, itemCount));
  }

  ASSERT_EQ(
      BucketStorage::FetchStatus::SUCCESS,
      storage.fetch(9, ids[9], str, itemCount));
  ASSERT_EQ("test1", str);
  ASSERT_EQ(100, itemCount);
}

TEST(BucketStorageTest, BigData) {
  TemporaryDirectory dir("gorilla_data_block");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "12"));
  int64_t shardId = 12;

  BucketStorage storage(10, shardId, dir.dirname());
  vector<BucketStorage::BucketStorageId> ids(5);
  for (int i = 0; i < 5; i++) {
    string data(30000, '0' + i);
    ids[i] = storage.store(100, data.c_str(), data.length(), 100 + i, i * 10);
    ASSERT_NE(BucketStorage::kInvalidId, ids[i]);
  }
  storage.finalizeBucket(100);

  for (int i = 0; i < 5; i++) {
    string expectedData(30000, '0' + i);
    string str;
    uint16_t itemCount;

    ASSERT_EQ(
        BucketStorage::FetchStatus::SUCCESS,
        storage.fetch(100, ids[i], str, itemCount));
    ASSERT_EQ(expectedData, str);
    ASSERT_EQ(100 + i, itemCount);
  }

  usleep(10000);

  vector<uint32_t> timeSeriesIds;
  vector<uint64_t> storageIds;
  DataBlockReader reader(shardId, dir.dirname());
  set<uint32_t> files = reader.findCompletedBlockFiles();
  ASSERT_EQ(1, files.size());
  auto blocks = reader.readBlocks(*files.begin(), timeSeriesIds, storageIds);
  ASSERT_EQ(5, timeSeriesIds.size());
  ASSERT_EQ(5, storageIds.size());

  for (int i = 0; i < 5; i++) {
    uint32_t pageIndex, pageOffset;
    uint16_t dataLength, itemCount;
    BucketStorage::parseId(
        storageIds[i], pageIndex, pageOffset, dataLength, itemCount);
    ASSERT_EQ(100 + i, itemCount);
    ASSERT_EQ(30000, dataLength);
    ASSERT_LT(pageIndex, blocks.size());

    string expectedData(30000, '0' + i);
    string actualData(blocks[pageIndex]->data + pageOffset, 30000);
    ASSERT_EQ(expectedData, actualData);
    ASSERT_EQ(i * 10, timeSeriesIds[i]);
  }
}

TEST(BucketStorageTest, BigDataFromDisk) {
  TemporaryDirectory dir("gorilla_data_block");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "12"));
  int64_t shardId = 12;

  vector<BucketStorage::BucketStorageId> ids(5);
  vector<uint32_t> timeSeriesIds = {100, 200, 300, 400, 500};

  // Scoped BucketStorage object to let the destructor free the memory.
  {
    BucketStorage storage(10, shardId, dir.dirname());
    for (int i = 0; i < 5; i++) {
      string data(30000, '0' + i);
      ids[i] = storage.store(
          100, data.c_str(), data.length(), 100 + i, timeSeriesIds[i]);
      ASSERT_NE(BucketStorage::kInvalidId, ids[i]);
    }
    storage.finalizeBucket(100);

    usleep(10000);
  }

  vector<uint32_t> timeSeriesIds2;
  vector<uint64_t> storageIds;
  BucketStorage storage(10, shardId, dir.dirname());
  ASSERT_TRUE(storage.loadPosition(100, timeSeriesIds2, storageIds));
  ASSERT_EQ(ids, storageIds);
  ASSERT_EQ(timeSeriesIds, timeSeriesIds2);

  for (int i = 0; i < 5; i++) {
    string expectedData(30000, '0' + i);
    string str;
    uint16_t itemCount;

    ASSERT_EQ(
        BucketStorage::FetchStatus::SUCCESS,
        storage.fetch(100, ids[i], str, itemCount));
    ASSERT_EQ(expectedData, str);
    ASSERT_EQ(100 + i, itemCount);
  }
}

TEST(BucketStorageTest, DedupedDataFromDisk) {
  TemporaryDirectory dir("gorilla_data_block");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "12"));
  int64_t shardId = 12;

  vector<pair<BucketStorage::BucketStorageId, string>> ids(2048);
  set<BucketStorage::BucketStorageId> dedupedIds;
  set<pair<string, int>> dedupedValues;

  // Scoped BucketStorage object to let the destructor free the memory.
  {
    BucketStorage storage(10, shardId, dir.dirname());
    for (int i = 0; i < 2048; i++) {
      string data(30000, char(random() % 256));
      dedupedValues.insert(make_pair(data, i % 16));
      ids[i].first =
          storage.store(100, data.c_str(), data.length(), 100 + (i % 16), i);
      ids[i].second = data;
      dedupedIds.insert(ids[i].first);
      ASSERT_NE(BucketStorage::kInvalidId, ids[i].first);
    }
    storage.finalizeBucket(100);

    usleep(10000);
  }

  ASSERT_EQ(dedupedValues.size(), dedupedIds.size());

  vector<uint32_t> timeSeriesIds2;
  vector<uint64_t> storageIds;
  BucketStorage storage(10, shardId, dir.dirname());
  ASSERT_TRUE(storage.loadPosition(100, timeSeriesIds2, storageIds));
  ASSERT_EQ(2048, storageIds.size());
  ASSERT_EQ(2048, timeSeriesIds2.size());

  for (int i = 0; i < 2048; i++) {
    string str;
    uint16_t itemCount;

    ASSERT_EQ(
        BucketStorage::FetchStatus::SUCCESS,
        storage.fetch(100, ids[i].first, str, itemCount));
    ASSERT_EQ(ids[i].second, str);
    ASSERT_EQ(100 + (i % 16), itemCount);
    ASSERT_EQ(ids[i].first, storageIds[i]);
    ASSERT_EQ(i, timeSeriesIds2[i]);
  }
}

TEST(BucketStorageTest, StoringToExpiredBuckets) {
  BucketStorage storage(5, 0, "");

  for (int i = 1; i < 10; i++) {
    auto id = storage.store(i, "test1", 5, 100);
    ASSERT_NE(BucketStorage::kInvalidId, id);
  }

  // Buckets from 1 to 4 have expired
  for (int i = 1; i < 5; i++) {
    auto id = storage.store(i, "test1", 5, 100);
    ASSERT_EQ(BucketStorage::kInvalidId, id);
  }

  for (int i = 5; i < 10; i++) {
    auto id = storage.store(i, "test1", 5, 100);
    ASSERT_NE(BucketStorage::kInvalidId, id);
  }
}

TEST(BucketStorageTest, SpikeInData) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "0"));

  BucketStorage storage(1, 0, dir.dirname());

  for (int i = 0; i < 100000; i++) {
    auto id = storage.store(100, "test1", 5, 100);
    ASSERT_NE(BucketStorage::kInvalidId, id);
  }

  auto id = storage.store(101, "test2", 5, 101);
  ASSERT_NE(BucketStorage::kInvalidId, id);

  string str;
  uint16_t itemCount;
  ASSERT_EQ(
      BucketStorage::FetchStatus::SUCCESS,
      storage.fetch(101, id, str, itemCount));
  ASSERT_EQ("test2", str);
  ASSERT_EQ(101, itemCount);

  // This should force a resize because position 100 used more pages
  // than position 101.
  id = storage.store(102, "test3", 5, 102);
  ASSERT_NE(BucketStorage::kInvalidId, id);

  ASSERT_EQ(
      BucketStorage::FetchStatus::SUCCESS,
      storage.fetch(102, id, str, itemCount));
  ASSERT_EQ("test3", str);
  ASSERT_EQ(102, itemCount);
}

TEST(BucketStorageTest, Disable) {
  BucketStorage storage(5, 0, "");

  auto id = storage.store(11, "test", 4, 100);
  ASSERT_NE(BucketStorage::kInvalidId, id);

  storage.clearAndDisable();

  string str;
  uint16_t itemCount;

  ASSERT_EQ(
      BucketStorage::FetchStatus::FAILURE,
      storage.fetch(11, id, str, itemCount));
}

TEST(BucketStorageTest, DisableAndEnable) {
  BucketStorage storage(5, 0, "");

  auto id = storage.store(11, "test", 4, 100);
  ASSERT_NE(BucketStorage::kInvalidId, id);

  storage.clearAndDisable();
  storage.enable();

  string str;
  uint16_t itemCount;

  ASSERT_EQ(
      BucketStorage::FetchStatus::FAILURE,
      storage.fetch(11, id, str, itemCount));
}

TEST(BucketStorageTest, DisableAndEnableAndReuse) {
  BucketStorage storage(5, 0, "");

  auto id = storage.store(11, "test", 4, 100);
  ASSERT_NE(BucketStorage::kInvalidId, id);

  storage.clearAndDisable();
  storage.enable();

  id = storage.store(11, "derp", 4, 101);
  ASSERT_NE(BucketStorage::kInvalidId, id);

  string str;
  uint16_t itemCount;
  ASSERT_EQ(
      BucketStorage::FetchStatus::SUCCESS,
      storage.fetch(11, id, str, itemCount));
  ASSERT_EQ("derp", str);
  ASSERT_EQ(101, itemCount);
}

TEST(BucketStorageTest, StoreAfterFinalize) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "0"));

  BucketStorage storage(5, 0, dir.dirname());

  auto id1 = storage.store(11, "test1", 5, 101);
  ASSERT_NE(BucketStorage::kInvalidId, id1);
  auto id2 = storage.store(12, "test2", 5, 102);
  ASSERT_NE(BucketStorage::kInvalidId, id2);

  storage.finalizeBucket(11);
  auto id3 = storage.store(11, "test3", 5, 103);
  ASSERT_EQ(BucketStorage::kInvalidId, id3);
  auto id4 = storage.store(12, "test4", 5, 104);
  ASSERT_NE(BucketStorage::kInvalidId, id4);

  storage.finalizeBucket(12);
  auto id5 = storage.store(12, "test5", 5, 105);
  ASSERT_EQ(BucketStorage::kInvalidId, id5);

  string str;
  uint16_t itemCount;

  ASSERT_EQ(
      BucketStorage::FetchStatus::SUCCESS,
      storage.fetch(11, id1, str, itemCount));
  ASSERT_EQ("test1", str);
  ASSERT_EQ(101, itemCount);

  ASSERT_EQ(
      BucketStorage::FetchStatus::SUCCESS,
      storage.fetch(12, id2, str, itemCount));
  ASSERT_EQ("test2", str);
  ASSERT_EQ(102, itemCount);

  ASSERT_EQ(
      BucketStorage::FetchStatus::SUCCESS,
      storage.fetch(12, id4, str, itemCount));
  ASSERT_EQ("test4", str);
  ASSERT_EQ(104, itemCount);
}
