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

#include "folly/Conv.h"

#include "beringei/lib/BucketStorage.h"
#include "beringei/lib/BucketStorageHotCold.h"
#include "beringei/lib/DataBlockIO.h"

using namespace ::testing;
using namespace facebook;
using namespace facebook::gorilla;
using namespace google;
using namespace std;

namespace {
enum class StorageType { STORAGE_SINGLE, STORAGE_SINGLE_V1, STORAGE_HOT_COLD };
}

class BucketStorageHeatTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<StorageType> {
 protected:
  template <typename... Types>
  std::unique_ptr<BucketStorage> createStorage(
      StorageType type,
      Types... args) {
    switch (type) {
      case StorageType::STORAGE_SINGLE:
        return std::make_unique<BucketStorageSingle>(args...);

      case StorageType::STORAGE_SINGLE_V1:
        return std::make_unique<BucketStorageSingle>(
            args...,
            BucketStorage::kDefaultToNumBuckets,
            DataBlockVersion::V_0_UNCOMPRESSED);

      case StorageType::STORAGE_HOT_COLD:
        return std::make_unique<BucketStorageHotCold>(args...);
    }
    CHECK(false);
  }
};

class BucketStoragePageTest : public ::testing::Test,
                              public ::testing::WithParamInterface<bool> {};
// Work with entries stored in successive positions
class BucketStoragePersistenceTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<bool> {
 protected:
  std::unique_ptr<BucketStorageSingle> createStorage(
      const TemporaryDirectory& dir,
      bool page);

  // @param[IN] offset Nth entry in test
  static uint32_t position(size_t offset) {
    return kPositionBase_ + offset;
  }

  static std::string data(size_t offset);

  static void storageAssertLoad(BucketStorage& storage);

  static BucketStorage::BucketStorageId storageStore(
      BucketStorage& storage,
      size_t offset);

  static void storageAssertFetch(
      BucketStorage& storage,
      size_t offset,
      BucketStorage::BucketStorageId id,
      folly::Optional<BucketStorage::FetchType> type = folly::none);

  static void storageFinalize(BucketStorage& storage, size_t offset);

 private:
  static const int64_t kShardId_;
  static const uint8_t kNumBuckets_;
  static const uint8_t kNumMemoryBuckets_;
  static const uint32_t kPositionBase_;
  static const uint32_t kCountBase_;
};

const int64_t BucketStoragePersistenceTest::kShardId_ = 0;
const uint8_t BucketStoragePersistenceTest::kNumBuckets_ = 5;
const uint8_t BucketStoragePersistenceTest::kNumMemoryBuckets_ = 2;
const uint32_t BucketStoragePersistenceTest::kPositionBase_ = 10;
const uint32_t BucketStoragePersistenceTest::kCountBase_ = 100;

std::unique_ptr<BucketStorageSingle>
BucketStoragePersistenceTest::createStorage(
    const TemporaryDirectory& dir,
    bool page) {
  /* 5 buckets, 2 in-core */
  return std::make_unique<BucketStorageSingle>(
      kNumBuckets_,
      kShardId_,
      dir.dirname(),
      page ? kNumMemoryBuckets_ : BucketStorage::kDefaultToNumBuckets,
      page ? DataBlockVersion::V_0_UNCOMPRESSED : DataBlockVersion::V_0);
};

std::string BucketStoragePersistenceTest::data(size_t offset) {
  return std::string("test") + folly::to<std::string>(offset);
}

void BucketStoragePersistenceTest::storageAssertLoad(BucketStorage& storage) {
  auto positions = storage.findCompletedPositions();
  for (auto position = positions.rbegin(); position != positions.rend();
       ++position) {
    SCOPED_TRACE(
        std::string("load position ") + folly::to<std::string>(*position));
    std::vector<uint32_t> timeSeriesIds;
    std::vector<uint64_t> storageIds;
    bool success = storage.loadPosition(*position, timeSeriesIds, storageIds);
    ASSERT_TRUE(success);
  }
}

BucketStorage::BucketStorageId BucketStoragePersistenceTest::storageStore(
    BucketStorage& storage,
    size_t offset) {
  std::string in(data(offset));
  return storage.store(
      kPositionBase_ + offset, in.c_str(), in.length(), kCountBase_ + offset);
}

void BucketStoragePersistenceTest::storageAssertFetch(
    BucketStorage& storage,
    size_t offset,
    BucketStorage::BucketStorageId id,
    folly::Optional<BucketStorage::FetchType> type) {
  SCOPED_TRACE(std::string("fetch ") + folly::to<std::string>(offset));
  auto expect = data(offset);
  std::string out;
  uint16_t count;
  BucketStorage::FetchType outType;
  auto status =
      storage.fetch(kPositionBase_ + offset, id, out, count, &outType);
  ASSERT_EQ(status, BucketStorage::FetchStatus::SUCCESS);
  ASSERT_EQ(out, expect);
  ASSERT_EQ(count, kCountBase_ + offset);
  if (type) {
    ASSERT_EQ(outType, *type);
  }
}

void BucketStoragePersistenceTest::storageFinalize(
    BucketStorage& storage,
    size_t offset) {
  storage.finalizeBucket(kPositionBase_ + offset);
};

TEST_P(BucketStorageHeatTest, SmallStoreAndFetch) {
  auto type = GetParam();
  auto storage = createStorage(type, 5, 0, "");
  bool cold = type == StorageType::STORAGE_HOT_COLD;

  auto id = storage->store(11, "test", 4, 100, 42, cold);
  ASSERT_NE(BucketStorage::kInvalidId, id);
  ASSERT_EQ(BucketStorage::coldId(id), cold);

  string str;
  uint16_t itemCount;

  ASSERT_EQ(
      BucketStorage::FetchStatus::SUCCESS,
      storage->fetch(11, id, str, itemCount));
  ASSERT_EQ("test", str);
  ASSERT_EQ(100, itemCount);
}

TEST(BucketStorageTest, DedupData) {
  BucketStorageSingle storage(5, 0, "");

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
  BucketStorageSingle storage(4, 0, "");

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
  BucketStorageSingle storage(1, 0, "");

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

  BucketStorageSingle storage(10, shardId, dir.dirname());
  vector<BucketStorage::BucketStorageId> ids(5);
  for (int i = 0; i < 5; i++) {
    bool cold = i % 2;
    string data(30000, '0' + i);
    ids[i] =
        storage.store(100, data.c_str(), data.length(), 100 + i, i * 10, cold);
    ASSERT_NE(BucketStorage::kInvalidId, ids[i]);
    EXPECT_EQ(BucketStorage::coldId(ids[i]), cold);
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

  /* sleep override */ usleep(10000);

  vector<uint32_t> timeSeriesIds;
  vector<uint64_t> storageIds;
  DataBlockIO reader(shardId, dir.dirname());
  set<uint32_t> files = reader.findCompletedBlockFiles();
  ASSERT_EQ(1, files.size());
  auto blocks = reader.readBlocks(*files.begin(), timeSeriesIds, storageIds);
  ASSERT_EQ(5, timeSeriesIds.size());
  ASSERT_EQ(5, storageIds.size());

  for (int i = 0; i < 5; i++) {
    bool cold = i % 2;
    uint32_t pageIndex, pageOffset;
    uint16_t dataLength, itemCount;
    BucketStorage::parseId(
        storageIds[i], pageIndex, pageOffset, dataLength, itemCount);
    ASSERT_EQ(100 + i, itemCount);
    ASSERT_EQ(30000, dataLength);
    ASSERT_LT(pageIndex, blocks.size());
    EXPECT_EQ(BucketStorage::coldId(ids[i]), cold);

    string expectedData(30000, '0' + i);
    string actualData(blocks[pageIndex]->data + pageOffset, 30000);
    ASSERT_EQ(expectedData, actualData);
    ASSERT_EQ(i * 10, timeSeriesIds[i]);
  }
}

TEST_P(BucketStorageHeatTest, BigDataFromDisk) {
  const StorageType type = GetParam();
  TemporaryDirectory dir("gorilla_data_block");
  int64_t shardId = 12;

  vector<BucketStorage::BucketStorageId> ids(5);
  vector<uint32_t> timeSeriesIds = {100, 200, 300, 400, 500};

  // Scoped BucketStorageSingle object to let the destructor free the memory.
  {
    auto storage = createStorage(type, 10, shardId, dir.dirname());
    storage->createDirectories();
    for (int i = 0; i < 5; i++) {
      string data(30000, '0' + i);
      bool cold = i % 5;
      ids[i] = storage->store(
          100, data.c_str(), data.length(), 100 + i, timeSeriesIds[i], cold);
      ASSERT_NE(BucketStorage::kInvalidId, ids[i]);
      EXPECT_EQ(BucketStorage::coldId(ids[i]), cold);
    }
    storage->finalizeBucket(100);

    /* sleep override */ usleep(10000);
  }

  vector<uint32_t> timeSeriesIds2;
  vector<uint64_t> storageIds;
  auto storage = createStorage(type, 10, shardId, dir.dirname());
  ASSERT_TRUE(storage->loadPosition(100, timeSeriesIds2, storageIds));
  ASSERT_EQ(ids, storageIds);
  ASSERT_EQ(timeSeriesIds, timeSeriesIds2);

  for (int i = 0; i < 5; i++) {
    string expectedData(30000, '0' + i);
    string str;
    uint16_t itemCount;
    ASSERT_EQ(
        BucketStorage::FetchStatus::SUCCESS,
        storage->fetch(100, ids[i], str, itemCount));
    ASSERT_EQ(expectedData, str);
    ASSERT_EQ(100 + i, itemCount);
    EXPECT_EQ(BucketStorage::coldId(ids[i]), (bool)(i % 5));
  }
}

INSTANTIATE_TEST_CASE_P(
    Cold,
    BucketStorageHeatTest,
    ::testing::Values(
        StorageType::STORAGE_SINGLE,
        StorageType::STORAGE_SINGLE_V1,
        StorageType::STORAGE_HOT_COLD));

TEST(BucketStorageTest, BigDataStoreAfterCleanupWithoutFinalize) {
  TemporaryDirectory dir("gorilla_data_block");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "12"));
  int64_t shardId = 12;

  vector<BucketStorage::BucketStorageId> ids(5);
  vector<uint32_t> timeSeriesIds = {100, 200, 300, 400, 500};

  {
    BucketStorageSingle storage(10, shardId, dir.dirname());
    for (int i = 0; i < 5; i++) {
      string data(30000, '0' + i);
      ids[i] = storage.store(
          100, data.c_str(), data.length(), 100 + i, timeSeriesIds[i], false);
      ASSERT_NE(BucketStorage::kInvalidId, ids[i]);
    }
    storage.clearAndDisable();
    storage.enable();

    for (int i = 0; i < 5; i++) {
      string data(30000, '0' + i);
      ids[i] = storage.store(
          100, data.c_str(), data.length(), 100 + i, timeSeriesIds[i], false);
      ASSERT_NE(BucketStorage::kInvalidId, ids[i]);
    }
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

  // Scoped BucketStorageSingle object to let the destructor free the memory.
  {
    BucketStorageSingle storage(10, shardId, dir.dirname());
    for (int i = 0; i < 2048; i++) {
      string data(30000, char(random() % 256));
      dedupedValues.insert(make_pair(data, i % 16));
      ids[i].first = storage.store(
          100, data.c_str(), data.length(), 100 + (i % 16), i, false);
      ids[i].second = data;
      dedupedIds.insert(ids[i].first);
      ASSERT_NE(BucketStorage::kInvalidId, ids[i].first);
    }
    storage.finalizeBucket(100);

    /* sleep override */ usleep(10000);
  }

  ASSERT_EQ(dedupedValues.size(), dedupedIds.size());

  vector<uint32_t> timeSeriesIds2;
  vector<uint64_t> storageIds;
  BucketStorageSingle storage(10, shardId, dir.dirname());
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
  BucketStorageSingle storage(5, 0, "");

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

  BucketStorageSingle storage(1, 0, dir.dirname());

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
  BucketStorageSingle storage(5, 0, "");

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
  BucketStorageSingle storage(5, 0, "");

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
  BucketStorageSingle storage(5, 0, "");

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

  BucketStorageSingle storage(5, 0, dir.dirname());

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

TEST_P(BucketStoragePersistenceTest, Switchover) {
  const bool page = GetParam();
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "0"));

  std::vector<BucketStorage::BucketStorageId> ids;

  auto storage = createStorage(dir, page);

  // Fill memory slots
  for (auto i : {1, 2}) {
    SCOPED_TRACE(std::string("memory fill ") + folly::to<std::string>(i));
    ids.push_back(storageStore(*storage, i));
    ASSERT_NO_FATAL_FAILURE(storageAssertFetch(
        *storage, i, ids[i - 1], BucketStorage::FetchType::MEMORY));
    storageFinalize(*storage, i);
    ASSERT_NO_FATAL_FAILURE(storageAssertFetch(
        *storage, i, ids[i - 1], BucketStorage::FetchType::MEMORY));
  }

  // Force memory eviction and fill disk slots
  for (auto i : {3, 4, 5}) {
    SCOPED_TRACE(std::string("persistent fill ") + folly::to<std::string>(i));
    ids.push_back(storageStore(*storage, i));
    storageFinalize(*storage, i);
    for (unsigned j = 1; j <= i; ++j) {
      SCOPED_TRACE(std::string("fetch ") + folly::to<std::string>(j));
      ASSERT_NO_FATAL_FAILURE(storageAssertFetch(*storage, j, ids[j - 1]));
    }
  }

  // Force first disk eviction
  ids.push_back(storageStore(*storage, 6));
  storageFinalize(*storage, 6);
  std::string out;
  uint16_t count;
  ASSERT_EQ(
      storage->fetch(position(1), ids[0], out, count),
      BucketStorage::FetchStatus::FAILURE);

  storage->clearAndDisable();

  // Switch nodes with shared storage
  auto storage2 = createStorage(dir, page);
  {
    SCOPED_TRACE(std::string("switch over load"));
    ASSERT_NO_FATAL_FAILURE(storageAssertLoad(*storage2));
  }
  for (unsigned i = 2; i <= 6; ++i) {
    SCOPED_TRACE(std::string("switch over fetch " + folly::to<std::string>(i)));
    ASSERT_NO_FATAL_FAILURE(storageAssertFetch(
        *storage2,
        i,
        ids[i - 1],
        page && i < 5 ? BucketStorage::FetchType::DISK
                      : BucketStorage::FetchType::MEMORY));
  }

  // Add a bucket before switching back
  ids.push_back(storageStore(*storage2, 7));
  storageFinalize(*storage2, 7);
  storage2.reset();

  // Switch back
  storage->enable();
  {
    SCOPED_TRACE(std::string("switch back load"));
    ASSERT_NO_FATAL_FAILURE(storageAssertLoad(*storage));
  }
  for (unsigned i = 3; i <= 7; ++i) {
    SCOPED_TRACE(std::string("switch back fetch " + folly::to<std::string>(i)));
    ASSERT_NO_FATAL_FAILURE(storageAssertFetch(
        *storage,
        i,
        ids[i - 1],
        page && i < 6 ? BucketStorage::FetchType::DISK
                      : BucketStorage::FetchType::MEMORY));
  }
}

INSTANTIATE_TEST_CASE_P(
    Page,
    BucketStoragePersistenceTest,
    ::testing::Values(false));
