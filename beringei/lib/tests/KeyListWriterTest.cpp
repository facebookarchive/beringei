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

#include "beringei/lib/KeyListReader.h"
#include "beringei/lib/KeyListWriter.h"
#include "beringei/lib/PersistentKeyList.h"

using namespace ::testing;
using namespace facebook;
using namespace facebook::gorilla;
using namespace google;
using namespace std;

class KeyListWriterTest : public testing::Test {
 protected:
  void SetUp() override {
    dir_.reset(new TemporaryDirectory("gorilla_test"));
    FileUtils files(321, "", dir_->dirname());
    files.createDirectories();
  }

  void TearDown() override {}

  std::unique_ptr<TemporaryDirectory> dir_;
};

TEST_F(KeyListWriterTest, writeAndRead) {
  KeyListWriter keyWriter(dir_->dirname(), 10);
  keyWriter.startShard(321);
  keyWriter.addKey(321, 6, "hi", 43, 60);
  keyWriter.stopShard(321);
  keyWriter.addKey(321, 7, "bye", 44, 61);
  keyWriter.flushQueue();

  uint32_t id;
  string key;
  uint16_t category;
  int32_t timestamp;
  LocalKeyReader reader(321, dir_->dirname());
  int keys = reader.readKeys([&](uint32_t _id,
                                 const char* _key,
                                 uint16_t _category,
                                 int32_t _timestamp,
                                 bool,
                                 uint64_t) {
    id = _id;
    key = _key;
    category = _category;
    timestamp = _timestamp;
    return true;
  });

  ASSERT_EQ(6, id);
  ASSERT_EQ("hi", key);
  ASSERT_EQ(1, keys);
  ASSERT_EQ(43, category);
  ASSERT_EQ(60, timestamp);
}

TEST_F(KeyListWriterTest, compactAndRead) {
  KeyListWriter keyWriter(dir_->dirname(), 10);
  keyWriter.startShard(321);
  keyWriter.flushQueue();

  auto key = std::make_tuple<uint32_t, const char*, uint16_t, int32_t>(
      6, "hi", 72, 90);
  keyWriter.compact(321, [&]() {
    auto key2 = key;
    get<1>(key) = nullptr;
    return key2;
  });

  uint32_t id;
  string key2;
  uint16_t category;
  int32_t timestamp;
  LocalKeyReader reader(321, dir_->dirname());
  int keys = reader.readKeys([&](uint32_t _id,
                                 const char* _key,
                                 uint16_t _category,
                                 int32_t _timestamp,
                                 bool,
                                 uint64_t) {
    id = _id;
    key2 = _key;
    category = _category;
    timestamp = _timestamp;
    return true;
  });

  ASSERT_EQ(6, id);
  ASSERT_EQ("hi", key2);
  ASSERT_EQ(1, keys);
  ASSERT_EQ(72, category);
  ASSERT_EQ(90, timestamp);
}

TEST_F(KeyListWriterTest, CompactMore) {
  KeyListWriter keyWriter(dir_->dirname(), 10);
  keyWriter.startShard(321);
  keyWriter.flushQueue();

  vector<std::tuple<uint32_t, string, uint16_t, int32_t>> keys = {
      make_tuple<uint32_t, string, uint16_t, int32_t>(15, "Testing1", 414, 900),
      make_tuple<uint32_t, string, uint16_t, int32_t>(25, "Testing2", 415, 901),
      make_tuple<uint32_t, string, uint16_t, int32_t>(35, "Testing3", 416, 902),
      make_tuple<uint32_t, string, uint16_t, int32_t>(
          45, "Testing4", 417, 903)};

  int i = -1;
  keyWriter.compact(321, [&]() {
    i++;
    if (i >= keys.size()) {
      return std::tuple<uint32_t, const char*, uint16_t, int32_t>{
          0, nullptr, 0, 0};
    }
    return std::tuple<uint32_t, const char*, uint16_t, int32_t>{
        get<0>(keys[i]),
        get<1>(keys[i]).c_str(),
        get<2>(keys[i]),
        get<3>(keys[i])};
  });

  vector<std::tuple<uint32_t, string, uint16_t, int32_t>> keys2;
  LocalKeyReader reader(321, dir_->dirname());
  int numKeys = reader.readKeys([&](uint32_t id,
                                    const char* key,
                                    uint16_t category,
                                    int32_t timestamp,
                                    bool,
                                    uint64_t) {
    keys2.push_back(make_tuple(id, key, category, timestamp));
    return true;
  });
  ASSERT_EQ(keys.size(), numKeys);

  ASSERT_EQ(keys, keys2);
}
