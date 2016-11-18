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

#include "beringei/lib/KeyListWriter.h"
#include "beringei/lib/PersistentKeyList.h"

using namespace ::testing;
using namespace facebook;
using namespace facebook::gorilla;
using namespace google;
using namespace std;

class KeyListWriterTest : public testing::Test {
 protected:
  void SetUp() {
    dir_.reset(new TemporaryDirectory("gorilla_test"));
    FileUtils files(321, "", dir_->dirname());
    files.createDirectories();
  }

  void TearDown() {}

  std::unique_ptr<TemporaryDirectory> dir_;
};

TEST_F(KeyListWriterTest, writeAndRead) {
  KeyListWriter keyWriter(dir_->dirname(), 10);
  keyWriter.startShard(321);
  keyWriter.addKey(321, 6, "hi", 43);
  keyWriter.stopShard(321);
  keyWriter.addKey(321, 7, "bye", 44);
  keyWriter.flushQueue();

  uint32_t id;
  string key;
  uint16_t category;
  int keys = PersistentKeyList::readKeys(
      321,
      dir_->dirname(),
      [&](uint32_t _id, const char* _key, uint16_t _category) {
        id = _id;
        key = _key;
        category = _category;
        return true;
      });

  ASSERT_EQ(6, id);
  ASSERT_EQ("hi", key);
  ASSERT_EQ(1, keys);
  ASSERT_EQ(43, category);
}

TEST_F(KeyListWriterTest, compactAndRead) {
  KeyListWriter keyWriter(dir_->dirname(), 10);
  keyWriter.startShard(321);
  keyWriter.flushQueue();

  auto key = std::make_tuple<uint32_t, const char*>(6, "hi", 72);
  keyWriter.compact(321, [&]() {
    auto key2 = key;
    get<1>(key) = nullptr;
    return key2;
  });

  uint32_t id;
  string key2;
  uint16_t category;
  int keys = PersistentKeyList::readKeys(
      321,
      dir_->dirname(),
      [&](uint32_t _id, const char* _key, uint16_t _category) {
        id = _id;
        key2 = _key;
        category = _category;
        return true;
      });

  ASSERT_EQ(6, id);
  ASSERT_EQ("hi", key2);
  ASSERT_EQ(1, keys);
  ASSERT_EQ(72, category);
}

TEST_F(KeyListWriterTest, CompactMore) {
  KeyListWriter keyWriter(dir_->dirname(), 10);
  keyWriter.startShard(321);
  keyWriter.flushQueue();

  vector<std::tuple<uint32_t, string, uint16_t>> keys = {
      make_tuple<uint32_t, string, uint16_t>(15, "Testing1", 414),
      make_tuple<uint32_t, string, uint16_t>(25, "Testing2", 415),
      make_tuple<uint32_t, string, uint16_t>(35, "Testing3", 416),
      make_tuple<uint32_t, string, uint16_t>(45, "Testing4", 417)};

  int i = -1;
  keyWriter.compact(321, [&]() {
    i++;
    if (i >= keys.size()) {
      return std::tuple<uint32_t, const char*, uint16_t>{0, nullptr, 0};
    }
    return std::tuple<uint32_t, const char*, uint16_t>{
        get<0>(keys[i]), get<1>(keys[i]).c_str(), get<2>(keys[i])};
  });

  vector<std::tuple<uint32_t, string, uint16_t>> keys2;
  int numKeys = PersistentKeyList::readKeys(
      321,
      dir_->dirname(),
      [&](uint32_t id, const char* key, uint16_t category) {
        keys2.push_back(make_tuple(id, key, category));
        return true;
      });
  ASSERT_EQ(keys.size(), numKeys);

  ASSERT_EQ(keys, keys2);
}
