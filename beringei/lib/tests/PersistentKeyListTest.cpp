/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <gtest/gtest.h>

#include "beringei/lib/PersistentKeyList.h"

using namespace ::testing;
using namespace facebook;
using namespace facebook::gorilla;
using namespace std;

TEST(PersistentKeyListTest, writeAndRead) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "7"));
  PersistentKeyList keys(7, dir.dirname());

  bool called = false;
  keys.clearEntireListForTests();
  PersistentKeyList::readKeys(
      7,
      dir.dirname(),
      [&](uint32_t /*id*/, const char* /*key*/, uint16_t /*category*/) {
        called = true;
        return true;
      });
  ASSERT_FALSE(called);

  // Write some keys and read them out.
  keys.appendKey(5, "hi", 1);
  keys.appendKey(4, "test", 2);
  keys.appendKey(7, "bye", 3);
  keys.flush(true);

  vector<tuple<uint32_t, string, uint16_t>> out;
  PersistentKeyList::readKeys(
      7, dir.dirname(), [&](uint32_t id, const char* key, uint16_t category) {
        out.push_back(make_tuple(id, key, category));
        return true;
      });

  ASSERT_EQ(3, out.size());
  EXPECT_EQ(5, get<0>(out[0]));
  EXPECT_STREQ("hi", get<1>(out[0]).c_str());
  EXPECT_EQ(1, get<2>(out[0]));
  EXPECT_EQ(7, get<0>(out[2]));
  EXPECT_STREQ("bye", get<1>(out[2]).c_str());
  EXPECT_EQ(3, get<2>(out[2]));

  // Rewrite two keys.
  int i = 0;
  keys.compact([&]() {
    if (i < 2) {
      auto ret = make_tuple((uint32_t)1, "test2", 15);
      i++;
      return ret;
    }
    return make_tuple<uint32_t, const char*>(0, nullptr, 0);
  });

  // And another.
  keys.appendKey(8, "test3", 122);
  keys.flush(true);

  // Should get 3 keys.
  out.clear();
  PersistentKeyList::readKeys(
      7, dir.dirname(), [&](uint32_t id, const char* key, uint16_t category) {
        out.push_back(make_tuple(id, key, category));
        return true;
      });

  ASSERT_EQ(3, out.size());
  EXPECT_EQ(1, get<0>(out[0]));
  EXPECT_STREQ("test2", get<1>(out[0]).c_str());
  EXPECT_EQ(15, get<2>(out[0]));
  EXPECT_EQ(8, get<0>(out[2]));
  EXPECT_STREQ("test3", get<1>(out[2]).c_str());
  EXPECT_EQ(122, get<2>(out[2]));

  keys.clearEntireListForTests();
}

TEST(PersistentKeyListTest, partialData) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "8"));
  PersistentKeyList keys(8, dir.dirname());
  keys.clearEntireListForTests();

  FileUtils files(8, "key_list", dir.dirname());
  const char* str = "U\0\0\0\0a\0\1\0\0\0b";

  // Write a bunch of test files.
  for (int i = 0; i <= 13; i++) {
    FILE* f = files.open(i + 1, "wb", 0).file;
    fwrite(str, sizeof(char), i, f);
    fclose(f);
  }

  vector<int> results = {0, 0};
  PersistentKeyList::readKeys(
      8,
      dir.dirname(),
      [&](uint32_t id, const char* /*key*/, uint16_t /*category*/) {
        results.at(id)++;
        return true;
      });

  // First 6 files contain no valid data.
  // The rest contain the first key, and the last contains the second.
  EXPECT_EQ(7, results[0]);
  EXPECT_EQ(1, results[1]);
}
