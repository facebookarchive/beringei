/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <gtest/gtest.h>

#include "beringei/lib/FileUtils.h"
#include "beringei/lib/KeyListReader.h"
#include "beringei/lib/PersistentKeyList.h"

using namespace ::testing;
using namespace facebook;
using namespace facebook::gorilla;
using namespace std;

TEST(LocalKeyReaderTest, writeAndRead) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "7"));
  PersistentKeyList keys(7, dir.dirname());

  bool called = false;
  keys.clearEntireListForTests();
  LocalKeyReader reader(7, dir.dirname());
  reader.readKeys([&](uint32_t /*id*/,
                      const char* /*key*/,
                      uint16_t /*category*/,
                      int32_t /*timestamp*/,
                      bool /* isAppend */,
                      uint64_t) {
    called = true;
    return true;
  });
  ASSERT_FALSE(called);

  // Write some keys and read them out.
  keys.appendKey(5, "hi", 1, 17);
  keys.appendKey(4, "test", 2, 18);
  keys.appendKey(7, "bye", 3, 19);
  keys.flush(true);

  vector<tuple<uint32_t, string, uint16_t, int32_t>> out;
  reader.readKeys(
      [&](uint32_t id,
          const char* key,
          uint16_t category,
          int32_t timestamp,
          bool isAppend,
          uint64_t) -> bool {
        EXPECT_TRUE(isAppend);
        out.push_back(make_tuple(id, key, category, timestamp));
        return true;
      });

  ASSERT_EQ(3, out.size());
  EXPECT_EQ(make_tuple(5, "hi", 1, 17), out[0]);
  EXPECT_EQ(make_tuple(4, "test", 2, 18), out[1]);
  EXPECT_EQ(make_tuple(7, "bye", 3, 19), out[2]);

  // Rewrite two keys.
  int i = 0;
  keys.compact([&]() {
    if (i < 2) {
      auto ret = make_tuple((uint32_t)1, "test2", 15, 20);
      i++;
      return ret;
    }
    return make_tuple<uint32_t, const char*>(0, nullptr, 0, 0);
  });

  // And another.
  keys.appendKey(8, "test3", 122, 21);
  keys.flush(true);

  // Should get 3 keys.
  out.clear();
  reader.readKeys([&](uint32_t id,
                      const char* key,
                      uint16_t category,
                      int32_t timestamp,
                      bool isAppend,
                      uint64_t) {
    EXPECT_TRUE(isAppend);
    out.push_back(make_tuple(id, key, category, timestamp));
    return true;
  });

  ASSERT_EQ(3, out.size());
  EXPECT_EQ(make_tuple(1, "test2", 15, 20), out[0]);
  EXPECT_EQ(make_tuple(1, "test2", 15, 20), out[1]);
  EXPECT_EQ(make_tuple(8, "test3", 122, 21), out[2]);

  keys.clearEntireListForTests();
}

TEST(LocalKeyReaderTest, partialData) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "8"));
  PersistentKeyList keys(8, dir.dirname());
  keys.clearEntireListForTests();

  FileUtils files(8, "key_list", dir.dirname());
  const char* str = "3\0\0\0\0\0\0\0\0\0\0a\0\1\0\0\0\0\0\0\0\0\0b";

  // Write a bunch of test files.
  for (int i = 0; i <= 25; i++) {
    FILE* f = files.open(i + 1, "wb", 0).file;
    fwrite(str, sizeof(char), i, f);
    fclose(f);
  }

  vector<int> results = {0, 0};
  LocalKeyReader reader(8, dir.dirname());
  reader.readKeys([&](uint32_t id,
                      const char* /*key*/,
                      uint16_t /*category*/,
                      int32_t /*timestamp*/,
                      bool /* isAppend */,
                      uint64_t) {
    results.at(id)++;
    return true;
  });

  // First 12 files contain no valid data.
  // The rest contain the first key, and the last contains the second.
  EXPECT_EQ(13, results[0]);
  EXPECT_EQ(1, results[1]);
}

TEST(LocalKeyReaderTest, NoTimestampFiles) {
  TemporaryDirectory dir("gorilla_test");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "8"));
  PersistentKeyList keys(8, dir.dirname());
  keys.clearEntireListForTests();

  FileUtils files(8, "key_list", dir.dirname());
  const char* str = "1\0\0\0\0\1\0a\0\1\0\0\0\2\0b";

  FILE* f = files.open(1, "wb", 0).file;
  fwrite(str, sizeof(char), 17, f);
  fclose(f);

  vector<int> categories = {0, 0};
  int total = 0;
  LocalKeyReader reader(8, dir.dirname());
  reader.readKeys([&](uint32_t id,
                      const char* /*key*/,
                      uint16_t category,
                      int32_t /*timestamp*/,
                      bool /* isAppend */,
                      uint64_t) {
    categories.at(id) = category;
    total++;
    return true;
  });

  EXPECT_EQ(1, categories[0]);
  EXPECT_EQ(2, categories[1]);
}
