/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <gtest/gtest.h>
#include <unordered_map>

#include <folly/String.h>
#include "TestKeyList.h"

#include "beringei/lib/CaseUtils.h"

using namespace ::testing;
using namespace facebook::gorilla;

TEST(CaseUtilsTest, CaseEq) {
  CaseEq eq;
  EXPECT_TRUE(eq("foo", "FoO"));
  EXPECT_TRUE(eq("foo", "foo"));
  EXPECT_TRUE(eq("FOO", "foO"));
  EXPECT_FALSE(eq("foo", "bar"));
  EXPECT_FALSE(eq("foo", "b"));
}

TEST(CaseUtilsTest, CaseHash) {
  CaseHash hash;
  EXPECT_EQ(hash("foo"), hash("FoO"));
  EXPECT_EQ(hash("BaR"), hash("bAr"));
  EXPECT_NE(hash("foo"), hash("bar"));
}

const static int kNumHashes = 10000000;
const static int kKeyListSize = 400000;

TEST(CaseUtilsTest, Perf) {
  CaseHash hsh;
  TestKeyList keyList(kKeyListSize, 10);
  size_t x = 0;
  for (int i = 0; i < kNumHashes; i++) {
    x ^= hsh(keyList.testStr(i));
  }
  LOG(INFO) << x;
}

TEST(CaseUtilsTest, PerfComparison) {
  std::hash<std::string> hsh;
  TestKeyList keyList(kKeyListSize);
  size_t x = 0;
  for (int i = 0; i < kNumHashes; i++) {
    x ^= hsh(keyList.testStr(i));
  }
  LOG(INFO) << x;
}
