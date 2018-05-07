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

using namespace ::testing;
using namespace facebook;
using namespace facebook::gorilla;
using namespace std;

TEST(FileUtilsTest, SingleWriter) {
  TemporaryDirectory dir("gorilla_data_block");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "5"));

  FileUtils files(5, "test", dir.dirname());
  files.clearTo(1000);

  FILE* f = files.open(7, "w", 1024).file;
  fputs("hi", f);
  fclose(f);

  f = files.open(5, "a", 0).file;
  fclose(f);
  files.rename(5, 6);

  // No exceptions.
  files.rename(10, 20);

  // ls should give us our two files in order.
  vector<int64_t> list = files.ls();
  vector<int64_t> expected = {6, 7};
  EXPECT_EQ(expected, list);

  // clearTo should remove the files.
  files.clearTo(1000);
  EXPECT_EQ(nullptr, files.open(7, "r", 0).file);
  EXPECT_EQ(0, files.ls().size());
}

TEST(FileUtilsTest, MultiWriter) {
  TemporaryDirectory dir("gorilla_data_block");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "6"));

  FileUtils files1(6, "test123", dir.dirname());
  FileUtils files2(6, "test456", dir.dirname());

  files1.clearTo(1000);
  files2.clearTo(1000);

  // Create 2 in one namespace and 1 in the other.
  fclose(files1.open(5, "w", 0).file);
  fclose(files1.open(6, "w", 0).file);
  fclose(files2.open(5, "w", 0).file);

  EXPECT_EQ(2, files1.ls().size());
  EXPECT_EQ(1, files2.ls().size());

  // Emptying one doesn't affect the other.
  files1.clearTo(1000);
  EXPECT_EQ(1, files2.ls().size());

  files2.clearTo(1000);
}

TEST(FileUtilsTest, BigIntTest) {
  TemporaryDirectory dir("gorilla_data_block");
  boost::filesystem::create_directories(
      FileUtils::joinPaths(dir.dirname(), "6"));

  FileUtils files1(6, "test123", dir.dirname());
  fclose(files1.open(206158474775, "w", 0).file);

  FileUtils files2(6, "test123", dir.dirname());
  auto ids = files2.ls();
  ASSERT_EQ(1, ids.size());
  ASSERT_EQ(206158474775, ids[0]);
}
