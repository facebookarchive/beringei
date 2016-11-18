/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "beringei/lib/GorillaDumperUtils.h"

#include <gtest/gtest.h>
#include <string>

using namespace facebook::gorilla;
using namespace std;

const Compression kCompression = Compression::NONE;
const int32_t kCount = 120;
string kData =
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit. \
               Aliquam lacinia tempor arcu, nec volutpat nisi pretium \
               id. Donec sed orci nisi. Maecenas tristique tincidunt \
               ipsum, nec lacinia sapien iaculis at. Duis consequat \
               tellus erat, eget pulvinar est scelerisque nec. In \
               nec mattis diam.";

TEST(GorillaDumperUtilsTest, WriteReadTimeSeriesBlock) {
  TimeSeriesBlock testBlock;
  testBlock.compression = kCompression;
  testBlock.count = kCount;
  testBlock.data = kData;

  string bin;
  TimeSeriesBlock block;
  GorillaDumperUtils::writeTimeSeriesBlock(testBlock, bin);
  GorillaDumperUtils::readTimeSeriesBlock(block, bin);

  ASSERT_EQ(testBlock.compression, block.compression);
  ASSERT_EQ(testBlock.count, block.count);
  ASSERT_EQ(testBlock.data, block.data);
}
