/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <gtest/gtest.h>

#include "beringei/lib/BitUtil.h"

using namespace ::testing;
using namespace facebook::gorilla;
using namespace folly;

TEST(BitUtilTest, InsertOneBit) {
  fbstring str;
  uint32_t length = 0;
  BitUtil::addValueToBitString(1, 1, str, length);
  ASSERT_EQ(1, length);
  ASSERT_EQ(1, str.length());
  ASSERT_EQ('\x80', str[0]);
}

TEST(BitUtilTest, Ones) {
  fbstring str;
  uint32_t length = 0;

  for (int i = 0; i < 16; i++) {
    BitUtil::addValueToBitString(1, 1, str, length);
  }

  ASSERT_EQ(16, length);
  ASSERT_EQ(2, str.length());
  ASSERT_EQ('\xFF', str[0]);
  ASSERT_EQ('\xFF', str[1]);

  uint64_t bitPos = 0;
  folly::StringPiece data(str.c_str(), str.size());
  for (int i = 0; i < 16; i++) {
    uint64_t v = BitUtil::readValueFromBitString(data, bitPos, 1);
    ASSERT_EQ(1, v);
  }
}

TEST(BitUtilTest, Facebook) {
  fbstring str;
  uint32_t length = 0;

  uint32_t value = 0xFACEB00C;

  BitUtil::addValueToBitString(value, 32, str, length);
  ASSERT_EQ(32, length);
  ASSERT_EQ(4, str.length());
  ASSERT_EQ('\xFA', str[0]);
  ASSERT_EQ('\xCE', str[1]);
  ASSERT_EQ('\xB0', str[2]);
  ASSERT_EQ('\x0C', str[3]);

  folly::StringPiece data(str.c_str(), str.size());
  uint64_t bitPos = 0;
  uint64_t v1 = BitUtil::readValueFromBitString(data, bitPos, 16);
  ASSERT_EQ(16, bitPos);
  uint64_t v2 = BitUtil::readValueFromBitString(data, bitPos, 16);
  ASSERT_EQ(32, bitPos);

  ASSERT_EQ(0xFACE, v1);
  ASSERT_EQ(0xB00C, v2);
}

TEST(BitUtilTest, SmallChunks) {
  fbstring str;
  uint32_t length = 0;

  BitUtil::addValueToBitString(11, 6, str, length); // 001011
  BitUtil::addValueToBitString(12, 6, str, length); // 001100
  BitUtil::addValueToBitString(13, 6, str, length); // 001101
  BitUtil::addValueToBitString(14, 6, str, length); // 001110

  ASSERT_EQ(24, length);
  ASSERT_EQ(3, str.length());

  ASSERT_EQ('\x2C', str[0]);
  ASSERT_EQ('\xC3', str[1]);
  ASSERT_EQ('\x4E', str[2]);

  folly::StringPiece data(str.c_str(), str.size());
  uint64_t bitPos = 0;
  uint64_t v = BitUtil::readValueFromBitString(data, bitPos, 6);
  ASSERT_EQ(11, v);
  v = BitUtil::readValueFromBitString(data, bitPos, 6);
  ASSERT_EQ(12, v);
  v = BitUtil::readValueFromBitString(data, bitPos, 6);
  ASSERT_EQ(13, v);
  v = BitUtil::readValueFromBitString(data, bitPos, 6);
  ASSERT_EQ(14, v);
}

TEST(BitUtilTest, BigAndSmallChunks) {
  fbstring str;
  uint32_t length = 0;

  BitUtil::addValueToBitString(511, 9, str, length); // 111111111
  BitUtil::addValueToBitString(512, 10, str, length); // 1000000000
  BitUtil::addValueToBitString(5, 3, str, length); // 101
  BitUtil::addValueToBitString(511, 9, str, length); // 111111111
  BitUtil::addValueToBitString(512, 10, str, length); // 1000000000
  BitUtil::addValueToBitString(5, 3, str, length); // 101

  ASSERT_EQ(44, length);
  ASSERT_EQ(6, str.length());

  ASSERT_EQ('\xFF', str[0]);
  ASSERT_EQ('\xC0', str[1]);
  ASSERT_EQ('\x17', str[2]);
  ASSERT_EQ('\xFF', str[3]);
  ASSERT_EQ('\x00', str[4]);
  ASSERT_EQ('\x50', str[5]);

  folly::StringPiece data(str.c_str(), str.size());
  uint64_t bitPos = 0;
  uint64_t v = BitUtil::readValueFromBitString(data, bitPos, 9);
  ASSERT_EQ(511, v);
  v = BitUtil::readValueFromBitString(data, bitPos, 10);
  ASSERT_EQ(512, v);
  v = BitUtil::readValueFromBitString(data, bitPos, 3);
  ASSERT_EQ(5, v);
  v = BitUtil::readValueFromBitString(data, bitPos, 9);
  ASSERT_EQ(511, v);
  v = BitUtil::readValueFromBitString(data, bitPos, 10);
  ASSERT_EQ(512, v);
  v = BitUtil::readValueFromBitString(data, bitPos, 3);
  ASSERT_EQ(5, v);
}

TEST(BitUtilTest, ReadTooMuch) {
  fbstring str;
  uint64_t bitPos = 0;
  folly::StringPiece data(str.c_str(), str.size());
  ASSERT_ANY_THROW(BitUtil::readValueThroughFirstZero(data, bitPos, 10));
}
