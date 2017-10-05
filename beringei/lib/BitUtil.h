/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <folly/FBString.h>
#include <folly/String.h>

namespace facebook {
namespace gorilla {

class BitUtil {
 public:
  // Adds a value to a bit string. `bitsInValue` specifies the number
  // of least significant bits that will be added to the bit
  // string. The bits from `value` will be added from the most
  // significant bit to the least significant bit.
  static void addValueToBitString(
      uint64_t value,
      uint64_t bitsInValue,
      folly::fbstring& bitString,
      uint32_t& numBits);

  // Same as above except this function will throw if too many bits
  // are attempted to read.
  // Reads a value from a bit string. `bitPos` is updated by
  // `bitsToRead`. `bitsToRead` must be 64 or less.
  static uint64_t readValueFromBitString(
      folly::StringPiece data,
      uint64_t& bitPos,
      uint32_t bitsToRead);

  // Finds the first zero bit and returns its distance from bitPos. If
  // not found within limit, returns limit.
  static uint32_t findTheFirstZeroBit(
      folly::StringPiece data,
      uint64_t& bitPos,
      uint32_t limit);

  // Reads a value until the first zero bit is found or limit reached.
  // The zero is included in the value as the least significant bit.
  // `limit` must be 32 or less. Throws an exception if too many bits
  // are being read.
  static uint32_t readValueThroughFirstZero(
      folly::StringPiece data,
      uint64_t& bitPos,
      uint32_t limit);
};
}
} // facebook::gorilla
