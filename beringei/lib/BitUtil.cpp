/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "BitUtil.h"

#include <stdexcept>

namespace facebook {
namespace gorilla {

void BitUtil::addValueToBitString(
    uint64_t value,
    uint64_t bitsInValue,
    folly::fbstring& bitString,
    uint32_t& numBits) {
  uint32_t bitsAvailable = (numBits & 0x7) ? (8 - (numBits & 0x7)) : 0;
  numBits += bitsInValue;

  if (bitsInValue <= bitsAvailable) {
    // Everything fits inside the last byte
    bitString[bitString.length() - 1] +=
        (value << (bitsAvailable - bitsInValue));
    return;
  }

  uint32_t bitsLeft = bitsInValue;
  if (bitsAvailable > 0) {
    // Fill up the last byte
    bitString[bitString.length() - 1] +=
        (value >> (bitsInValue - bitsAvailable));
    bitsLeft -= bitsAvailable;
  }

  while (bitsLeft >= 8) {
    // Enough bits for a dedicated byte
    char ch = (value >> (bitsLeft - 8)) & 0xFF;
    bitString += ch;
    bitsLeft -= 8;
  }

  if (bitsLeft != 0) {
    // Start a new byte with the rest of the bits
    char ch = (value & ((1 << bitsLeft) - 1)) << (8 - bitsLeft);
    bitString += ch;
  }
}

uint64_t BitUtil::readValueFromBitString(
    folly::StringPiece data,
    uint64_t& bitPos,
    uint32_t bitsToRead) {
  if (bitPos + bitsToRead > data.size() * 8) {
    throw std::runtime_error("Trying to read too many bits");
  }
  uint64_t value = 0;
  for (int i = 0; i < bitsToRead; i++) {
    value <<= 1;
    uint64_t bit = (data.data()[bitPos >> 3] >> (7 - (bitPos & 0x7))) & 1;
    value += bit;
    bitPos++;
  }
  return value;
}

uint32_t BitUtil::findTheFirstZeroBit(
    folly::StringPiece data,
    uint64_t& bitPos,
    uint32_t limit) {
  uint32_t bits = 0;
  while (bits < limit) {
    uint32_t bit = BitUtil::readValueFromBitString(data, bitPos, 1);
    if (bit == 0) {
      return bits;
    }
    bits++;
  }
  return bits;
}

uint32_t BitUtil::readValueThroughFirstZero(
    folly::StringPiece data,
    uint64_t& bitPos,
    uint32_t limit) {
  uint32_t value = 0;
  for (uint32_t bits = 0; bits < limit; bits++) {
    uint32_t bit = BitUtil::readValueFromBitString(data, bitPos, 1);
    value = (value << 1) + bit;
    if (bit == 0) {
      return value;
    }
  }
  return value;
}
}
} // facebook::gorilla
