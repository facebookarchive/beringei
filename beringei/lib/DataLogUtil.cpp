/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "DataLogUtil.h"
#include "BitUtil.h"

namespace {
// The algorithm for encoding data tries to take a full use of
// bytes. In the optimal case everything will fit 3 bytes. This is
// possible when the value doesn't change and the timestamp is the
// same as before. If timestamp is different, but value doesn't
// change, it's possible to use 4 bytes. If the value changes, a
// variable number of bytes will be used.

// 3 bytes with with three unused bits. One of the is used for the control bit.
const static int kShortIdBits = 21;

// 4 bytes with with three unused bits. One of the is used for the control bit.
const static int kLongIdBits = 29;
const static int kShortIdControlBit = 0;
const static int kLongIdControlBit = 1;

// 7 + 2 control bits -> 7 bits left in the byte.
const static int kShortDeltaBits = 7;
const static int kShortDeltaMin = -(1 << (kShortDeltaBits - 1)) + 1;
const static int kShortDeltaMax = (1 << (kShortDeltaBits - 1));

// 14 + 3 control bits -> 7 bits left in the byte.
const static int kMediumDeltaBits = 14;
const static int kMediumDeltaMin = -(1 << (kMediumDeltaBits - 1)) + 1;
const static int kMediumDeltaMax = (1 << (kMediumDeltaBits - 1));

const static int kLargeDeltaBits = 32;
const static int32_t kLargeDeltaMin = std::numeric_limits<int32_t>::min();

// Control bits for the timestamp type
const static int kZeroDeltaControlValue = 0; // 0
const static int kShortDeltaControlValue = 2; // 10
const static int kMediumDeltaControlValue = 6; // 110
const static int kLargeDeltaControlValue = 7; // 111

const static int kPreviousValuesVectorSizeIncrement = 1000;

const static int kBlockSizeBits = 6;
const static int kLeadingZerosBits = 5;
const static int kMinBytesNeeded = 3;

const static int kSameValueControlBit = 0;
const static int kDifferentValueControlBit = 1;
} // namespace

namespace facebook {
namespace gorilla {

void DataLogUtil::appendId(
    uint32_t id,
    folly::fbstring& bits,
    uint32_t& numBits) {
  if (id >= (1 << kShortIdBits)) {
    BitUtil::addValueToBitString(kLongIdControlBit, 1, bits, numBits);
    BitUtil::addValueToBitString(id, kLongIdBits, bits, numBits);
  } else {
    BitUtil::addValueToBitString(kShortIdControlBit, 1, bits, numBits);
    BitUtil::addValueToBitString(id, kShortIdBits, bits, numBits);
  }
}

void DataLogUtil::appendTimestampDelta(
    int64_t delta,
    folly::fbstring& bits,
    uint32_t& numBits) {
  if (delta == 0) {
    BitUtil::addValueToBitString(kZeroDeltaControlValue, 1, bits, numBits);
  } else if (delta >= kShortDeltaMin && delta <= kShortDeltaMax) {
    delta -= kShortDeltaMin;
    CHECK_LT(delta, 1 << kShortDeltaBits);

    BitUtil::addValueToBitString(kShortDeltaControlValue, 2, bits, numBits);
    BitUtil::addValueToBitString(delta, kShortDeltaBits, bits, numBits);
  } else if (delta >= kMediumDeltaMin && delta <= kMediumDeltaMax) {
    delta -= kMediumDeltaMin;
    CHECK_LT(delta, 1 << kMediumDeltaBits);

    BitUtil::addValueToBitString(kMediumDeltaControlValue, 3, bits, numBits);
    BitUtil::addValueToBitString(delta, kMediumDeltaBits, bits, numBits);
  } else {
    delta -= kLargeDeltaMin;
    BitUtil::addValueToBitString(kLargeDeltaControlValue, 3, bits, numBits);
    BitUtil::addValueToBitString(delta, kLargeDeltaBits, bits, numBits);
  }
}

// Append xor'd delta to data log buffer
void DataLogUtil::appendValueXor(
    uint64_t xorWithPrevious,
    folly::fbstring& bits,
    uint32_t& numBits) {
  if (xorWithPrevious == 0) {
    // Same as previous value, just store a single bit.
    BitUtil::addValueToBitString(kSameValueControlBit, 1, bits, numBits);
  } else {
    BitUtil::addValueToBitString(kDifferentValueControlBit, 1, bits, numBits);

    // Check TimeSeriesStream.cpp for more information about this
    // algorithm.
    int leadingZeros = __builtin_clzll(xorWithPrevious);
    int trailingZeros = __builtin_ctzll(xorWithPrevious);
    if (leadingZeros > 31) {
      leadingZeros = 31;
    }
    int blockSize = 64 - leadingZeros - trailingZeros;
    uint64_t blockValue = xorWithPrevious >> trailingZeros;

    BitUtil::addValueToBitString(
        leadingZeros, kLeadingZerosBits, bits, numBits);
    BitUtil::addValueToBitString(blockSize - 1, kBlockSizeBits, bits, numBits);
    BitUtil::addValueToBitString(blockValue, blockSize, bits, numBits);
  }
}

int DataLogUtil::readLog(
    const char* buffer,
    size_t len,
    int64_t baseTime,
    size_t maxAllowedTimeSeriesId,
    std::function<bool(uint32_t, int64_t, double)> out) {
  std::vector<double> previousValues{};
  return readLog(
      buffer, len, baseTime, maxAllowedTimeSeriesId, previousValues, out);
}

int DataLogUtil::readLog(
    const char* buffer,
    size_t len,
    int64_t baseTime,
    size_t maxAllowedTimeSeriesId,
    std::vector<double>& previousValues,
    std::function<bool(uint32_t, int64_t, double)> out) {
  // Read out all the available points.
  int points = 0;
  int64_t prevTime = baseTime;
  uint64_t bitPos = 0;
  folly::StringPiece data(buffer, len);
  // Need at least three bytes for a complete value.
  while (bitPos <= len * 8 - kMinBytesNeeded * 8) {
    try {
      // Read the id of the time series.
      int idControlBit = BitUtil::readValueFromBitString(data, bitPos, 1);
      uint32_t id;
      if (idControlBit == kShortIdControlBit) {
        id = BitUtil::readValueFromBitString(data, bitPos, kShortIdBits);
      } else {
        id = BitUtil::readValueFromBitString(data, bitPos, kLongIdBits);
      }

      if (id > maxAllowedTimeSeriesId) {
        LOG(ERROR) << "Corrupt file. ID is too large " << id;
        break;
      }

      // Read the time stamp delta based on the the number of bits in
      // the delta.
      uint32_t timeDeltaControlValue =
          BitUtil::readValueThroughFirstZero(data, bitPos, 3);
      int64_t timeDelta = 0;
      switch (timeDeltaControlValue) {
        case kZeroDeltaControlValue:
          break;
        case kShortDeltaControlValue:
          timeDelta =
              BitUtil::readValueFromBitString(data, bitPos, kShortDeltaBits) +
              kShortDeltaMin;
          break;
        case kMediumDeltaControlValue:
          timeDelta =
              BitUtil::readValueFromBitString(data, bitPos, kMediumDeltaBits) +
              kMediumDeltaMin;
          break;
        case kLargeDeltaControlValue:
          timeDelta =
              BitUtil::readValueFromBitString(data, bitPos, kLargeDeltaBits) +
              kLargeDeltaMin;
          break;
        default:
          LOG(ERROR) << "Invalid time delta control value "
                     << timeDeltaControlValue;
          return points;
      }

      int64_t unixTime = prevTime + timeDelta;
      prevTime = unixTime;

      if (id >= previousValues.size()) {
        previousValues.resize(id + kPreviousValuesVectorSizeIncrement, 0);
      }

      // Finally read the value.
      double value;
      uint32_t sameValueControlBit =
          BitUtil::readValueFromBitString(data, bitPos, 1);
      if (sameValueControlBit == kSameValueControlBit) {
        value = previousValues[id];
      } else {
        uint32_t leadingZeros =
            BitUtil::readValueFromBitString(data, bitPos, kLeadingZerosBits);
        uint32_t blockSize =
            BitUtil::readValueFromBitString(data, bitPos, kBlockSizeBits) + 1;
        uint64_t blockValue =
            BitUtil::readValueFromBitString(data, bitPos, blockSize);

        // Shift to left by the number of trailing zeros
        blockValue <<= (64 - blockSize - leadingZeros);

        uint64_t* previousValue = (uint64_t*)&previousValues[id];
        uint64_t xorredValue = blockValue ^ *previousValue;
        double* temp = (double*)&xorredValue;
        value = *temp;
      }

      previousValues[id] = value;

      // Each tuple (id, unixTime, value) in the file is byte aligned.
      if (bitPos % 8 != 0) {
        bitPos += 8 - (bitPos % 8);
      }

      if (!out(id, unixTime, value)) {
        // Callback doesn't accept more points.
        break;
      }
      points++;

    } catch (std::exception& e) {
      // Most likely too many bits were being read.
      LOG(ERROR) << e.what();
      break;
    }
  }

  return points;
}

} // namespace gorilla
} // namespace facebook
