/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <gflags/gflags.h>

#include "BitUtil.h"

DECLARE_int64(gorilla_blacklisted_time_min);
DECLARE_int64(gorilla_blacklisted_time_max);

namespace facebook {
namespace gorilla {

namespace {

template <typename T>
using is_vector = std::
    is_same<T, std::vector<typename T::value_type, typename T::allocator_type>>;

template <typename T>
inline typename std::enable_if<is_vector<T>::value>::type
addValueToOutput(T& out, int64_t unixTime, double value) {
  out.emplace_back();
  out.back().unixTime = unixTime;
  out.back().value = value;
}

template <typename T>
inline typename std::enable_if<!is_vector<T>::value>::type
addValueToOutput(T& out, int64_t unixTime, double value) {
  out[unixTime] = value;
}

// Call reserve() only if it exists.
template <typename T>
inline typename std::enable_if<
    std::is_member_function_pointer<decltype(&T::reserve)>::value>::type
reserve(T* out, size_t n) {
  out->reserve(n);
}

inline void reserve(...) {}

} // namespace

template <typename T>
int TimeSeriesStream::readValues(
    T& out,
    folly::StringPiece data,
    int n,
    int64_t begin,
    int64_t end) {
  if (data.empty() || n == 0) {
    return 0;
  }
  int count = 0;
  try {
    reserve(&out, out.size() + n);

    uint64_t previousValue = 0;
    uint64_t previousLeadingZeros = 0;
    uint64_t previousTrailingZeros = 0;
    uint64_t bitPos = 0;
    int64_t previousTimestampDelta = kDefaultDelta;

    int64_t firstTimestamp =
        BitUtil::readValueFromBitString(data, bitPos, kBitsForFirstTimestamp);
    double firstValue = readNextValue(
        data,
        bitPos,
        previousValue,
        previousLeadingZeros,
        previousTrailingZeros);
    int64_t previousTimestamp = firstTimestamp;

    // If the first data point is after the query range, return nothing.
    if (firstTimestamp > end) {
      return 0;
    }

    if (firstTimestamp >= begin) {
      addValueToOutput(out, firstTimestamp, firstValue);
      count++;
    }

    for (int i = 1; i < n; i++) {
      int64_t unixTime = readNextTimestamp(
          data, bitPos, previousTimestamp, previousTimestampDelta);
      double value = readNextValue(
          data,
          bitPos,
          previousValue,
          previousLeadingZeros,
          previousTrailingZeros);

      if (unixTime > end) {
        break;
      }

      if (unixTime >= begin) {
        if (unixTime < FLAGS_gorilla_blacklisted_time_min ||
            unixTime > FLAGS_gorilla_blacklisted_time_max) {
          addValueToOutput(out, unixTime, value);
          count++;
        }
      }
    }
  } catch (const std::runtime_error& e) {
    LOG(ERROR) << "Error decoding data from Gorilla: " << e.what();
  }
  return count;
}
}
} // facebook::gorilla
