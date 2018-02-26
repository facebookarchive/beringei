/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <cstdint>
#include <vector>

#include <folly/FBString.h>

namespace facebook {
namespace gorilla {

class DataLogUtil {
 public:
  // Append timeseries id to data log buffer
  static void appendId(uint32_t id, folly::fbstring& bits, uint32_t& numBits);

  // Append timestamp delta to data log buffer
  static void
  appendTimestampDelta(int64_t delta, folly::fbstring& bits, uint32_t& numBits);

  // Append xor'd delta to data log buffer
  static void appendValueXor(
      uint64_t xorWithPrevious,
      folly::fbstring& bits,
      uint32_t& numBits);

  static int readLog(
      const char* buffer,
      size_t len,
      int64_t baseTime,
      size_t maxAllowedTimeSeriesId,
      std::function<bool(uint32_t, int64_t, double)> out);

  static int readLog(
      const char* buffer,
      size_t len,
      int64_t baseTime,
      size_t maxAllowedTimeSeriesId,
      std::vector<double>& previousValues,
      std::function<bool(uint32_t, int64_t, double)> out);
};

} // namespace gorilla
} // namespace facebook
