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

#include "TimeSeriesStream.h"
#include "beringei/if/gen-cpp2/beringei_data_types.h"

namespace facebook {
namespace gorilla {

class TimeSeries {
 public:
  // Build a TimeSeriesBlock from the given TimeValues.
  static void writeValues(
      const std::vector<TimeValuePair>& values,
      TimeSeriesBlock& block);

  // Append all uncompressed data points that fall between begin and
  // end inclusive to the given datastructure.
  template <typename T>
  static void getValues(
      const TimeSeriesBlock& block,
      T& values,
      int64_t begin,
      int64_t end) {
    TimeSeriesStream::readValues(values, block.data, block.count, begin, end);
  }

  template <typename T>
  static void getValues(
      const std::vector<TimeSeriesBlock>& in,
      T& values,
      int64_t begin,
      int64_t end) {
    for (const auto& block : in) {
      TimeSeries::getValues(block, values, begin, end);
    }
  }

  // Merge all uncompressed data points that fall between begin and end
  // inclusive to the given datastructure.  When non-null, inSize becomes number
  // of entries from in, and mismatches the number of data points from in which
  // did not match out. The merge is stable, as in it prefers datapoints from
  // out over datapoints from in when they have the same timestamp.
  //
  // @param[in] minTimestampDelta ignore new input values not at
  // least this much newer than the last output added
  // @param[in] compareValues increment *mismatches when an ignored
  // (within minTimestampDelta) input value has an absolute or relative
  // difference that is at greater than mismatchEpsilon newest output
  // @param[in] mismatchEpsilon threshold for compareValues
  static void mergeValues(
      const std::vector<TimeSeriesBlock>& in,
      std::vector<facebook::gorilla::TimeValuePair>& out,
      int64_t begin,
      int64_t end,
      int32_t minTimestampDelta,
      bool compareValues,
      double mismatchEpsilon,
      int64_t* inSize,
      int64_t* mismatches);

  static void mergeValues(
      std::vector<facebook::gorilla::TimeValuePair>&& in,
      std::vector<facebook::gorilla::TimeValuePair>& out,
      int32_t minTimestampDelta,
      bool compareValues,
      double mismatchEpsilon,
      int64_t* mismatches);
};
} // namespace gorilla
} // namespace facebook
