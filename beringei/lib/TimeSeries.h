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
  // Build a TimeSeriesBlock from the given TimeValues. Test comment.
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
};
}
} // facebook::gorilla
