/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "beringei/lib/TimeSeries.h"

#include <string>

namespace facebook {
namespace gorilla {

class GorillaDumperUtils {
 public:
  // Binary format used by next two functions:
  // SeriesBlockHeader at the beginning, followed by compressed data

  // converts binary data to TimeSeriesBlock
  static void readTimeSeriesBlock(
      TimeSeriesBlock& block,
      const std::string& bin);
  // converts TimeSeriesBlock to binary data
  static void writeTimeSeriesBlock(
      const TimeSeriesBlock& block,
      std::string& bin);
};
}
}
