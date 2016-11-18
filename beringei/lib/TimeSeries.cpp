/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "TimeSeries.h"

namespace facebook {
namespace gorilla {

void TimeSeries::writeValues(
    const std::vector<TimeValuePair>& values,
    TimeSeriesBlock& block) {
  TimeSeriesStream stream;

  for (const auto& value : values) {
    if (stream.append(value, 0)) {
      block.count++;
    }
  }
  stream.readData(block.data);
}
}
} // facebook::gorilla
