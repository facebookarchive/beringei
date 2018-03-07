/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <string>
#include <vector>

#include "beringei/if/gen-cpp2/beringei_data_types.h"

namespace facebook {
namespace gorilla {
struct BeringeiScanShardResult {
  explicit BeringeiScanShardResult(size_t size = 0)
      : status(StatusCode::OK), keys(size), data(size), queriedRecently(size) {}
  BeringeiScanShardResult(const BeringeiScanShardResult&) = delete;
  BeringeiScanShardResult& operator=(const BeringeiScanShardResult&) = delete;
  BeringeiScanShardResult(BeringeiScanShardResult&&) = default;
  BeringeiScanShardResult& operator=(BeringeiScanShardResult&&) = default;

  // @pre *this and rhs do not violate the class invariant
  bool operator==(const BeringeiScanShardResult& rhs) const;

  StatusCode status;

  // Class invariant is keys.size() == data.size() == queriedRecently.size()
  std::vector<std::string> keys;
  std::vector<std::vector<TimeValuePair>> data;
  std::vector<bool> queriedRecently;
};
} // namespace gorilla
} // namespace facebook
