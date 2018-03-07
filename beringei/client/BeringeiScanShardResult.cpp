/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <folly/container/Enumerate.h>
#include <glog/logging.h>

#include "BeringeiScanShardResult.h"

namespace facebook {
namespace gorilla {
bool BeringeiScanShardResult::operator==(
    const facebook::gorilla::BeringeiScanShardResult& rhs) const {
  // Fail predictably on *this and rhs class invariant violations
  CHECK_EQ(data.size(), keys.size());
  CHECK_EQ(queriedRecently.size(), keys.size());
  CHECK_EQ(rhs.data.size(), rhs.keys.size());
  CHECK_EQ(rhs.queriedRecently.size(), rhs.keys.size());

  bool ret = status == rhs.status && keys.size() == rhs.keys.size();

  if (ret) {
    std::map<std::string, size_t> lhsToIndex;
    for (const auto& i : folly::enumerate(keys)) {
      const auto insertResult = lhsToIndex.insert(std::make_pair(*i, i.index));
      CHECK(insertResult.second);
    }

    std::map<unsigned, unsigned> lhsToRhs;
    for (size_t i = 0; ret && i < rhs.keys.size(); ++i) {
      const auto lhsIndex = lhsToIndex.find(rhs.keys[i]);
      ret = ret && lhsIndex != lhsToIndex.end();
      if (ret) {
        const auto insertResult =
            lhsToRhs.insert(std::make_pair(lhsIndex->second, i));
        CHECK(insertResult.second);
      }
    }

    for (size_t lhsIndex = 0; ret && lhsIndex < keys.size(); ++lhsIndex) {
      const unsigned rhsIndex = lhsToRhs[lhsIndex];
      ret = ret && data[lhsIndex] == rhs.data[rhsIndex];
      ret = ret && queriedRecently[lhsIndex] == rhs.queriedRecently[rhsIndex];
    }
  }

  return ret;
}
} // namespace gorilla
} // namespace facebook
