/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <folly/Range.h>
#include <cstring>

namespace facebook {
namespace gorilla {

// Case-insensitive comparison.
struct CaseEq {
  bool operator()(const char* s1, const char* s2) const;
};

// Case-insensitive hash.
struct CaseHash {
  size_t operator()(const char* s) const;

  static uint64_t hash(folly::StringPiece s, uint64_t seed);
};
}
} // facebook::gorilla
