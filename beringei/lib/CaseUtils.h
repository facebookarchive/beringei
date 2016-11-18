/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <cstring>

namespace facebook {
namespace gorilla {

// A 12x faster implementation of tolower().
inline unsigned char fastToLower(unsigned char c) {
  return c >= 'A' && c <= 'Z' ? c + ('a' - 'A') : c;
}

// Case-insensitive comparison.
struct CaseEq {
  bool operator()(const char* s1, const char* s2) const;
};

// Case-insensitive hash.
struct CaseHash {
  size_t operator()(const char* s) const;
};
}
} // facebook::gorilla
