/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "CaseUtils.h"

#include <folly/SpookyHashV2.h>

namespace facebook {
namespace gorilla {

// This is the max gorilla key length as specified in GorillaServiceHandler.
const static size_t kBufferSize = 400;

bool CaseEq::operator()(const char* s1, const char* s2) const {
  return strcasecmp(s1, s2) == 0;
}

size_t CaseHash::operator()(const char* s) const {
  // Making a copy and then using a word-at-a-time hash is faster than feeding
  // individual bytes into a byte-at-a-time hash.
  char buf[kBufferSize];
  int n = 0;
  for (; s[n]; n++) {
    buf[n] = fastToLower(s[n]);
  }
  return folly::hash::SpookyHashV2::Hash64(buf, n, 0);
}
}
} // facebook::gorilla
