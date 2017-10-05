/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "CaseUtils.h"

#include <folly/String.h>
#include <folly/hash/SpookyHashV2.h>

namespace facebook {
namespace gorilla {

// This is the max gorilla key length as specified in GorillaServiceHandler.
const static size_t kBufferSize = 400;

// Hash seed unique to this class.
const static uint64_t kCaseHashSeed = 0xCA5E4A54;

bool CaseEq::operator()(const char* s1, const char* s2) const {
  return strcasecmp(s1, s2) == 0;
}

size_t CaseHash::operator()(const char* s) const {
  return CaseHash::hash(s, kCaseHashSeed);
}

uint64_t CaseHash::hash(folly::StringPiece s, uint64_t seed) {
  char buf[kBufferSize];
  folly::hash::SpookyHashV2 spooky;
  spooky.Init(seed, seed);

  // Repeatedly copy parts of the string onto the stack, downcase them in place,
  // and feed them into SpookyHash.
  // We could probably speed this up slightly by modifying SpookyHash to
  // downcase words as it goes in order to avoid the copy, but this should be
  // good enough.
  do {
    size_t l = std::min(s.size(), kBufferSize);
    memcpy(buf, s.data(), l);
    folly::toLowerAscii(buf, l);
    spooky.Update(buf, l);
    s.advance(l);
  } while (UNLIKELY(s.size() > 0));

  uint64_t hash1, hash2;
  spooky.Final(&hash1, &hash2);
  return hash1;
}
}
} // facebook::gorilla
