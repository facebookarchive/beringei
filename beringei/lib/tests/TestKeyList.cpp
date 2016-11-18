/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "TestKeyList.h"

#include <folly/Random.h>
#include <algorithm>
#include <cassert>
#include <iterator>

const int TestKeyList::kExpectedKeyLength = 80;
const int TestKeyList::kExpectedKeyLengthSpread = 30;
const int TestKeyList::kAllowedASCIILowerBound = 32;
const int TestKeyList::kAllowedASCIIUpperBound = 126;

TestKeyList::TestKeyList(int listSize) : size_(listSize) {
  assert(size_ > 0);
  generateKeys();
}

void TestKeyList::generateKeys() {
  keyList_.reserve(size_);
  std::generate_n(std::back_inserter(keyList_), size_, [&] {
    std::string str;
    int length = kExpectedKeyLength +
        folly::Random::rand32(
                     -kExpectedKeyLengthSpread, kExpectedKeyLengthSpread);
    str.reserve(length);
    std::generate_n(std::back_inserter(str), length, [&]() {
      return static_cast<char>(folly::Random::rand32(
          kAllowedASCIILowerBound, kAllowedASCIIUpperBound + 1));
    });
    assert(str.length() == length);
    return str;
  });
}

const char* TestKeyList::testStr(int i) {
  int offset = i / size_;
  auto& key = keyList_[i % size_];
  // If this assertion fails. Generated key set is too small for test load.
  assert(key.length() > offset);
  // Using a substring of a key we generated if the tests actually needs a
  // lot more than we generated.
  return key.c_str() + offset;
}
