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

class TestKeyList {
 public:
  explicit TestKeyList(int listSize);
  // Return a random c style string.
  const char* testStr(int i);

 private:
  static const int kExpectedKeyLength;
  static const int kExpectedKeyLengthSpread;
  // Inclusive bounds for character range allowed in keys.
  static const int kAllowedASCIILowerBound;
  static const int kAllowedASCIIUpperBound;

  void generateKeys();

  int size_;
  std::vector<std::string> keyList_;
};
