/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <stdint.h>

namespace facebook {
namespace gorilla {

class BucketUtils {
 public:
  // Conversions between bucket number and timestamp.
  static uint32_t bucket(uint64_t unixTime, uint64_t windowSize, int shardId);
  static uint64_t timestamp(uint32_t bucket, uint64_t windowSize, int shardId);

  // Conversions between duration and number of buckets.
  static uint64_t duration(uint32_t buckets, uint64_t windowSize);
  static uint32_t buckets(uint64_t duration, uint64_t windowSize);

  // Gets the timestamp of the bucket the original timestamp is in.
  static uint64_t
  floorTimestamp(uint64_t unixTime, uint64_t windowSize, int shardId);
};
}
}
