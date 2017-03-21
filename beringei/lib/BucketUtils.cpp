/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "BucketUtils.h"

namespace facebook {
namespace gorilla {

uint32_t
BucketUtils::bucket(uint64_t unixTime, uint64_t windowSize, int shardId) {
  return (uint32_t)(unixTime / windowSize);
}

uint64_t
BucketUtils::timestamp(uint32_t bucket, uint64_t windowSize, int shardId) {
  return bucket * windowSize;
}

uint64_t BucketUtils::duration(uint32_t buckets, uint64_t windowSize) {
  return buckets * windowSize;
}

uint32_t BucketUtils::buckets(uint64_t duration, uint64_t windowSize) {
  return duration / windowSize;
}

uint64_t BucketUtils::floorTimestamp(
    uint64_t unixTime,
    uint64_t windowSize,
    int shardId) {
  return timestamp(bucket(unixTime, windowSize, shardId), windowSize, shardId);
}

uint64_t BucketUtils::ceilTimestamp(
    uint64_t unixTime,
    uint64_t windowSize,
    int shardId) {
  if (unixTime == 0) {
    return 0;
  }

  auto b = bucket(unixTime - 1, windowSize, shardId);
  return timestamp(b + 1, windowSize, shardId);
}
}
} // facebook::gorilla
