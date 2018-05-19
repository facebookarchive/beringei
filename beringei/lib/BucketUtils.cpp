/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "BucketUtils.h"

#include <folly/Format.h>
#include <glog/logging.h>

DEFINE_int32(gorilla_shards, 100, "Number of maps for gorilla to use");

namespace facebook {
namespace gorilla {

uint32_t
BucketUtils::bucket(uint64_t unixTime, uint64_t windowSize, int shardId) {
  if (unixTime < shardId * windowSize / FLAGS_gorilla_shards) {
    LOG(ERROR) << folly::sformat(
        "Shard: {}. Window size: {}. TS {} falls into a negative bucket",
        shardId,
        windowSize,
        unixTime);
    return 0;
  }
  return (uint32_t)(
      (unixTime - (shardId * windowSize / FLAGS_gorilla_shards)) / windowSize);
}

uint64_t
BucketUtils::timestamp(uint32_t bucket, uint64_t windowSize, int shardId) {
  return bucket * windowSize + (shardId * windowSize / FLAGS_gorilla_shards);
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
  // Check against first timestamp to avoid underflow, should never happen.
  auto firstTimestamp = timestamp(0, windowSize, shardId);
  if (unixTime <= firstTimestamp) {
    return firstTimestamp;
  }

  auto b = bucket(unixTime - 1, windowSize, shardId);
  return timestamp(b + 1, windowSize, shardId);
}

uint32_t BucketUtils::alignedBucket(uint64_t unixTime, uint64_t windowSize) {
  return (uint32_t)(unixTime / windowSize);
}

uint64_t BucketUtils::alignedTimestamp(uint32_t bucket, uint64_t windowSize) {
  return bucket * windowSize;
}

uint64_t BucketUtils::floorAlignedTimestamp(
    uint64_t unixTime,
    uint64_t windowSize) {
  return alignedTimestamp(alignedBucket(unixTime, windowSize), windowSize);
}

bool BucketUtils::isAlignedBucketTimestamp(
    uint64_t unixTime,
    uint64_t windowSize) {
  return unixTime % windowSize == 0;
}
} // namespace gorilla
} // namespace facebook
