/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

include "beringei/if/beringei_data.thrift"

namespace cpp2 facebook.gorilla
namespace py facebook.gorilla.beringei

service BeringeiService {
  /**
   * Get data for a group of timeseries between two timestamps.
   * This can over-fetch.
   */
  beringei_data.GetDataResult getData(1: beringei_data.GetDataRequest req),
  /**
   * Append data points to their respective timeseries.
   * Unowned points will be returned back to the client.
   */
  beringei_data.PutDataResult putDataPoints(1: beringei_data.PutDataRequest req),

  /**
   * DEPRECATED
   * This will be removed soon. Prefer scanShard().
   *
   * Get all the data for a batch of window beginning at begin
   * and ending at end. It might return less data than limit in case if
   * blacklisted items were filtered out or that's the last batch.
   */
  beringei_data.GetShardDataBucketResult getShardDataBucket(
      1: i64 begin, 2: i64 end, 3: i64 shardId, 4: i32 offset, 5: i32 limit),

  /**
   * Get all data for a shard between two timestamps.
   */
  beringei_data.ScanShardResult scanShard(
      1: beringei_data.ScanShardRequest req),

  /**
   * Gets the last update times for time series.
   */
  beringei_data.GetLastUpdateTimesResult getLastUpdateTimes(
      1: beringei_data.GetLastUpdateTimesRequest req),
}
