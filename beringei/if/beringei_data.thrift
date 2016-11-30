/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

namespace cpp2 facebook.gorilla
namespace py facebook.gorilla.beringei_data

struct Key {
  1: string key,
  2: i64 shardId,
}

// getData structs
enum Compression {
  NONE,
  ZLIB,
}

// DO NOT interact with this struct directly. Feed it into TimeSeries.h.
struct TimeSeriesBlock {
  1: Compression compression,
  2: i32 count,
  3: binary data,
}

enum StatusCode {
  OK,
  DONT_OWN_SHARD,
  KEY_MISSING,
  RPC_FAIL,
  SHARD_IN_PROGRESS,
  BUCKET_NOT_FINALIZED,
  ZIPPY_STORAGE_FAIL,
  MISSING_TOO_MUCH_DATA,
}

struct TimeSeriesData {
  1: list<TimeSeriesBlock> data,
  2: StatusCode status = OK,
}

struct GetDataRequest {
  1: list<Key> keys,
  2: i64 begin,
  3: i64 end,
}

struct GetDataResult {
  1: list<TimeSeriesData> results,
}

// putData structs

struct TimeValuePair {
  1: i64 unixTime,
  2: double value,
}

struct DataPoint {
  1: Key key,
  2: TimeValuePair value,
  3: i32 categoryId,
}

struct PutDataRequest {
  1: list<DataPoint> data,
}

struct PutDataResult {
  // return not owned data points
  1: list<DataPoint> data,
}

struct GetShardDataBucketResult {
  1: StatusCode status,
  2: list<string> keys,
  3: list<list<TimeSeriesBlock>> data,
  4: list<bool> recentRead,
  5: bool moreEntries,
}

// Structs that represent the configuration of Beringei services.

// Represents which shard is owned by which host
struct ShardInfo {
  // Zero based index.
  1: i32 shardId,

  // Hostname of service that owns the shard.
  2: string hostAddress,

  // Port on which the Beringei service is running on.
  3: i32 port,
}

// Represents a Beringie service and it's shard ownership information.
struct ServiceMap {
  // The name of the Beringei service.
  1: string serviceName,

  // Friendly name for the location of the Beringei service that
  // can be used to identify the nearest Beringei service.
  2: string location,

  // Enables logging of newly created keys for lib/KeyLoggerBase.
  3: bool isLoggingNewKeysEnabled,

  // Shard ownership information for the service.
  4: list<ShardInfo> shardMap,
}

// Represents all Beringie services.
struct ConfigurationInfo {
  // Total number shards used by Beringei.
  1: i32 shardCount,

  // List of Beringei services.
  2: list<ServiceMap> serviceMap,
}
