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
  NONE = 0,
  ZLIB = 1,
}

// DO NOT interact with this struct directly. Feed it into TimeSeries.h.
struct TimeSeriesBlock {
  1: Compression compression,
  2: i32 count,
  3: binary data,
}

enum StatusCode {
  OK = 0,
  DONT_OWN_SHARD = 1,
  KEY_MISSING = 2,
  RPC_FAIL = 3,
  SHARD_IN_PROGRESS = 4,
  BUCKET_NOT_FINALIZED = 5,
  ZIPPY_STORAGE_FAIL = 6,
  MISSING_TOO_MUCH_DATA = 7,
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

// Query all data from a single shard between two inclusive timestamps.
// This may return additional data on either end of the time range due to
// internal bucket boundaries.
//
// It is possible to request a fraction of the data via subsharding.
// Issuing multiple requests with `numSubshards` == n and `subshard` scanning
// over [0, n) will return each key once.
struct ScanShardRequest {
  1: i64 shardId,
  2: i64 begin,
  3: i64 end,
  4: i64 subshard = 0,
  5: i64 numSubshards = 1,
}

struct ScanShardResult {
  1: StatusCode status,
  2: list<string> keys,
  3: list<list<TimeSeriesBlock>> data,

  // True for each key if data for that key has been queried recently.
  4: list<bool> queriedRecently,
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

struct GetLastUpdateTimesRequest {
  // Which shard to query.
  1: i64 shardId,

  // Minimum last update time in seconds since epoch.
  2: i32 minLastUpdateTime,

  // Offset within the shard when splitting the calls.
  3: i32 offset,

  // The maximum number of results that will be returned. There might still be
  // more results even if the number of results is less than this value.
  4: i32 limit,
}

struct KeyUpdateTime {
  1: string key,
  2: i32 categoryId,
  3: i32 updateTime,

  // True if this key was queried from Beringei in the last ~24 hours,
  // false otherwise.
  4: bool queriedRecently,
}

struct GetLastUpdateTimesResult {
  1: list<KeyUpdateTime> keys,

  // Set to true if there are more results in the shard.
  2: bool moreResults,
}

// Key and Data Logging Structures

enum CheckpointStatus {
  NO_CHECKPOINT = 0,

  // Future calls will summarize all previous entries before this one.
  BEGIN_CHECKPOINT =  1,

  // A checkpoint has finished.
  // Future readers may now ignore all logs before the most recent
  // BEGIN_CHECKPOINT.
  COMPLETE_CHECKPOINT = 2,
}

struct KeyMapping {
  1: i32 shardId,
  2: i32 keyId,
  3: string key,
  4: i32 categoryId,

  // Data for this ID before this timestamp should be discarded.
  // In normal operation, IDs are not be re-used until after the corresponding
  // data has completely aged out. However, this ensures frequent shard movement
  // coupled with intermittent I/O errors causes data loss instead of data
  // corruption.
  5: i32 creationTime,
}

struct AppendKeysRequest {
  1: list<KeyMapping> keys,
  2: CheckpointStatus status,
}

struct DataPointWithID {
  1: i32 shardId,
  2: i32 keyId,
  3: TimeValuePair point,
}

struct LogDataPointsRequest {
  1: list<DataPointWithID> points,
}

enum CheckShardDrainCode {
  SUCCESS = 0,
  NOT_OWN_SHARD = 1,
  INVALID_REQUEST = 2,
}

struct ShardDrainResponse {
  1: CheckShardDrainCode status,
  2: bool isDrained,
  3: i32 shardId,
  4: i64 sequence,
}

struct CheckShardDrainResponse {
  1: list<ShardDrainResponse> shards,
}
