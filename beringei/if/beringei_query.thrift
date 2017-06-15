/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

namespace cpp2 facebook.gorilla
namespace py facebook.gorilla.beringei_query

struct KeyData {
  1: i64 keyId,
  2: string key,
  10: optional string displayName,
  11: optional string linkName,
  20: optional string node,
  21: optional string nodeName,
  30: optional string siteName,
}

struct Query {
  1: string type,
  2: list<i64> key_ids,
  3: list<KeyData> data,
  4: optional i32 min_ago,
  5: string agg_type,

  // period to search (unixtime)
  10: i64 start_ts,
  11: i64 end_ts,
}

struct QueryRequest {
  1: list<Query> queries,
}
