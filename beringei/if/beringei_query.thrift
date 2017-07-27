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
  // extra title for link
  12: optional string linkTitleAppend,

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

struct Stat {
  1: string key,
  2: i64 ts,
  3: double value,
}

struct Agent {
  1: string mac,
  2: string name,
  3: string site,
  4: list<Stat> stats,
}

struct Topology {
  1: string name,
}

struct WriteRequest {
  1: Topology topology,
  2: list<Agent> agents,
}

struct MySqlNodeData {
  1: i64 id,
  2: string node,
  3: string mac,
  4: string network,
  5: string site,
}
