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

struct Event {
  1: string category,
  2: i64 ts, /* Timestamp in us */
  3: string sample,
}

struct Log {
  1: i64 ts, /* Timestamp in us */
  2: string file, /* Filename */
  3: string log, /* Log line */
}

struct NodeStates {
  1: string mac,
  2: string name,
  3: string site,
  4: list<Stat> stats,
}

struct NodeLogs {
  1: string mac,
  2: string name,
  3: string site,
  4: list<Log> logs,
}

struct NodeEvents {
  1: string mac,
  2: string name,
  3: string site,
  4: list<Event> events,
}

struct Topology {
  1: string name,
}

struct StatsWriteRequest {
  1: Topology topology,
  2: list<NodeStates> agents,
}

struct LogsWriteRequest {
  1: Topology topology,
  2: list<NodeLogs> agents,
}

struct EventsWriteRequest {
  1: Topology topology,
  2: list<NodeEvents> agents,
}

struct MySqlNodeData {
  1: i64 id,
  2: string node,
  3: string mac,
  4: string network,
  5: string site,
}

struct MySqlEventData {
  1: string sample,
  2: i64 timestamp,
  3: i64 category_id,
}

struct AlertsWriteRequest {
  1: i64 timestamp,
  2: string node_mac,
  3: string node_name,
  4: string node_site,
  5: string node_topology,
  6: string alert_id,
  7: string alert_regex,
  8: double alert_threshold,
  9: string alert_comparator,
  10: string alert_level,
  11: string trigger_key,
  12: double trigger_value,
}

struct MySqlAlertData {
  1: i64 node_id,
  2: i64 timestamp,
  3: string alert_id,
  4: string alert_regex,
  5: double alert_threshold,
  6: string alert_comparator,
  7: string alert_level,
  8: string trigger_key,
  9: double trigger_value,
}
