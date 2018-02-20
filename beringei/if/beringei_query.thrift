/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

include "beringei/if/Topology.thrift"

namespace cpp2 facebook.gorilla.query
namespace py facebook.gorilla.beringei_query

enum KeyUnit {
  NONE = 0,
  BPS = 1,
}

struct KeyData {
  1: i64 keyId,
  2: string key,
  3: KeyUnit unit,

  10: string displayName,
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

struct TypeAheadRequest {
  1: string topologyName,
  2: string input,
  // TODO - site/node restrictions
}

struct TableQuery {
  1: string name,
  // event, event_sec..?
  2: string type,
  // e.g. fw_uptime
  3: string metric,

  10: i64 start_ts,
  11: i64 end_ts,
  // use over start/end if set
  12: optional i32 min_ago,
}

struct TableQueryRequest {
  1: string topologyName,
  // no distinction between node/link queries yet
  10: list<TableQuery> nodeQueries,
  11: list<TableQuery> linkQueries,
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

struct StatsWriteRequest {
  1: Topology.Topology topology,
  2: list<NodeStates> agents,
}

struct LogsWriteRequest {
  1: Topology.Topology topology,
  2: list<NodeLogs> agents,
}

struct EventsWriteRequest {
  1: Topology.Topology topology,
  2: list<NodeEvents> agents,
}

struct MySqlNodeData {
  1: i64 id,
  2: string node,
  3: string mac,
  4: string network,
  5: string site,
  6: map<i64, string> keyList,
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

// scan responses// transmit and receive beamforming indices of a micro route
enum ScanType {
  PBF = 1,      // Periodic beamforming
  IM = 2,       // Interference measurement
  RTCAL = 3,    // Runtime calibration
  CBF_TX = 4,   // Coordinated beamforming (aka interference nulling), tx side
  CBF_RX = 5,   // Same, rx side
}

enum ScanMode {
  COARSE = 1,
  FINE = 2,
  SELECTIVE = 3,
}

// Runtime Calibration
enum RTCal {
  NO_CAL = 0, // No calibration, init state
  TOP_RX_CAL = 1, // Top Panel, responder Rx cal with fixed intiator Tx beam
  TOP_TX_CAL = 2, // Top Panel, intiator Tx cal with fixed responder Rx beam
  BOT_RX_CAL = 3, // Bot Panel, responder Rx cal with fixed intiator Tx beam
  BOT_TX_CAL = 4, // Bot Panel, intiator Tx cal with fixed responder Rx beam
  VBS_RX_CAL = 5, // Top + Bot, responder Rx cal with fixed intiator Tx beam
  VBS_TX_CAL = 6, // Top + Bot, intiator Tx cal with fixed responder Rx beam
  RX_CBF_AGGRESSOR = 7, // RX Coordinated BF Nulling, Aggressor link
  RX_CBF_VICTIM = 8,    // RX Coordinated BF Nulling, Victim link
  TX_CBF_AGGRESSOR = 9, // TX Coordinated BF Nulling, Aggressor link
  TX_CBF_VICTIM = 10,   // TX Coordinated BF Nulling, Victim link
  RT_CAL_INVALID = 11,
}

enum ScanFwStatus {
  COMPLETE = 0,
  INVALID_TYPE = 1,
  INVALID_START_TSF = 2,
  INVALID_STA = 3,
  AWV_IN_PROG = 4,
  STA_NOT_ASSOC = 5,
  REQ_BUFFER_FULL = 6,
  LINK_SHUT_DOWN = 7,
  UNKNOWN_ERROR = 8,
}

// json_obj is the entire unedited ScanRespTop as received from the Controller
//  as a (compressed) blob other fields in the table are used for searching
// the mysql table for convenience
struct MySqlScanResp {
  1: string json_obj;
  2: i32 token;
  3: i32 node_id; // numeric id corresponding to the mac_addr
  4: i16 scan_type;
  5: i16 scan_sub_type;
  6: string network; // topology name
  7: bool apply_flag; // flag to intruct responder to apply new beams
  // if there is an error indicated in status, you need to look at the json_obj
  // to see the reason
  // completion_status is a bitmask: bit 0 tx, bits 1... for each rx; 1 is error
  8: i16 completion_status;
  9: i64 start_bwgd;
  10: i16 scan_mode;
  11: i32 status;
  12: i16 tx_power;
}

struct MicroRoute {
  1: i16 tx;
  2: i16 rx;
}

// individual micro-route measurement/report
struct RouteInfo {
  1: MicroRoute route; // beamforming indices of micro route
  2: double rssi;      // received signal strength, in dBm
  3: double snrEst;    // measured during the short training field, in dB
  4: double postSnr;    // not valid during a scan - ignore it
  5: i32 rxStart;      // relative arrival time of the packet, in us
  6: byte packetIdx;   // Repeat count of this packet, 0-based
  7: i16 sweepidx; // in case of multiple sweeps, indicates the index
}

// without "optional" keyword: tx acts as required, rx gets default value
// with "optional" keyword: tx acts as optional, rx check __isset
// with "required" rx fails if parameter is missing
// if any required fields are missing, deserialization will fail and
// it will return a 500 status to the controller
struct ScanResp {
   1: required i32 token; // token to match request to response
   2: i64 curSuperframeNum; // time-stamp of measurement
   3: list<RouteInfo> routeInfoList; // list of routes
   4: i16 txPwrIndex; // tx power used during scan (tx node only)
   5: i16 azimuthBeam; // before any new beams are applied
   6: i16 oldBeam;  // PBF: the old azimuth beam; RTCAL, VBS and CBF: old phase
   7: i16 newBeam;  // PBF new azimuth beam; RTCAL new phase
   8: i16 numSweeps; //Number of times beams were scanned
   9: i64 startSuperframeNum; // Start of BW Alloc for Scan
   10: i64 endSuperframeNum; // End of BW Alloc for scan
   11: required i16 status; // whether it completed normally or not (and why)
   12: i16 sweepStartBeam; // Applicable for selective scan only
   13: i16 sweepEndBeam; // Applicable for selective scan only
   14: required i16 role; // Initiator (0) or Responder (1)
   15: required string macAddr; // xx:xx:xx:xx:xx:xx format
}

struct ScanRespTop {
  // only one responses per message
  1: required map<string /* nodename */, ScanResp> responses;
  2: required i64 startBwgdIdx; // starting BWGD
  3: required i16 type; // PBF, RTCAL, etc.
  4: i16 subType; // applies only to RTCAL
  5: i16 mode; // coarse, fine, selective
  6: bool apply; // flag to intruct responder to apply new beam
  7: string txNode; // name of the tx node
}
