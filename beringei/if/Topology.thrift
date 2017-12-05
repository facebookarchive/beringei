/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

namespace cpp2 facebook.gorilla.query
namespace py facebook.gorilla.Topology

enum NodeType {
    CN = 1
    DN = 2
}

enum PolarityType {
    ODD = 1
    EVEN = 2
}

enum LinkType {
    WIRELESS = 1
    ETHERNET = 2
}

struct GolayIdx {
    1: i64 txGolayIdx
    2: i64 rxGolayIdx
}

struct Location {
    1: double latitude
    2: double longitude
    3: double altitude
}

struct Site {
    1: string name
    2: Location location
}

# Typical DN's Lifecycle:
#     OFFLINE -> ONLINE (message exchange with controller)
#             -> ONLINE_INITIATOR (GPS enabled, can act as an initiator)
# Typical CN's Lifecycle:
#     OFFLINE -> ONLINE (message exchange with controller)
enum NodeStatusType {
    OFFLINE = 1
    ONLINE = 2
    ONLINE_INITIATOR = 3  # node is online and can act as an initiator
}

struct Node {
    1: string name
    2: NodeType node_type
    3: optional bool is_primary
    4: string mac_addr
    5: bool pop_node
    7: optional PolarityType polarity
    8: optional GolayIdx golay_idx # default golay for all links from this node
    9: optional NodeStatusType status  # modified by controller
    100: optional string site_name  # not used in e2e
    101: optional double ant_azimuth  # not used in e2e
    102: optional double ant_elevation  # not used in e2e
    103: optional bool has_cpe # node has attached CPE
}

struct Link {
    1: string name
    2: string a_node_name
    3: string z_node_name
    4: LinkType link_type
    5: optional bool is_alive  # modified by controller
    6: optional i64 linkup_attempts  # modified by controller
    7: optional GolayIdx golay_idx # link specific golay index
}

struct Topology {
  1: string name
  2: list<Node> nodes
  3: list<Link> links
  4: list<Site> sites
}
