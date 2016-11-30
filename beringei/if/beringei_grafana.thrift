/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

namespace cpp2 facebook.gorilla
namespace py facebook.gorilla.beringei_grafana

/*
  Thrift structure used to parse a grafana query request sent using the
  simple-json-datasource (https://github.com/grafana/simple-json-datasource).

  Sample Query that needs to be parsed

  {
    "panelId": 1,
    "range": {
      "from": "2016-10-31T06:33:44.866Z",
      "to": "2016-10-31T12:33:44.866Z",
      "raw": {
        "from": "now-6h",
        "to": "now"
      }
    },
    "rangeRaw": {
      "from": "now-6h",
      "to": "now"
    },
    "interval": "30s",
    "intervalMs": 30000,
    "targets": [
      { "target": "upper_50", refId: "A" },
      { "target": "upper_75", refId: "B" }
    ],
    "format": "json",
    "maxDataPoints": 550
  }

  We don't need thrift structures for the responses because we are using folly::dynamic
  and folly::toJson().

*/

struct QueryRawRange {
  1: string from,
  2: string to,
}

struct QueryRange {
  1: string from,
  2: string to,
  3: QueryRawRange raw,
}

struct QueryTarget {
  1: string target,
  2: string refId,
}

struct QueryRequest {
  1: i32 panelId,
  2: QueryRange range,
  3: QueryRawRange rangeRaw,
  4: string interval,
  5: i32 intervalMs,
  6: list<QueryTarget> targets,
  7: string format,
  8: i32 maxDataPoints,
}
