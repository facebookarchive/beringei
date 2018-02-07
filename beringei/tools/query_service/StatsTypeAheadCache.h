/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "MySqlClient.h"

#include <folly/Memory.h>
#include <folly/dynamic.h>
#include <folly/futures/Future.h>

#include "beringei/client/BeringeiClient.h"
#include "beringei/client/BeringeiConfigurationAdapterIf.h"
#include "beringei/if/gen-cpp2/beringei_query_types_custom_protocol.h"
#include "beringei/if/gen-cpp2/Topology_types_custom_protocol.h"

namespace facebook {
namespace gorilla {

/**
 * Hold the type-ahead meta-data for a topology
 */
class StatsTypeAheadCache {
public:
  explicit StatsTypeAheadCache(std::shared_ptr<MySqlClient> mySqlClient);

  void fetchMetricNames(query::Topology &request);

  folly::dynamic createLinkMetric(const query::Node &aNode,
                                  const query::Node &zNode,
                                  const std::string &title,
                                  const std::string &description,
                                  const std::string &keyName,
                                  const query::KeyUnit& keyUnit = query::KeyUnit::NONE,
                                  const std::string &keyPrefix = "tgf");
  folly::dynamic createLinkMetricAsymmetric(
      const query::Node &aNode, const query::Node &zNode,
      const std::string &title, const std::string &description,
      const std::string &keyNameA, const std::string &keyNameZ,
      const query::KeyUnit& keyUnit = query::KeyUnit::NONE,
      const std::string &keyPrefix = "tgf");

  folly::dynamic getLinkMetrics(const std::string &metricName,
                                const query::Node &aNode,
                                const query::Node &zNode);

  // fetch topology-wide key data
  std::vector<query::KeyData> getKeyData(const std::string& metricName);

  // type-ahead search
  std::vector<std::vector<query::KeyData>>
    searchMetrics(const std::string &metricName, const int limit = 100); 

private:
  std::vector<std::string> linkMetricKeyNames_;
  std::unordered_set<std::string> macNodes_{};
  std::map<std::string, query::Node> nodesByName_{};
  // --- Metrics per node --- //
  // map node mac -> key names
  std::unordered_map<std::string,
                     std::unordered_map<std::string, std::shared_ptr<query::KeyData>> >
  nodeMacToKeyList_;

  // --- Metrics for all nodes --- //
  // key names => [metric id]
  std::unordered_map<std::string, std::vector<int> > keyToMetricIds_;
  // short names => [metric ids]
  std::unordered_map<std::string, std::vector<int> > nameToMetricIds_;
  // metric id => meta data
  std::unordered_map<int, std::shared_ptr<query::KeyData>> metricIdMetadata_;
  // graph struct for quick traversal
/*  struct 
  map<string
  [s->n->r->]
  std::map<char, same struct> typeaheadSearch_;*/
  

  // mysql client
  std::shared_ptr<MySqlClient> mySqlClient_;
};

using TACacheMap = std::unordered_map<std::string, StatsTypeAheadCache>;


}
} // facebook::gorilla
