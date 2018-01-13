/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "StatsTypeAheadCache.h"

#include <iostream>
#include <regex>
#include <thrift/lib/cpp/util/ThriftSerializer.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

using apache::thrift::SimpleJSONSerializer;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;

namespace facebook {
namespace gorilla {

/**
 * Hold the type-ahead meta-data for a topology
 */
StatsTypeAheadCache::StatsTypeAheadCache(
    std::shared_ptr<MySqlClient> mySqlClient)
    : mySqlClient_(mySqlClient) {
  // node metrics
  // minion_uptime,
  // link metrics
  linkMetricKeyNames_ = {
    "snr",         "rssi",        "mcs",           "per",        "fw_uptime",
    "tx_power",    "rx_bytes",    "tx_bytes",      "rx_pps",     "tx_pps",
    "tx_fail", "tx_ok",
    "rx_errors",   "tx_errors",   "rx_dropped",    "tx_dropped", "rx_frame",
    "rx_overruns", "tx_overruns", "tx_collisions", "speed"
  };
}
  
std::vector<query::KeyData> 
StatsTypeAheadCache::getKeyData(const std::string& metricName) {
  std::vector<query::KeyData> retKeyData;
  // return KeyData based on a metric name
  auto metricId = keyToMetricIds_.find(metricName);
  auto shortMetricId = nameToMetricIds_.find(metricName);
  if (shortMetricId != nameToMetricIds_.end()) {
    for (const auto& keyId : shortMetricId->second) {
      // metric ids
      auto keyDataIt = metricIdMetadata_.find(keyId);
      if (keyDataIt == metricIdMetadata_.end()) {
        continue;
      }
      // add KeyData
      retKeyData.push_back(*keyDataIt->second);
    }
  }
  if (metricId != keyToMetricIds_.end()) {
    for (const auto& keyId : metricId->second) {
      // metric ids
      auto keyDataIt = metricIdMetadata_.find(keyId);
      if (keyDataIt == metricIdMetadata_.end()) {
        continue;
      }
      // add KeyData
      retKeyData.push_back(*keyDataIt->second);
    }
  }
  return retKeyData;
}

void StatsTypeAheadCache::fetchMetricNames(query::Topology &request) {
  folly::dynamic nodeData;

  if (request.nodes.empty()) {
    LOG(ERROR) << "No nodes in topology, failing request";
    return;
  }

  for (auto &node : request.nodes) {
    macNodes_.insert(node.mac_addr);
    nodesByName_[node.name] = node;
  }

  auto dbNodes = mySqlClient_->getNodesWithKeys(macNodes_);
  for (const auto &node : dbNodes) {
    for (const auto &key : node->keyList) {
      folly::StringPiece keyName = folly::StringPiece(key.second);
      if (keyName.endsWith("count.0") || keyName.endsWith("count.600") ||
          keyName.endsWith("count.3600") || keyName.endsWith("count.60") ||
          keyName.endsWith("avg.60") || keyName.endsWith("sum.0")) {
        continue;
      }
      // create KeyData for each metric
      auto keyData = std::make_shared<query::KeyData>();
      keyData->keyId = key.first;
      keyData->key = key.second;
      keyData->displayName = key.second;
      keyData->node = node->mac;
      keyData->nodeName = node->node;
      keyData->siteName = node->site;
      metricIdMetadata_[key.first] = keyData;
      // index the key name to its db id
      keyToMetricIds_[key.second].push_back(key.first);
      nodeMacToKeyList_[node->mac][key.second] = keyData;
    }
  }

  for (auto &link : request.links) {
    // skip wired links
    if (link.link_type != query::LinkType::WIRELESS) {
      continue;
    }
    auto aNode = nodesByName_[link.a_node_name];
    auto zNode = nodesByName_[link.z_node_name];
    for (auto &metricName : linkMetricKeyNames_) {
      folly::dynamic linkMetrics = getLinkMetrics(metricName, aNode, zNode);
      for (auto &key : linkMetrics["keys"]) {
        auto node = SimpleJSONSerializer::deserialize<query::Node>(
            key["node"].asString());
        auto mac = node.mac_addr;
        auto keyName = key["keyName"].asString();
        // match case
        std::transform(keyName.begin(), keyName.end(), keyName.begin(),
                       ::tolower);
        if (!nodeMacToKeyList_.count(mac) ||
            !nodeMacToKeyList_[mac].count(keyName)) {
          continue;
        }
        auto keyData = nodeMacToKeyList_[mac][keyName];
        // insert key / short name references
        keyToMetricIds_[keyName].push_back(keyData->keyId);
        nameToMetricIds_[metricName].push_back(keyData->keyId);
        // push key data for link metric
        keyData->linkName = link.name;
        keyData->linkTitleAppend = key["titleAppend"].asString();
        keyData->displayName = metricName;
      }
    }
  }
}

folly::dynamic StatsTypeAheadCache::createLinkMetric(
    const query::Node &aNode, const query::Node &zNode,
    const std::string &title, const std::string &description,
    const std::string &keyName,
    const query::KeyUnit& keyUnit,
    const std::string &keyPrefix) {
  return folly::dynamic::object("title", title)("description",
                                                description)("scale", NULL)(
      "keys",
      folly::dynamic::array(
          folly::dynamic::object(
              "node", SimpleJSONSerializer::serialize<std::string>(aNode))(
              "keyName", keyPrefix + "." + zNode.mac_addr + "." +
                             keyName)("titleAppend", " (A)"),
          folly::dynamic::object(
              "node", SimpleJSONSerializer::serialize<std::string>(zNode))(
              "keyName", keyPrefix + "." + aNode.mac_addr + "." +
                             keyName)("titleAppend", " (Z)")));
}

folly::dynamic StatsTypeAheadCache::createLinkMetricAsymmetric(
    const query::Node &aNode, const query::Node &zNode,
    const std::string &title, const std::string &description,
    const std::string &keyNameA, const std::string &keyNameZ,
    const query::KeyUnit& keyUnit,
    const std::string &keyPrefix) {
  return folly::dynamic::object("title", title)("description",
                                                description)("scale", NULL)(
      "keys",
      folly::dynamic::array(
          folly::dynamic::object(
              "node", SimpleJSONSerializer::serialize<std::string>(aNode))(
              "keyName", keyPrefix + "." + zNode.mac_addr + "." +
                             keyNameA)("titleAppend", " (A)"),
          folly::dynamic::object(
              "node", SimpleJSONSerializer::serialize<std::string>(zNode))(
              "keyName", keyPrefix + "." + aNode.mac_addr + "." +
                             keyNameZ)("titleAppend", " (Z)")));
}

folly::dynamic
StatsTypeAheadCache::getLinkMetrics(const std::string &metricName,
                                    const query::Node &aNode,
                                    const query::Node &zNode) {
  if (metricName == "rssi") {
    return createLinkMetric(aNode, zNode, "RSSI",
                            "Received Signal Strength Indicator",
                            "phystatus.srssi");
  } else if (metricName == "snr" || metricName == "alive_snr" ||
             metricName == "alive_perc") {
    return createLinkMetric(aNode, zNode, "SnR", "Signal to Noise Ratio",
                            "phystatus.ssnrEst");
  } else if (metricName == "mcs") {
    return createLinkMetric(aNode, zNode, "MCS", "MCS Index", "staPkt.mcs");
  } else if (metricName == "per") {
    return createLinkMetric(aNode, zNode, "PER", "Packet Error Rate",
                            "staPkt.perE6");
  } else if (metricName == "fw_uptime") {
    if (aNode.node_type == query::NodeType::DN &&
        zNode.node_type == query::NodeType::DN) {
      return createLinkMetric(aNode, zNode, "FW Uptime",
                              "Mgmt Tx Keepalive Count", "mgmtTx.keepAlive");
    } else if (aNode.node_type == query::NodeType::DN &&
               zNode.node_type == query::NodeType::CN) {
      return createLinkMetricAsymmetric(
          aNode, zNode, "FW Uptime", "Mgmt Tx ULBWREQ and HB",
          "mgmtTx.heartBeat", "mgmtTx.uplinkBwreq");
    } else if (aNode.node_type == query::NodeType::CN &&
               zNode.node_type == query::NodeType::DN) {
      return createLinkMetricAsymmetric(
          zNode, aNode, "FW Uptime", "Mgmt Tx ULBWREQ and HB",
          "mgmtTx.heartBeat", "mgmtTx.uplinkBwreq");
    } else {
      LOG(ERROR) << "Unhandled node type combination: " << aNode.name << " / "
                 << zNode.name;
    }
  } else if (metricName == "rx_ok") {
    return createLinkMetric(aNode, zNode, "RX Packets", "Received packets",
                            "staPkt.rxOk");
  } else if (metricName == "tx_ok") {
    return createLinkMetric(aNode, zNode, "txOk", "successful MPDUs",
                            "staPkt.txOk");
  } else if (metricName == "tx_fail") {
    return createLinkMetric(aNode, zNode, "txFail", "failed MPDUs",
                            "staPkt.txFail");
  } else if (metricName == "tx_bytes") {
    return createLinkMetric(aNode, zNode, "TX bps", "Transferred bits/second",
                            "tx_bytes", query::KeyUnit::BPS, "link");
  } else if (metricName == "rx_bytes") {
    return createLinkMetric(aNode, zNode, "RX bps", "Received bits/second",
                            "rx_bytes", query::KeyUnit::BPS, "link");
  } else if (metricName == "tx_errors") {
    return createLinkMetric(aNode, zNode, "TX errors", "Transmit errors/second",
                            "tx_errors", query::KeyUnit::NONE, "link");
  } else if (metricName == "rx_errors") {
    return createLinkMetric(aNode, zNode, "RX errors", "Receive errors/second",
                            "rx_errors", query::KeyUnit::NONE, "link");
  } else if (metricName == "tx_dropped") {
    return createLinkMetric(aNode, zNode, "TX dropped",
                            "Transmit dropped/second", "tx_dropped", query::KeyUnit::NONE, "link");
  } else if (metricName == "rx_dropped") {
    return createLinkMetric(aNode, zNode, "RX dropped",
                            "Receive dropped/second", "rx_dropped", query::KeyUnit::NONE, "link");
  } else if (metricName == "tx_pps") {
    return createLinkMetric(aNode, zNode, "TX pps", "Transmit packets/second",
                            "tx_packets", query::KeyUnit::NONE, "link");
  } else if (metricName == "rx_pps") {
    return createLinkMetric(aNode, zNode, "RX pps", "Receive packets/second",
                            "rx_packets", query::KeyUnit::NONE, "link");
  } else if (metricName == "tx_power") {
    return createLinkMetric(aNode, zNode, "TX Power", "Transmit Power",
                            "staPkt.txPowerIndex");
  } else if (metricName == "rx_frame") {
    return createLinkMetric(aNode, zNode, "RX Frame", "RX Frame", "rx_frame", query::KeyUnit::NONE,
                            "link");
  } else if (metricName == "rx_overruns") {
    return createLinkMetric(aNode, zNode, "RX Overruns", "RX Overruns",
                            "rx_overruns", query::KeyUnit::NONE, "link");
  } else if (metricName == "tx_overruns") {
    return createLinkMetric(aNode, zNode, "TX Overruns", "TX Overruns",
                            "tx_overruns", query::KeyUnit::NONE, "link");
  } else if (metricName == "tx_collisions") {
    return createLinkMetric(aNode, zNode, "TX Collisions", "TX Collisions",
                            "tx_collisions", query::KeyUnit::NONE, "link");
  } else if (metricName == "speed") {
    return createLinkMetric(aNode, zNode, "Speed", "Speed (mbps)", "speed",
                            query::KeyUnit::BPS, "link");
  } else if (metricName == "link_status") {
    // TODO - reported by controller (zero-mac)
    return folly::dynamic::object("title", "Link status")(
        "description", "Link status reported by controller")("scale", NULL)(
        "keys",
        folly::dynamic::array(folly::dynamic::object(
            "node", SimpleJSONSerializer::serialize<std::string>(aNode))(
            "keyName", "e2e_controller.link_status.WIRELESS." + aNode.mac_addr +
                           "." + zNode.mac_addr)("titleAppend", " (A)")));
  }
}

// type-ahead search
std::vector<std::vector<query::KeyData>>
StatsTypeAheadCache::searchMetrics(const std::string &metricName, const int limit) {
  std::vector<std::vector<query::KeyData>> retMetrics;
  std::regex metricRegex;
  try {
    metricRegex = std::regex(metricName, std::regex::icase);
  } catch (const std::regex_error& ex) {
    LOG(ERROR) << "Invalid regexp: " << metricName;
    return retMetrics;
  }
  std::set<int> usedShortMetricIds;
  int retMetricId = 0;
  // search short-name metrics
  for (const auto &metric : nameToMetricIds_) {
    std::smatch metricMatch;
    if (std::regex_search(metric.first, metricMatch, metricRegex)) {
      std::vector<query::KeyData> metricKeyList;
      // insert keydata
      for (const auto &keyId : metric.second) {
        auto metricIt = metricIdMetadata_.find(keyId);
        if (metricIt != metricIdMetadata_.end()) {
          auto& kd = metricIt->second;
          metricKeyList.push_back(*metricIt->second);
          usedShortMetricIds.insert(keyId);
        }
        if (retMetrics.size() >= limit) {
          return retMetrics;
        }
      }
      retMetrics.push_back(metricKeyList);
    }
  }
  for (const auto &metric : keyToMetricIds_) {
    std::smatch metricMatch;
    if (std::regex_search(metric.first, metricMatch, metricRegex)) {
      std::vector<query::KeyData> metricKeyList;
      // insert keydata
      for (const auto &keyId : metric.second) {
        auto metricIt = metricIdMetadata_.find(keyId);
        // Skip metric name if used by a short/alias key
        if (metricIt != metricIdMetadata_.end() &&
            !usedShortMetricIds.count(keyId)) {
          metricKeyList.push_back(*metricIt->second);
        }
        if (retMetrics.size() >= limit) {
          return retMetrics;
        }
      }
      if (metricKeyList.size() > 0) {
        retMetrics.push_back(metricKeyList);
      }
    }
  }
  return retMetrics;
}
}
} // facebook::gorilla
