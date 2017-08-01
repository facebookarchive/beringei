/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <folly/Memory.h>
#include <folly/dynamic.h>
#include <folly/futures/Future.h>

#include "beringei/if/gen-cpp2/beringei_query_types_custom_protocol.h"
#include "mysql_connection.h"
#include "mysql_driver.h"

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>

namespace facebook {
namespace gorilla {

typedef std::unordered_map<std::string, MySqlNodeData> MacToNodeMap;
typedef std::unordered_map<int64_t, std::unordered_map<std::string, int64_t>>
    NodeKeyMap;

class MySqlClient {
 public:
  MySqlClient();
  void addNodes(
      std::unordered_map<std::string, MySqlNodeData> newNodes) noexcept;

  void refreshNodes() noexcept;

  void updateNodeKeys(
      std::unordered_map<int64_t, std::unordered_set<std::string>>
          nodeKeys) noexcept;

  void refreshNodeKeys() noexcept;

  folly::Optional<int64_t> getNodeId(const std::string& macAddr) const;
  folly::Optional<int64_t> getKeyId(
      const int64_t nodeId,
      const std::string& keyName) const;

 private:
  sql::Driver* driver_;
  std::unique_ptr<sql::Connection> connection_;
  MacToNodeMap macAddrToNode_{};
  NodeKeyMap nodeKeyIds_{};
};
}
} // facebook::gorilla
