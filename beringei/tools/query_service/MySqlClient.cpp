/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "MySqlClient.h"

#include <utility>

#include <folly/DynamicConverter.h>
#include <folly/io/IOBuf.h>
#include <thrift/lib/cpp/util/ThriftSerializer.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "mysql_connection.h"
#include "mysql_driver.h"

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>

DEFINE_string(mysql_url, "localhost", "mysql host");
DEFINE_string(mysql_user, "root", "mysql user");
DEFINE_string(mysql_pass, "", "mysql passward");
DEFINE_string(mysql_database, "cxl", "mysql database");

namespace facebook {
namespace gorilla {

MySqlClient::MySqlClient() {
  refreshNodes();
  refreshNodeKeys();
}

void MySqlClient::refreshNodes() noexcept {
  try {
    sql::Driver* driver = sql::mysql::get_driver_instance();
    /* Using the Driver to create a connection */
    std::unique_ptr<sql::Connection> con(
        driver->connect(FLAGS_mysql_url, FLAGS_mysql_user, FLAGS_mysql_pass));
    con->setSchema(FLAGS_mysql_database);

    std::unique_ptr<sql::Statement> stmt(con->createStatement());
    std::unique_ptr<sql::ResultSet> res(
        stmt->executeQuery("SELECT * FROM `nodes`"));

    LOG(INFO) << "refreshNodes: Number of nodes: " << res->rowsCount();
    while (res->next()) {
      MySqlNodeData node{};
      node.id = res->getInt("id");
      node.node = res->getString("node");
      node.mac = res->getString("mac");
      node.network = res->getString("network");
      node.site = res->getString("site");

      std::transform(
          node.mac.begin(), node.mac.end(), node.mac.begin(), ::tolower);
      macAddrToNode_[node.mac] = node;
    }
  } catch (sql::SQLException& e) {
    LOG(ERROR) << "ERR: " << e.what();
    LOG(ERROR) << " (MySQL error code: " << e.getErrorCode();
  }
}

void MySqlClient::refreshNodeKeys() noexcept {
  try {
    sql::Driver* driver = sql::mysql::get_driver_instance();
    /* Using the Driver to create a connection */
    std::unique_ptr<sql::Connection> con(
        driver->connect(FLAGS_mysql_url, FLAGS_mysql_user, FLAGS_mysql_pass));
    con->setSchema(FLAGS_mysql_database);

    std::unique_ptr<sql::Statement> stmt(con->createStatement());
    std::unique_ptr<sql::ResultSet> res(
        stmt->executeQuery("SELECT `id`, `node_id`, `key` FROM `ts_key`"));

    LOG(INFO) << "refreshNodeKeys: Number of keys: " << res->rowsCount();
    while (res->next()) {
      int64_t keyId = res->getInt("id");
      int64_t nodeId = res->getInt("node_id");
      std::string keyName = res->getString("key");

      std::transform(
          keyName.begin(), keyName.end(), keyName.begin(), ::tolower);
      auto itNode = nodeKeyIds_.find(nodeId);
      if (nodeKeyIds_.find(nodeId) == nodeKeyIds_.end()) {
        nodeKeyIds_[nodeId] = {};
      }
      nodeKeyIds_[nodeId][keyName] = keyId;
    }
  } catch (sql::SQLException& e) {
    LOG(ERROR) << "ERR: " << e.what();
    LOG(ERROR) << " (MySQL error code: " << e.getErrorCode();
  }
}

void MySqlClient::addNodes(
    std::unordered_map<std::string, MySqlNodeData> newNodes) noexcept {
  if (!newNodes.size()) {
    return;
  }
  try {
    sql::Driver* driver = sql::mysql::get_driver_instance();
    /* Using the Driver to create a connection */
    std::unique_ptr<sql::Connection> con(
        driver->connect(FLAGS_mysql_url, FLAGS_mysql_user, FLAGS_mysql_pass));
    con->setSchema(FLAGS_mysql_database);

    std::unique_ptr<sql::PreparedStatement> prep_stmt(
        con->prepareStatement("INSERT IGNORE INTO `nodes` (`mac`, `node`, "
                              "`site`, `network`) VALUES (?, ?, ?, ?)"));

    for (const auto& node : newNodes) {
      prep_stmt->setString(1, node.second.mac);
      prep_stmt->setString(2, node.second.node);
      prep_stmt->setString(3, node.second.site);
      prep_stmt->setString(4, node.second.network);
      prep_stmt->execute();
      LOG(INFO) << "addNode => mac: " << node.second.mac
                << " Network: " << node.second.network;
    }
  } catch (sql::SQLException& e) {
    LOG(ERROR) << "ERR: " << e.what();
    LOG(ERROR) << " (MySQL error code: " << e.getErrorCode();
  }

  refreshNodes();
}

void MySqlClient::updateNodeKeys(
    std::unordered_map<int64_t, std::unordered_set<std::string>>
        nodeKeys) noexcept {
  if (!nodeKeys.size()) {
    return;
  }
  LOG(INFO) << "updateNodeKeys for " << nodeKeys.size() << " nodes";
  try {
    sql::Driver* driver = sql::mysql::get_driver_instance();
    /* Using the Driver to create a connection */
    std::unique_ptr<sql::Connection> con(
        driver->connect(FLAGS_mysql_url, FLAGS_mysql_user, FLAGS_mysql_pass));
    con->setSchema(FLAGS_mysql_database);

    sql::PreparedStatement* prep_stmt;
    prep_stmt = con->prepareStatement(
        "INSERT IGNORE INTO `ts_key` (`node_id`, `key`) VALUES (?, ?)");

    for (const auto& keys : nodeKeys) {
      LOG(INFO) << "updateNodeKeys => node_id: " << keys.first
                << " Num of keys: " << keys.second.size();
      for (const auto& nodeKey : keys.second) {
        prep_stmt->setInt(1, keys.first);
        prep_stmt->setString(2, nodeKey);
        prep_stmt->execute();
      }
    }
  } catch (sql::SQLException& e) {
    LOG(ERROR) << "ERR: " << e.what();
    LOG(ERROR) << " (MySQL error code: " << e.getErrorCode();
  }

  refreshNodeKeys();
}

folly::Optional<int64_t> MySqlClient::getNodeId(
    const std::string& macAddr) const {
  std::string macAddrLower = macAddr;
  std::transform(
      macAddrLower.begin(),
      macAddrLower.end(),
      macAddrLower.begin(),
      ::tolower);
  auto it = macAddrToNode_.find(macAddrLower);
  if (it != macAddrToNode_.end()) {
    return (it->second.id);
  }
  return folly::none;
}

folly::Optional<int64_t> MySqlClient::getKeyId(
    const int64_t nodeId,
    const std::string& keyName) const {
  std::string keyNameLower = keyName;
  std::transform(
      keyNameLower.begin(),
      keyNameLower.end(),
      keyNameLower.begin(),
      ::tolower);

  auto itNode = nodeKeyIds_.find(nodeId);
  if (itNode != nodeKeyIds_.end()) {
    auto itKey = itNode->second.find(keyNameLower);
    if (itKey != itNode->second.end()) {
      return (itKey->second);
    }
  }
  return folly::none;
}
}
} // facebook::gorilla
