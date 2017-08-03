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

DEFINE_string(mysql_url, "localhost", "mysql host");
DEFINE_string(mysql_user, "root", "mysql user");
DEFINE_string(mysql_pass, "", "mysql passward");
DEFINE_string(mysql_database, "cxl", "mysql database");

namespace facebook {
namespace gorilla {

MySqlClient::MySqlClient() {
  try {
    driver_ = sql::mysql::get_driver_instance();
    connection_ = std::unique_ptr<sql::Connection>(
        driver_->connect(FLAGS_mysql_url, FLAGS_mysql_user, FLAGS_mysql_pass));
    connection_->setSchema(FLAGS_mysql_database);
  } catch (sql::SQLException& e) {
    LOG(ERROR) << "ERR: " << e.what();
    LOG(ERROR) << " (MySQL error code: " << e.getErrorCode();
  }
  refreshNodes();
  refreshStatKeys();
  refreshEventCategories();
}

void MySqlClient::refreshNodes() noexcept {
  try {
    std::unique_ptr<sql::Statement> stmt(connection_->createStatement());
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

void MySqlClient::refreshStatKeys() noexcept {
  try {
    std::unique_ptr<sql::Statement> stmt(connection_->createStatement());
    std::unique_ptr<sql::ResultSet> res(
        stmt->executeQuery("SELECT `id`, `node_id`, `key` FROM `ts_key`"));

    LOG(INFO) << "refreshStatKeys: Number of keys: " << res->rowsCount();
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

void MySqlClient::refreshEventCategories() noexcept {
  try {
    std::unique_ptr<sql::Statement> stmt(connection_->createStatement());
    std::unique_ptr<sql::ResultSet> res(stmt->executeQuery(
        "SELECT `id`, `node_id`, `category` FROM `event_categories`"));

    LOG(INFO) << "refreshEventCategories: Number of categories: "
              << res->rowsCount();
    while (res->next()) {
      int64_t categoryId = res->getInt("id");
      int64_t nodeId = res->getInt("node_id");
      std::string categoryName = res->getString("category");

      std::transform(
          categoryName.begin(),
          categoryName.end(),
          categoryName.begin(),
          ::tolower);
      auto itNode = nodeCategoryIds_.find(nodeId);
      if (nodeCategoryIds_.find(nodeId) == nodeCategoryIds_.end()) {
        nodeCategoryIds_[nodeId] = {};
      }
      nodeCategoryIds_[nodeId][categoryName] = categoryId;
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
    std::unique_ptr<sql::PreparedStatement> prep_stmt(
        connection_->prepareStatement(
            "INSERT IGNORE INTO `nodes` (`mac`, `node`, "
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

void MySqlClient::addStatKeys(
    std::unordered_map<int64_t, std::unordered_set<std::string>>
        nodeKeys) noexcept {
  if (!nodeKeys.size()) {
    return;
  }
  LOG(INFO) << "addStatKeys for " << nodeKeys.size() << " nodes";
  try {
    sql::PreparedStatement* prep_stmt;
    prep_stmt = connection_->prepareStatement(
        "INSERT IGNORE INTO `ts_key` (`node_id`, `key`) VALUES (?, ?)");

    for (const auto& keys : nodeKeys) {
      LOG(INFO) << "addStatKeys => node_id: " << keys.first
                << " Num of keys: " << keys.second.size();
      for (const auto& keyName : keys.second) {
        prep_stmt->setInt(1, keys.first);
        prep_stmt->setString(2, keyName);
        prep_stmt->execute();
      }
    }
  } catch (sql::SQLException& e) {
    LOG(ERROR) << "ERR: " << e.what();
    LOG(ERROR) << " (MySQL error code: " << e.getErrorCode();
  }

  refreshStatKeys();
}

void MySqlClient::addEventCategories(
    std::unordered_map<int64_t, std::unordered_set<std::string>>
        eventCategories) noexcept {
  if (!eventCategories.size()) {
    return;
  }
  LOG(INFO) << "addEventCategories for " << eventCategories.size() << " nodes";
  try {
    sql::PreparedStatement* prep_stmt;
    prep_stmt = connection_->prepareStatement(
        "INSERT IGNORE INTO `event_categories` (`node_id`, `category`) VALUES (?, ?)");

    for (const auto& categories : eventCategories) {
      LOG(INFO) << "addEventCategories => node_id: " << categories.first
                << " Num of categories: " << categories.second.size();
      for (const auto& category : categories.second) {
        prep_stmt->setInt(1, categories.first);
        prep_stmt->setString(2, category);
        prep_stmt->execute();
      }
    }
  } catch (sql::SQLException& e) {
    LOG(ERROR) << "ERR: " << e.what();
    LOG(ERROR) << " (MySQL error code: " << e.getErrorCode();
  }

  refreshEventCategories();
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

folly::Optional<int64_t> MySqlClient::getEventCategoryId(
    const int64_t nodeId,
    const std::string& category) const {
  std::string categoryLower = category;
  std::transform(
      categoryLower.begin(),
      categoryLower.end(),
      categoryLower.begin(),
      ::tolower);

  auto itNode = nodeCategoryIds_.find(nodeId);
  if (itNode != nodeCategoryIds_.end()) {
    auto itCategory = itNode->second.find(categoryLower);
    if (itCategory != itNode->second.end()) {
      return (itCategory->second);
    }
  }
  return folly::none;
}

/*
void MySqlClient::addEvents(
    std::vector<MySqlEventData> events) noexcept {
  if (!events.size()) {
    return;
  }
  try {
    std::unique_ptr<sql::PreparedStatement> prep_stmt(
        connection_->prepareStatement("INSERT INTO `events` (`sample`,
`timestamp`, `category_id`) VALUES (?, ?, ?)"));

    LOG(INFO) << "addEvents: " << events.size();
    for (const auto& event : events) {
      prep_stmt->setString(1, event.sample);
      prep_stmt->setInt(2, event.timestamp);
      prep_stmt->setInt(3, event.category_id);
      prep_stmt->execute();
    }
  } catch (sql::SQLException& e) {
    LOG(ERROR) << "ERR: " << e.what();
    LOG(ERROR) << " (MySQL error code: " << e.getErrorCode();
  }
}
*/

void MySqlClient::addEvents(std::vector<MySqlEventData> events) noexcept {
  if (!events.size()) {
    return;
  }
  try {
    std::string query =
        "INSERT INTO `events` (`sample`, `timestamp`, `category_id`) VALUES ";
    int64_t index = 0;
    for (const auto& event : events) {
      if (index++ == 0) {
        query += "(?, ?, ?)";
      } else {
        query += ",(?, ?, ?)";
      }
    }
    std::unique_ptr<sql::PreparedStatement> prep_stmt(
        connection_->prepareStatement(query));

    LOG(INFO) << "addEvents: " << events.size();
    index = 0;
    for (const auto& event : events) {
      prep_stmt->setString(++index, event.sample);
      prep_stmt->setDateTime(++index, event.timestamp);
      prep_stmt->setInt(++index, event.category_id);
    }
    prep_stmt->execute();
  } catch (sql::SQLException& e) {
    LOG(ERROR) << "ERR: " << e.what();
    LOG(ERROR) << " (MySQL error code: " << e.getErrorCode();
  }
}

void MySqlClient::addAlert(MySqlAlertData alert) noexcept {
  try {
    std::string query =
        "INSERT INTO `alerts` (`node_id`, `timestamp`, `alert_id`, `alert_regex`, `alert_threshold`, `alert_comparator`, `alert_level`, `trigger_key`, `trigger_value`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    std::unique_ptr<sql::PreparedStatement> prep_stmt(
        connection_->prepareStatement(query));

    LOG(INFO) << "addAlert: " << alert.alert_id;
    prep_stmt->setInt(1, alert.node_id);
    prep_stmt->setDateTime(2, alert.timestamp);
    prep_stmt->setString(3, alert.alert_id);
    prep_stmt->setString(4, alert.alert_regex);
    prep_stmt->setDouble(5, alert.alert_threshold);
    prep_stmt->setString(6, alert.alert_comparator);
    prep_stmt->setString(7, alert.alert_level);
    prep_stmt->setString(8, alert.trigger_key);
    prep_stmt->setDouble(9, alert.trigger_value);
    prep_stmt->execute();
  } catch (sql::SQLException& e) {
    LOG(ERROR) << "ERR: " << e.what();
    LOG(ERROR) << " (MySQL error code: " << e.getErrorCode();
  }
}
}
} // facebook::gorilla
