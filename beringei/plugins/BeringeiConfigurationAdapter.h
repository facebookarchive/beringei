/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "BeringeiConfigurationLoader.h"

#include <set>
#include <string>

#include <folly/io/async/EventBaseManager.h>
#include "beringei/client/BeringeiConfigurationAdapterIf.h"

#include <folly/Synchronized.h>
#include <folly/experimental/FunctionScheduler.h>

namespace facebook {
namespace gorilla {

class BeringeiConfigurationAdapter : public BeringeiConfigurationAdapterIf {
 public:
  explicit BeringeiConfigurationAdapter(
      bool autoRefresh = true,
      // used by tests instead of setting the GFLAG
      std::string configurationFilePath = "");

  int getShardCount(const std::string& serviceName) override;

  bool getHostForShardId(
      int shardId,
      const std::string& serviceName,
      std::pair<std::string, int>& hostInfo) override;

  void getShardsForHost(
      const std::pair<std::string, int>& hostInfo,
      const std::string& serviceName,
      std::set<int64_t>& shardList) override;

  uint64_t getShardForKey(
      folly::StringPiece key,
      uint64_t totalShards,
      uint64_t seed) override;

  std::vector<std::string> getReadServices() override;

  std::vector<std::string> getWriteServices() override;

  std::vector<std::string> getShadowServices() override;

  std::string getNearestReadService() override;

  bool isLoggingNewKeysEnabled(const std::string& serviceName) override;

  bool isValidReadService(const std::string& serviceName) override;

 private:
  void refreshConfiguration();

  BeringeiConfigurationLoader loader_;
  folly::Synchronized<BeringeiInternalConfiguration> configuration_;

  static const uint32_t kConfigurationUpdateIntervalSecs;
  folly::FunctionScheduler configurationRefresher_;

  std::vector<std::string> readServices_;
  std::vector<std::string> writeServices_;
  std::vector<std::string> shadowServices_;
};
}
} // facebook::gorilla
