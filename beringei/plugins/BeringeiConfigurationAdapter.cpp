/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "BeringeiConfigurationAdapter.h"

#include <folly/String.h>

#include "beringei/lib/CaseUtils.h"

DEFINE_string(
    beringei_configuration_path,
    "",
    "Path to the beringei configuration file");

DEFINE_string(
    beringei_services,
    "beringei",
    "Name of the beringei services enabled for reads");

DEFINE_string(
    beringei_write_services,
    "beringei",
    "Name of the beringei services enabled for writes");

DEFINE_string(
    beringei_shadow_write_services,
    "",
    "Name of the shadow beringei service. "
    "Only shadow shards will be sent to it");

namespace facebook {
namespace gorilla {

const uint32_t BeringeiConfigurationAdapter::kConfigurationUpdateIntervalSecs =
    60;

std::vector<std::string> parseServices(const std::string& servicesString) {
  std::vector<std::string> parts;
  std::vector<std::string> result;
  folly::split(',', servicesString, parts);
  for (auto part : parts) {
    part = folly::trimWhitespace(part).str();
    if (!part.empty()) {
      result.push_back(part);
    }
  }
  return result;
}

BeringeiConfigurationAdapter::BeringeiConfigurationAdapter(
    bool autoRefresh,
    std::string configurationFilePath) {
  if (configurationFilePath.empty()) {
    configurationFilePath = FLAGS_beringei_configuration_path;
  }

  if (configurationFilePath.empty()) {
    LOG(FATAL) << "Beringei configuration path empty. Please specify "
               << "configuration path with --beringei_configuration_path.";
  }

  auto configuration = loader_.loadFromJsonFile(configurationFilePath);
  if (!loader_.isValidConfiguration(configuration)) {
    throw std::runtime_error("Invalid Beringei Configuration");
  }

  configuration_ = loader_.getInternalConfiguration(configuration);

  if (autoRefresh) {
    configurationRefresher_.addFunction(
        std::bind(&BeringeiConfigurationAdapter::refreshConfiguration, this),
        std::chrono::seconds(kConfigurationUpdateIntervalSecs),
        "refreshConfiguration");
    configurationRefresher_.start();
  }

  writeServices_ = parseServices(FLAGS_beringei_write_services);
  readServices_ = parseServices(FLAGS_beringei_services);
  shadowServices_ = parseServices(FLAGS_beringei_shadow_write_services);
};

int BeringeiConfigurationAdapter::getShardCount(
    const std::string& serviceName) {
  int shardCount = 0;
  SYNCHRONIZED(configuration_) {
    auto serviceIterator = configuration_.serviceMap.find(serviceName);

    if (serviceIterator == configuration_.serviceMap.end()) {
      return 0;
    }

    shardCount = configuration_.shardCount;
  }

  return shardCount;
}

bool BeringeiConfigurationAdapter::getHostForShardId(
    int shardId,
    const std::string& serviceName,
    std::pair<std::string, int>& hostInfo) {
  SYNCHRONIZED(configuration_) {
    auto serviceIterator = configuration_.serviceMap.find(serviceName);

    if (serviceIterator == configuration_.serviceMap.end()) {
      return false;
    }

    auto& service = serviceIterator->second;

    if (shardId >= service.shardMap.size() || shardId < 0) {
      return false;
    }
    const auto& shardHostInfo = service.shardMap.at(shardId);

    hostInfo.first = shardHostInfo.hostAddress;
    hostInfo.second = shardHostInfo.port;
    return true;
  }

  return false;
}

void BeringeiConfigurationAdapter::getShardsForHost(
    const std::pair<std::string, int>& hostInfo,
    const std::string& serviceName,
    std::set<int64_t>& shardList) {
  shardList.clear();
  SYNCHRONIZED(configuration_) {
    auto serviceIterator = configuration_.serviceMap.find(serviceName);
    if (serviceIterator == configuration_.serviceMap.end()) {
      return;
    }
    auto& service = serviceIterator->second;

    std::string compactHostInfo =
        hostInfo.first + ":" + folly::to<std::string>(hostInfo.second);
    if (service.shardsPerHostMap.find(compactHostInfo) ==
        service.shardsPerHostMap.end()) {
      return;
    }

    shardList = service.shardsPerHostMap[compactHostInfo];
  }
  return;
}

uint64_t BeringeiConfigurationAdapter::getShardForKey(
    folly::StringPiece key,
    uint64_t totalShards,
    uint64_t seed) {
  return CaseHash::hash(key, seed) % totalShards;
}

std::string BeringeiConfigurationAdapter::getNearestReadService() {
  // returns the first service in the list as the nearest
  SYNCHRONIZED(configuration_) {
    return configuration_.serviceMap.begin()->first;
  }

  return "";
}

std::vector<std::string> BeringeiConfigurationAdapter::getReadServices() {
  std::vector<std::string> readServices;

  SYNCHRONIZED(configuration_) {
    for (auto iterator = configuration_.serviceMap.begin();
         iterator != configuration_.serviceMap.end();
         iterator++) {
      readServices.push_back(iterator->first);
    }
  }
  return readServices;
}

std::vector<std::string> BeringeiConfigurationAdapter::getWriteServices() {
  return writeServices_;
}

std::vector<std::string> BeringeiConfigurationAdapter::getShadowServices() {
  return shadowServices_;
}

bool BeringeiConfigurationAdapter::isLoggingNewKeysEnabled(
    const std::string& serviceName) {
  SYNCHRONIZED(configuration_) {
    auto serviceIterator = configuration_.serviceMap.find(serviceName);

    if (serviceIterator != configuration_.serviceMap.end()) {
      return serviceIterator->second.isLoggingNewKeysEnabled;
    }
  }

  return false;
}

// if there is an error, continue running with the stale configuration
void BeringeiConfigurationAdapter::refreshConfiguration() {
  if (FLAGS_beringei_configuration_path.empty()) {
    LOG(ERROR) << "Cannot refresh BeringeiConfiguration without "
               << "a path to the configuration file";
    return;
  }

  ConfigurationInfo configuration;
  try {
    configuration = loader_.loadFromJsonFile(FLAGS_beringei_configuration_path);
  } catch (std::exception& e) {
    LOG(ERROR)
        << "Encountered exception when refreshing Beringei Configuration: "
        << e.what();
    return;
  }

  if (!loader_.isValidConfiguration(configuration)) {
    LOG(ERROR) << "Failed to refresh Beringei Configuration "
               << "- New configuration is invalid";
    return;
  }

  SYNCHRONIZED(configuration_) {
    configuration_ = loader_.getInternalConfiguration(configuration);
  }
}

bool isValidService(
    const std::string& serviceName,
    const std::vector<std::string>& services) {
  for (auto service : services) {
    if (service == serviceName) {
      return true;
    }
  }

  return false;
}

bool BeringeiConfigurationAdapter::isValidReadService(
    const std::string& serviceName) {
  return isValidService(serviceName, readServices_);
}
}
} // facebook::gorilla
