/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "BeringeiConfigurationLoader.h"

#include <folly/FileUtil.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

using apache::thrift::SimpleJSONSerializer;
using namespace std;

namespace facebook {
namespace gorilla {

ConfigurationInfo BeringeiConfigurationLoader::loadFromJsonFile(
    const string& fullFile) const {
  string outString;
  folly::readFile(fullFile.c_str(), outString);
  return SimpleJSONSerializer::deserialize<ConfigurationInfo>(outString);
}

bool logInvalidAndReturn(const string& msg) {
  LOG(ERROR) << "BeringeiConfigurationInfo invalid: " << msg;
  return false;
}

bool BeringeiConfigurationLoader::isValidConfiguration(
    const ConfigurationInfo& configuration) const {
  if (configuration.shardCount <= 0) {
    return logInvalidAndReturn("Shard Count must be greater than 0");
  }

  if (!configuration.serviceMap.size()) {
    return logInvalidAndReturn("No Beringei services in configuration");
  }

  for (auto& service : configuration.serviceMap) {
    if (service.serviceName.empty()) {
      return logInvalidAndReturn("ServiceName cannot be empty");
    }

    if (service.location.empty()) {
      return logInvalidAndReturn("Location cannot be empty");
    }

    if (service.shardMap.size() != configuration.shardCount) {
      // not an error. Just warning and continue
      LOG(WARNING) << "Shard Map does not match the shard count."
                   << " Some shards are owned.";
    }

    bool shardList[configuration.shardCount];
    for (auto& shard : shardList) {
      shard = false;
    }

    for (auto& shard : service.shardMap) {
      if (shard.port <= 0) {
        return logInvalidAndReturn("Port # has to be greater than 0");
      }

      if (shard.hostAddress.empty()) {
        return logInvalidAndReturn("HostAddress cannot be empty");
      }

      if (shard.shardId < 0 || shard.shardId >= configuration.shardCount) {
        return logInvalidAndReturn("Invalid Shard Id");
      }

      if (shardList[shard.shardId]) {
        return logInvalidAndReturn(
            folly::format(
                "Shard Map contains conflicting shard information for shard {0}",
                shard.shardId)
                .str());
      }

      shardList[shard.shardId] = true;
    }
  }

  LOG_EVERY_N(INFO, 5000) << "Beringei Configuration is Valid";
  return true;
}

BeringeiInternalConfiguration
BeringeiConfigurationLoader::getInternalConfiguration(
    const ConfigurationInfo& configuration) const {
  BeringeiInternalConfiguration out;

  if (!isValidConfiguration(configuration)) {
    return out;
  }

  out.shardCount = configuration.shardCount;
  for (auto& service : configuration.serviceMap) {
    BeringeiInternalServiceInfo serviceInfo;
    serviceInfo.location = service.location;
    serviceInfo.isLoggingNewKeysEnabled = service.isLoggingNewKeysEnabled;
    serviceInfo.shardMap.resize(service.shardMap.size());

    for (auto& shard : service.shardMap) {
      BeringeiInternalHostInfo hostInfo;
      hostInfo.hostAddress = shard.hostAddress;
      hostInfo.port = shard.port;

      std::string compactHostInfo =
          hostInfo.hostAddress + ":" + folly::to<std::string>(hostInfo.port);

      serviceInfo.shardMap[shard.shardId] = std::move(hostInfo);
      serviceInfo.shardsPerHostMap[compactHostInfo].insert(shard.shardId);
    }

    out.serviceMap[service.serviceName] = std::move(serviceInfo);
  }

  return out;
}
}
}
