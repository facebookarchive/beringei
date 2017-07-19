/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <map>

namespace facebook {
namespace gorilla {
// This file contains internal structures that are very similar
// to thrift structure but uses maps for easier lookup

class BeringeiInternalHostInfo {
 public:
  std::string hostAddress;

  int port;
};

class BeringeiInternalServiceInfo {
 public:
  std::string location;

  bool isLoggingNewKeysEnabled;

  // map of <shardId, InternalHostInfo>
  std::vector<BeringeiInternalHostInfo> shardMap;

  std::unordered_map<std::string, std::set<int64_t>> shardsPerHostMap;
};

class BeringeiInternalConfiguration {
 public:
  int shardCount;

  // map of <serviceName, internalServiceInfo>
  std::map<std::string, BeringeiInternalServiceInfo> serviceMap;
};
}
}
