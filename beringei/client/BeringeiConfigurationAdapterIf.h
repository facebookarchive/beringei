/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <memory>
#include <set>
#include <string>

#include <folly/io/async/EventBaseManager.h>

namespace facebook {
namespace gorilla {

class BeringeiConfigurationAdapterIf {
 public:
  virtual ~BeringeiConfigurationAdapterIf() {}

  // Return the total number of shards.
  virtual int getShardCount(const std::string& serviceName) = 0;

  // Return the host that owns the beringei shard in hostInfo
  // Result is returned via hostInfo as a pair of <hostname, port>.
  virtual bool getHostForShardId(
      int shardId,
      const std::string& serviceName,
      std::pair<std::string, int>& hostInfo) = 0;

  // Return the list of shards a particular beringei host owns
  // Result is returned as shardList (empty if none or invalid host)
  virtual void getShardsForHost(
      const std::pair<std::string, int>& hostInfo,
      const std::string& serviceName,
      std::set<int64_t>& shardList) = 0;

  // Return the service name of the nearest beringei service
  // to read from
  virtual std::string getNearestReadService() = 0;

  // Return service names of all beringei that are available to read from.
  virtual std::vector<std::string> getReadServices() = 0;

  // Return service name of all beringei that should be written to.
  virtual std::vector<std::string> getWriteServices() = 0;

  // Return service name of all the shadow beringei service that should be
  // written to.
  virtual std::vector<std::string> getShadowServices() = 0;

  // Return whether new keys that are created by beringei need to be
  // logged or not.
  virtual bool isLoggingNewKeysEnabled(const std::string& serviceName) = 0;

  // Return whether the service name is a valid beringei service. Checks
  // against a static list.
  virtual bool isValidReadService(const std::string& serviceName) = 0;
};
}
} // facebook::gorilla
