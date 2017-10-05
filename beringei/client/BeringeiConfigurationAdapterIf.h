/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <folly/Range.h>
#include <memory>
#include <set>
#include <string>

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

  // Return a shard ID given a key and a total number of shards.
  // This is used internally by Beringei, in particular for sub-sharding the
  // scanShard() thrift call.
  //
  // This may be, but is not required to be, the same algorithm used by
  // clients to assign keys to Beringei shards.
  //
  // If it is the same algorithm, clients should use a seed that is not also
  // used internally by Beringei (such as 0).
  //
  // If it is not the same algorithm, it may still be desirable to retain the
  // same properties. For instance if the data model makes it advantageous to
  // ensure related keys are mapped to the same shard.
  virtual uint64_t getShardForKey(
      folly::StringPiece key,
      uint64_t totalShards,
      uint64_t seed) = 0;

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
