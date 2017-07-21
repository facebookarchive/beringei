/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <gmock/gmock.h>

#include <set>

#include "beringei/client/BeringeiConfigurationAdapterIf.h"

namespace facebook {
namespace gorilla {

class MockConfigurationAdapter : public BeringeiConfigurationAdapterIf {
 public:
  MOCK_METHOD1(getShardCount, int(const std::string& serviceName));

  MOCK_METHOD3(
      getHostForShardId,
      bool(int, const std::string&, std::pair<std::string, int>&));

  MOCK_METHOD3(
      getShardsForHost,
      void(
          const std::pair<std::string, int>&,
          const std::string&,
          std::set<int64_t>&));

  MOCK_METHOD3(
      getShardForKey,
      uint64_t(folly::StringPiece, uint64_t, uint64_t));

  MOCK_METHOD0(getRegionString, std::string(void));

  MOCK_METHOD0(getServiceMap, std::map<std::string, std::string>(void));

  MOCK_METHOD0(getReadServices, std::vector<std::string>(void));

  MOCK_METHOD0(getWriteServices, std::vector<std::string>(void));

  MOCK_METHOD0(getShadowServices, std::vector<std::string>(void));

  MOCK_METHOD0(getNearestReadService, std::string(void));

  MOCK_METHOD1(isValidReadService, bool(const std::string&));

  bool isLoggingNewKeysEnabled(const std::string& /*serviceName*/) override {
    return false;
  }
};
}
}
