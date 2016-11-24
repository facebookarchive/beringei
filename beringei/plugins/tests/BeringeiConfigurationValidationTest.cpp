/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <folly/Format.h>
#include "beringei/if/gen-cpp2/beringei_data_types.h"
#include "beringei/plugins/BeringeiConfigurationLoader.h"

using namespace ::testing;
using namespace facebook::gorilla;
using namespace std;

class BeringeiConfigurationValidationTest : public testing::Test {
 public:
  BeringeiConfigurationValidationTest() {
    auto filePath = "beringei/plugins/tests/testfiles/BeringeiConfig2.json";
    configurationBase_ = loader_.loadFromJsonFile(filePath);
  }

  ConfigurationInfo configurationBase_;
  BeringeiConfigurationLoader loader_;
};

TEST_F(BeringeiConfigurationValidationTest, ZeroShardCount) {
  auto configuration = configurationBase_;
  configuration.shardCount = 0;
  EXPECT_FALSE(loader_.isValidConfiguration(configuration));
}

TEST_F(BeringeiConfigurationValidationTest, EmptyServiceMap) {
  auto configuration = configurationBase_;
  configuration.serviceMap.clear();
  EXPECT_FALSE(loader_.isValidConfiguration(configuration));
}

TEST_F(BeringeiConfigurationValidationTest, EmptyServiceName) {
  auto configuration = configurationBase_;
  configuration.serviceMap[0].serviceName = "";
  EXPECT_FALSE(loader_.isValidConfiguration(configuration));
}

TEST_F(BeringeiConfigurationValidationTest, ConflictingShardInfo) {
  auto configuration = configurationBase_;
  configuration.serviceMap[0].shardMap[0].shardId = 1;
  EXPECT_FALSE(loader_.isValidConfiguration(configuration));
}

TEST_F(BeringeiConfigurationValidationTest, InvalidShardNumber) {
  auto configuration = configurationBase_;
  configuration.serviceMap[0].shardMap[0].shardId = 5;
  EXPECT_FALSE(loader_.isValidConfiguration(configuration));
}

TEST_F(BeringeiConfigurationValidationTest, EmptyHostAddress) {
  auto configuration = configurationBase_;
  configuration.serviceMap[0].shardMap[1].hostAddress = "";
  EXPECT_FALSE(loader_.isValidConfiguration(configuration));
}

TEST_F(BeringeiConfigurationValidationTest, NegativePort) {
  auto configuration = configurationBase_;
  configuration.serviceMap[0].shardMap[1].port = -3;
  EXPECT_FALSE(loader_.isValidConfiguration(configuration));
}

// Valid Configuration Tests
TEST_F(BeringeiConfigurationValidationTest, LoadValidConfig) {
  EXPECT_TRUE(loader_.isValidConfiguration(configurationBase_));
}

TEST_F(BeringeiConfigurationValidationTest, IncompleteShardInfo) {
  auto configuration = configurationBase_;
  configuration.serviceMap[0].shardMap.pop_back();
  EXPECT_TRUE(loader_.isValidConfiguration(configuration));
}

TEST_F(BeringeiConfigurationValidationTest, EmptyShardMap) {
  auto configuration = configurationBase_;
  configuration.serviceMap[0].shardMap.clear();
  EXPECT_TRUE(loader_.isValidConfiguration(configuration));
}
