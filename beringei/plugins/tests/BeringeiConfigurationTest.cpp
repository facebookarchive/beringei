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

class BeringeiConfigurationTest : public testing::Test {};

TEST_F(BeringeiConfigurationTest, CanLoadSimpleConfiguration) {
  auto filePath = "beringei/plugins/tests/testfiles/BeringeiConfig1.json";
  BeringeiConfigurationLoader loader;
  auto configurationInfo = loader.loadFromJsonFile(filePath);
  ASSERT_EQ(4, configurationInfo.shardCount);
  ASSERT_EQ(1, configurationInfo.serviceMap.size());
  EXPECT_EQ("beringei-west", configurationInfo.serviceMap[0].serviceName);
  EXPECT_EQ("west", configurationInfo.serviceMap[0].location);
  EXPECT_TRUE(configurationInfo.serviceMap[0].isLoggingNewKeysEnabled);
  ASSERT_EQ(4, configurationInfo.serviceMap[0].shardMap.size());
  EXPECT_EQ(0, configurationInfo.serviceMap[0].shardMap[0].shardId);
  EXPECT_EQ(
      "beringei-host-1",
      configurationInfo.serviceMap[0].shardMap[0].hostAddress);
  EXPECT_EQ(9999, configurationInfo.serviceMap[0].shardMap[0].port);
  EXPECT_TRUE(loader.isValidConfiguration(configurationInfo));
}
