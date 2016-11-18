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

#include <set>

#include "beringei/plugins/BeringeiConfigurationAdapter.h"

using namespace ::testing;
using namespace facebook::gorilla;

class BeringeiConfigurationAdapterTest : public testing::Test {
 public:
  const std::string configFile1_ =
      "beringei/plugins/tests/testfiles/BeringeiConfig1.json";

  const std::string configFile2_ =
      "beringei/plugins/tests/testfiles/BeringeiConfig2.json";

  BeringeiConfigurationAdapterTest()
      : configurationAdapter1_(false, configFile1_),
        configurationAdapter2_(false, configFile2_) {
    westServiceName_ = "beringei-west";
    invalidServiceName_ = "beringei-does-not-exist";
  }

  BeringeiConfigurationAdapter configurationAdapter1_;
  BeringeiConfigurationAdapter configurationAdapter2_;
  std::string westServiceName_;
  std::string invalidServiceName_;
};

TEST_F(BeringeiConfigurationAdapterTest, GetShardCountTest) {
  EXPECT_EQ(4, configurationAdapter1_.getShardCount(westServiceName_));
  EXPECT_EQ(3, configurationAdapter2_.getShardCount(westServiceName_));
}

TEST_F(BeringeiConfigurationAdapterTest, GetShardsForNonExistentHostTest) {
  auto hostInfo = std::make_pair(std::string("beringei-no-host-1"), 9999);
  std::set<int64_t> shardsRead;

  configurationAdapter1_.getShardsForHost(
      hostInfo, westServiceName_, shardsRead);
  EXPECT_EQ(0, shardsRead.size());
}

TEST_F(BeringeiConfigurationAdapterTest, GetShardsForHostConfig1Test) {
  std::set<int64_t> configShards1 = {0, 1, 2, 3};
  auto hostInfo = std::make_pair(std::string("beringei-host-1"), 9999);
  std::set<int64_t> shardsRead;

  configurationAdapter1_.getShardsForHost(
      hostInfo, westServiceName_, shardsRead);
  EXPECT_EQ(configShards1.size(), shardsRead.size());
  EXPECT_TRUE(shardsRead == configShards1);
}

TEST_F(BeringeiConfigurationAdapterTest, GetShardsForHostConfig2Test) {
  auto hostInfo1 = std::make_pair(std::string("beringei-host-1"), 9999);
  auto hostInfo2 = std::make_pair(std::string("beringei-host-2"), 9999);
  auto hostInfo3 = std::make_pair(std::string("beringei-host-3"), 9999);
  auto noHost = std::make_pair(std::string("not-an-existing-host"), 9999);
  auto noPort = std::make_pair(std::string("beringei-host-1"), 8765);

  std::set<int64_t> shardsRead;

  configurationAdapter2_.getShardsForHost(
      hostInfo1, westServiceName_, shardsRead);
  EXPECT_EQ(shardsRead.size(), 1);
  EXPECT_EQ(shardsRead.count(0), 1);

  configurationAdapter2_.getShardsForHost(
      hostInfo2, westServiceName_, shardsRead);
  EXPECT_EQ(shardsRead.size(), 1);
  EXPECT_EQ(shardsRead.count(1), 1);

  configurationAdapter2_.getShardsForHost(
      hostInfo3, westServiceName_, shardsRead);
  EXPECT_EQ(shardsRead.size(), 1);
  EXPECT_EQ(shardsRead.count(2), 1);

  configurationAdapter2_.getShardsForHost(noHost, westServiceName_, shardsRead);

  EXPECT_EQ(shardsRead.size(), 0);

  configurationAdapter2_.getShardsForHost(noPort, westServiceName_, shardsRead);

  EXPECT_EQ(shardsRead.size(), 0);
}

TEST_F(BeringeiConfigurationAdapterTest, GetShardCountTestForInvalidService) {
  EXPECT_EQ(0, configurationAdapter1_.getShardCount(invalidServiceName_));
  EXPECT_EQ(0, configurationAdapter2_.getShardCount(invalidServiceName_));
}

TEST_F(BeringeiConfigurationAdapterTest, GetHostForShardIdTestWithAdapter1) {
  std::pair<std::string, int> hostInfo;
  for (int i = 0; i < 4; i++) {
    EXPECT_TRUE(configurationAdapter1_.getHostForShardId(
        i, westServiceName_, hostInfo));
  }
  EXPECT_EQ("beringei-host-1", hostInfo.first);
  EXPECT_EQ(9999, hostInfo.second);
}

TEST_F(BeringeiConfigurationAdapterTest, GetHostForShardIdTestWithAdapter2) {
  std::pair<std::string, int> hostInfo;
  EXPECT_TRUE(
      configurationAdapter2_.getHostForShardId(0, westServiceName_, hostInfo));
  EXPECT_EQ("beringei-host-1", hostInfo.first);
  EXPECT_EQ(9999, hostInfo.second);

  EXPECT_TRUE(
      configurationAdapter2_.getHostForShardId(1, westServiceName_, hostInfo));
  EXPECT_EQ("beringei-host-2", hostInfo.first);
  EXPECT_EQ(9999, hostInfo.second);

  EXPECT_TRUE(
      configurationAdapter2_.getHostForShardId(2, westServiceName_, hostInfo));
  EXPECT_EQ("beringei-host-3", hostInfo.first);
  EXPECT_EQ(9999, hostInfo.second);
}

TEST_F(BeringeiConfigurationAdapterTest, GetHostForInvalidShardId) {
  std::pair<std::string, int> hostInfo;
  EXPECT_FALSE(
      configurationAdapter1_.getHostForShardId(5, westServiceName_, hostInfo));
  EXPECT_FALSE(configurationAdapter2_.getHostForShardId(
      100, westServiceName_, hostInfo));
}

TEST_F(BeringeiConfigurationAdapterTest, GetHostForInvalidService) {
  std::pair<std::string, int> hostInfo;
  EXPECT_FALSE(configurationAdapter1_.getHostForShardId(
      0, invalidServiceName_, hostInfo));
  EXPECT_FALSE(configurationAdapter2_.getHostForShardId(
      0, invalidServiceName_, hostInfo));
}

TEST_F(BeringeiConfigurationAdapterTest, GetReadServicesTest) {
  std::vector<std::string> expected;
  expected.push_back("beringei-west");
  std::vector<std::string> readServices;

  readServices = configurationAdapter1_.getReadServices();
  EXPECT_EQ(expected, readServices);

  readServices = configurationAdapter2_.getReadServices();
  EXPECT_EQ(expected, readServices);
}

TEST_F(BeringeiConfigurationAdapterTest, GetNearestReadServiceTest) {
  EXPECT_EQ("beringei-west", configurationAdapter1_.getNearestReadService());
  EXPECT_EQ("beringei-west", configurationAdapter2_.getNearestReadService());
}

TEST_F(BeringeiConfigurationAdapterTest, IsLoggingNewKeysEnabledTest) {
  EXPECT_TRUE(configurationAdapter1_.isLoggingNewKeysEnabled("beringei-west"));
  EXPECT_TRUE(configurationAdapter2_.isLoggingNewKeysEnabled("beringei-west"));
  EXPECT_FALSE(
      configurationAdapter1_.isLoggingNewKeysEnabled("invalid-service"));
}
