/**
* Copyright (c) 2016-present, Facebook, Inc.
* All rights reserved.
* This source code is licensed under the BSD-style license found in the
* LICENSE file in the root directory of this source tree. An additional grant
* of patent rights can be found in the PATENTS file in the same directory.
*/

#include "beringei/client/BeringeiClient.h"
#include "beringei/plugins/BeringeiConfigurationAdapter.h"

#include <chrono>
#include <memory>
#include <set>
#include <string>

#include <folly/Conv.h>
#include <folly/init/Init.h>

using namespace facebook;

DECLARE_string(beringei_configuration_path);

int main(int argc, char** argv) {
  gflags::SetUsageMessage("[<options>] <key> <value> [<timestamp>]");
  folly::init(&argc, &argv, true);

  auto beringeiConfig =
      std::make_shared<gorilla::BeringeiConfigurationAdapter>(true);
  auto beringeiClient =
      std::make_shared<gorilla::BeringeiClient>(beringeiConfig, 10, 5, true);

  if (argc < 3) {
    gflags::ShowUsageWithFlagsRestrict(argv[0], "BeringeiPut.cpp");
    return 1;
  }

  int shardCount = beringeiClient->getNumShardsFromWriteClient();
  LOG(INFO) << "Config knows about these write services: ";
  for (const auto& wservice : beringeiConfig->getWriteServices()) {
    LOG(INFO) << "  " << wservice;
  }
  LOG(INFO) << "Beringei has " << shardCount << " shards";

  if (shardCount == 0) {
    LOG(FATAL) << "Shard count can't be zero, though.";
  }

  std::vector<gorilla::DataPoint> dps;
  dps.emplace_back();
  dps.back().key.key = std::string(argv[1]);
  dps.back().key.shardId =
      std::hash<std::string>()(dps.back().key.key) % shardCount;
  dps.back().value.unixTime =
      std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  dps.back().value.value = folly::to<double>(argv[2]);

  LOG(INFO) << "Num pts: " << dps.size();

  if (argc > 3) {
    dps.back().value.unixTime = folly::to<int64_t>(argv[3]);
  }

  auto pushedPoints = beringeiClient->putDataPoints(dps);

  if (!pushedPoints) {
    LOG(ERROR) << "Failed to perform the put!";
  }

  beringeiClient->flushQueue();

  return 0;
}
