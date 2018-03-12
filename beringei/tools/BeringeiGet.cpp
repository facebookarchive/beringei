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
#include <iostream>
#include <memory>
#include <set>
#include <string>

#include <folly/Conv.h>
#include <folly/init/Init.h>

using namespace facebook;

template <class T>
std::chrono::seconds to_epoch(T tp) {
  return std::chrono::duration_cast<std::chrono::seconds>(
      tp.time_since_epoch());
}

DECLARE_string(beringei_configuration_path);
DEFINE_int32(
    shard_id,
    -1,
    "Override the calculated shard_id. -1 to use the calculated shard.");
DEFINE_int64(
    start_time,
    0,
    "Unix timestamp of the start time to query. 0 means 'now'.");
DEFINE_int64(
    end_time,
    0,
    "Unix timestamp of the end time to query. Must be > --start_time.");

int main(int argc, char** argv) {
  gflags::SetUsageMessage("[<options>] <key>");
  folly::init(&argc, &argv, true);

  auto beringeiConfig =
      std::make_shared<gorilla::BeringeiConfigurationAdapter>(true);
  auto beringeiClient =
      std::make_shared<gorilla::BeringeiClient>(beringeiConfig, 1, 0, false);

  if (FLAGS_start_time == 0) {
    FLAGS_start_time =
        to_epoch(std::chrono::system_clock::now() - std::chrono::hours(3))
            .count();
  }
  if (FLAGS_end_time == 0) {
    FLAGS_end_time = to_epoch(std::chrono::system_clock::now()).count();
  }
  LOG(INFO) << "Start time: " << FLAGS_start_time
            << "; End time: " << FLAGS_end_time;

  if (argc < 2) {
    gflags::ShowUsageWithFlagsRestrict(argv[0], "beringei");
    return 1;
  }

  int shardCount = beringeiClient->getMaxNumShards();
  LOG(INFO) << "Config knows about these read services: ";
  for (const auto& rservice : beringeiConfig->getReadServices()) {
    LOG(INFO) << "  " << rservice;
  }
  LOG(INFO) << "Beringei has " << shardCount << " shards.";

  if (shardCount == 0) {
    LOG(FATAL) << "Shard count can't be zero, though.";
  }

  LOG(INFO) << "Now: " << to_epoch(std::chrono::system_clock::now()).count();

  std::string keyName = std::string(argv[1]);

  if (FLAGS_shard_id == -1) {
    FLAGS_shard_id = std::hash<std::string>()(keyName) % shardCount;
  }

  LOG(INFO) << "Key is in shard_id: " << FLAGS_shard_id;

  gorilla::ScanShardRequest shardRequest;

  shardRequest.shardId = FLAGS_shard_id;
  shardRequest.begin = FLAGS_start_time - 1201;
  shardRequest.end = FLAGS_end_time - 599;
  shardRequest.subshard = 0;
  shardRequest.numSubshards = 1;

  gorilla::ScanShardResult shardResult;
  beringeiClient->scanShard(shardRequest, shardResult);

  LOG(INFO) << "Get whole shard stats:";
  LOG(INFO) << "Request status: " << (int)shardResult.status;
  LOG(INFO) << "Keys: " << folly::join(", ", shardResult.keys);

  gorilla::GetDataRequest request;
  request.keys.emplace_back();
  request.keys.back().key = keyName;
  request.keys.back().shardId = FLAGS_shard_id;
  request.begin = FLAGS_start_time;
  request.end = FLAGS_end_time;

  gorilla::GorillaResultVector result;
  beringeiClient->get(request, result);

  for (const auto& keyData : result) {
    const auto& keyName = keyData.first.key;
    for (const auto& timeValue : keyData.second) {
      std::cout << keyName << "\t" << timeValue.value << "\t"
                << timeValue.unixTime << std::endl;
    }
  }
  return 0;
}
