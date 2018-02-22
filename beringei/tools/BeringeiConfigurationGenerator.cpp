/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <folly/FileUtil.h>
#include <folly/init/Init.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <iostream>

#include "beringei/if/gen-cpp2/beringei_data_types_custom_protocol.h"

using apache::thrift::SimpleJSONSerializer;
using namespace facebook::gorilla;
using namespace std;

DEFINE_string(host_names, "", "Host names separated by commas");

DEFINE_int32(port, 9999, "Port");

DEFINE_string(service_name, "beringei", "Service name for beringei");

DEFINE_int32(num_shards, 100, "Total number of shards to use");

DEFINE_string(file_path, "", "Location to save the configuration");

std::vector<string> parseHostNames(string hostNames) {
  std::vector<std::string> parts;
  folly::split(',', hostNames, parts);
  return parts;
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv, true);

  auto hostNames = parseHostNames(fLS::FLAGS_host_names);
  if (fLS::FLAGS_host_names.empty() || hostNames.size() == 0) {
    std::cout << "--host_names is empty" << std::endl;
    return 1;
  }

  if (fLS::FLAGS_file_path.empty()) {
    std::cout << "--file_path is empty" << std::endl;
    return 1;
  }

  ConfigurationInfo configuration;
  configuration.shardCount = fLI::FLAGS_num_shards;

  ServiceMap serviceMap;
  serviceMap.serviceName = fLS::FLAGS_service_name;
  serviceMap.location = "facebook";

  for (int i = 0; i < fLI::FLAGS_num_shards; i++) {
    ShardInfo shardInfo;
    shardInfo.shardId = i;
    shardInfo.hostAddress = hostNames[i % hostNames.size()];
    shardInfo.port = fLI::FLAGS_port;

    serviceMap.shardMap.push_back(shardInfo);
  }

  configuration.serviceMap.push_back(serviceMap);

  auto filePath = fLS::FLAGS_file_path;
  if (!folly::writeFile(
          folly::toPrettyJson(folly::parseJson(
              SimpleJSONSerializer::serialize<string>(configuration))),
          filePath.c_str())) {
    std::cout << "ERROR: failed to write data to file " << filePath
              << std::endl;
    return 1;
  }

  std::cout << "Configuration written to file :" << filePath << std::endl;
  return 0;
}
