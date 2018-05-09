/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "BeringeiServiceHandler.h"

#include "beringei/lib/GorillaStatsManager.h"
#include "beringei/lib/SimpleMemoryUsageGuard.h"
#include "beringei/plugins/BeringeiConfigurationAdapter.h"

#include <folly/init/Init.h>
#include <thrift/lib/cpp/concurrency/Mutex.h>
#include <thrift/lib/cpp/concurrency/PosixThreadFactory.h>
#include <thrift/lib/cpp/concurrency/Thread.h>
#include <thrift/lib/cpp/transport/THeader.h>
#include <thrift/lib/cpp2/async/AsyncProcessor.h>
#include <thrift/lib/cpp2/server/BaseThriftServer.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <thrift/lib/cpp2/util/ScopedServerThread.h>

#include <chrono>

DEFINE_int32(port, 9999, "Port for the server thrift service");

DEFINE_int32(
    num_thrift_worker_threads,
    32,
    "Number of thrift worker threads. Recommended value is # of cores");
DEFINE_int32(
    num_thrift_pool_threads,
    64,
    "Number of thrift pool threads. Recommended value is 2x # of cores");
DEFINE_int64(
    task_expire_time_ms,
    10000, // 10 second default
    "Task expire time");

DEFINE_string(
    service_name,
    "beringei",
    "The name this service will be run under");

using namespace facebook;
using namespace facebook::gorilla;

BeringeiServiceHandler* handlerPtr;
std::shared_ptr<apache::thrift::ThriftServer> server;

int main(int argc, char** argv) {
  folly::init(&argc, &argv, true);

  // Don't actually do anything with the stats.
  GorillaStatsManager::initialize(fLS::FLAGS_service_name, nullptr);

  server = std::make_shared<apache::thrift::ThriftServer>();

  std::unique_ptr<BeringeiServiceHandler> handler(new BeringeiServiceHandler(
      std::make_shared<BeringeiConfigurationAdapter>(),
      std::make_shared<SimpleMemoryUsageGuard>(),
      fLS::FLAGS_service_name,
      fLI::FLAGS_port));
  handlerPtr = handler.get();

  // spin up the thrift
  server->setPort(FLAGS_port);
  server->setInterface(std::move(handler));
  server->setNWorkerThreads(fLI::FLAGS_num_thrift_worker_threads);
  server->setNPoolThreads(fLI::FLAGS_num_thrift_pool_threads);
  server->setTaskExpireTime(
      std::chrono::milliseconds(fLI64::FLAGS_task_expire_time_ms));
  server->setStopWorkersOnStopListening(false);
  LOG(INFO) << fLS::FLAGS_service_name
            << " brought up on port: " << fLI::FLAGS_port << ", with "
            << fLI::FLAGS_num_thrift_worker_threads
            << " thrift worker threads, " << fLI::FLAGS_num_thrift_pool_threads
            << " thrift pool threads and a task expire time of "
            << fLI64::FLAGS_task_expire_time_ms << "ms.";
  server->serve();

  return 0;
}
