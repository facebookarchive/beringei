/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "QueryServiceFactory.h"

#include "AlertsWriteHandler.h"
#include "EventsWriteHandler.h"
#include "LogsWriteHandler.h"
#include "NotFoundHandler.h"
#include "QueryHandler.h"
#include "StatsWriteHandler.h"

#include "beringei/plugins/BeringeiConfigurationAdapter.h"

using folly::EventBase;
using folly::EventBaseManager;
using folly::SocketAddress;

DEFINE_int32(writer_queue_size, 100000, "Beringei writer queue size");

namespace facebook {
namespace gorilla {

QueryServiceFactory::QueryServiceFactory() : RequestHandlerFactory() {
  configurationAdapter_ = std::make_shared<BeringeiConfigurationAdapter>();
  mySqlClient_ = std::make_shared<MySqlClient>();
  beringeiReadClient_ = std::make_shared<BeringeiClient>(
      configurationAdapter_, 1, BeringeiClient::kNoWriterThreads);
  beringeiWriteClient_ = std::make_shared<BeringeiClient>(
      configurationAdapter_, FLAGS_writer_queue_size, 5);
}

void QueryServiceFactory::onServerStart(folly::EventBase* evb) noexcept {}

void QueryServiceFactory::onServerStop() noexcept {}

proxygen::RequestHandler* QueryServiceFactory::onRequest(
    proxygen::RequestHandler* /* unused */,
    proxygen::HTTPMessage* httpMessage) noexcept {
  auto path = httpMessage->getPath();
  LOG(INFO) << "Received a request for path " << path;

  if (path == "/stats_writer") {
    return new StatsWriteHandler(
        configurationAdapter_, mySqlClient_, beringeiWriteClient_);
  } else if (path == "/query") {
    return new QueryHandler(configurationAdapter_, beringeiReadClient_);
  } else if (path == "/events_writer") {
    return new EventsWriteHandler(mySqlClient_);
  } else if (path == "/alerts_writer") {
    return new AlertsWriteHandler(mySqlClient_);
  } else if (path == "/logs_writer") {
    return new LogsWriteHandler();
  }

  // return not found for all other uris
  return new NotFoundHandler();
}
}
} // facebook::gorilla
