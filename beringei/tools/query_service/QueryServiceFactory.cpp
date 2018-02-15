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
#include "TableQueryHandler.h"
#include "StatsWriteHandler.h"
#include "StatsTypeAheadCache.h"
#include "StatsTypeAheadHandler.h"
#include "StatsTypeAheadCacheHandler.h"

#include "beringei/plugins/BeringeiConfigurationAdapter.h"

using folly::EventBase;
using folly::EventBaseManager;
using folly::SocketAddress;

namespace facebook {
namespace gorilla {

QueryServiceFactory::QueryServiceFactory(
  std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
  std::shared_ptr<MySqlClient> mySqlClient,
  std::shared_ptr<TACacheMap> typeaheadCache,
  std::shared_ptr<BeringeiClient> beringeiReadClient,
  std::shared_ptr<BeringeiClient> beringeiWriteClient)
  : RequestHandlerFactory(),
    configurationAdapter_(configurationAdapter),
    mySqlClient_(mySqlClient),
    typeaheadCache_(typeaheadCache),
    beringeiReadClient_(beringeiReadClient),
    beringeiWriteClient_(beringeiWriteClient) {
}

void QueryServiceFactory::onServerStart(folly::EventBase *evb) noexcept {}

void QueryServiceFactory::onServerStop() noexcept {}

proxygen::RequestHandler *
QueryServiceFactory::onRequest(proxygen::RequestHandler * /* unused */,
                               proxygen::HTTPMessage *httpMessage) noexcept {
  auto path = httpMessage->getPath();
  LOG(INFO) << "Received a request for path " << path;

  if (path == "/stats_writer") {
    return new StatsWriteHandler(configurationAdapter_, mySqlClient_,
                                 beringeiWriteClient_);
  } else if (path == "/query") {
    return new QueryHandler(configurationAdapter_, beringeiReadClient_);
  } else if (path == "/table_query") {
    return new TableQueryHandler(configurationAdapter_, beringeiReadClient_, typeaheadCache_);
  } else if (path == "/events_writer") {
    return new EventsWriteHandler(mySqlClient_);
  } else if (path == "/alerts_writer") {
    return new AlertsWriteHandler(mySqlClient_);
  } else if (path == "/logs_writer") {
    return new LogsWriteHandler();
  } else if (path == "/stats_typeahead") {
    // pass a cache client that stores metric names
    return new StatsTypeAheadHandler(mySqlClient_, typeaheadCache_);
  } else if (path == "/stats_typeahead_cache") {
    // accepts a topology object to refresh the cache
    return new StatsTypeAheadCacheHandler(mySqlClient_, typeaheadCache_);
  }

  // return not found for all other uris
  return new NotFoundHandler();
}
}
} // facebook::gorilla
