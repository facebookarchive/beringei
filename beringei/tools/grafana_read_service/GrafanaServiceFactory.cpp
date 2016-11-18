/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "GrafanaServiceFactory.h"

#include "NotFoundHandler.h"
#include "QueryHandler.h"
#include "TestConnectionHandler.h"

#include "beringei/plugins/BeringeiConfigurationAdapter.h"

using folly::EventBase;
using folly::EventBaseManager;
using folly::SocketAddress;

namespace facebook {
namespace gorilla {

GrafanaServiceFactory::GrafanaServiceFactory() : RequestHandlerFactory() {
  configurationAdapter_ = std::make_shared<BeringeiConfigurationAdapter>();
}

void GrafanaServiceFactory::onServerStart(
    folly::EventBase* /* unused */) noexcept {}

void GrafanaServiceFactory::onServerStop() noexcept {}

proxygen::RequestHandler* GrafanaServiceFactory::onRequest(
    proxygen::RequestHandler* /* unused */,
    proxygen::HTTPMessage* httpMessage) noexcept {
  auto path = httpMessage->getPath();
  LOG(INFO) << "Received a request for path " << path;

  // current we only support 2/4 apis required by grafana
  if (path == "/") {
    // grafana request that is trying to test connection
    return new TestConnectionHandler();
  } else if (path == "/query") {
    // grafana request querying for data
    return new QueryHandler(configurationAdapter_);
  }

  // return not found for all other uris
  return new NotFoundHandler();
}
}
} // facebook::gorilla
