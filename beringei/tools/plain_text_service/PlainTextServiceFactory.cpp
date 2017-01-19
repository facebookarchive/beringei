/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "PlainTextServiceFactory.h"

#include "FetchHandler.h"
#include "NotFoundHandler.h"
#include "TestConnectionHandler.h"
#include "UpdateHandler.h"

#include "beringei/plugins/BeringeiConfigurationAdapter.h"

using folly::EventBase;
using folly::EventBaseManager;
using folly::SocketAddress;

namespace facebook {
namespace gorilla {

PlainTextServiceFactory::PlainTextServiceFactory() : RequestHandlerFactory() {
  configurationAdapter_ = std::make_shared<BeringeiConfigurationAdapter>();
}

void PlainTextServiceFactory::onServerStart(
    folly::EventBase* /* unused */) noexcept {}

void PlainTextServiceFactory::onServerStop() noexcept {}

proxygen::RequestHandler* PlainTextServiceFactory::onRequest(
    proxygen::RequestHandler* /* unused */,
    proxygen::HTTPMessage* httpMessage) noexcept {
  auto path = httpMessage->getPath();
  LOG(INFO) << "Received a request for path " << path;

  if (path == "/") {
    return new TestConnectionHandler();
  } else if (path == "/fetch") {
    return new FetchHandler(configurationAdapter_);
  } else if (path == "/update") {
    return new UpdateHandler(configurationAdapter_);
  }

  // Return not found for all other uris.
  return new NotFoundHandler();
}
}
} // facebook::gorilla
