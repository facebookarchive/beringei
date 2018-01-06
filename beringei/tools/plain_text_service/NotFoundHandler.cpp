/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "NotFoundHandler.h"

#include <proxygen/httpserver/ResponseBuilder.h>

using namespace proxygen;

namespace facebook {
namespace gorilla {

void NotFoundHandler::onRequest(
    std::unique_ptr<HTTPMessage> /* unused */) noexcept {
  // Nothing to do.
}

void NotFoundHandler::onBody(
    std::unique_ptr<folly::IOBuf> /* unused */) noexcept {
  // Nothing to do.
}

void NotFoundHandler::onEOM() noexcept {
  ResponseBuilder(downstream_).status(404, "NOT FOUND").sendWithEOM();
}

void NotFoundHandler::onUpgrade(UpgradeProtocol /* unused */) noexcept {
  // Handler doesn't support upgrades.
}

void NotFoundHandler::requestComplete() noexcept {
  // In PlainTextServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}

void NotFoundHandler::onError(ProxygenError /* unused */) noexcept {
  // In PlainTextServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}
}
} // facebook::gorilla
