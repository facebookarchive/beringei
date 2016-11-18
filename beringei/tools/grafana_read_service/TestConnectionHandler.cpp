/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "TestConnectionHandler.h"

#include <proxygen/httpserver/ResponseBuilder.h>

using namespace proxygen;

namespace facebook {
namespace gorilla {

void TestConnectionHandler::onRequest(
    std::unique_ptr<HTTPMessage> /* unused */) noexcept {
  // nothing to do
}

void TestConnectionHandler::onBody(
    std::unique_ptr<folly::IOBuf> /* unused */) noexcept {
  // nothing to do
}

void TestConnectionHandler::onEOM() noexcept {
  // return ok
  ResponseBuilder(downstream_).status(200, "OK").body("").sendWithEOM();
}

void TestConnectionHandler::onUpgrade(UpgradeProtocol /* unused */) noexcept {
  // handler doesn't support upgrades
}

void TestConnectionHandler::requestComplete() noexcept {
  // In GrafanaServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}

void TestConnectionHandler::onError(ProxygenError /* unused */) noexcept {
  // In GrafanaServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}
}
} // facebook::gorilla
