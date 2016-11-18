/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include "beringei/client/BeringeiConfigurationAdapterIf.h"

#include <folly/Memory.h>
#include <folly/Portability.h>
#include <folly/io/async/EventBaseManager.h>
#include <gflags/gflags.h>
#include <proxygen/httpserver/HTTPServer.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>

namespace facebook {
namespace gorilla {

// Request handler factory that figures out the right handler based on the uri
class GrafanaServiceFactory : public proxygen::RequestHandlerFactory {
 public:
  GrafanaServiceFactory();

  void onServerStart(folly::EventBase* evb) noexcept override;

  void onServerStop() noexcept override;

  proxygen::RequestHandler* onRequest(
      proxygen::RequestHandler*,
      proxygen::HTTPMessage*) noexcept override;

 private:
  std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter_;
};
}
} // facebook::gorilla
