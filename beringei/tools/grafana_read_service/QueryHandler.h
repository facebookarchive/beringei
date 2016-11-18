/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <folly/Memory.h>
#include <folly/dynamic.h>
#include <proxygen/httpserver/RequestHandler.h>

#include "beringei/client/BeringeiClient.h"
#include "beringei/client/BeringeiConfigurationAdapterIf.h"
#include "beringei/if/gen-cpp2/beringei_grafana_types_custom_protocol.h"

namespace facebook {
namespace gorilla {

class QueryHandler : public proxygen::RequestHandler {
 public:
  explicit QueryHandler(
      std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter);
  void onRequest(
      std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError err) noexcept override;

 private:
  void logRequest(QueryRequest request);

  int getShardId(const std::string& key, const int numShards);

  GetDataRequest createBeringeiRequest(
      const QueryRequest& request,
      const int numShards);

  folly::fbstring createJsonResponse(
      const std::vector<QueryTarget>& targets,
      const std::vector<std::pair<Key, std::vector<TimeValuePair>>>&
          beringeiResponse);

  folly::dynamic createResponseObject(
      const std::string& target,
      const std::vector<std::pair<Key, std::vector<TimeValuePair>>>&
          beringeiResponse);

  std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter_;
  std::unique_ptr<folly::IOBuf> body_;
};
}
} // facebook::gorilla
