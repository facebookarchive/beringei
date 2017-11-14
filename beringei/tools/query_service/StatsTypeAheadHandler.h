/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "boost/variant.hpp"
#include "MySqlClient.h"

#include <folly/Memory.h>
#include <folly/dynamic.h>
#include <folly/futures/Future.h>
#include <proxygen/httpserver/RequestHandler.h>

#include "beringei/client/BeringeiClient.h"
#include "beringei/client/BeringeiConfigurationAdapterIf.h"
#include "beringei/if/gen-cpp2/beringei_query_types_custom_protocol.h"

namespace facebook {
namespace gorilla {

class StatsTypeAheadHandler : public proxygen::RequestHandler {
 public:
  explicit StatsTypeAheadHandler(
      std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
      std::shared_ptr<MySqlClient> mySqlClient,
      std::shared_ptr<BeringeiClient> beringeiClient);

  void onRequest(
      std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void fetchMetricNames(query::Topology& request);

  folly::dynamic createLinkMetric(query::MySqlNodeData& aNode, query::MySqlNodeData& zNode,
			     	std::string title, std::string description,
				std::string keyName, std::string keyPrefix = "tgf");

  folly::dynamic getLinkMetrics(std::string& metricName, query::MySqlNodeData& aNode, query::MySqlNodeData& zNode);

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError err) noexcept override;

 private:
  std::vector<std::string> metricKeyNames_;
  std::vector<std::string> macNodes_{};
  folly::dynamic nodeMetrics_;
  folly::dynamic siteMetrics_;
  std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter_;
  std::shared_ptr<MySqlClient> mySqlClient_;
  std::shared_ptr<BeringeiClient> beringeiClient_;
  std::unique_ptr<folly::IOBuf> body_;
};
}
} // facebook::gorilla
