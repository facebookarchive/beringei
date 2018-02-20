/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "MySqlClient.h"

#include <folly/Memory.h>
#include <folly/dynamic.h>
#include <folly/futures/Future.h>
#include <proxygen/httpserver/RequestHandler.h>

#include "beringei/if/gen-cpp2/beringei_query_types_custom_protocol.h"
#include "beringei/if/gen-cpp2/Topology_types_custom_protocol.h"

enum ERROR_CODES { SCAN_RESP_SUCCESS, SCAN_REP_NO_TOPOLOGY, SCAN_REP_NO_NODE,
                   SCAN_REP_UNDEFINED };

namespace facebook {
namespace gorilla {

class ScanRespWriteHandler : public proxygen::RequestHandler {
 public:
  explicit ScanRespWriteHandler(std::shared_ptr<MySqlClient> mySqlClient);

  void
  onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError err) noexcept override;

 private:
  void logRequest(query::ScanRespTop request);

  int writeData(query::ScanRespTop scanRespTop);

  int64_t getTimestamp(int64_t timeInUsec);

  std::shared_ptr<MySqlClient> mySqlClient_;
  std::unique_ptr<folly::IOBuf> body_;
  const std::vector<std::string> errorMessages = {"Success",
                            "MAC addr has no matching topology name",
                            "MAC addr has no matching node",
                            "undefined error in ScanRespWriteHandler"};
  const std::vector<int> errorCodes = {200, 500, 500, 500};
  const int BF_ROLE_INITIATOR = 0;
};
}
} // facebook::gorilla
