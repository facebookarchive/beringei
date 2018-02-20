/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "ScanRespWriteHandler.h"
#include "mysql_connection.h"
#include "mysql_driver.h"

#include <algorithm>
#include <ctime>
#include <utility>

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <folly/DynamicConverter.h>
#include <folly/io/IOBuf.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <thrift/lib/cpp/util/ThriftSerializer.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

using apache::thrift::SimpleJSONSerializer;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::microseconds;
using std::chrono::system_clock;
using namespace proxygen;

namespace facebook {
namespace gorilla {

ScanRespWriteHandler::ScanRespWriteHandler(std::shared_ptr<MySqlClient> mySqlClient)
    : RequestHandler(), mySqlClient_(mySqlClient) {}

void ScanRespWriteHandler::onRequest(
    std::unique_ptr<HTTPMessage> /* unused */) noexcept {
  // nothing to do
}

void ScanRespWriteHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {
  if (body_) {
    body_->prependChain(move(body));
  } else {
    body_ = move(body);
  }
}

// each row in the mySQL table corresponds to one tx and one rx node
// if a scan has multiple responders, they are each a separate row in the table
// all having the same token and start_bwgd
int ScanRespWriteHandler::writeData(query::ScanRespTop scanRespTop) {
  query::MySqlScanResp mySqlScanResponse;

  try {
    LOG(INFO) << "ScanRespWriteHandler::writeData:" << folly::toPrettyJson(folly::parseJson(SimpleJSONSerializer::serialize<std::string>(scanRespTop)));
    // LOG(INFO) << "ScanRespWriteHandler::writeData (without all of the folly):" << SimpleJSONSerializer::serialize<std::string>(scanRespTop);

    auto it = scanRespTop.responses.begin();
    auto topologyName = mySqlClient_->getTopologyName(it->second.macAddr);
    auto nodeId = mySqlClient_->getNodeId(it->second.macAddr);
    if (!topologyName) {
      LOG(INFO) << "Error no topologyName corresponding to MAC: " << it->second.macAddr;
      return SCAN_REP_NO_TOPOLOGY;
    }
    if (!nodeId) {
      LOG(INFO) << "Error no node ID corresponding to MAC: " << it->second.macAddr;
      return SCAN_REP_NO_NODE;
    }

    mySqlScanResponse.network = *topologyName;
    mySqlScanResponse.node_id = *nodeId;
    mySqlScanResponse.token = it->second.token;
    mySqlScanResponse.start_bwgd = scanRespTop.startBwgdIdx;
    mySqlScanResponse.apply_flag = scanRespTop.apply;
    mySqlScanResponse.scan_type = scanRespTop.type;
    mySqlScanResponse.scan_sub_type = scanRespTop.subType;
    mySqlScanResponse.scan_mode = scanRespTop.mode;
    // mySqlScanResponse.json_obj = folly::toPrettyJson(folly::parseJson(SimpleJSONSerializer::serialize<std::string>(scanRespTop.responses)));
    mySqlScanResponse.json_obj = SimpleJSONSerializer::serialize<std::string>(scanRespTop.responses);
    mySqlScanResponse.status = it->second.status;
    mySqlScanResponse.tx_power = it->second.txPwrIndex;
    LOG(INFO) << "DEBUG: mySqlScanResponse.status:" << mySqlScanResponse.status << " it->second.status:" << it->second.status;

    if (it->second.role == BF_ROLE_INITIATOR) {
      mySqlClient_->writeTxScanResponse(mySqlScanResponse);
    }
    else { // RESPONDER
      mySqlClient_->writeRxScanResponse(mySqlScanResponse);
    }
    return SCAN_RESP_SUCCESS;
  }
  catch (const std::exception &ex) {
    LOG(INFO) << "Error writing scan response" << ex.what();
    return SCAN_REP_UNDEFINED;
  }
}

// request is type ScanRespTop (see thrift)
// each request contains a scan response from a single node
void ScanRespWriteHandler::onEOM() noexcept {
  auto body = body_->moveToFbString();
  query::ScanRespTop request;
  try {
    request =
        SimpleJSONSerializer::deserialize<query::ScanRespTop>(body);
  }
  catch (const std::exception & ex) {
    LOG(INFO) << "Error deserializing scan response request "
              << folly::exceptionStr(ex);
    ResponseBuilder(downstream_)
        .status(500, "OK")
        .header("Content-Type", "application/json")
        .body("Failed de-serializing scan response request")
        .sendWithEOM();
    return;
  }
  logRequest(request);
  LOG(INFO) << "Scan response request token " << request.responses.begin()->second.token;
  int errCode = 0;
  try {
    errCode = writeData(request);
  }
  catch (const std::exception &ex) {
    LOG(ERROR) << "Unable to handle scan response request: " << ex.what();
    ResponseBuilder(downstream_)
        .status(500, "OK")
        .header("Content-Type", "application/json")
        .body("Failed handling scan response request")
        .sendWithEOM();
    return;
  }
  ResponseBuilder(downstream_)
      .status(errorCodes[errCode], "OK")
      .header("Content-Type", "application/json")
      .body(errorMessages[errCode])
      .sendWithEOM();
}

void ScanRespWriteHandler::onUpgrade(UpgradeProtocol /* unused */) noexcept {}

void ScanRespWriteHandler::requestComplete() noexcept { delete this; }

void ScanRespWriteHandler::onError(ProxygenError /* unused */) noexcept {
  LOG(ERROR) << "Proxygen reported error";
  // In QueryServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}

void ScanRespWriteHandler::logRequest(query::ScanRespTop request) {}
}
} // facebook::gorilla
