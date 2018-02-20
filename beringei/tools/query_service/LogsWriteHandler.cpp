/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "LogsWriteHandler.h"

#include <algorithm>
#include <ctime>
#include <errno.h>
#include <fstream>
#include <sys/stat.h>
#include <utility>

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

DEFINE_string(data_folder_path, "/home/nms/data/", "Path to data folder");

namespace facebook {
namespace gorilla {

LogsWriteHandler::LogsWriteHandler() : RequestHandler() {}

void LogsWriteHandler::onRequest(
    std::unique_ptr<HTTPMessage> /* unused */) noexcept {
  // nothing to do
}

void LogsWriteHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {
  if (body_) {
    body_->prependChain(move(body));
  } else {
    body_ = move(body);
  }
}

void LogsWriteHandler::writeData(query::LogsWriteRequest request) {
  auto startTime = (int64_t)duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()).count();
  time_t curr_time;
  tm *curr_tm;
  char date_string[100];
  time(&curr_time);
  curr_tm = localtime(&curr_time);
  // Create a log file for each hour so it's easier to rotate logs
  // Year-Month-Day-Hour
  strftime(date_string, 50, "%Y-%m-%d-%H", curr_tm);
  std::string day(date_string);

  for (const auto &agent : request.agents) {
    std::string folder = FLAGS_data_folder_path + agent.mac + '/';
    errno = 0;
    const int dir_err =
        mkdir(folder.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    if (-1 == dir_err && errno != EEXIST) {
      LOG(ERROR) << "Error creating directory " << folder;
      continue;
    }
    for (const auto &logMsg : agent.logs) {
      std::string fileName = folder + day + '_' + logMsg.file + ".log";
      std::ofstream outfile;
      outfile.open(fileName, std::ios_base::app);
      if (outfile) {
        outfile << logMsg.log << "\n";
      } else {
        LOG(ERROR) << "Unable to open file " << fileName;
      }
    }
  }
  auto endTime = (int64_t)duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()).count();
  LOG(INFO) << "Writing logs complete. "
            << "Total: " << (endTime - startTime) << "ms.";
}

void LogsWriteHandler::onEOM() noexcept {
  auto body = body_->moveToFbString();
  query::LogsWriteRequest request;
  try {
    request = SimpleJSONSerializer::deserialize<query::LogsWriteRequest>(body);
  }
  catch (const std::exception &) {
    LOG(INFO) << "Error deserializing logs_writer request";
    ResponseBuilder(downstream_)
        .status(500, "OK")
        .header("Content-Type", "application/json")
        .body("Failed de-serializing logs_writer request")
        .sendWithEOM();
    return;
  }
  logRequest(request);
  LOG(INFO) << "Logs writer request from \"" << request.topology.name
            << "\" for " << request.agents.size() << " nodes";

  try {
    writeData(request);
  }
  catch (const std::exception &ex) {
    LOG(ERROR) << "Unable to handle logs_writer request: " << ex.what();
    ResponseBuilder(downstream_)
        .status(500, "OK")
        .header("Content-Type", "application/json")
        .body("Failed handling logs_writer request")
        .sendWithEOM();
    return;
  }
  ResponseBuilder(downstream_)
      .status(200, "OK")
      .header("Content-Type", "application/json")
      .body("Success")
      .sendWithEOM();
}

void LogsWriteHandler::onUpgrade(UpgradeProtocol /* unused */) noexcept {}

void LogsWriteHandler::requestComplete() noexcept { delete this; }

void LogsWriteHandler::onError(ProxygenError /* unused */) noexcept {
  LOG(ERROR) << "Proxygen reported error";
  // In QueryServiceFactory, we created this handler using new.
  // Proxygen does not delete the handler.
  delete this;
}

void LogsWriteHandler::logRequest(query::LogsWriteRequest request) {}
}
} // facebook::gorilla
