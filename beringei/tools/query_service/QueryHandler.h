/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <folly/futures/Future.h>
#include <folly/Memory.h>
#include <folly/dynamic.h>
#include <proxygen/httpserver/RequestHandler.h>

#include "beringei/client/BeringeiClient.h"
#include "beringei/client/BeringeiConfigurationAdapterIf.h"
#include "beringei/if/gen-cpp2/beringei_query_types_custom_protocol.h"
#include "beringei/if/gen-cpp2/Topology_types_custom_protocol.h"


namespace facebook {
namespace gorilla {

typedef std::vector<std::pair<Key, std::vector<TimeValuePair>>> TimeSeries;

class QueryHandler : public proxygen::RequestHandler {
 public:
  explicit QueryHandler(
      std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
      std::shared_ptr<BeringeiClient> beringeiClient);
  void onRequest(
      std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError err) noexcept override;

 private:
  void logRequest(query::QueryRequest request);

  int getShardId(const std::string& key, const int numShards);

  void validateQuery(const query::Query& request);
  GetDataRequest createBeringeiRequest(
      const query::Query& request,
      const int numShards);

  /**
   * Determine column names to use based on KeyData
   */
  void columnNames();
  /**
   * Transform the data from TimeValuePairs into lists of
   * data points, filling missing data with 0s.
   * timeBuckets_ size should be the same as each key's time series
   */
  folly::dynamic transform();
  folly::dynamic handleQuery();
  folly::dynamic eventHandler(int dataPointIncrementMs,
                              const std::string& metricName);
  folly::dynamic analyzerTable(int beringeiTimeWindowS);
  folly::dynamic makeEvent(int64_t startIndex, int64_t endIndex);
  std::string getTimeStr(time_t timeSec);
  double calculateAverage(double *timeSeries, bool *valid,
      int timeSeriesStartIndex, int minIdx, int maxIdx, bool mcsflag);

  std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter_;
  std::shared_ptr<BeringeiClient> beringeiClient_;
  std::unique_ptr<folly::IOBuf> body_;
  // request data
  query::Query query_;
  time_t startTime_;
  time_t endTime_;
  std::vector<std::string> columnNames_;
  TimeSeries beringeiTimeSeries_;
  // regular key time series
  std::vector<std::vector<double>> timeSeries_;
  // aggregated series (avg, min, max, sum, count)
  // all displayed by default
  std::unordered_map<std::string, std::vector<double>> aggSeries_;
};
}
} // facebook::gorilla
