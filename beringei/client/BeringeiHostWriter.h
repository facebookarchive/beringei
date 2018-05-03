#pragma once

#include "folly/stop_watch.h"

#include "beringei/if/gen-cpp2/beringei_data_types.h"

namespace facebook {
namespace gorilla {
class BeringeiHostWriter {
 public:
  BeringeiHostWriter(const std::pair<std::string, int>& hostInfo);
  void addDataPoint(DataPoint& dp);
  bool isReady() const;
  void collectBatch(std::vector<DataPoint>& datapoints);
  std::pair<std::string, int>& getHostInfo();
  std::atomic<int> inFlightRequests{0};

 private:
  std::vector<DataPoint> batch_;
  folly::stop_watch<std::chrono::milliseconds> watch_;
  std::pair<std::string, int> hostInfo_;
};

} // namespace gorilla
} // namespace facebook
