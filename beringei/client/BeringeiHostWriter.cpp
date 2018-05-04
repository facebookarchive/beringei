#include "beringei/client/BeringeiHostWriter.h"

DEFINE_int32(
    gorilla_host_batch_size,
    10000,
    "Max data points to send to single gorilla host");

DEFINE_int32(
    gorilla_batch_delay_ms,
    1000,
    "How long to wait before sending an incomplete batch. Milliseconds.");

namespace facebook {
namespace gorilla {

BeringeiHostWriter::BeringeiHostWriter(
    const std::pair<std::string, int>& hostInfo)
    : hostInfo_(hostInfo) {}

void BeringeiHostWriter::addDataPoint(DataPoint& dp) {
  if (batch_.size() == 0) {
    watch_.reset();
  }

  batch_.push_back(std::move(dp));
}

bool BeringeiHostWriter::isReady() const {
  return batch_.size() >= FLAGS_gorilla_host_batch_size ||
      watch_.elapsed().count() >= FLAGS_gorilla_batch_delay_ms;
}

void BeringeiHostWriter::collectBatch(std::vector<DataPoint>& datapoints) {
  if (batch_.size() == 0) {
    return;
  }

  datapoints.insert(
      datapoints.end(),
      std::make_move_iterator(batch_.begin()),
      std::make_move_iterator(batch_.end()));
  batch_ = std::vector<DataPoint>();
}

std::pair<std::string, int>& BeringeiHostWriter::getHostInfo() {
  return hostInfo_;
}

} // namespace gorilla
} // namespace facebook
