#include <folly/String.h>

#include "beringei/lib/GorillaStatsManager.h"

#include "beringei/client/BeringeiWriter.h"

DEFINE_int32(
    gorilla_min_queue_size,
    100,
    "slow down writes if the queue contains fewer elements than this");
// Sleep for 100ms between puts if the queue contains <100 elements.
DEFINE_int32(
    gorilla_sleep_per_put_us,
    100000,
    "sleep for this long between puts if the queue is near-empty");

namespace facebook {
namespace gorilla {

const static int kMaxRetryBatchSize = 10000;
const static std::string kQueueSizeKey = "gorilla_client.queue_size.";

BeringeiWriter::BeringeiWriter(std::shared_ptr<WriteClient> writeClient)
    : writeClient_(writeClient),
      worker_(&BeringeiWriter::writeDataPointsForever, this) {
  VLOG(2) << "Creating BeringeiWriter";
}

BeringeiWriter::~BeringeiWriter() {
  stop();
}

void BeringeiWriter::stop() {
  keepWriting_.store(false);
  if (worker_.joinable()) {
    worker_.join();
  }
}

void BeringeiWriter::flush(std::shared_ptr<BeringeiHostWriter>& hostWriter) {
  auto& networkClient = writeClient_->client;

  PutDataRequest request;
  hostWriter->collectBatch(request.data);

  if (request.data.size() == 0) {
    VLOG(2) << "Tried flushing but DPs were empty.";
    return;
  }

  VLOG(2) << "Flushing " << request.data.size() << " datapoints to "
          << networkClient->getServiceName();
  writeClient_->putWithRetry(request, hostWriter);
}

void BeringeiWriter::updateShardWriterMap() {
  auto newSnapshotVersion = writeClient_->getSnapshotVersion();
  if (newSnapshotVersion == snapshotVersion_) {
    return;
  }

  snapshotVersion_ = newSnapshotVersion;
  auto shards = writeClient_->getHostsSnapshot();
  if (shards.size() != numShards_) {
    numShards_ = shards.size();
    hostWriters_.resize(numShards_);
  }

  std::set<std::pair<std::string, int>> activeDestinations;
  std::vector<int> orphanShards;

  for (int shard = 0; shard < numShards_; shard++) {
    auto& hostInfo = shards[shard];
    if (hostInfo.first == "") {
      orphanShards.push_back(shard);
      hostWriters_[shard] = nullptr;
      continue;
    }

    // Make sure we have a single writer per destination.
    auto iter = destinationToWriterMap_.find(hostInfo);
    if (iter != destinationToWriterMap_.end()) {
      hostWriters_[shard] = iter->second;
    } else {
      // This is a new destination, create a new writer.
      auto writer = std::make_shared<BeringeiHostWriter>(hostInfo);
      destinationToWriterMap_[hostInfo] = writer;
      hostWriters_[shard] = writer;
    }

    // Keep track of all the active writers
    activeDestinations.insert(hostInfo);
  }

  if (orphanShards.size() > 0) {
    LOG(ERROR) << "Failed to fetch hosts for the following shards: "
               << folly::join(',', orphanShards);
  }

  // Cleanup inactive writers and put their batched datapoints to retry queue.
  for (auto iter = destinationToWriterMap_.begin();
       iter != destinationToWriterMap_.end();) {
    if (activeDestinations.count(iter->first)) {
      iter++;
    } else {
      std::vector<DataPoint> datapoints;
      iter->second->collectBatch(datapoints);
      writeClient_->retry(datapoints);

      destinationToWriterMap_.erase(iter++);
    }
  }
}

void BeringeiWriter::writeDataPointsForever() {
  auto& networkClient = writeClient_->client;
  auto& writeQueue = writeClient_->queue;

  std::vector<DataPoint> droppedDataPoints;
  writeQueue.popForever([&](DataPoint& dp) {
    updateShardWriterMap();

    // Each Beringei instance might have different shard counts. Let's ask the
    // network client.
    dp.key.shardId = networkClient->getShardForDataPoint(dp);

    if (dp.key.shardId >= hostWriters_.size() ||
        hostWriters_[dp.key.shardId] == nullptr) {
      droppedDataPoints.push_back(dp);
    } else {
      auto& hostWriter = hostWriters_[dp.key.shardId];
      if (hostWriter->addDataPoint(dp)) {
        flush(hostWriter);
      }
    }

    if (droppedDataPoints.size() >= kMaxRetryBatchSize) {
      writeClient_->retry(std::move(droppedDataPoints));
      droppedDataPoints.clear();
    }

    // TODO(firatb): Remove this from the hot path.
    GorillaStatsManager::addStatValue(
        kQueueSizeKey + networkClient->getServiceName(), writeQueue.size());

    return keepWriting_.load();
  });

  LOG(WARNING) << "Shutting down BeringeiWriter";

  // Let's flush all the data we have first.
  for (auto& writer : hostWriters_) {
    if (writer != nullptr) {
      flush(writer);
    }
  }
}

} // namespace gorilla
} // namespace facebook
