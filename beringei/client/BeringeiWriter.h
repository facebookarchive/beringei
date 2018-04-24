#pragma once

#include <thread>
#include "beringei/client/BeringeiHostWriter.h"

#include "beringei/client/WriteClient.h"

namespace facebook {
namespace gorilla {

class BeringeiWriter {
 public:
  explicit BeringeiWriter(std::shared_ptr<WriteClient> writeClient);
  ~BeringeiWriter();
  void stop();

 private:
  void writeDataPointsForever();
  void flushAll(bool force = false);
  void flush(std::shared_ptr<BeringeiHostWriter>& hostWriter);
  void updateShardWriterMap();

  std::shared_ptr<WriteClient> writeClient_;
  std::thread worker_;
  std::atomic<bool> keepWriting_{true};

  int numShards_ = 0;
  std::size_t snapshotVersion_ = 0;
  std::vector<std::shared_ptr<BeringeiHostWriter>> hostWriters_;
  std::unordered_map<
      std::pair<std::string, int>,
      std::shared_ptr<BeringeiHostWriter>>
      destinationToWriterMap_;

  std::string counterQueueSize_;
};
} // namespace gorilla
} // namespace facebook
