/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "DataBlockIO.h"

#include "BucketStorage.h"
#include "GorillaStatsManager.h"

#include <folly/compression/Compression.h>
#include <folly/io/IOBuf.h>
#include <folly/synchronization/CallOnce.h>

namespace facebook {
namespace gorilla {

namespace {

const std::string kDataPrefix = "block_data";

// These files are only used as marker files to indicate which
// blocks have been completed. The files are empty but the file name
// has the id of the completed block.
const std::string kCompletePrefix = "complete_block";

} // namespace

DataBlockIO::DataBlockIO(
    int64_t shardId,
    const std::string& dataDirectory,
    DataBlockVersion writeVersion)
    : shardId_(shardId),
      writeVersion_(writeVersion),
      dataFiles_(shardId, kDataPrefix, dataDirectory),
      completeFiles_(shardId, kCompletePrefix, dataDirectory) {}

std::vector<std::unique_ptr<DataBlock>> DataBlockIO::readBlocks(
    uint32_t position,
    std::vector<uint32_t>& timeSeriesIds,
    std::vector<uint64_t>& storageIds) {
  DataBlockPosition reader(shardId_, position, writeVersion_);
  return reader.readBlocks(dataFiles_, timeSeriesIds, storageIds);
}

void DataBlockIO::write(
    uint32_t position,
    const std::vector<std::shared_ptr<DataBlock>>& pages,
    uint32_t activePages,
    const std::vector<uint32_t>& timeSeriesIds,
    const std::vector<uint64_t>& storageIds) {
  DataBlockPosition writer(shardId_, position, writeVersion_);
  writer.write(
      dataFiles_,
      completeFiles_,
      pages,
      activePages,
      timeSeriesIds,
      storageIds);
}

std::set<uint32_t> DataBlockIO::findCompletedBlockFiles() const {
  const std::vector<int64_t> files = completeFiles_.ls();
  std::set<uint32_t> completedBlockFiles(files.begin(), files.end());

  return completedBlockFiles;
}

void DataBlockIO::clearTo(int64_t position) {
  completeFiles_.clearTo(position);
  dataFiles_.clearTo(position);
}

void DataBlockIO::createDirectories() {
  dataFiles_.createDirectories();
  completeFiles_.createDirectories();
}

void DataBlockIO::remove(int64_t position) {
  dataFiles_.remove(position);
  completeFiles_.remove(position);
}
} // namespace gorilla
} // namespace facebook
