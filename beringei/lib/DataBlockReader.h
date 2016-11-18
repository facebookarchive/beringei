/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <functional>
#include <set>
#include <string>
#include <vector>

#include "DataBlock.h"
#include "FileUtils.h"

namespace facebook {
namespace gorilla {

class DataBlockReader {
 public:
  explicit DataBlockReader(int64_t shardId, const std::string& dataDirectory);

  // Returns allocated blocks for every page in the given position.
  // Fills in timeSeriesIds and storageIds with the metadata associated with
  // the blocks.
  std::vector<std::unique_ptr<DataBlock>> readBlocks(
      uint32_t position,
      std::vector<uint32_t>& timeSeriesIds,
      std::vector<uint64_t>& storageIds);

  // Returns the file ids for the completed blocks.
  std::set<uint32_t> findCompletedBlockFiles();

 private:
  FileUtils dataFiles_;
  FileUtils completedFiles_;
};
}
} // facebook:gorilla
