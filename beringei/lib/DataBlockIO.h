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
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "DataBlock.h"
#include "FileUtils.h"

namespace facebook {
namespace gorilla {

enum class DataBlockVersion : int32_t {
  V_UNKNOWN = -1,
  V_0 = 0,
  // Pageable
  V_0_UNCOMPRESSED,
  V_MAX
};

class DataBlockIO {
 public:
  DataBlockIO(
      int64_t shardId,
      const std::string& dataDirectory,
      DataBlockVersion writeVersion = DataBlockVersion::V_0);

  // Returns allocated blocks for every page in the given position.
  // Fills in timeSeriesIds and storageIds with the metadata associated with
  // the blocks.
  std::vector<std::unique_ptr<DataBlock>> readBlocks(
      uint32_t position,
      std::vector<uint32_t>& timeSeriesIds,
      std::vector<uint64_t>& storageIds);

  void write(
      uint32_t position,
      const std::vector<std::shared_ptr<DataBlock>>& pages,
      uint32_t activePages,
      const std::vector<uint32_t>& timeSeriesIds,
      const std::vector<uint64_t>& storageIds);

  // Returns the file ids for the completed blocks.
  std::set<uint32_t> findCompletedBlockFiles() const;

  // Delete buckets older than the specified position
  void clearTo(int64_t position);

  // Create necessary persistence directories. NOP when they already exist
  void createDirectories();

  // Remove file
  void remove(int64_t id);

 private:
  const DataBlockVersion writeVersion_;

  FileUtils dataFiles_;
  FileUtils completeFiles_;
};
} // namespace gorilla
} // namespace facebook
