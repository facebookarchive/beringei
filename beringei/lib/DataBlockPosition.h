/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <cstdint>
#include <memory>
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

// Class DataBlockPosition writing a single bucket (typically two hours)
// to persistent storage, reading it back, and allowing paging when using a
// compatible DataBlockVersion
class DataBlockPosition {
 public:
  // DataBlockPosition arguments
  DataBlockPosition(
      int64_t shardId,
      uint32_t position,
      DataBlockVersion writeVersion);

  // Returns allocated blocks for every page.
  // Fills in timeSeriesIds and storageIds with the metadata associated with
  // the blocks.
  std::vector<std::unique_ptr<DataBlock>> readBlocks(
      FileUtils& dataFiles,
      std::vector<uint32_t>& timeSeriesIds,
      std::vector<uint64_t>& storageIds);

  void write(
      FileUtils& dataFiles,
      FileUtils& completeFiles,
      const std::vector<std::shared_ptr<DataBlock>>& pages,
      uint32_t activePages,
      const std::vector<uint32_t>& timeSeriesIds,
      const std::vector<uint64_t>& storageIds);

 private:
  DataBlockPosition(const DataBlockPosition&) = delete;
  DataBlockPosition(DataBlockPosition&&) = delete;
  DataBlockPosition& operator=(const DataBlockPosition&) = delete;
  DataBlockPosition& operator=(DataBlockPosition&&) = delete;

  const int64_t shardId_;
  const uint32_t position_;
  const DataBlockVersion writeVersion_;
};

} // namespace gorilla
} // namespace facebook
