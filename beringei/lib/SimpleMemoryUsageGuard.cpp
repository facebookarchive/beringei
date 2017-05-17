/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "SimpleMemoryUsageGuard.h"

#include <gflags/gflags.h>
#include <fstream>
#include "beringei/lib/GorillaStatsManager.h"

DEFINE_uint64(
    soft_memory_cap_mb,
    0, // unlimited
    "Soft memory cap for gorilla (mb).  Memory usage may exceed this cap, "
    "but new time series creation may be blocked. 0 indicates unlimited.");

namespace facebook {
namespace gorilla {

static const std::chrono::seconds kMemoryStatsUpdateInterval(1);

static constexpr size_t kBytesPerMB = 1024 * 1024;
static const std::string kMemoryTotal = "gorilla.memory_total_mb";
static const std::string kStatFile = "/proc/self/statm";

SimpleMemoryUsageGuard::SimpleMemoryUsageGuard()
    : isFreeMemoryRatioLow_(false), memoryStatsUpdateRunner_() {
  memoryStatsUpdateRunner_.addFunction(
      std::bind(&SimpleMemoryUsageGuard::updateMemoryStats, this),
      kMemoryStatsUpdateInterval,
      "updateMemoryStats");
  GorillaStatsManager::addStatExportType(
      kMemoryTotal, GorillaStatsExportType::AVG);
  memoryStatsUpdateRunner_.start();
}

bool SimpleMemoryUsageGuard::weAreLowOnMemory() {
  return isFreeMemoryRatioLow_;
}

void SimpleMemoryUsageGuard::updateMemoryStats() {
  if (FLAGS_soft_memory_cap_mb == 0) {
    isFreeMemoryRatioLow_ = false;
    return;
  }

  size_t vPages, rPages;
  std::ifstream statm(kStatFile);
  statm >> vPages >> rPages; // Ignore the rest of the fields.

  if (!statm.good()) {
    LOG(ERROR) << "Failed to update memory usage";
    statm.close();
    return;
  }

  statm.close();

  size_t rssMB = rPages * getpagesize() / kBytesPerMB;

  GorillaStatsManager::setCounter(kMemoryTotal, rssMB);
  GorillaStatsManager::addStatValue(kMemoryTotal, rssMB);

  isFreeMemoryRatioLow_ = rssMB >= FLAGS_soft_memory_cap_mb;
}
}
} // facebook:gorilla
