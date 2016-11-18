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
#include <sys/resource.h>
#include "beringei/lib/GorillaStatsManager.h"

DEFINE_uint64(
    soft_memory_cap_mb,
    0, // unlimited
    "Soft memory cap for gorilla (mb).  Memory usage may exceed this cap, "
    "but new time series creation may be blocked. 0 indicates unlimited.");
namespace facebook {
namespace gorilla {

static const std::chrono::seconds kMemoryStatsUpdateInterval(1);

static const std::string kMemoryTotal = "gorilla.memory_total";

SimpleMemoryUsageGuard::SimpleMemoryUsageGuard()
    : isFreeMemoryRatioLow_(false), memoryStatsUpdateRunner_() {
  memoryStatsUpdateRunner_.addFunction(
      std::bind(&SimpleMemoryUsageGuard::updateMemoryStats, this),
      kMemoryStatsUpdateInterval,
      "updateMemoryStats");
  memoryStatsUpdateRunner_.start();
  memLimitToEnforceKb_ = fLU64::FLAGS_soft_memory_cap_mb * 1024;
}

bool SimpleMemoryUsageGuard::weAreLowOnMemory() {
  return isFreeMemoryRatioLow_;
}

void SimpleMemoryUsageGuard::updateMemoryStats() {
  if (memLimitToEnforceKb_ == 0) {
    isFreeMemoryRatioLow_ = false;
    return;
  }

  struct rusage currentUsage;

  int retVal = getrusage(RUSAGE_SELF, &currentUsage);
  if (retVal != 0) {
    LOG(ERROR) << "Failed to retrieve usage info with error code: " << retVal;
    return;
  }

  long maxRssInKb = currentUsage.ru_maxrss;
  GorillaStatsManager::addStatValue(kMemoryTotal, maxRssInKb);

  isFreeMemoryRatioLow_ = maxRssInKb >= memLimitToEnforceKb_;
}
}
} // facebook:gorilla
