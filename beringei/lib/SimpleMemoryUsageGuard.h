/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "MemoryUsageGuardIf.h"

#include <folly/experimental/FunctionScheduler.h>

#include <atomic>

namespace facebook {
namespace gorilla {

class SimpleMemoryUsageGuard : public MemoryUsageGuardIf {
 public:
  SimpleMemoryUsageGuard();
  explicit SimpleMemoryUsageGuard(double memFractionToUse);

  bool weAreLowOnMemory() override;

 private:
  void updateMemoryStats();

  std::atomic<bool> isFreeMemoryRatioLow_;
  folly::FunctionScheduler memoryStatsUpdateRunner_;
};
}
} // facebook:gorilla
