/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <folly/experimental/FunctionScheduler.h>

#include "beringei/lib/MemoryUsageGuardIf.h"

#include <atomic>

namespace facebook {
namespace gorilla {

class MockMemoryUsageGuard : public MemoryUsageGuardIf {
 public:
  MockMemoryUsageGuard() {}

  bool weAreLowOnMemory() override {
    return isMemoryLow_;
  }

  void setMemoryIsLow(bool isMemoryLow) {
    isMemoryLow_ = isMemoryLow;
  }

 protected:
  bool isMemoryLow_ = false;
};
}
} // facebook:gorilla
