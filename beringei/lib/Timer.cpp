/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "Timer.h"

#include <chrono>

using clk = std::chrono::high_resolution_clock;

namespace facebook {
namespace gorilla {

Timer::Timer(bool autoStart) : start_(Timer::INVALID_TIME), sum_(0) {
  if (autoStart) {
    start();
  }
}

bool Timer::running() const {
  return start_ != Timer::INVALID_TIME;
}

void Timer::start() {
  start_ = getNow();
}

int64_t Timer::get() const {
  int64_t sum = sum_;
  if (running()) {
    sum += getNow() - start_;
  }
  return sum;
}

int64_t Timer::stop() {
  if (running()) {
    record();
  }
  start_ = Timer::INVALID_TIME;
  return sum_;
}

int64_t Timer::reset() {
  if (running()) {
    record();
    start();
  }
  int64_t sum = sum_;
  sum_ = 0;
  return sum;
}

int64_t Timer::getNow() const {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             clk::now().time_since_epoch())
      .count();
}

void Timer::record() {
  sum_ += getNow() - start_;
}
}
} // facebook::gorilla
