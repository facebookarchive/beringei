/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <chrono>

#include <gtest/gtest_prod.h>

namespace facebook {
namespace gorilla {

class Timer {
 public:
  typedef int64_t TimeVal;

  explicit Timer(bool autoStart = false);

  virtual ~Timer() {}

  // Checks if timer is running.
  bool running() const;

  // (Re)starts the timer, regardless if the timer is running.
  void start();

  // Returns the total time elapsed.
  int64_t get() const;

  // Stops the timer. Returns the total time elapsed.
  int64_t stop();

  // Returns total time and resets it. Restarts timer immediately.
  int64_t reset();

 protected:
  static const int64_t INVALID_TIME = -1;

  // Gets microseconds since Timer's Epoch (not unix timestamp).
  virtual int64_t getNow() const;

  // Adds elapsed time to the total time in record.
  void record();

  int64_t start_;
  int64_t sum_;
};
}
} // facebook::gorilla
