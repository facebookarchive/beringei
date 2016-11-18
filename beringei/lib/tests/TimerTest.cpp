/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "beringei/lib/Timer.h"

using namespace ::testing;

using clk = std::chrono::high_resolution_clock;

namespace facebook {
namespace gorilla {

class TestTimer : public Timer {
 public:
  explicit TestTimer(bool autoStart)
      : Timer(autoStart), now_(folly::Random::rand64(1000000000)) {}
  void burn(int duration) {
    now_ += duration;
  }
  int64_t getNow() const override {
    return now_;
  }
  int64_t getSum() const {
    return sum_;
  }

 private:
  int64_t now_;
};

TEST(TimerTest, Constructor) {
  TestTimer timer(false);
  ASSERT_FALSE(timer.running());
  ASSERT_EQ(timer.getSum(), 0);

  TestTimer autoStartTimer(true);
  ASSERT_TRUE(autoStartTimer.running());
  ASSERT_EQ(autoStartTimer.getSum(), 0);
}

TEST(TimerTest, Get) {
  TestTimer timer(false);
  EXPECT_EQ(timer.get(), 0);
  timer.burn(100);
  EXPECT_EQ(timer.get(), 0);

  timer.start();
  timer.burn(100);
  EXPECT_EQ(timer.get(), 100);
  timer.burn(100);
  EXPECT_EQ(timer.get(), 200);
  timer.stop();

  timer.burn(100);
  EXPECT_EQ(timer.get(), 200);
  timer.start();
  timer.burn(100);
  EXPECT_EQ(timer.get(), 300);
  timer.stop();
}

TEST(TimerTest, Stop) {
  TestTimer timer(false);
  EXPECT_EQ(timer.stop(), 0);
  EXPECT_FALSE(timer.running());

  timer.start();
  timer.burn(100);
  EXPECT_EQ(timer.stop(), 100);
  EXPECT_FALSE(timer.running());

  timer.start();
  timer.burn(100);
  EXPECT_EQ(timer.stop(), 200);
  EXPECT_FALSE(timer.running());
}

TEST(TimerTest, Reset) {
  TestTimer timer(false);
  EXPECT_EQ(timer.reset(), 0);
  EXPECT_FALSE(timer.running());

  timer.start();
  timer.burn(100);
  ASSERT_EQ(timer.stop(), 100);
  EXPECT_EQ(timer.reset(), 100);
  EXPECT_FALSE(timer.running());
  ASSERT_EQ(timer.get(), 0);

  timer.start();
  timer.burn(100);
  ASSERT_EQ(timer.stop(), 100);

  timer.start();
  timer.burn(100);
  ASSERT_EQ(timer.get(), 200);
  EXPECT_EQ(timer.reset(), 200);
  EXPECT_TRUE(timer.running());
  timer.stop();
}
}
} // facebook::gorilla
