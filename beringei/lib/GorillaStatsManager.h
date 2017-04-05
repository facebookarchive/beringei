/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <memory>
#include <string>

namespace facebook {
namespace gorilla {

enum GorillaStatsExportType {
  SUM,
  COUNT,
  AVG,
  RATE,
  PERCENT,
};

// This class is a wrapper around ServiceData which prefixes all the counters
// with a specific string.
//
// The static methods will not do anything until `initialize()` is called.
class GorillaStatsManager {
 public:
  virtual ~GorillaStatsManager();

  // Set the prefix for all counters. This is not thread-safe and should be
  // called once, preferably from main().
  static void initialize(
      const std::string& keyPrefix,
      std::unique_ptr<GorillaStatsManager> stats);

  static void addStatValue(const std::string& key, int64_t value = 1);
  static void addStatValue(
      const std::string& key,
      int64_t value,
      GorillaStatsExportType type);
  static void setCounter(const std::string& key, int64_t value);
  static void incrementCounter(const std::string& key, int64_t amount = 1);

  static void addStatValueAggregated(
      const std::string& key,
      int64_t sum,
      int64_t numSamples);

  static void addStatExportType(
      const std::string& key,
      GorillaStatsExportType exportType);
  static void addHistAndStatExports(
      const std::string& key,
      const std::string& stats,
      int64_t bucketWidth,
      int64_t min,
      int64_t max);

 protected:
  GorillaStatsManager();

  virtual void addStatValueInternal(
      const std::string& key,
      int64_t value = 1) = 0;
  virtual void addStatValueInternal(
      const std::string& key,
      int64_t value,
      GorillaStatsExportType type) = 0;
  virtual void setCounterInternal(const std::string& key, int64_t value) = 0;
  virtual void incrementCounterInternal(
      const std::string& key,
      int64_t amount = 1) = 0;
  virtual void addStatValueAggregatedInternal(
      const std::string& key,
      int64_t sum,
      int64_t numSamples) = 0;
  virtual void addStatExportTypeInternal(
      const std::string& key,
      GorillaStatsExportType exportType) = 0;
  virtual void addHistAndStatExportsInternal(
      const std::string& key,
      const std::string& stats,
      int64_t bucketWidth,
      int64_t min,
      int64_t max) = 0;

 private:
  static std::string keyPrefix_;
  static std::unique_ptr<GorillaStatsManager> stats_;
};
}
} // facebook:gorilla
