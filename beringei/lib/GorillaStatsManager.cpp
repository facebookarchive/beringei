/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "GorillaStatsManager.h"

namespace facebook {
namespace gorilla {

std::string GorillaStatsManager::keyPrefix_ = "";
std::unique_ptr<GorillaStatsManager> GorillaStatsManager::stats_ = nullptr;

GorillaStatsManager::GorillaStatsManager() {}

GorillaStatsManager::~GorillaStatsManager() {}

void GorillaStatsManager::initialize(
    const std::string& keyPrefix,
    std::unique_ptr<GorillaStatsManager> stats) {
  if (!keyPrefix.empty()) {
    keyPrefix_ = keyPrefix + '.';
  }
  stats_ = std::move(stats);
}

void GorillaStatsManager::addStatValue(const std::string& key, int64_t value) {
  if (stats_.get()) {
    stats_->addStatValueInternal(keyPrefix_ + key, value);
  }
}

void GorillaStatsManager::addStatValue(
    const std::string& key,
    int64_t value,
    GorillaStatsExportType type) {
  if (stats_.get()) {
    stats_->addStatValueInternal(keyPrefix_ + key, value, type);
  }
}

void GorillaStatsManager::addStatValueAggregated(
    const std::string& key,
    int64_t sum,
    int64_t numSamples) {
  if (stats_.get()) {
    stats_->addStatValueAggregatedInternal(keyPrefix_ + key, sum, numSamples);
  }
}

void GorillaStatsManager::setCounter(const std::string& key, int64_t value) {
  if (stats_.get()) {
    stats_->setCounterInternal(keyPrefix_ + key, value);
  }
}

void GorillaStatsManager::incrementCounter(
    const std::string& key,
    int64_t amount) {
  if (stats_.get()) {
    stats_->incrementCounterInternal(keyPrefix_ + key, amount);
  }
}

void GorillaStatsManager::addStatExportType(
    const std::string& key,
    GorillaStatsExportType exportType) {
  if (stats_.get()) {
    stats_->addStatExportTypeInternal(keyPrefix_ + key, exportType);
  }
}

void GorillaStatsManager::addHistAndStatExports(
    const std::string& key,
    const std::string& stats,
    int64_t bucketWidth,
    int64_t min,
    int64_t max) {
  if (stats_.get()) {
    stats_->addHistAndStatExportsInternal(
        keyPrefix_ + key, stats, bucketWidth, min, max);
  }
}
}
} // facebook:gorilla
