/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <stdint.h>
#include <time.h>

namespace facebook {
namespace gorilla {

const int64_t kGorillaMsPerSecond = 1000;
const int64_t kGorillaUsecPerMs = 1000;
const int64_t kGorillaNsecPerUsec = 1000;
const int64_t kGorillaNsecPerMs = kGorillaNsecPerUsec * kGorillaUsecPerMs;
const int64_t kGorillaUsecPerSecond = kGorillaUsecPerMs * kGorillaMsPerSecond;
const int64_t kGorillaNsecPerSecond =
    kGorillaNsecPerUsec * kGorillaUsecPerSecond;

const int64_t kGorillaSecondsPerMinute = 60;
const int64_t kGorillaMinutesPerHour = 60;
const int64_t kGorillaHoursPerDay = 24;
const int64_t kGorillaDaysPerWeek = 7;
const int64_t kGorillaDaysPerYear = 365;

const int64_t kGorillaMsPerMinute =
    kGorillaSecondsPerMinute * kGorillaMsPerSecond;
const int64_t kGorillaMsPerHour = kGorillaMsPerMinute * kGorillaMinutesPerHour;
const int64_t kGorillaMsPerDay = kGorillaMsPerHour * kGorillaHoursPerDay;

const int64_t kGorillaUsecPerMinute = kGorillaUsecPerMs * kGorillaMsPerMinute;
const int64_t kGorillaUsecPerHour = kGorillaUsecPerMs * kGorillaMsPerHour;
const int64_t kGorillaUsecPerDay = kGorillaUsecPerMs * kGorillaMsPerDay;

const int64_t kGorillaSecondsPerHour =
    kGorillaSecondsPerMinute * kGorillaMinutesPerHour;
const int64_t kGorillaSecondsPerDay =
    kGorillaSecondsPerHour * kGorillaHoursPerDay;
const int64_t kGorillaSecondsPerWeek =
    kGorillaSecondsPerDay * kGorillaDaysPerWeek;
const int64_t kGorillaSecondsPerYear =
    kGorillaSecondsPerDay * kGorillaDaysPerYear;

const int64_t kGorillaMinutesPerDay =
    kGorillaMinutesPerHour * kGorillaHoursPerDay;
const int64_t kGorillaMinutesPerWeek =
    kGorillaMinutesPerDay * kGorillaDaysPerWeek;
const int64_t kGorillaMinutesPerYear =
    kGorillaMinutesPerDay * kGorillaDaysPerYear;

const int64_t kGorillaHoursPerWeek = kGorillaHoursPerDay * kGorillaDaysPerWeek;
const int64_t kGorillaHoursPerYear = kGorillaHoursPerDay * kGorillaDaysPerYear;

// based on folly::detail::timespecDiff, without asserts
inline uint64_t timespecDiff(timespec end, timespec begin) {
  if (end.tv_sec == begin.tv_sec) {
    return end.tv_nsec - begin.tv_nsec;
  }
  auto diff = uint64_t(end.tv_sec - begin.tv_sec);
  return diff * 1000000000UL + end.tv_nsec - begin.tv_nsec;
}
}
}
