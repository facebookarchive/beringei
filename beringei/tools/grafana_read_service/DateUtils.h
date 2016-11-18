/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <ctime>

namespace facebook {
namespace gorilla {

class DateUtils {
 public:
  // Helper function to parse timestamps sent by grafana
  // For example, "2016-11-17T02:12:40.708Z".
  static bool stringParseGrafanaTimestamp(const std::string& str, time_t* t) {
    const char* format = "%Y-%m-%dT%H:%M:%S";
    struct tm timeinfo;
    memset(&timeinfo, 0, sizeof(struct tm));
    if (strptime(str.c_str(), format, &timeinfo) == nullptr) {
      return false;
    }

    timeinfo.tm_isdst = -1; // no DST information available
    if (t) {
      time_t timeSinceEpoch = timegm(&timeinfo);
      LOG(INFO) << timeSinceEpoch;
      *t = timeSinceEpoch;
    }

    return true;
  }
};
}
} // facebook::gorilla
