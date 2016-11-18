/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "NetworkUtils.h"

#include <unistd.h>

#include "gflags/gflags.h"
#include "glog/logging.h"

DEFINE_int32(
    host_name_max_chars,
    255,
    "Maximum number of characters in a hostname.");

namespace facebook {
namespace gorilla {

std::string NetworkUtils::getLocalHost() {
  // need a char * for the syscall, not a const char * or a std::string
  // add one to handle the string null termination
  char hostname[FLAGS_host_name_max_chars + 1];
  if (0 == gethostname(hostname, sizeof(hostname))) {
    std::string ret(hostname);
    return ret;
  }
  LOG(ERROR) << "NetworkUtils::getLocalHost() failed, errno: " << errno;
  // empty string indicates error
  return std::string();
}
}
}
