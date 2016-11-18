/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

namespace facebook {
namespace gorilla {

const uint32_t kDataBlockSize = 65536;

struct DataBlock {
  char data[kDataBlockSize];
};
}
} // facebook:gorilla
