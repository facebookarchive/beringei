/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "BeringeiClient.h"

namespace facebook {
namespace gorilla {

BeringeiClient::BeringeiClient(
    std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
    int queueCapacity,
    int writerThreads,
    bool throwExceptionOnTransientFailure,
    int readServicesUpdateInterval)
    : BeringeiClientImpl(
          configurationAdapter,
          throwExceptionOnTransientFailure) {
  initialize(queueCapacity, writerThreads, readServicesUpdateInterval);
}
}
} // facebook:gorilla
