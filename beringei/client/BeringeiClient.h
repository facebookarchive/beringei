/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "beringei/client/BeringeiClientImpl.h"

namespace facebook {
namespace gorilla {

class BeringeiClient : public BeringeiClientImpl {
 public:
  explicit BeringeiClient(
      std::shared_ptr<BeringeiConfigurationAdapterIf> configurationAdapter,
      int queueCapacity = 0,
      int writerThreads = 0,
      bool throwExceptionOnTransientFailure = false,
      int readServicesUpdateInvterval = kDefaultReadServicesUpdateInterval);
};
}
}
