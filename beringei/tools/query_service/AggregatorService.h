/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "StatsTypeAheadCache.h"

#include <folly/io/async/EventBaseManager.h>

#include "beringei/client/BeringeiClient.h"
#include "beringei/if/gen-cpp2/Topology_types_custom_protocol.h"

namespace facebook {
namespace gorilla {

class AggregatorService {
 public:
  explicit AggregatorService(
    std::shared_ptr<TACacheMap> typeaheadCache,
    std::shared_ptr<BeringeiClient> beringeiClient);

  // run eventbase
  void start();
  void timerCallback();
  query::Topology fetchTopology();

 private:
  folly::EventBase eb_;
  std::unique_ptr<folly::AsyncTimeout> timer_{nullptr};
  // from queryservicefactory
  std::shared_ptr<TACacheMap> typeaheadCache_;
  std::shared_ptr<BeringeiClient> beringeiClient_;
};
}
} // facebook::gorilla
