/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <map>
#include <string>
#include <vector>

#include "beringei/if/gen-cpp2/beringei_data_types_custom_protocol.h"
#include "beringei/plugins/BeringeiInternalConfiguration.h"

namespace facebook {
namespace gorilla {

class BeringeiConfigurationLoader {
 public:
  ConfigurationInfo loadFromJsonFile(const std::string& fullFile) const;

  bool isValidConfiguration(const ConfigurationInfo& configuration) const;

  BeringeiInternalConfiguration getInternalConfiguration(
      const ConfigurationInfo& configuration) const;
};
}
} // facebook::gorilla
