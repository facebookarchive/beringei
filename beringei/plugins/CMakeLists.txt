# Copyright (c) 2016-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

add_library(
    beringei_plugin STATIC

    BeringeiConfigurationAdapter.h
    BeringeiConfigurationLoader.h
    BeringeiInternalConfiguration.h
    BeringeiConfigurationAdapter.cpp
    BeringeiConfigurationLoader.cpp
)
target_link_libraries(
    beringei_plugin

    beringei_core
    beringei_client
    beringei_thrift
    ${FOLLY_LIBRARIES}
)

add_subdirectory(tests)
