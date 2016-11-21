# Copyright (c) 2016-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

# Needed for relative imports amongst headers below.
include_directories(gen-cpp2)

# Uplevel the path to this so lower things can use it.
set(BERINGEI_THRIFT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2 PARENT)

add_library(
    beringei_thrift STATIC
    gen-cpp2/BeringeiService.h
    gen-cpp2/BeringeiService_custom_protocol.h
    gen-cpp2/beringei_constants.h
    gen-cpp2/beringei_data_constants.h
    gen-cpp2/beringei_data_types.h
    gen-cpp2/beringei_data_types_custom_protocol.h
    gen-cpp2/beringei_types.h
    gen-cpp2/beringei_types_custom_protocol.h
    gen-cpp2/BeringeiService.cpp
    gen-cpp2/BeringeiService_client.cpp
    gen-cpp2/BeringeiService_processmap_binary.cpp
    gen-cpp2/BeringeiService_processmap_compact.cpp
    gen-cpp2/beringei_constants.cpp
    gen-cpp2/beringei_data_constants.cpp
    gen-cpp2/beringei_data_types.cpp
    gen-cpp2/beringei_types.cpp
    gen-cpp2/BeringeiService.tcc
    gen-cpp2/beringei_data_types.tcc
    gen-cpp2/beringei_types.tcc
    gen-cpp2/beringei_grafana_constants.cpp
    gen-cpp2/beringei_grafana_constants.h
    gen-cpp2/beringei_grafana_types.cpp
    gen-cpp2/beringei_grafana_types_custom_protocol.h
    gen-cpp2/beringei_grafana_types.h
    gen-cpp2/beringei_grafana_types.tcc
)

target_link_libraries(
    beringei_thrift
    ${FBTHRIFT_LIBRARIES}
)
