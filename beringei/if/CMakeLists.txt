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

set(
    THRIFT_FILES
    beringei_data.thrift
    beringei_grafana.thrift
    beringei.thrift
)

set(
    THRIFT_GEN
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/BeringeiService.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/BeringeiService.h
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/BeringeiService.tcc
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/BeringeiService_client.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/BeringeiService_custom_protocol.h
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/BeringeiService_processmap_binary.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/BeringeiService_processmap_compact.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_constants.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_constants.h
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_data.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_data.h
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_data_constants.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_data_constants.h
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_data_data.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_data_data.h
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_data_types.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_data_types.h
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_data_types.tcc
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_data_types_custom_protocol.h
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_grafana_constants.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_grafana_constants.h
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_grafana_data.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_grafana_data.h
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_grafana_types.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_grafana_types.h
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_grafana_types.tcc
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_grafana_types_custom_protocol.h
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_types.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_types.h
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_types.tcc
    ${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp2/beringei_types_custom_protocol.h
)

add_custom_command(
  OUTPUT ${THRIFT_GEN}
  COMMAND bash "build_thrift.sh" ${THRIFT_FILES}
  MAIN_DEPENDENCY build_thrift.sh
  DEPENDS ${THRIFT_FILES} ${FBTHRIFT_INCLUDE_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

set_source_files_properties(
    ${THRIFT_GEN}
    PROPERTIES GENERATED TRUE
)

add_library(
    beringei_thrift STATIC
    ${THRIFT_GEN}
)

target_link_libraries(
    beringei_thrift
    ${FBTHRIFT_LIBRARIES}
)
