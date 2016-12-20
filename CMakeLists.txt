# Copyright (c) 2016-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

cmake_minimum_required(VERSION 2.8.7 FATAL_ERROR)

# This is necessary so that find_package can find the "Find$x.cmake" files.
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/beringei/cmake" ${CMAKE_MODULE_PATH})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# package information
set(PACKAGE_NAME      "beringei")
set(PACKAGE_VERSION   "1")
set(PACKAGE_STRING    "${PACKAGE_NAME} ${PACKAGE_VERSION}")
set(PACKAGE_TARNAME   "${PACKAGE_NAME}-${PACKAGE_VERSION}")
set(PACKAGE_BUGREPORT "https://github.com/facebookincubator/beringei/issues")

project(${PACKAGE_NAME} C CXX)

# Check Platform Requirement
if(NOT CMAKE_SIZEOF_VOID_P EQUAL 8)
    message(FATAL_ERROR "Beringei requires a 64bit OS")
endif()

find_package(
    Boost 1.58.0 REQUIRED
    COMPONENTS
        filesystem
        system
)

find_package(Folly REQUIRED)
find_package(FBThrift REQUIRED)
find_package(GFlags REQUIRED)
find_package(Glog REQUIRED)
find_package(Threads REQUIRED)
find_package(Wangle REQUIRED)
find_package(Proxygen REQUIRED)

include(CheckFunctionExists)
include(BeringeiCompilerOptions)

set(BERINGEI_HOME ${CMAKE_CURRENT_SOURCE_DIR})

message("Building Required Thrift Files")
execute_process(
  COMMAND bash "-e" "build_thrift.sh"
  WORKING_DIRECTORY ${BERINGEI_HOME}/beringei/if
  RESULT_VARIABLE thrift_build_result)

if(NOT "${thrift_build_result}" STREQUAL "0")
    message(FATAL_ERROR "Could not build thrfft file.")
endif()

set(TP_PROJECTS_DIR "beringei/third-party")
# So that qualified includes work. E.g. #include "beringei/client/$x.h"
include_directories(${BERINGEI_HOME})

# So that all subsequent directories have access to
# folly, thrift, proxygen and wangle
include_directories(${FOLLY_INCLUDE_DIR} ${FBTHRIFT_INCLUDE_DIR} ${PROXYGEN_INCLUDE_DIR} ${WANGLE_INCLUDE_DIR})

add_subdirectory(${TP_PROJECTS_DIR}/gtest)
include_directories(${GTEST_INCLUDE_DIRS} ${GMOCK_INCLUDE_DIRS})

enable_testing()
add_subdirectory("beringei")

# modules / depends
file(GLOB BERINGEI_CMAKE_FILES "cmake/*.cmake")
install(
    FILES ${BERINGEI_CMAKE_FILES}
    DESTINATION cmake
    COMPONENT dev
)
