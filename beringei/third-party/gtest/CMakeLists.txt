# Copyright (c) 2016-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

cmake_minimum_required(VERSION 2.8.8)
project(gtest_builder C CXX)
include(ExternalProject)

set(GTEST_FORCE_SHARED_CRT ON)
set(GTEST_DISABLE_PTHREADS OFF)

find_package(Threads REQUIRED)

set(GTEST_DIR "${CMAKE_BINARY_DIR}/beringei/third-party/gtest/src")

externalproject_add(
    gtest_tp
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG release-1.8.0
    CMAKE_ARGS
        -Dgtest_force_shared_crt=${GTEST_FORCE_SHARED_CRT}
        -Dgtest_disable_pthreads=${GTEST_DISABLE_PTHREADS}
        -DBUILD_GTEST=ON
        -DBUILD_GMOCK=ON
    PREFIX "${CMAKE_CURRENT_BINARY_DIR}"
    BUILD_BYPRODUCTS "${GTEST_DIR}/gtest_tp-build/googlemock/libgmock_main.a"
    # Disable install step
    INSTALL_COMMAND ""
    )

# Specify include dir
externalproject_get_property(gtest_tp SOURCE_DIR)
set(GTEST_INCLUDE_DIRS ${SOURCE_DIR}/googletest/include PARENT_SCOPE)
set(GMOCK_INCLUDE_DIRS ${SOURCE_DIR}/googlemock/include PARENT_SCOPE)

# Specify MainTest's link libraries
externalproject_get_property(gtest_tp BINARY_DIR)
set(GMOCK_LIBS_DIR ${BINARY_DIR}/googlemock)

add_library(gmock_main STATIC IMPORTED GLOBAL)
set_target_properties(
    gmock_main
    PROPERTIES
        IMPORTED_LINK_INTERFACE_LANGUAGES CXX
        IMPORTED_LOCATION ${GMOCK_LIBS_DIR}/libgmock_main.a
)

set(GTEST_HARNESS_LIBS gmock_main Threads::Threads PARENT_SCOPE)
