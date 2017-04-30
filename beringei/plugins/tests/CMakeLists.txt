# Copyright (c) 2016-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

add_executable(
    beringei_plugin_test_bin

    BeringeiConfigurationAdapterTest.cpp
    BeringeiConfigurationTest.cpp
    BeringeiConfigurationValidationTest.cpp
    TestMain.cpp
)
add_dependencies(beringei_plugin_test_bin gtest_tp)

target_link_libraries(
    beringei_plugin_test_bin

    beringei_thrift
    beringei_plugin
    ${FOLLY_LIBRARIES}
    ${GTEST_HARNESS_LIBS}
    ${LIBGLOG_LIBRARY}
    ${GFLAGS_LIBRARIES}
)

add_test(
  NAME beringei_plugin_tests
  COMMAND ${CMAKE_CURRENT_BINARY_DIR}/beringei_plugin_test_bin
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
