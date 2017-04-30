# Copyright (c) 2016-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

add_executable(
    beringei_service_test_bin

    BeringeiServiceHandlerTest.cpp
    TestMain.cpp
)
add_dependencies(beringei_service_test_bin gtest_tp)

target_link_libraries(
    beringei_service_test_bin
    beringei_core
    beringei_service
    beringei_plugin
    beringei_client
    ${FOLLY_LIBRARIES}
    ${GTEST_HARNESS_LIBS}
    ${GMOCK_HARNESS_LIBS}
    ${LIBGLOG_LIBRARY}
    ${GFLAGS_LIBRARIES}
)

add_test(beringei_service_tests beringei_service_test_bin)
