# Copyright (c) 2016-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

add_library(
    beringei_service STATIC

    BeringeiServiceHandler.h
    BeringeiServiceHandler.cpp
)
target_link_libraries(
    beringei_service

    beringei_core
    beringei_client
    beringei_thrift
    ${FOLLY_LIBRARIES}
)

add_executable(
    beringei_main

    BeringeiMain.cpp
)
target_link_libraries(
    beringei_main

    beringei_plugin
    beringei_service
    ${FBTHRIFT_LIBRARIES}
    ${GFLAGS_LIBRARIES}
    ${LIBGLOG_LIBRARIES}
    Threads::Threads
)

add_subdirectory(tests)
