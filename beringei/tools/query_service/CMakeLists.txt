# Copyright (c) 2016-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

set(BERINGEI_QUERY_SRCS
    QueryServiceFactory.cpp
    QueryServiceFactory.h
    NotFoundHandler.cpp
    NotFoundHandler.h
    QueryHandler.h
    QueryHandler.cpp
)

set(BERINGEI_QUERY_SERVICE_SRCS
  BeringeiQueryServer.cpp
)

add_library(beringei_query_lib STATIC ${BERINGEI_QUERY_SRCS})
target_link_libraries(
    beringei_query_lib
    beringei_client
    beringei_plugin
    ${FBTHRIFT_LIBRARIES}
    ${FOLLY_LIBRARIES}
    ${GFLAGS_LIBRARIES}
    ${LIBGLOG_LIBRARIES}
    ${PROXYGEN_LIBRARIES}
    Threads::Threads
)

add_executable(beringei_query_service ${BERINGEI_QUERY_SERVICE_SRCS})

target_link_libraries(
    beringei_query_service
    beringei_query_lib
    ${FBTHRIFT_LIBRARIES}
    ${FOLLY_LIBRARIES}
    ${GFLAGS_LIBRARIES}
    ${LIBGLOG_LIBRARIES}
    ${PROXYGEN_LIBRARIES}
    Threads::Threads
)
