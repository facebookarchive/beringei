# Copyright (c) 2016-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

set(BERINGEI_PLAIN_TEXT_SRCS
    DateUtils.h
    PlainTextServiceFactory.cpp
    PlainTextServiceFactory.h
    NotFoundHandler.cpp
    NotFoundHandler.h
    FetchHandler.h
    FetchHandler.cpp
    UpdateHandler.h
    UpdateHandler.cpp
    TestConnectionHandler.cpp
    TestConnectionHandler.h
)

set(BERINGEI_PLAIN_TEXT_SERVICE_SRCS
  BeringeiPlainTextServer.cpp
)

add_library(beringei_plain_text_lib STATIC ${BERINGEI_PLAIN_TEXT_SRCS})
target_link_libraries(
    beringei_plain_text_lib
    beringei_client
    beringei_plugin
    ${FBTHRIFT_LIBRARIES}
    ${FOLLY_LIBRARIES}
    ${GFLAGS_LIBRARIES}
    ${LIBGLOG_LIBRARIES}
    ${PROXYGEN_LIBRARIES}
    Threads::Threads
)

add_executable(beringei_plain_text_service ${BERINGEI_PLAIN_TEXT_SERVICE_SRCS})

target_link_libraries(
    beringei_plain_text_service
    beringei_plain_text_lib
    ${FBTHRIFT_LIBRARIES}
    ${FOLLY_LIBRARIES}
    ${GFLAGS_LIBRARIES}
    ${LIBGLOG_LIBRARIES}
    ${PROXYGEN_LIBRARIES}
    Threads::Threads
)
