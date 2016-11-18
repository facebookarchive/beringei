# Copyright (c) 2016-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

set(BERINGEI_GRAFANA_SRCS
    DateUtils.h
    GrafanaServiceFactory.cpp
    GrafanaServiceFactory.h
    NotFoundHandler.cpp
    NotFoundHandler.h
    QueryHandler.h
    QueryHandler.cpp
    TestConnectionHandler.cpp
    TestConnectionHandler.h
)

set(BERINGEI_GRAFANA_SERVICE_SRCS
  BeringeiGrafanaServer.cpp
)

add_library(beringei_grafana_lib STATIC ${BERINGEI_GRAFANA_SRCS})
target_link_libraries(
    beringei_grafana_lib
    beringei_client
    beringei_plugin
    ${FBTHRIFT_LIBRARIES}
    ${FOLLY_LIBRARIES}
    ${GFLAGS_LIBRARIES}
    ${LIBGLOG_LIBRARIES}
    ${PROXYGEN_LIBRARIES}
    Threads::Threads
)

add_executable(beringei_grafana_service ${BERINGEI_GRAFANA_SERVICE_SRCS})

target_link_libraries(
    beringei_grafana_service
    beringei_grafana_lib
    ${FBTHRIFT_LIBRARIES}
    ${FOLLY_LIBRARIES}
    ${GFLAGS_LIBRARIES}
    ${LIBGLOG_LIBRARIES}
    ${PROXYGEN_LIBRARIES}
    Threads::Threads
)
