# Copyright (c) 2016-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

add_library(
    beringei_test_util STATIC

    TestKeyList.h
    TestKeyList.cpp
    TestDataLoader.h
    TestDataLoader.cpp
    TestMain.cpp
)
add_dependencies(beringei_test_util gtest_tp)

add_executable(
    beringei_core_test_bin

    MockMemoryUsageGuard.h
    BitUtilTest.cpp
    BucketLogWriterTest.cpp
    BucketStorageTest.cpp
    BucketedTimeSeriesTest.cpp
    CaseUtilsTest.cpp
    DataLogTest.cpp
    FileUtilsTest.cpp
    GorillaDumperUtilsTest.cpp
    KeyListWriterTest.cpp
    PersistentKeyListTest.cpp
    TimeSeriesStreamTest.cpp
    TimeSeriesTest.cpp
    TimerTest.cpp
)
add_executable(
    beringei_bucket_map_test_bin

    BucketMapTest.cpp
)

target_link_libraries(
    beringei_core_test_bin

    beringei_core
    beringei_test_util
    ${FOLLY_LIBRARIES}
    ${GTEST_HARNESS_LIBS}
    ${LIBGLOG_LIBRARY}
    ${GFLAGS_LIBRARIES}
)

target_link_libraries(
    beringei_bucket_map_test_bin

    beringei_core
    beringei_test_util
    ${FOLLY_LIBRARIES}
    ${GTEST_HARNESS_LIBS}
    ${LIBGLOG_LIBRARY}
    ${GFLAGS_LIBRARIES}
)

add_test(
  NAME beringei_lib_tests
  COMMAND ${CMAKE_CURRENT_BINARY_DIR}/beringei_core_test_bin
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
  NAME beringei_bucket_map_test
  COMMAND ${CMAKE_CURRENT_BINARY_DIR}/beringei_bucket_map_test_bin
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
