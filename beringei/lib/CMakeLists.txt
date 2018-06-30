# Copyright (c) 2016-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

add_library(
    encoding STATIC

    BitUtil.cpp
    BitUtil.h
    TimeSeriesStream-inl.h
    TimeSeriesStream.cpp
    TimeSeriesStream.h
)
target_link_libraries(
    encoding

    beringei_thrift
    ${FOLLY_LIBRARIES}
)

add_library(
    beringei_core STATIC

    BucketLogWriter.cpp
    BucketLogWriter.h
    BucketMap.cpp
    BucketMap.h
    BucketStorage.cpp
    BucketStorage.h
    BucketedTimeSeries.cpp
    BucketedTimeSeries.h
    BucketUtils.cpp
    BucketUtils.h
    CaseUtils.cpp
    CaseUtils.h
    DataBlock.h
    DataBlockIO.cpp
    DataBlockIO.h
    DataLog.cpp
    DataLog.h
    FileUtils.cpp
    FileUtils.h
    GorillaDumperUtils.cpp
    GorillaDumperUtils.h
    GorillaStatsManager.cpp
    GorillaStatsManager.h
    GorillaTimeConstants.h
    KeyListWriter.cpp
    KeyListWriter.h
    MemoryUsageGuardIf.h
    NetworkUtils.cpp
    NetworkUtils.h
    PersistentKeyList.cpp
    PersistentKeyList.h
    ShardData.cpp
    ShardData.h
    SimpleMemoryUsageGuard.cpp
    SimpleMemoryUsageGuard.h
    TimeSeries.cpp
    TimeSeries.h
    Timer.cpp
    Timer.h
)
target_link_libraries(
    beringei_core

    encoding
    beringei_thrift
    ${FOLLY_LIBRARIES}
    Boost::filesystem
)

add_subdirectory(tests)
