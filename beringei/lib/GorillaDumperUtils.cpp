/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "beringei/lib/GorillaDumperUtils.h"

#include <folly/io/IOBuf.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

using apache::thrift::BinarySerializer;

namespace facebook {
namespace gorilla {

void GorillaDumperUtils::readTimeSeriesBlock(
    TimeSeriesBlock& block,
    const std::string& bin) {
  auto buf = folly::IOBuf::wrapBuffer(&bin[0], bin.length());
  BinarySerializer::deserialize<TimeSeriesBlock>(buf.get(), block);
}

void GorillaDumperUtils::writeTimeSeriesBlock(
    const TimeSeriesBlock& block,
    std::string& bin) {
  auto out = folly::IOBufQueue();
  BinarySerializer::serialize<TimeSeriesBlock>(block, &out);
  bin = ((folly::StringPiece)out.move()->coalesce()).str();
}
}
} // facebook::gorilla
