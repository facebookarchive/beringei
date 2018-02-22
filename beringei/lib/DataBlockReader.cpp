/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "DataBlockReader.h"

#include "BucketStorage.h"

#ifndef BERINGEI_AUTOSETUP
#include <folly/compression/Compression.h>
#else
#include <folly/io/Compression.h>
#endif

#include <folly/io/IOBuf.h>

namespace facebook {
namespace gorilla {

DataBlockReader::DataBlockReader(
    int64_t shardId,
    const std::string& dataDirectory)
    : dataFiles_(shardId, BucketStorage::kDataPrefix, dataDirectory),
      completedFiles_(shardId, BucketStorage::kCompletePrefix, dataDirectory) {}

std::vector<std::unique_ptr<DataBlock>> DataBlockReader::readBlocks(
    uint32_t position,
    std::vector<uint32_t>& timeSeriesIds,
    std::vector<uint64_t>& storageIds) {
  std::vector<std::unique_ptr<DataBlock>> pointers;

  auto f = dataFiles_.open(position, "rb", 0);
  if (!f.file) {
    LOG(ERROR) << "Could not open block file for reading : " << position;
    return pointers;
  }

  fseek(f.file, 0, SEEK_END);
  size_t len = ftell(f.file);
  if (len == 0) {
    LOG(WARNING) << "Empty data file " << f.name;
    fclose(f.file);
    return pointers;
  }

  fseek(f.file, 0, SEEK_SET);
  std::unique_ptr<char[]> buffer(new char[len]);
  int bytesRead = fread(buffer.get(), sizeof(char), len, f.file);

  if (bytesRead != len) {
    PLOG(ERROR) << "Could not read metadata from " << f.name;
    fclose(f.file);
    return pointers;
  }
  fclose(f.file);

  std::unique_ptr<folly::IOBuf> uncompressed;
  try {
    auto codec = folly::io::getCodec(folly::io::CodecType::ZLIB);
    auto ioBuffer = folly::IOBuf::wrapBuffer(buffer.get(), len);
    uncompressed = codec->uncompress(ioBuffer.get());
    uncompressed->coalesce();
  } catch (std::exception& e) {
    LOG(ERROR) << e.what();
    return pointers;
  }

  if (uncompressed->length() < sizeof(uint32_t) + sizeof(uint32_t)) {
    LOG(ERROR) << "Not enough data";
    return pointers;
  }

  const char* ptr = (const char*)uncompressed->data();
  uint32_t count;
  uint32_t activePages;
  memcpy(&count, ptr, sizeof(uint32_t));
  ptr += sizeof(uint32_t);
  memcpy(&activePages, ptr, sizeof(uint32_t));
  ptr += sizeof(uint32_t);

  size_t expectedLength = sizeof(uint32_t) + sizeof(uint32_t) +
      count * sizeof(uint32_t) + count * sizeof(uint64_t) +
      activePages * BucketStorage::kPageSize;

  if (uncompressed->length() != expectedLength) {
    LOG(ERROR) << "Corrupt data file: expected " << expectedLength
               << " bytes, got " << uncompressed->length() << " bytes.";
    return pointers;
  }

  timeSeriesIds.resize(count);
  storageIds.resize(count);
  memcpy(&timeSeriesIds[0], ptr, count * sizeof(uint32_t));
  ptr += count * sizeof(uint32_t);
  memcpy(&storageIds[0], ptr, count * sizeof(uint64_t));
  ptr += count * sizeof(uint64_t);

  // Reorganize into individually allocated blocks because
  // BucketStorage doesn't know how to deal with a single pointer.
  for (int i = 0; i < activePages; i++) {
    pointers.emplace_back(new DataBlock);
    memcpy(pointers.back()->data, ptr, BucketStorage::kPageSize);
    ptr += BucketStorage::kPageSize;
  }

  return pointers;
}

std::set<uint32_t> DataBlockReader::findCompletedBlockFiles() {
  const std::vector<int64_t> files = completedFiles_.ls();
  std::set<uint32_t> completedBlockFiles(files.begin(), files.end());

  return completedBlockFiles;
}
}
} // facebook:gorilla
