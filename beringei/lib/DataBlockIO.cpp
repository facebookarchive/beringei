/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "DataBlockIO.h"

#include "BucketStorage.h"
#include "GorillaStatsManager.h"

#include <folly/compression/Compression.h>
#include <folly/io/IOBuf.h>
#include <folly/synchronization/CallOnce.h>

namespace facebook {
namespace gorilla {

namespace {

constexpr union {
  uint32_t i;
  char c[4];
} dataBlockMagic = {.c = {'D', 'A', 'T', 'A'}};

// see
// https://stackoverflow.com/questions/9050260/what-does-a-zlib-header-look-like
constexpr size_t CMF = 0;
constexpr size_t FLG = 1;
static_assert(
    (static_cast<uint8_t>(dataBlockMagic.c[CMF]) * 256 +
     static_cast<uint8_t>(dataBlockMagic.c[FLG])) %
            31 !=
        0,
    "new file format must not be misinterpreted as old zlib compressed data");

struct DataBlockHeader {
  explicit DataBlockHeader(DataBlockVersion versionArg)
      : magic_(dataBlockMagic.i), version_(static_cast<int32_t>(versionArg)) {}

  DataBlockHeader(const char* data, size_t len);

  bool valid() const;

  // @pre this->valid()
  const void* data() const;
  // @pre this->valid()
  size_t length() const;
  // @return whether the entire payload should be compressed
  // @pre this->valid()
  bool compress() const;

 private:
  uint32_t magic_;
  // Defer conversion until retrieval to preserve unknown values
  int32_t version_;
};

const std::string kDataPrefix = "block_data";

// These files are only used as marker files to indicate which
// blocks have been completed. The files are empty but the file name
// has the id of the completed block.
const std::string kCompletePrefix = "complete_block";

const size_t kLargeFileBuffer = 1024 * 1024;

// Byte size difference in compressed files
const std::string kBucketCompressedDelta = "bucket_compressed_delta";
// Percent of original size
const std::string kBucketCompressedPercent = "bucket_compressed_percent";

template <typename T>
T fromUnaligned(const void* ptr) {
  T ret;
  memcpy(&ret, ptr, sizeof(ret));
  return ret;
}

DataBlockHeader::DataBlockHeader(const char* data, size_t length)
    : magic_(), version_(static_cast<int32_t>(DataBlockVersion::V_UNKNOWN)) {
  if (length >= sizeof(*this)) {
    magic_ = fromUnaligned<uint32_t>(data);
    version_ = fromUnaligned<uint32_t>(data + sizeof(uint32_t));
  }
}

bool DataBlockHeader::valid() const {
  return (
      magic_ == dataBlockMagic.i &&
      static_cast<int32_t>(DataBlockVersion::V_0) <= version_ &&
      version_ < static_cast<int32_t>(DataBlockVersion::V_MAX));
}

const void* DataBlockHeader::data() const {
  return this;
}

size_t DataBlockHeader::length() const {
  CHECK_GE(version_, static_cast<int32_t>(DataBlockVersion::V_0));
  CHECK_LT(version_, static_cast<int32_t>(DataBlockVersion::V_MAX));
  return (
      version_ == static_cast<int32_t>(DataBlockVersion::V_0) ? 0
                                                              : sizeof(*this));
}

bool DataBlockHeader::compress() const {
  CHECK_GE(version_, static_cast<int32_t>(DataBlockVersion::V_0));
  CHECK_LT(version_, static_cast<int32_t>(DataBlockVersion::V_MAX));
  return version_ == static_cast<int32_t>(DataBlockVersion::V_0);
}
} // namespace

DataBlockIO::DataBlockIO(
    int64_t shardId,
    const std::string& dataDirectory,
    DataBlockVersion writeVersion)
    : writeVersion_(writeVersion),
      dataFiles_(shardId, kDataPrefix, dataDirectory),
      completeFiles_(shardId, kCompletePrefix, dataDirectory) {
  static folly::once_flag flag;
  folly::call_once(flag, [&]() {
    GorillaStatsManager::addStatExportType(kBucketCompressedDelta, SUM);
    GorillaStatsManager::addStatExportType(kBucketCompressedDelta, AVG);
    GorillaStatsManager::addStatExportType(kBucketCompressedPercent, AVG);
  });
}

std::vector<std::unique_ptr<DataBlock>> DataBlockIO::readBlocks(
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

  const DataBlockHeader header(buffer.get(), len);

  std::unique_ptr<folly::IOBuf> uncompressed;
  if (header.valid()) {
    uncompressed = folly::IOBuf::wrapBuffer(buffer.get(), len);
  } else {
    try {
      auto codec = folly::io::getCodec(folly::io::CodecType::ZLIB);
      auto ioBuffer = folly::IOBuf::wrapBuffer(buffer.get(), len);
      uncompressed = codec->uncompress(ioBuffer.get());
      uncompressed->coalesce();
    } catch (std::exception& e) {
      LOG(ERROR) << e.what();
      return pointers;
    }
  }

  if (uncompressed->length() < sizeof(uint32_t) + sizeof(uint32_t)) {
    LOG(ERROR) << "Not enough data";
    return pointers;
  }

  const char* ptr = (const char*)uncompressed->data();
  uint32_t count;
  uint32_t activePages;
  uint32_t headerLength = header.valid() ? header.length() : 0;

  ptr += headerLength;
  memcpy(&count, ptr, sizeof(uint32_t));
  ptr += sizeof(uint32_t);
  memcpy(&activePages, ptr, sizeof(uint32_t));
  ptr += sizeof(uint32_t);

  size_t expectedLength = headerLength + sizeof(uint32_t) + sizeof(uint32_t) +
      count * sizeof(uint32_t) + count * sizeof(uint64_t) +
      activePages * BucketStorage::kPageSize;

  if (uncompressed->length() != expectedLength) {
    LOG(ERROR) << "Corrupt data file: expected " << expectedLength
               << " bytes, got " << uncompressed->length() << " bytes.";
    return pointers;
  }

  timeSeriesIds.resize(count);
  storageIds.resize(count);
  memcpy(timeSeriesIds.data(), ptr, count * sizeof(uint32_t));
  ptr += count * sizeof(uint32_t);
  memcpy(storageIds.data(), ptr, count * sizeof(uint64_t));
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

void DataBlockIO::write(
    uint32_t position,
    const std::vector<std::shared_ptr<DataBlock>>& pages,
    uint32_t activePages,
    const std::vector<uint32_t>& timeSeriesIds,
    const std::vector<uint64_t>& storageIds) {
  CHECK_EQ(timeSeriesIds.size(), storageIds.size());
  CHECK_GE(
      static_cast<int32_t>(writeVersion_),
      static_cast<int32_t>(DataBlockVersion::V_0));
  CHECK_LT(
      static_cast<int32_t>(writeVersion_),
      static_cast<int32_t>(DataBlockVersion::V_MAX));

  auto dataFile = dataFiles_.open(position, "wb", kLargeFileBuffer);
  if (!dataFile.file) {
    LOG(ERROR) << "Opening data block file:" << dataFile.name << " failed";
    return;
  }

  const DataBlockHeader header(writeVersion_);
  uint32_t count = timeSeriesIds.size();
  size_t dataLen = header.length() + sizeof(uint32_t) + // count
      sizeof(uint32_t) + // active pages
      count * sizeof(uint32_t) + // time series ids
      count * sizeof(uint64_t) + // storage ids
      activePages * kDataBlockSize; // blocks

  std::unique_ptr<char[]> buffer(new char[dataLen]);
  char* ptr = buffer.get();
  memcpy(ptr, header.data(), header.length());
  ptr += header.length();

  memcpy(ptr, &count, sizeof(uint32_t));
  ptr += sizeof(uint32_t);
  memcpy(ptr, &activePages, sizeof(uint32_t));
  ptr += sizeof(uint32_t);

  memcpy(ptr, timeSeriesIds.data(), sizeof(uint32_t) * count);
  ptr += sizeof(uint32_t) * count;

  memcpy(ptr, storageIds.data(), sizeof(uint64_t) * count);
  ptr += sizeof(uint64_t) * count;

  for (int i = 0; i < activePages; i++) {
    memcpy(ptr, pages[i]->data, kDataBlockSize);
    ptr += kDataBlockSize;
  }

  CHECK_EQ(ptr - buffer.get(), dataLen);

  try {
    auto ioBuffer = folly::IOBuf::wrapBuffer(buffer.get(), dataLen);

    if (header.compress()) {
      auto codec = folly::io::getCodec(
          folly::io::CodecType::ZLIB, folly::io::COMPRESSION_LEVEL_BEST);
      auto compressed = codec->compress(ioBuffer.get());
      compressed->coalesce();

      GorillaStatsManager::addStatValue(
          kBucketCompressedDelta, ioBuffer->length() - compressed->length());
      GorillaStatsManager::addStatValue(
          kBucketCompressedDelta,
          compressed->length() * 100 / ioBuffer->length());

      ioBuffer = std::move(compressed);
    }

    if (fwrite(
            ioBuffer->data(),
            sizeof(char),
            ioBuffer->length(),
            dataFile.file) != ioBuffer->length()) {
      PLOG(ERROR) << "Writing data block file " << dataFile.name << " failed";
      FileUtils::closeFile(dataFile, false);
      return;
    }

    LOG(INFO) << "Wrote data block file " << dataFile.name
              << " dataLen:" << dataLen << " compressed:" << ioBuffer->length();

  } catch (std::exception& e) {
    LOG(ERROR) << e.what();
    FileUtils::closeFile(dataFile, false);
    return;
  }

  FileUtils::closeFile(dataFile, false);

  auto completeFile = completeFiles_.open(position, "wb", 0);
  if (!completeFile.file) {
    LOG(ERROR) << "Opening marker file " << completeFile.name << " failed";
    return;
  }
  FileUtils::closeFile(completeFile, false);
}

std::set<uint32_t> DataBlockIO::findCompletedBlockFiles() const {
  const std::vector<int64_t> files = completeFiles_.ls();
  std::set<uint32_t> completedBlockFiles(files.begin(), files.end());

  return completedBlockFiles;
}

void DataBlockIO::clearTo(int64_t position) {
  completeFiles_.clearTo(position);
  dataFiles_.clearTo(position);
}

void DataBlockIO::createDirectories() {
  dataFiles_.createDirectories();
  completeFiles_.createDirectories();
}

void DataBlockIO::remove(int64_t position) {
  dataFiles_.remove(position);
  completeFiles_.remove(position);
}

} // namespace gorilla
} // namespace facebook
