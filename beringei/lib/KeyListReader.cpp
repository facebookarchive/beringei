// Copyright 2004-present Facebook. All Rights Reserved.

#include "beringei/lib/KeyListReader.h"

#include <folly/compression/Compression.h>
#include <folly/io/IOBuf.h>

#include "beringei/lib/FileUtils.h"

namespace facebook {
namespace gorilla {

const static int kTempFileId = 0;
const static std::string kFileType = "key_list";
const static std::string kFailedCounter = "failed_writes." + kFileType;

// Marker bytes to determine if the file is compressed or not and if
// there are categories or not.
const static char kCompressedFileMarker = 'C';
const static char kUncompressedFileMarker = 'U';
const static char kCompressedFileWithCategoriesMarker = '0';
const static char kUncompressedFileWithCategoriesMarker = '1';
const static char kCompressedFileWithTimestampsMarker = '2';
const static char kUncompressedFileWithTimestampsMarker = '3';

LocalKeyReader::LocalKeyReader(int64_t shardId, const std::string& directory)
    : shardId_(shardId), dataDirectory_(directory) {}

ssize_t LocalKeyReader::readKeys(KeyReaderCallbackWithSeq f) const {
  LOG(INFO) << "Reading keys from shard " << shardId_;
  FileUtils files(shardId_, kFileType, dataDirectory_);

  // Read all the keys from all the relevant files.
  std::vector<int64_t> ids = files.ls();
  size_t keys = 0;
  for (int64_t fileId : ids) {
    // Ignore leftover files from a failed call to compact().
    if (fileId == kTempFileId) {
      continue;
    }

    auto file = files.open(fileId, "rb", 0);
    if (!file.file) {
      LOG(ERROR) << "Opening file failed: " << file.name;
      continue;
    }

    // Read the entire file.
    fseek(file.file, 0, SEEK_END);
    size_t len = ftell(file.file);

    if (len <= 1) {
      fclose(file.file);
      continue;
    }

    std::unique_ptr<char[]> buffer(new char[len]);
    fseek(file.file, 0, SEEK_SET);
    if (fread(buffer.get(), 1, len, file.file) != len) {
      PLOG(ERROR) << "Failed to read " << file.name;
      fclose(file.file);
      continue;
    }

    // Local key log has **no** operation marker, since it's always appending.
    int keysFound = KeyReaderUtils::readKeys(
        buffer.get(), len, KeyReaderUtils::translateCallback(f, 0), false);
    if (keysFound == 0) {
      LOG(ERROR) << file.name << " contains no valid data";
    }
    keys += keysFound;
    fclose(file.file);
  }

  LOG(INFO) << "Read " << keys << " keys from " << ids.size()
            << " files for shard " << shardId_;
  return keys;
}

KeyReaderCallbackWithSeq KeyReaderUtils::translateCallback(
    KeyReaderCallback f) {
  return [f](uint32_t id,
             const char* key,
             uint16_t category,
             int32_t unixTime,
             bool isAppend,
             uint64_t) mutable -> bool {
    return f(id, key, category, unixTime, isAppend);
  };
}

KeyReaderCallback KeyReaderUtils::translateCallback(
    KeyReaderCallbackWithSeq f,
    uint64_t seq) {
  return [f, seq](
             uint32_t id,
             const char* key,
             uint16_t category,
             int32_t unixTime,
             bool isAppend) mutable -> bool {
    return f(id, key, category, unixTime, isAppend, seq);
  };
}

size_t KeyReaderUtils::readKeys(
    const char* buffer,
    size_t len,
    facebook::gorilla::KeyReaderCallback f,
    bool hasDelete) {
  size_t keysFound = 0;
  if (buffer[0] == kCompressedFileMarker ||
      buffer[0] == kCompressedFileWithCategoriesMarker ||
      buffer[0] == kCompressedFileWithTimestampsMarker) {
    try {
      auto codec = folly::io::getCodec(
          folly::io::CodecType::ZLIB, folly::io::COMPRESSION_LEVEL_BEST);
      auto ioBuffer = folly::IOBuf::wrapBuffer(buffer + 1, len - 1);
      auto uncompressed = codec->uncompress(ioBuffer.get());

      // It's a chained buffer. This will make it a single buffer.
      uncompressed->coalesce();

      // Compressed buffer has no delete operations.
      keysFound = readKeysFromBuffer(
          (const char*)uncompressed->data(),
          uncompressed->length(),
          buffer[0] != kCompressedFileMarker,
          buffer[0] == kCompressedFileWithTimestampsMarker,
          f,
          false);
    } catch (std::exception& e) {
      LOG(ERROR) << "Uncompression failed: " << e.what();
    }
  } else if (
      buffer[0] == kUncompressedFileMarker ||
      buffer[0] == kUncompressedFileWithCategoriesMarker ||
      buffer[0] == kUncompressedFileWithTimestampsMarker) {
    keysFound = readKeysFromBuffer(
        buffer + 1,
        len - 1,
        buffer[0] != kUncompressedFileMarker,
        buffer[0] == kUncompressedFileWithTimestampsMarker,
        f,
        hasDelete);
  } else {
    LOG(ERROR) << "Unknown marker byte " << buffer[0];
  }

  return keysFound;
}

size_t KeyReaderUtils::readKeysFromBuffer(
    const char* buffer,
    size_t len,
    bool categoryPresent,
    bool timestampPresent,
    std::function<bool(uint32_t, const char*, uint16_t, int32_t, bool)> f,
    bool hasDelete) {
  // Back up until the buffer ends with a zero byte.
  // This should come from the last byte in a string, but it could be a byte
  // in an id.
  while (buffer[len - 1] != '\0') {
    len--;
    if (len == 0) {
      return 0;
    }
  }

  size_t keys = 0;

  size_t minRecordLength = sizeof(uint32_t) + 1;
  if (categoryPresent) {
    minRecordLength += sizeof(uint16_t);
  }
  if (timestampPresent) {
    minRecordLength += sizeof(uint32_t);
  }

  // Read the records one-by-one until too few bytes remain.
  // A minimum record is an uint32 (+uint16) (+uint32) and a zero-length
  // string.
  const char* pos = buffer;
  const char* endPos = pos + len - minRecordLength;
  uint16_t defaultCategory = 0;
  int32_t defaultTimestamp = 0;
  while (pos <= endPos) {
    uint32_t* id;
    const char* key;
    uint16_t* category = &defaultCategory;
    int32_t* timestamp = &defaultTimestamp;

    // By default, it's always appending operation. However, if there might be
    // delete operation, it would be encoded as the first character of every
    // message.
    bool isAppend = true;

    if (hasDelete) {
      // Operation is encoded as the first byte of every message.
      char op = *pos;
      if (op == 'd') {
        isAppend = false;
      }
      ++pos;
    }

    id = (uint32_t*)pos;
    pos += sizeof(uint32_t);
    if (categoryPresent) {
      category = (uint16_t*)pos;
      pos += sizeof(uint16_t);
    }
    if (timestampPresent) {
      timestamp = (int32_t*)pos;
      pos += sizeof(int32_t);
    }
    key = pos;

    if (!f(*id, key, *category, *timestamp, isAppend)) {
      // Callback doesn't accept more keys.
      break;
    }
    keys++;
    pos += strlen(key) + 1;
  }

  return keys;
}

std::unique_ptr<KeyListReaderIf> LocalKeyListReaderFactory::getKeyReader(
    int64_t shardId,
    const std::string& directory) const {
  return std::make_unique<LocalKeyReader>(shardId, directory);
}

} // namespace gorilla
} // namespace facebook
