// Copyright 2004-present Facebook. All Rights Reserved.

#pragma once

#include <functional>
#include <memory>

namespace facebook {
namespace gorilla {

// (id, key, category, first timestamp, isAppend, seq number) -> should stop.
using KeyReaderCallback =
    std::function<bool(uint32_t, const char*, uint16_t, int32_t, bool)>;
using KeyReaderCallbackWithSeq = std::function<
    bool(uint32_t, const char*, uint16_t, int32_t, bool, uint64_t)>;

class KeyListReaderIf {
 public:
  virtual ~KeyListReaderIf() {}
  virtual ssize_t readKeys(KeyReaderCallbackWithSeq f) const = 0;
  virtual ssize_t
  streamKeys(KeyReaderCallbackWithSeq, std::atomic<bool>&, uint64_t) const {
    return 0;
  }
};

// Utility class to read keys.
class KeyReaderUtils {
 public:
  static size_t readKeys(
      const char* buffer,
      size_t len,
      KeyReaderCallback f,
      bool hasDelete = false);

  static size_t readKeysFromBuffer(
      const char* buffer,
      size_t len,
      bool categoryPresent,
      bool timestampPresent,
      KeyReaderCallback f,
      bool hasDelete = false);

  static KeyReaderCallback translateCallback(
      KeyReaderCallbackWithSeq f,
      uint64_t seq);

  static KeyReaderCallbackWithSeq translateCallback(KeyReaderCallback f);
};

class LocalKeyReader : public KeyListReaderIf {
 public:
  LocalKeyReader(int64_t shardId, const std::string& directory);

  // Call f on each key in the list.
  // The callback should return false if reading should be stopped.
  ssize_t readKeys(KeyReaderCallbackWithSeq f) const override;

 private:
  // Appends id, key, category to the given buffer. The buffer can be
  // later written to disk. Does not clear the buffer before
  // appending.
  static size_t readKeysFromBuffer(
      const char* buffer,
      size_t len,
      bool categoryPresent,
      bool timestampPresent,
      std::function<bool(uint32_t, const char*, uint16_t, int32_t)> f);

  int64_t shardId_;
  std::string dataDirectory_;
};

class KeyListReaderFactory {
 public:
  virtual ~KeyListReaderFactory() {}
  virtual std::unique_ptr<KeyListReaderIf> getKeyReader(
      int64_t shardId,
      const std::string& directory) const = 0;
};

class LocalKeyListReaderFactory : public KeyListReaderFactory {
 public:
  std::unique_ptr<KeyListReaderIf> getKeyReader(
      int64_t shardId,
      const std::string& directory) const override;
};

} // namespace gorilla
} // namespace facebook
