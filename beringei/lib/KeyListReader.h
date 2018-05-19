// Copyright 2004-present Facebook. All Rights Reserved.

#pragma once

#include <functional>
#include <memory>

namespace facebook {
namespace gorilla {

using KeyReaderCallback =
    std::function<bool(uint32_t, const char*, uint16_t, int32_t)>;

class KeyListReaderIf {
 public:
  virtual ~KeyListReaderIf() {}
  virtual size_t readKeys(KeyReaderCallback f) const = 0;
};

class LocalKeyReader : public KeyListReaderIf {
 public:
  LocalKeyReader(int64_t shardId, const std::string& directory);

  // Call f on each key in the list.
  // The callback should return false if reading should be stopped.
  size_t readKeys(KeyReaderCallback f) const override;

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
