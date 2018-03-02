// Copyright 2004-present Facebook. All Rights Reserved.

#pragma once

#include <cstddef>
#include <functional>
#include <memory>

namespace facebook {
namespace gorilla {

using DataPointCallback =
    std::function<void(uint32_t, int64_t, double, uint32_t&, int64_t&)>;

/// Reader for all data points from the log.
class LogReader {
 public:
  virtual ~LogReader() {}

  /// Read all data points from the log from lastBlock onwards.
  /// @param[in] lastBlock Where to start reading data points from.
  /// @param[out] lastTimestamp The last timestamp read from the log.
  /// @param[out] unknownKeys Number of unknown keys from the logs.
  virtual void readLog(
      uint32_t lastBlock,
      int64_t& lastTimestamp,
      uint32_t& unknownKeys) = 0;
};

/// Read log from a local directory.
class LocalLogReader : public LogReader {
 public:
  LocalLogReader(
      uint32_t shardId,
      const std::string& dataDir,
      int64_t windowSize,
      DataPointCallback&& func);

  /// @see LogReader.
  void readLog(
      uint32_t lastBlock,
      int64_t& lastTimestamp,
      uint32_t& unknownKeys) override;

 private:
  uint32_t shardId_;
  std::string dataDirectory_;
  int64_t windowSize_;
  DataPointCallback cb_;
};

class LogReaderFactory {
 public:
  virtual ~LogReaderFactory() {}
  virtual std::unique_ptr<LogReader> getLogReader(
      uint32_t shardId,
      int64_t windowSize,
      DataPointCallback&& func) const = 0;
};

class LocalLogReaderFactory : public LogReaderFactory {
 public:
  explicit LocalLogReaderFactory(const std::string& dir);
  virtual std::unique_ptr<LogReader> getLogReader(
      uint32_t shardId,
      int64_t windowSize,
      DataPointCallback&& func) const override;

 private:
  std::string dataDir_;
};

} // namespace gorilla
} // namespace facebook
