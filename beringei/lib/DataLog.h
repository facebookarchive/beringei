/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <stdint.h>
#include <cstdio>
#include <functional>
#include <memory>
#include <vector>

#include "beringei/lib/DataLogUtil.h"
#include "beringei/lib/FileUtils.h"

namespace facebook {
namespace gorilla {

// class DataLogWriter
//
// This class appends data points to a log file.
class DataLogWriter {
 public:
  // Initialize a DataLogWriter which will append data to the given file.
  DataLogWriter(FileUtils::File&& out, int64_t baseTime);

  virtual ~DataLogWriter();

  // Appends a data point to the internal buffer. This operation is
  // not thread safe. Caller is responsible for locking. Data will be
  // written to disk when buffer is full or `flushBuffer` is called or
  // destructor is called.
  void append(uint32_t id, int64_t unixTime, double value);

  // Flushes the buffer that has been created with `append` calls to
  // disk. Returns true if writing was successful, false otherwise.
  bool flushBuffer();

 private:
  FileUtils::File out_;
  int64_t lastTimestamp_;
  std::unique_ptr<char[]> buffer_;
  size_t bufferSize_;
  std::vector<double> previousValues_;
};

class DataLogReader {
 public:
  // Pull all the data points from the file.
  // Returns the number of points read, or -1 if the file could not be read.
  // The callback should return false if reading should be stopped.
  static int readLog(
      const FileUtils::File& file,
      int64_t baseTime,
      std::function<bool(uint32_t, int64_t, double)>);
};

} // namespace gorilla
} // namespace facebook
