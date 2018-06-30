/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <folly/String.h>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>

#include "gflags/gflags.h"

DECLARE_bool(gorilla_async_file_close);

namespace facebook {
namespace gorilla {

// class FileUtils
//
// Useful methods for managing groups of files of the form
// /path/to/data/shardId/prefix.XXXX
class FileUtils {
 public:
  struct File {
    FILE* file;
    std::string name;
  };

  FileUtils(
      int64_t shardId,
      const std::string& prefix,
      const std::string& dataDirectory);

  // Get the file with the given id.
  // Does fully-buffered IO with a buffer of the given size or unbuffered
  // (direct) IO if bufferSize is 0.
  // Returns nullptr on failure.
  File open(int64_t id, const char* mode, size_t bufferSize) const;

  // Remove all files with id less than this.
  void clearTo(int64_t id);

  void clearAll();

  // Get the sorted list of valid ids for this prefix.
  std::vector<int64_t> ls() const;

  // Replace a file with another.
  void rename(int64_t from, int64_t to);

  // Rename with another prefix. The id will stay the same.
  void rename(int64_t id, const std::string& toPrefix);

  // Creates directories. Must be called before other file operations
  // are used.
  void createDirectories() const;

  // Remove a file with the given id.
  void remove(int64_t id);

  static std::string joinPaths(
      const std::string& path1,
      const std::string& path2,
      const std::string& path3 = "");

  static void splitPath(
      const std::string& path,
      std::string* baseName,
      std::string* dirName);

  static bool isDirectory(const std::string& filename);

  static void startMonitoring();

  // Creates a thread and closes files asynchronously. This is useful
  // in cases where flushing out the files to disk takes a while and
  // the caller doesn't care when this is actually happens.
  static void closeFile(File& file, bool asyncClose);

 private:
  boost::filesystem::path filePath(int64_t id) const;
  boost::filesystem::path filePath(int64_t id, const std::string& prefix) const;

  boost::filesystem::path directory_;
  std::string prefix_;
};

/**
 * Create a temporary directory, recursively deleting it on exit (if
 * requested).
 */
class TemporaryDirectory {
 public:
  /**
   * Create a temporary directory in directory "dir" (maybe NULL for a
   * system-specific default; on Unix, we look at the environment variable
   * TMPDIR first, and if that doesn't exist, we try /tmp).
   * The directory name will start with dirname_prefix.
   *
   * If "keep" is true, we ensure that the directory will survive the
   * destruction of the TemporaryFile object.
   */
  explicit TemporaryDirectory(const char* dirnamePrefix);
  ~TemporaryDirectory();

  std::string dirname() const {
    return dirname_;
  }

 private:
  std::string dirname_;
};
} // namespace gorilla
} // namespace facebook
