/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "FileUtils.h"

#include <dirent.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#include <thread>

#include <folly/String.h>
#include "gflags/gflags.h"
#include "glog/logging.h"

#include <boost/filesystem/convenience.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

#include "GorillaStatsManager.h"
#include "GorillaTimeConstants.h"
#include "Timer.h"

DEFINE_bool(
    gorilla_async_file_close,
    true,
    "Close files in an asynchronous thread");

namespace facebook {
namespace gorilla {

static const std::string kMsPerFileOpen = "ms_per_file_open";
static const std::string kFileOpenFailures = "file_open_failures";

FileUtils::FileUtils(
    int64_t shardId,
    const std::string& prefix,
    const std::string& dataDirectory)
    : directory_(dataDirectory), prefix_(prefix) {
  directory_ /= std::to_string(shardId);
}

FileUtils::File
FileUtils::open(int64_t id, const char* mode, size_t bufferSize) {
  boost::filesystem::path path = filePath(id);
  LOG(INFO) << "Opening file: " << path.c_str();

  Timer timer(true);
  FILE* file = fopen(path.c_str(), mode);
  GorillaStatsManager::addStatValue(
      kMsPerFileOpen, timer.get() / kGorillaUsecPerMs);
  if (file != nullptr) {
    if (bufferSize > 0) {
      setvbuf(file, nullptr, _IOFBF, bufferSize);
    } else {
      setvbuf(file, nullptr, _IONBF, 0);
    }
  } else {
    PLOG(ERROR) << "Failed to open file: " << path.c_str();
    GorillaStatsManager::addStatValue(kFileOpenFailures);
  }

  return File{file, path.c_str()};
}

void FileUtils::clearTo(int64_t id) {
  for (int64_t fileId : ls()) {
    if (fileId >= id) {
      return;
    }

    remove(fileId);
  }
}

void FileUtils::clearAll() {
  clearTo(std::numeric_limits<int64_t>::max());
}

std::vector<int64_t> FileUtils::ls() {
  std::vector<int64_t> files;

  boost::filesystem::directory_iterator end;
  for (boost::filesystem::directory_iterator it(directory_); it != end; it++) {
    if (it->path().stem().native() == prefix_) {
      try {
        int id = std::stoll(it->path().extension().native().substr(1));
        files.push_back(id);
      } catch (...) {
        LOG(ERROR) << "Couldn't parse filename " << it->path();
      }
    }
  }

  std::sort(files.begin(), files.end());
  return files;
}

void FileUtils::rename(int64_t from, int64_t to) {
  boost::filesystem::path fromPath = filePath(from);
  boost::filesystem::path toPath = filePath(to);

  boost::system::error_code ec;
  boost::filesystem::rename(fromPath, toPath, ec);
  if (ec) {
    LOG(ERROR) << "Move " << fromPath << " to " << toPath << " failed: " << ec;
  }
}

void FileUtils::rename(int64_t id, const std::string& toPrefix) {
  boost::filesystem::path fromPath = filePath(id);
  boost::filesystem::path toPath = filePath(id, toPrefix);

  boost::system::error_code ec;
  boost::filesystem::rename(fromPath, toPath, ec);
  if (ec) {
    LOG(ERROR) << "Move " << fromPath << " to " << toPath << " failed: " << ec;
  }
}

void FileUtils::createDirectories() {
  boost::filesystem::create_directories(directory_);
}

TemporaryDirectory::TemporaryDirectory(const char* dirnamePrefix) {
  static const char* mkdtempSuffix = ".XXXXXX";

  boost::filesystem::path tmpDir = boost::filesystem::temp_directory_path();

  // add one for "/" and one for string null terminator
  int tmpDirLen = tmpDir.string().size() + strlen(dirnamePrefix) +
      strlen(mkdtempSuffix) + 2;

  // this actually needs to be a char * (not a const char * and not a
  // std::string) to pass to mkdtemp, because mkdtemp mutates the template
  // string to create a randomized location
  char templateDirName[tmpDirLen];
  snprintf(
      templateDirName,
      tmpDirLen,
      "%s/%s%s",
      tmpDir.string().c_str(),
      dirnamePrefix,
      mkdtempSuffix);

  // mkdtemp does magic where it overwrites templateDirName with the finalized
  // value and then also returns it (thus dn) if it succeeds or null if failure
  char* dn = mkdtemp(templateDirName);
  dirname_ = templateDirName;
  PCHECK(dn != nullptr) << "mkdtemp(" << dirname_ << ")";
}

TemporaryDirectory::~TemporaryDirectory() {
  boost::filesystem::remove_all(dirname_);
}

boost::filesystem::path FileUtils::filePath(int64_t id) {
  return filePath(id, prefix_);
}

boost::filesystem::path FileUtils::filePath(
    int64_t id,
    const std::string& prefix) {
  return directory_ / (prefix + "." + std::to_string(id));
}

void FileUtils::remove(int64_t id) {
  boost::system::error_code ec;
  boost::filesystem::path path = filePath(id);
  boost::filesystem::remove(path, ec);
  if (ec) {
    LOG(WARNING) << "Unlink " << path << " failed: " << ec;
  }
}

std::string FileUtils::joinPaths(
    const std::string& path1,
    const std::string& path2,
    const std::string& path3) {
  boost::filesystem::path p(path1);
  p /= path2;
  p /= path3;
  return p.string();
}

void FileUtils::splitPath(
    const std::string& path,
    std::string* baseName,
    std::string* dirName) {
  size_t lastSlashIndex = path.find_last_of("/");
  if (std::string::npos == lastSlashIndex) {
    if (baseName != nullptr) {
      *baseName = path;
    }
    if (dirName != nullptr) {
      *dirName = "";
    }
    return;
  }
  if (baseName != nullptr) {
    *baseName = path;
    baseName->erase(0, lastSlashIndex + 1);
  }
  if (dirName != nullptr) {
    *dirName = path;
    if (lastSlashIndex == 0) {
      lastSlashIndex++;
    }
    dirName->erase(lastSlashIndex, dirName->length());
  }
}

bool FileUtils::isDirectory(const std::string& filename) {
  struct stat st;
  return !stat(filename.c_str(), &st) && S_ISDIR(st.st_mode) &&
      !access(filename.c_str(), R_OK);
}

void FileUtils::startMonitoring() {
  GorillaStatsManager::addStatExportType(kMsPerFileOpen, AVG);
  GorillaStatsManager::addStatExportType(kMsPerFileOpen, COUNT);
  GorillaStatsManager::addStatExportType(kFileOpenFailures, SUM);
}

void FileUtils::closeFile(FILE* file) {
  if (FLAGS_gorilla_async_file_close) {
    std::thread t([file]() { fclose(file); });
    t.detach();
  } else {
    fclose(file);
  }
}
}
} // facebook:gorilla
