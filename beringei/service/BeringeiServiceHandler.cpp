/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "BeringeiServiceHandler.h"

#include <algorithm>
#include <iostream>

#include <folly/Random.h>
#include <folly/experimental/FunctionScheduler.h>
#include "beringei/lib/BucketLogWriter.h"
#include "beringei/lib/BucketMap.h"
#include "beringei/lib/BucketStorage.h"
#include "beringei/lib/FileUtils.h"
#include "beringei/lib/GorillaStatsManager.h"
#include "beringei/lib/GorillaTimeConstants.h"
#include "beringei/lib/KeyListWriter.h"
#include "beringei/lib/NetworkUtils.h"
#include "beringei/lib/TimeSeries.h"
#include "beringei/lib/Timer.h"

DEFINE_int32(shards, 100, "Number of maps to use");
DEFINE_int32(buckets, 13, "Number of historical buckets to use");
DEFINE_int32(
    bucket_size,
    2 * facebook::gorilla::kGorillaSecondsPerHour,
    "Size of each bucket in seconds");
DEFINE_int32(
    allowed_timestamp_ahead,
    facebook::gorilla::kGorillaSecondsPerMinute,
    "Number of seconds a timestamp is allowed to be ahead of the current time");
DEFINE_int32(
    allowed_timestamp_behind,
    15 * facebook::gorilla::kGorillaSecondsPerMinute,
    "Number of seconds a timestamp is allowed to be behind current time");
DEFINE_string(
    data_directory,
    "/tmp/gorilla_data",
    "Directory in which to store time series");
DEFINE_int32(add_shard_threads, 32, "The number of threads for adding shards");
DEFINE_int32(
    key_writer_queue_size,
    500000,
    "The size of queue for each key writer thread. Set this extremely high if "
    "starting the service from scratch with no persistent data.");
DEFINE_int32(key_writer_threads, 2, "The number of key writer threads");
DEFINE_int32(
    log_writer_queue_size,
    1000,
    "The size of queue for each log writer thread");
DEFINE_int32(log_writer_threads, 8, "The number of log writer threads");
DEFINE_int32(
    block_writer_threads,
    4,
    "The number of threads for writing completed blocks");
DEFINE_bool(
    create_directories,
    false,
    "Creates data directories for each shard on startup");
DEFINE_int64(
    sleep_between_bucket_finalization_secs,
    600, // 10 min
    "Time to sleep between finalizing buckets");
DEFINE_bool(
    disable_shard_refresh,
    false,
    "Disable shard refresh thread. Primarily used by tests, affects default "
    "shard map ownership assumptions.");

namespace facebook {
namespace gorilla {

const static std::string kPurgedTimeSeries = ".purged_time_series";
const static std::string kPurgedTimeSeriesInCategoryPrefix =
    ".purged_time_series_in_category_";
const int kPurgeInterval = facebook::gorilla::kGorillaSecondsPerHour;
const static std::string kTooSlowToFinalizeBuckets =
    ".too_slow_to_finalize_buckets";
const static std::string kMsPerFinalizeShardBucket =
    ".ms_per_finalize_shard_bucket";
const static std::string kUsPerGet = ".us_per_get";
const static std::string kUsPerGetPerKey = ".us_per_get_per_key";
const static std::string kUsPerPut = ".us_per_put";
const static std::string kUsPerPutPerKey = ".us_per_put_per_key";
const static std::string kKeysPut = ".keys_put";
const static std::string kKeysGot = ".keys_got";
static const std::string kMissingTooMuchData = ".status_missing_too_much_data";
const static std::string kNewKeys = ".new_keys";
const static std::string kDatapointsAdded = ".datapoints_added";
const static std::string kDatapointsDropped = ".datapoints_dropped";
const static std::string kDatapointsBehind = ".datapoints_behind";
const static std::string kDatapointsAhead = ".datapoints_ahead";
const static std::string kDatapointsNotOwned = ".datapoints_not_owned";
const static std::string kTooLongKeys = ".too_long_keys";
const static std::string kNewTimeSeriesBlocked = ".new_time_series_blocked";
const static std::string kNumShards = ".num_shards";
const static std::string kMsPerShardAdd = ".ms_per_shard_add";
const static std::string kShardsAdded = ".shards_added";
const static std::string kShardsBeingAdded = ".shards_being_added";
const static std::string kShardsDropped = ".shards_dropped";

// Max size for ODS key is 256 and entity 128. This will fit those and
// some extra characters.
const int kMaxKeyLength = 400;
const int kRefreshShardMapInterval = 60; // poll every minute

const int BeringeiServiceHandler::kAsyncDropShardsDelaySecs = 30;

static const int kStopThreadShardId = -1;

BeringeiServiceHandler::BeringeiServiceHandler(
    std::shared_ptr<BeringeiConfigurationAdapterIf> configAdapter,
    std::shared_ptr<MemoryUsageGuardIf> memoryUsageGuard,
    const std::string& serviceName,
    const int32_t port)
    : configAdapter_(std::move(configAdapter)),
      memoryUsageGuard_(std::move(memoryUsageGuard)),
      serviceName_(serviceName),
      port_(port),
      numShards_(0),
      numShardsBeingAdded_(0),
      addShardQueue_(fLI::FLAGS_shards),
      readBlocksShardQueue_(fLI::FLAGS_shards),
      dropShardQueue_(fLI::FLAGS_shards),
      lastFinalizedBucket_(0) {
  // the number of threads for each thread pool must exceed 0
  CHECK_GT(fLI::FLAGS_add_shard_threads, 0);
  CHECK_GT(fLI::FLAGS_key_writer_threads, 0);
  CHECK_GT(fLI::FLAGS_log_writer_threads, 0);
  CHECK_GT(fLI::FLAGS_block_writer_threads, 0);

  if (!FLAGS_create_directories) {
    std::string shardZero = FileUtils::joinPaths(FLAGS_data_directory, "0");
    if (!FileUtils::isDirectory(shardZero)) {
      LOG(FATAL) << "Data directory '" << shardZero << " does not exist. "
                 << "If you are running beringei for the first time, "
                 << "please pass --create_directories flag.";
    }
  }

  // start monitoring
  FileUtils::startMonitoring();
  BucketLogWriter::startMonitoring();
  BucketMap::startMonitoring();
  KeyListWriter::startMonitoring();
  BucketStorage::startMonitoring();

  BucketLogWriter::setNumShards(FLAGS_shards);

  std::vector<std::shared_ptr<KeyListWriter>> keyWriters;
  for (int i = 0; i < FLAGS_key_writer_threads; i++) {
    keyWriters.emplace_back(
        new KeyListWriter(FLAGS_data_directory, FLAGS_key_writer_queue_size));
  }

  std::vector<std::shared_ptr<BucketLogWriter>> bucketLogWriters;
  for (int i = 0; i < FLAGS_log_writer_threads; i++) {
    bucketLogWriters.emplace_back(new BucketLogWriter(
        FLAGS_bucket_size,
        FLAGS_data_directory,
        FLAGS_log_writer_queue_size,
        FLAGS_allowed_timestamp_behind));
  }

  srandom(folly::randomNumberSeed());
  for (int i = 0; i < FLAGS_shards; i++) {
    // Select the bucket log writer and block writer for each shard by
    // random instead of by modulo to allow better distribution
    // because sharding algorithm used by Shard Manager is
    // unknown. The distribution doesn't have to be even. As long as
    // it's somewhat distributed it should be fine.
    auto keyWriter = keyWriters[random() % keyWriters.size()];
    auto bucketLogWriter = bucketLogWriters[random() % bucketLogWriters.size()];
    auto map = new BucketMap(
        FLAGS_buckets,
        FLAGS_bucket_size,
        i,
        FLAGS_data_directory,
        keyWriter,
        bucketLogWriter,
        BucketMap::UNOWNED);

    if (FLAGS_create_directories) {
      FileUtils utils(i, "", FLAGS_data_directory);
      utils.createDirectories();
    }

    // If we won't be refreshing the shard map, then assume we own everything.
    // Otherwise, default to owning nothing.
    if (fLB::FLAGS_disable_shard_refresh) {
      LOG(INFO) << "Running with shard refresh disabled, "
                << "defaulting to owning all shards";
      map->setState(BucketMap::PRE_OWNED);
      map->readKeyList();
      map->readData();
      while (map->readBlockFiles()) {
        // Nothing here...
      }
    }
    data_.emplace_back(map);
  }

  // If we should be refreshing from a shard map, read the config and add shards
  // we should own.
  if (!fLB::FLAGS_disable_shard_refresh) {
    refreshShardConfig();
    LOG(INFO) << "Successfully read shard config for the first time!";
  }

  purgeThread_.addFunction(
      std::bind(&BeringeiServiceHandler::purgeThread, this),
      std::chrono::seconds(kPurgeInterval),
      "Purge Thread",
      std::chrono::seconds(kPurgeInterval));
  purgeThread_.start();

  // Bucket finalizer thread runs at an interval slightly less than two hours.
  // We wait for a cycle before actually starting the thread to allow shards to
  // be loaded first before trying to finalize anything.
  bucketFinalizerThread_.addFunction(
      std::bind(&BeringeiServiceHandler::finalizeBucketsThread, this),
      std::chrono::seconds(FLAGS_sleep_between_bucket_finalization_secs),
      "Bucket Finalizer Thread",
      std::chrono::seconds(FLAGS_sleep_between_bucket_finalization_secs));
  bucketFinalizerThread_.start();

  for (int i = 0; i < fLI::FLAGS_add_shard_threads; i++) {
    addShardThreads_.push_back(
        std::thread(&BeringeiServiceHandler::addShardThread, this));
  }
  dropShardThread_ =
      std::thread(&BeringeiServiceHandler::dropShardThread, this);

  if (!FLAGS_disable_shard_refresh) {
    refreshShardConfigThread_.addFunction(
        std::bind(&BeringeiServiceHandler::refreshShardConfig, this),
        std::chrono::seconds(kRefreshShardMapInterval),
        "Refresh Shard Map Thread",
        std::chrono::seconds(kRefreshShardMapInterval));
    refreshShardConfigThread_.start();
  }
}

BeringeiServiceHandler::~BeringeiServiceHandler() {
  purgeThread_.shutdown();
  bucketFinalizerThread_.shutdown();
  refreshShardConfigThread_.shutdown();

  dropShardQueue_.write(std::make_pair(0, kStopThreadShardId));
  dropShardThread_.join();

  for (auto& t : addShardThreads_) {
    addShardQueue_.write(kStopThreadShardId);
  }
  for (auto& t : addShardThreads_) {
    t.join();
  }
}

void BeringeiServiceHandler::putDataPoints(
    PutDataResult& response,
    std::unique_ptr<PutDataRequest> req) {
  Timer timer(true);

  int newTimeSeries = 0;
  int datapointsAdded = 0;
  auto now = time(nullptr);
  int notOwned = 0;
  int newTimeSeriesBlocked = 0;

  for (auto& dp : req->data) {
    auto originalUnixTime = dp.value.unixTime;

    // Set time 0 to now.
    if (dp.value.unixTime == 0) {
      dp.value.unixTime = now;
    }

    if (dp.value.unixTime < now - FLAGS_allowed_timestamp_behind) {
      dp.value.unixTime = now;
      GorillaStatsManager::addStatValue(kDatapointsBehind);
    }

    if (dp.value.unixTime > now + FLAGS_allowed_timestamp_ahead) {
      dp.value.unixTime = now;
      GorillaStatsManager::addStatValue(kDatapointsAhead);
    }

    if (dp.key.key.length() > kMaxKeyLength) {
      GorillaStatsManager::addStatValue(kTooLongKeys);
      continue;
    }

    auto map = getShardMap(dp.key.shardId);
    if (!map) {
      continue;
    }

    if (map->get(dp.key.key) == nullptr &&
        memoryUsageGuard_->weAreLowOnMemory()) {
      ++newTimeSeriesBlocked;
      continue;
    }

    // The put call will do the check for the shard ownership
    auto ret = map->put(dp.key.key, dp.value, dp.categoryId);

    if (ret.first == BucketMap::kNotOwned) {
      dp.value.unixTime = originalUnixTime;
      response.data.push_back(dp);
      notOwned++;
    } else {
      newTimeSeries += ret.first;
      datapointsAdded += ret.second;
    }
  }

  GorillaStatsManager::addStatValue(kUsPerPut, timer.get());
  GorillaStatsManager::addStatValue(
      kUsPerPutPerKey, timer.get() / (double)req->data.size());
  GorillaStatsManager::addStatValue(kKeysPut, req->data.size());
  GorillaStatsManager::addStatValue(kNewKeys, newTimeSeries);
  GorillaStatsManager::addStatValue(kDatapointsAdded, datapointsAdded);
  GorillaStatsManager::addStatValue(
      kDatapointsDropped, req->data.size() - datapointsAdded);
  GorillaStatsManager::addStatValue(kDatapointsNotOwned, notOwned);
  GorillaStatsManager::addStatValue(
      kNewTimeSeriesBlocked, newTimeSeriesBlocked);
}

void BeringeiServiceHandler::getData(
    GetDataResult& ret,
    std::unique_ptr<GetDataRequest> req) {
  Timer timer(true);
  ret.results.resize(req->keys.size());
  int keysFound = 0;

  for (int i = 0; i < req->keys.size(); i++) {
    const Key& key = req->keys[i];
    auto map = getShardMap(key.shardId);
    if (!map || key.key.length() > kMaxKeyLength) {
      ret.results[i].status = StatusCode::KEY_MISSING;
      continue;
    }

    BucketMap::State state = map->getState();
    if (state == BucketMap::UNOWNED) {
      // Not owning this shard, caller has stale shard information.
      ret.results[i].status = StatusCode::DONT_OWN_SHARD;
    } else if (
        state >= BucketMap::PRE_OWNED &&
        state < BucketMap::READING_BLOCK_DATA) {
      // Not ready to serve reads yet.
      ret.results[i].status = StatusCode::SHARD_IN_PROGRESS;
    } else {
      auto row = map->get(key.key);
      if (row.get()) {
        keysFound++;
        row->second.get(
            data_[0]->bucket(req->begin),
            data_[0]->bucket(req->end),
            ret.results[i].data,
            map->getStorage());
        row->second.setQueried();
        if (state == BucketMap::READING_BLOCK_DATA) {
          // Some of the data hasn't been read yet. Let the client
          // decide what to do with the results, i.e., ask the other
          // coast if possible.
          ret.results[i].status = StatusCode::SHARD_IN_PROGRESS;
        } else if (req->begin < map->getReliableDataStartTime()) {
          ret.results[i].status = StatusCode::MISSING_TOO_MUCH_DATA;
          GorillaStatsManager::addStatValue(kMissingTooMuchData, 1);
        } else {
          ret.results[i].status = StatusCode::OK;
        }
      } else {
        // There's no such key.
        ret.results[i].status = StatusCode::KEY_MISSING;
      }
    }
  }

  GorillaStatsManager::addStatValue(kUsPerGet, timer.get());
  GorillaStatsManager::addStatValue(
      kUsPerGetPerKey, timer.get() / (double)req->keys.size());
  GorillaStatsManager::addStatValue(kKeysGot, keysFound);
}

void BeringeiServiceHandler::getShardDataBucket(
    GetShardDataBucketResult& ret,
    int64_t beginTs,
    int64_t endTs,
    int64_t shardId,
    int32_t offset,
    int32_t limit) {
  beginTs -= beginTs % FLAGS_bucket_size;
  endTs -= endTs % FLAGS_bucket_size;

  LOG(INFO) << "Fetching data for shard " << shardId << " between time "
            << beginTs << " and " << endTs;

  Timer timer(true);

  ret.moreEntries = false;
  auto map = getShardMap(shardId);
  if (!map) {
    ret.status = StatusCode::RPC_FAIL;
    return;
  }
  auto state = map->getState();
  if (state != BucketMap::OWNED) {
    ret.status = state <= BucketMap::UNOWNED ? StatusCode::DONT_OWN_SHARD
                                             : StatusCode::SHARD_IN_PROGRESS;
    return;
  }

  // Don't allow data fetches until the bucket has been finalized.
  if (map->bucket(endTs) > map->getLastFinalizedBucket()) {
    ret.status = StatusCode::BUCKET_NOT_FINALIZED;
    return;
  }

  std::vector<BucketMap::Item> rows;
  ret.moreEntries = map->getSome(rows, offset, limit);
  auto storage = map->getStorage();

  ret.keys.reserve(rows.size());
  ret.data.reserve(rows.size());

  uint32_t begin = map->bucket(beginTs);
  uint32_t end = map->bucket(endTs);

  for (auto& row : rows) {
    if (row.get()) {
      std::vector<TimeSeriesBlock> blocks;
      row->second.get(begin, end, blocks, storage);

      if (blocks.size() > 0) {
        ret.keys.push_back(row->first);
        ret.data.push_back(std::move(blocks));
        ret.recentRead.push_back(
            row->second.getQueriedBucketsAgo() <=
            facebook::gorilla::kGorillaSecondsPerDay / FLAGS_bucket_size);
      }
    }
  }

  LOG(INFO) << "Data fetch for shard " << shardId << " complete in "
            << timer.get() << "us with " << ret.keys.size() << " keys returned";
}

void BeringeiServiceHandler::setShards(
    const std::set<int64_t>& shards,
    int dropDelay) {
  std::vector<int64_t> shardsToBeAdded;
  std::vector<int64_t> shardsToBeDropped;

  for (int i = 0; i < data_.size(); i++) {
    BucketMap::State state = data_[i]->getState();
    bool shouldBeOwned = shards.find(i) != shards.end();

    // Don't not touch the shards that are being added. We will attempt
    // to drop them again periodically
    if (shouldBeOwned) {
      if (state == BucketMap::UNOWNED) {
        shardsToBeAdded.push_back(i);
      } else if (state == BucketMap::PRE_UNOWNED) {
        // This shard was queued to be unowned but we never got that far.
        // Just mark it as owned and thread unowning this shard won't do
        // anything
        data_[i]->cancelUnowning();
      }
    } else if (state == BucketMap::OWNED) {
      shardsToBeDropped.push_back(i);
    }
  }

  for (auto& shard : shardsToBeAdded) {
    LOG(INFO) << "Adding shard " << shard << " based on current config.";
    addShardAsync(shard);
  }

  for (auto& shard : shardsToBeDropped) {
    LOG(INFO) << "Dropping shard " << shard << " based on current config.";
    dropShardAsync(shard, dropDelay);
  }
}

std::set<int64_t> BeringeiServiceHandler::getShards() {
  std::set<int64_t> shards;

  for (int i = 0; i < data_.size(); i++) {
    // Return anything that is owned or being added.
    if (data_[i]->getState() >= BucketMap::PRE_OWNED) {
      shards.insert(i);
    }
  }

  return shards;
}

void BeringeiServiceHandler::processOneShardAddition(int64_t shardId) {
  auto map = getShardMap(shardId);
  if (map) {
    BucketMap::State state = map->getState();
    if (state == BucketMap::PRE_OWNED) {
      map->readKeyList();

      // Put this shard back in the queue to read data.
      addShardQueue_.write(shardId);
    } else if (state == BucketMap::READING_KEYS_DONE) {
      map->readData();
      numShardsBeingAdded_--;
      numShards_++;
      GorillaStatsManager::setCounter(kShardsBeingAdded, numShardsBeingAdded_);
      GorillaStatsManager::setCounter(kNumShards, numShards_);
      GorillaStatsManager::addStatValue(kShardsAdded);
      GorillaStatsManager::addStatValue(
          kMsPerShardAdd, map->getAddTime() / kGorillaUsecPerMs);

      // Enqueue to read the compressed block files.
      readBlocksShardQueue_.write(shardId);
    } else {
      // Should never be reached.
      CHECK(false);
    }
  }
}

BeringeiServiceHandler::BeringeiShardState
BeringeiServiceHandler::addShardAsync(int64_t shardId) {
  auto map = getShardMap(shardId);
  if (map) {
    BucketMap::State state = map->getState();
    if (state >= BucketMap::PRE_OWNED) {
      return BeringeiShardState::SUCCESS;
    }
    if (!map->setState(BucketMap::PRE_OWNED)) {
      // Setting to pre owned failed which means it's currently being
      // dropped.
      return BeringeiShardState::IN_PROGRESS;
    }

    numShardsBeingAdded_++;
    GorillaStatsManager::setCounter(kShardsBeingAdded, numShardsBeingAdded_);
    addShardQueue_.write(shardId);
    return BeringeiShardState::SUCCESS;
  }
  return BeringeiShardState::ERROR;
}

BeringeiServiceHandler::BeringeiShardState
BeringeiServiceHandler::dropShardAsync(int64_t shardId, int64_t delay) {
  auto map = getShardMap(shardId);
  if (map) {
    BucketMap::State state = map->getState();
    if (state == BucketMap::UNOWNED) {
      return BeringeiShardState::SUCCESS;
    } else if (state != BucketMap::OWNED) {
      // Anything else other than OWNED and UNOWNED is considered to
      // be inprogess. This could mean that the shard is being added
      // or being dropped after a delay.
      return BeringeiShardState::IN_PROGRESS;
    }

    // PRE_UNOWNED is state that indicates that the shard will be
    // dropped after the delay.
    if (!map->setState(BucketMap::PRE_UNOWNED)) {
      return BeringeiShardState::IN_PROGRESS;
    }

    std::pair<uint32_t, int64_t> shard;
    shard.first = time(nullptr) + delay;
    shard.second = shardId;
    dropShardQueue_.write(shard);
    return BeringeiShardState::IN_PROGRESS;
  }

  return BeringeiShardState::ERROR;
}

void BeringeiServiceHandler::addShardThread() {
  // Read key lists for all the shards first before reading any data
  // to make reading key lists as fast as possible.

  while (true) {
    try {
      int64_t shardId;

      // Prioritize addShardQueue_ over readBlocksShardQueue_.
      // It's ok to do the blocking read on addShardQueue_ because these are the
      // only threads that insert into readBlocksShardQueue_.
      if (addShardQueue_.read(shardId)) {
        processOneShardAddition(shardId);
      } else if (readBlocksShardQueue_.read(shardId)) {
        auto map = getShardMap(shardId);
        if (map && map->readBlockFiles()) {
          // Put this shard back in the queue to read more block files.
          // This way, all shards are read starting from now and working back.
          readBlocksShardQueue_.write(shardId);
        }

      } else {
        addShardQueue_.blockingRead(shardId);
        processOneShardAddition(shardId);
      }

      if (shardId == kStopThreadShardId) {
        break;
      }
    } catch (std::exception& e) {
      LOG(ERROR) << e.what();
    }
  }
}

void BeringeiServiceHandler::dropShardThread() {
  while (true) {
    try {
      std::pair<uint32_t, int64_t> shard;
      dropShardQueue_.blockingRead(shard);
      if (shard.second == kStopThreadShardId) {
        break;
      }

      int delay = shard.first - time(nullptr);
      if (delay > 0) {
        // FIFO queue with a constant delay, so sleeping is always fine.
        /* sleep override */ sleep(delay);
      }

      auto map = getShardMap(shard.second);
      if (map) {
        if (map->getState() == BucketMap::PRE_UNOWNED) {
          // Only drop the shard if it's in PRE_UNOWNED state. This
          // guarantees that it is not being added back. If shard
          // manager really wants to get rid of this shard, it will ask
          // again.
          if (map->setState(BucketMap::UNOWNED)) {
            numShards_--;
            GorillaStatsManager::setCounter(kNumShards, numShards_);
            GorillaStatsManager::addStatValue(kShardsDropped);
          }
        }
      }
    } catch (std::exception& e) {
      LOG(ERROR) << e.what();
    }
  }
}

void BeringeiServiceHandler::refreshShardConfig() {
  std::string hostName = NetworkUtils::getLocalHost();
  auto hostInfo = std::make_pair(hostName, port_);

  // ShardList will be populated by getShardsForHost.
  std::set<int64_t> shardList;
  configAdapter_->getShardsForHost(hostInfo, serviceName_, shardList);

  // We will addShard everything we should own and dropShard all other shards.
  // For anything we already own and should (or do not own and shouldn't), this
  // is a noop.
  setShards(shardList);
}

BucketMap* BeringeiServiceHandler::getShardMap(int64_t shardId) {
  if (shardId < 0 || shardId >= data_.size()) {
    LOG(ERROR) << "Invalid shard " << shardId;
    return nullptr;
  }

  return data_[shardId].get();
}

void BeringeiServiceHandler::purgeThread() {
  int numPurged = purgeTimeSeries(FLAGS_buckets);
  LOG(INFO) << "Purged " << numPurged << " time series.";
  GorillaStatsManager::addStatValue(kPurgedTimeSeries, numPurged);
}

int BeringeiServiceHandler::purgeTimeSeries(uint8_t numBuckets) {
  int purgedTimeSeries = 0;

  try {
    std::unordered_map<int32_t, int64_t> purgedTSPerCategory;
    for (auto& bucketMap : data_) {
      if (bucketMap->getState() != BucketMap::OWNED) {
        continue;
      }

      std::vector<BucketMap::Item> timeSeriesData;
      bucketMap->getEverything(timeSeriesData);
      for (int i = 0; i < timeSeriesData.size(); i++) {
        if (timeSeriesData[i].get()) {
          uint16_t category = timeSeriesData[i]->second.getCategory();
          if (!timeSeriesData[i]->second.hasDataPoints(numBuckets)) {
            bucketMap->erase(i, timeSeriesData[i]);
            ++purgedTimeSeries;
            ++purgedTSPerCategory[category];
          }
        }
      }
    }

    for (auto item : purgedTSPerCategory) {
      GorillaStatsManager::setCounter(
          kPurgedTimeSeriesInCategoryPrefix + std::to_string(item.first),
          item.second);
    }
  } catch (std::exception& e) {
    LOG(ERROR) << e.what();
  }

  return purgedTimeSeries;
}

void BeringeiServiceHandler::finalizeBucketsThread() {
  // This is the last bucket that can be finalized at this
  // moment. It consideres the time timestamps can be late and adds
  // one minute buffer to allow the data to be processed.
  //
  // The same bucket is finalized multiple times on purpose to make
  // sure all the shard movements are caught.
  uint32_t bucketToFinalize = (time(nullptr) - FLAGS_allowed_timestamp_behind -
                               facebook::gorilla::kGorillaSecondsPerMinute) /
          FLAGS_bucket_size -
      1;

  if (lastFinalizedBucket_ != 0 &&
      bucketToFinalize - lastFinalizedBucket_ > 1) {
    GorillaStatsManager::addStatValue(kTooSlowToFinalizeBuckets);
    LOG(ERROR) << "Finalizing the previous bucket took too long!";
  }

  finalizeBucket(bucketToFinalize);
  lastFinalizedBucket_ = bucketToFinalize;
}

void BeringeiServiceHandler::finalizeBucket(uint32_t bucketToFinalize) {
  LOG(INFO) << "Finalizing bucket " << bucketToFinalize;

  // Put all the shards in the queue even if they are not owned
  // because they might be owned 5 minutes later.
  folly::MPMCQueue<uint32_t> queue(FLAGS_shards);
  for (int i = 0; i < FLAGS_shards; i++) {
    queue.write(i);
  }

  // Create a fixed number of threads and go through all the shards.
  std::vector<std::thread> threads;
  for (int i = 0; i < FLAGS_block_writer_threads; i++) {
    threads.emplace_back([&]() {
      while (true) {
        uint32_t shardId;
        if (!queue.read(shardId)) {
          break;
        }

        Timer timer(true);

        // If the shard is not owned or there are no buckets to
        // finalized, this will return immediately with 0.
        int count = data_[shardId]->finalizeBuckets(bucketToFinalize);

        GorillaStatsManager::addStatValueAggregated(
            kMsPerFinalizeShardBucket,
            timer.get() / facebook::gorilla::kGorillaUsecPerMs,
            count);
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
}
}
}
