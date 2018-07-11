/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "TimeSeries.h"

namespace {
struct DeltaInserter
    : public std::iterator<std::output_iterator_tag, void, void, void, void> {
  using container_type = std::vector<facebook::gorilla::TimeValuePair>;

  DeltaInserter(
      std::vector<facebook::gorilla::TimeValuePair>& data,
      int64_t delta)
      : data_(&data), delta_(delta) {}

  DeltaInserter& operator=(const facebook::gorilla::TimeValuePair& tv) {
    if (data_->empty() || (tv.unixTime - data_->back().unixTime) >= delta_) {
      data_->push_back(tv);
    }
    return *this;
  }

  DeltaInserter& operator=(facebook::gorilla::TimeValuePair&& tv) {
    if (data_->empty() || (tv.unixTime - data_->back().unixTime) >= delta_) {
      data_->push_back(std::move(tv));
    }
    return *this;
  }

  DeltaInserter& operator*() {
    return *this;
  }
  DeltaInserter& operator++() {
    return *this;
  }
  DeltaInserter& operator++(int) {
    return *this;
  }

  std::vector<facebook::gorilla::TimeValuePair>* data_;
  int64_t delta_;
};

// May be slightly more expensive.
struct DeltaCompareInserter
    : public std::iterator<std::output_iterator_tag, void, void, void, void> {
  using container_type = std::vector<facebook::gorilla::TimeValuePair>;

  DeltaCompareInserter(
      std::vector<facebook::gorilla::TimeValuePair>& data,
      int64_t delta,
      double epsilon,
      int64_t& mismatches)
      : data_(&data),
        delta_(delta),
        epsilon_(epsilon),
        mismatches_(mismatches) {}

  DeltaCompareInserter& operator=(const facebook::gorilla::TimeValuePair& tv) {
    if (data_->empty() || (tv.unixTime - data_->back().unixTime) >= delta_) {
      data_->push_back(tv);
    } else if (
        std::abs(tv.value - data_->back().value) >
        std::max(epsilon_ * std::abs(data_->back().value), epsilon_)) {
      mismatches_++;
    }
    return *this;
  }

  DeltaCompareInserter& operator=(facebook::gorilla::TimeValuePair&& tv) {
    if (data_->empty() || (tv.unixTime - data_->back().unixTime) >= delta_) {
      data_->push_back(std::move(tv));
    } else if (
        std::abs(tv.value - data_->back().value) >
        std::max(epsilon_ * std::abs(data_->back().value), epsilon_)) {
      mismatches_++;
    }
    return *this;
  }

  DeltaCompareInserter& operator*() {
    return *this;
  }
  DeltaCompareInserter& operator++() {
    return *this;
  }
  DeltaCompareInserter& operator++(int) {
    return *this;
  }

  std::vector<facebook::gorilla::TimeValuePair>* data_;
  int64_t delta_;
  double epsilon_;
  int64_t& mismatches_;
};
} // namespace

namespace facebook {
namespace gorilla {

void TimeSeries::writeValues(
    const std::vector<TimeValuePair>& values,
    TimeSeriesBlock& block) {
  TimeSeriesStream stream;

  for (const auto& value : values) {
    if (stream.append(value, 0)) {
      block.count++;
    }
  }
  stream.readData(block.data);
}

void TimeSeries::mergeValues(
    const std::vector<TimeSeriesBlock>& in,
    std::vector<facebook::gorilla::TimeValuePair>& out,
    int64_t begin,
    int64_t end,
    int32_t minTimestampDelta,
    bool compareValues,
    double mismatchEpsilon,
    int64_t* inSize,
    int64_t* mismatches) {
  std::vector<facebook::gorilla::TimeValuePair> resultData;
  TimeSeries::getValues(in, resultData, begin, end);

  if (inSize) {
    *inSize = resultData.size();
  }

  TimeSeries::mergeValues(
      std::move(resultData),
      out,
      minTimestampDelta,
      compareValues,
      mismatchEpsilon,
      mismatches);
}

void TimeSeries::mergeValues(
    std::vector<facebook::gorilla::TimeValuePair>&& in,
    std::vector<facebook::gorilla::TimeValuePair>& out,
    int32_t minTimestampDelta,
    bool compareValues,
    double mismatchEpsilon,
    int64_t* mismatchesOut) {
  // This is the first copy.
  if (out.empty()) {
    std::swap(out, in);
    return;
  }

  // Merge the results.
  std::vector<TimeValuePair> newData;
  newData.reserve(std::max(out.size(), in.size()));

  if (compareValues) {
    int64_t mismatches = 0;
    std::merge(
        out.begin(),
        out.end(),
        in.begin(),
        in.end(),
        DeltaCompareInserter(
            newData, minTimestampDelta, mismatchEpsilon, mismatches));
    if (mismatchesOut) {
      *mismatchesOut = mismatches;
    }
  } else {
    std::merge(
        out.begin(),
        out.end(),
        in.begin(),
        in.end(),
        DeltaInserter(newData, minTimestampDelta));
  }

  std::swap(out, newData);
}

} // namespace gorilla
} // namespace facebook
