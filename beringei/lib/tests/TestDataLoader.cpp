/**
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "TestDataLoader.h"
#include <folly/FileUtil.h>
#include <folly/String.h>
#include <stdio.h>

using namespace facebook;
using namespace facebook::gorilla;
using namespace std;

void loadData(vector<vector<TimeValuePair>>& samples) {
  FILE* f = fopen("beringei/lib/tests/samples5000.txt", "r");

  // this is C-style C++ and getline has semantics that depend on NULL
  char* line = NULL;

  int mode = 0;

  vector<TimeValuePair> currentValues;
  TimeValuePair value;
  int numberOfValues = 0;
  size_t lineSize = 0;

  while (getline(&line, &lineSize, f) != -1) {
    if (mode == 0) {
      numberOfValues = std::stoi(line);
      mode = 1;
    } else if (mode == 1) {
      value.unixTime = std::stoull(line);
      mode = 2;
    } else if (mode == 2) {
      value.value = std::stod(line);
      currentValues.push_back(value);

      if (currentValues.size() == numberOfValues) {
        mode = 0;
        samples.push_back(std::move(currentValues));
        currentValues.clear();
      } else {
        mode = 1;
      }
    }
  }
  // sticking with C-style checks because we start with line == NULL
  if (line != NULL) {
    free(line);
  }
}
