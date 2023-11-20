// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <array>
#include <cstring>
#include <functional>
#include <iostream>
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include "utils/logging.hpp"

/**
 * TODO (mferencevic): document
 */
class TestInputStream {
 public:
  uint8_t *data() { return data_.data(); }

  size_t size() { return data_.size(); }

  void Clear() { data_.clear(); }

  void Write(const uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; ++i) {
      data_.push_back(data[i]);
    }
  }

  void Write(const char *data, size_t len) { Write(reinterpret_cast<const uint8_t *>(data), len); }

  void Shift(size_t count) {
    MG_ASSERT(count <= data_.size());
    data_.erase(data_.begin(), data_.begin() + count);
  }

  void Resize(size_t len) {}

 private:
  std::vector<uint8_t> data_;
};

/**
 * TODO (mferencevic): document
 */
class TestOutputStream {
 public:
  bool Write(const uint8_t *data, size_t len, bool have_more = false) {
    if (!write_success_) return false;
    for (size_t i = 0; i < len; ++i) output.push_back(data[i]);
    return true;
  }

  void SetWriteSuccess(bool success) { write_success_ = success; }

  std::vector<uint8_t> output;

 protected:
  bool write_success_{true};
};

/**
 * TODO (mferencevic): document
 */
class TestBuffer {
 public:
  explicit TestBuffer(TestOutputStream &output_stream) : output_stream_(output_stream) {}

  void Write(const uint8_t *data, size_t n) { output_stream_.Write(data, n); }
  bool Flush(bool have_more = false) { return true; }

 private:
  TestOutputStream &output_stream_;
};

/**
 * TODO (mferencevic): document
 */
void PrintOutput(std::vector<uint8_t> &output) {
  fprintf(stderr, "output: ");
  for (uint8_t val : output) {
    fprintf(stderr, "%02X ", val);
  }
  fprintf(stderr, "\n");
}

/**
 * TODO (mferencevic): document
 */
void CheckOutput(std::vector<uint8_t> &output, const uint8_t *data, uint64_t len, bool clear = true) {
  if (clear) {
    ASSERT_EQ(len, output.size());
  } else {
    ASSERT_LE(len, output.size());
  }
  for (size_t i = 0; i < len; ++i) {
    EXPECT_EQ(output[i], data[i]) << i;
  }
  if (clear) {
    output.clear();
  } else {
    output.erase(output.begin(), output.begin() + len);
  }
}

/**
 * TODO (mferencevic): document
 */
void InitializeData(uint8_t *data, size_t size) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 255);
  for (size_t i = 0; i < size; ++i) {
    data[i] = dis(gen);
  }
}
