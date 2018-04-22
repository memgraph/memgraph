#include <array>
#include <cstring>
#include <functional>
#include <iostream>
#include <random>
#include <vector>

#include <glog/logging.h>

#include "gtest/gtest.h"

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

  void Write(const char *data, size_t len) {
    Write(reinterpret_cast<const uint8_t *>(data), len);
  }

  void Shift(size_t count) {
    CHECK(count <= data_.size());
    data_.erase(data_.begin(), data_.begin() + count);
  }

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
  explicit TestBuffer(TestOutputStream &output_stream)
      : output_stream_(output_stream) {}

  void Write(const uint8_t *data, size_t n) { output_stream_.Write(data, n); }
  void Chunk() {}
  bool Flush() { return true; }

 private:
  TestOutputStream &output_stream_;
};

/**
 * TODO (mferencevic): document
 */
void PrintOutput(std::vector<uint8_t> &output) {
  fprintf(stderr, "output: ");
  for (size_t i = 0; i < output.size(); ++i) {
    fprintf(stderr, "%02X ", output[i]);
  }
  fprintf(stderr, "\n");
}

/**
 * TODO (mferencevic): document
 */
void CheckOutput(std::vector<uint8_t> &output, const uint8_t *data,
                 uint64_t len, bool clear = true) {
  if (clear)
    ASSERT_EQ(len, output.size());
  else
    ASSERT_LE(len, output.size());
  for (size_t i = 0; i < len; ++i) EXPECT_EQ(output[i], data[i]);
  if (clear)
    output.clear();
  else
    output.erase(output.begin(), output.begin() + len);
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
