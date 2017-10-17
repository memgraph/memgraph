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
class TestSocket {
 public:
  explicit TestSocket(int socket) : socket_(socket) {}
  TestSocket(const TestSocket &) = default;
  TestSocket &operator=(const TestSocket &) = default;
  TestSocket(TestSocket &&) = default;
  TestSocket &operator=(TestSocket &&) = default;

  void Close() { socket_ = -1; }
  bool IsOpen() { return socket_ != -1; }

  int id() const { return socket_; }

  bool Write(const uint8_t *data, size_t len,
             const std::function<bool()> & = [] { return false; }) {
    if (!write_success_) return false;
    for (size_t i = 0; i < len; ++i) output.push_back(data[i]);
    return true;
  }

  void SetWriteSuccess(bool success) { write_success_ = success; }

  std::vector<uint8_t> output;

 protected:
  int socket_;
  bool write_success_{true};
};

/**
 * TODO (mferencevic): document
 */
class TestBuffer {
 public:
  TestBuffer(TestSocket &socket) : socket_(socket) {}

  void Write(const uint8_t *data, size_t n) { socket_.Write(data, n); }
  void Chunk() {}
  bool Flush() { return true; }

 private:
  TestSocket &socket_;
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
