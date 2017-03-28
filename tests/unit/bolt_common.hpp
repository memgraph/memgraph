#include <array>
#include <cassert>
#include <cstring>
#include <iostream>
#include <vector>

#include "dbms/dbms.hpp"
#include "gtest/gtest.h"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"

/**
 * TODO (mferencevic): document
 */
class TestSocket {
 public:
  TestSocket(int socket) : socket(socket) {}
  TestSocket(const TestSocket &s) : socket(s.id()){};
  TestSocket(TestSocket &&other) { *this = std::forward<TestSocket>(other); }

  TestSocket &operator=(TestSocket &&other) {
    this->socket = other.socket;
    other.socket = -1;
    return *this;
  }

  void Close() { socket = -1; }
  bool IsOpen() { return socket != -1; }

  int id() const { return socket; }

  int Write(const std::string &str) { return Write(str.c_str(), str.size()); }
  int Write(const char *data, size_t len) {
    return Write(reinterpret_cast<const uint8_t *>(data), len);
  }
  int Write(const uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; ++i) output.push_back(data[i]);
    return len;
  }

  std::vector<uint8_t> output;

 protected:
  int socket;
};

/**
 * TODO (mferencevic): document
 */
class TestBuffer {
 public:
  TestBuffer(TestSocket &socket) : socket_(socket) {}

  void Write(const uint8_t *data, size_t n) { socket_.Write(data, n); }
  void Chunk() {}
  void Flush() {}

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
