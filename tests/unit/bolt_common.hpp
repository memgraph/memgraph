#include <array>
#include <cassert>
#include <cstring>
#include <iostream>
#include <vector>

#include "gtest/gtest.h"

#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"

#include "dbms/dbms.hpp"


class TestSocket {
 public:
  TestSocket(int socket) : socket(socket) {}
  TestSocket(const TestSocket& s) : socket(s.id()){};
  TestSocket(TestSocket&& other) { *this = std::forward<TestSocket>(other); }

  TestSocket& operator=(TestSocket&& other) {
    this->socket = other.socket;
    other.socket = -1;
    return *this;
  }

  void Close() { socket = -1; }
  bool IsOpen() { return socket != -1; }

  int id() const { return socket; }

  int Write(const std::string& str) { return Write(str.c_str(), str.size()); }
  int Write(const char* data, size_t len) {
    return Write(reinterpret_cast<const uint8_t*>(data), len);
  }
  int Write(const uint8_t* data, size_t len) {
    for (int i = 0; i < len; ++i) output.push_back(data[i]);
    return len;
  }

  std::vector<uint8_t> output;

 protected:
  int socket;
};

void print_output(std::vector<uint8_t>& output) {
  fprintf(stderr, "output: ");
  for (int i = 0; i < output.size(); ++i) {
    fprintf(stderr, "%02X ", output[i]);
  }
  fprintf(stderr, "\n");
}

void check_output(std::vector<uint8_t>& output, const uint8_t* data,
                  uint64_t len, bool clear = true) {
  if (clear) ASSERT_EQ(len, output.size());
  else ASSERT_LE(len, output.size());
  for (int i = 0; i < len; ++i)
    EXPECT_EQ(output[i], data[i]);
  if (clear) output.clear();
  else output.erase(output.begin(), output.begin() + len);
}

void initialize_data(uint8_t* data, size_t size) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 255);
  for (int i = 0; i < size; ++i) {
    data[i] = dis(gen);
  }
}
