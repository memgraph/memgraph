#pragma once

#include <array>
#include <chrono>
#include <cstring>
#include <iostream>
#include <random>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "communication/server.hpp"
#include "database/graph_db_accessor.hpp"

static constexpr const int SIZE = 60000;
static constexpr const int REPLY = 10;

using io::network::Endpoint;
using io::network::Socket;

class TestData {};

class TestSession {
 public:
  TestSession(TestData &, communication::InputStream &input_stream,
              communication::OutputStream &output_stream)
      : input_stream_(input_stream), output_stream_(output_stream) {}

  void Execute() {
    if (input_stream_.size() < 2) return;
    const uint8_t *data = input_stream_.data();
    size_t size = data[0];
    size <<= 8;
    size += data[1];
    input_stream_.Resize(size + 2);
    if (input_stream_.size() < size + 2) return;

    for (int i = 0; i < REPLY; ++i)
      ASSERT_TRUE(output_stream_.Write(data + 2, size));

    input_stream_.Shift(size + 2);
  }

  communication::InputStream input_stream_;
  communication::OutputStream output_stream_;
};

using ServerT = communication::Server<TestSession, TestData>;

void client_run(int num, const char *interface, uint16_t port,
                const unsigned char *data, int lo, int hi) {
  std::stringstream name;
  name << "Client " << num;
  unsigned char buffer[SIZE * REPLY], head[2];
  int have, read;
  Endpoint endpoint(interface, port);
  Socket socket;
  ASSERT_TRUE(socket.Connect(endpoint));
  socket.SetTimeout(2, 0);
  DLOG(INFO) << "Socket create: " << socket.fd();
  for (int len = lo; len <= hi; len += 100) {
    have = 0;
    head[0] = (len >> 8) & 0xff;
    head[1] = len & 0xff;
    ASSERT_TRUE(socket.Write(head, 2));
    ASSERT_TRUE(socket.Write(data, len));
    DLOG(INFO) << "Socket write: " << socket.fd();
    while (have < len * REPLY) {
      read = socket.Read(buffer + have, SIZE);
      DLOG(INFO) << "Socket read: " << socket.fd();
      if (read == -1) break;
      have += read;
    }
    for (int i = 0; i < REPLY; ++i)
      for (int j = 0; j < len; ++j) ASSERT_EQ(buffer[i * len + j], data[j]);
  }
  DLOG(INFO) << "Socket done: " << socket.fd();
  socket.Close();
}

void initialize_data(unsigned char *data, int size) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 255);
  for (int i = 0; i < size; ++i) {
    data[i] = dis(gen);
  }
}
