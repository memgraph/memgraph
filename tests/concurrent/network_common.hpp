#pragma once

#include <array>
#include <chrono>
#include <cstring>
#include <iostream>
#include <random>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "communication/bolt/v1/decoder/buffer.hpp"
#include "communication/server.hpp"
#include "database/graph_db_accessor.hpp"
#include "io/network/epoll.hpp"
#include "io/network/socket.hpp"

static constexpr const int SIZE = 60000;
static constexpr const int REPLY = 10;

using io::network::Endpoint;
using io::network::Socket;

class TestData {};

class TestSession {
 public:
  TestSession(Socket &&socket, TestData &) : socket_(std::move(socket)) {
    event_.data.ptr = this;
  }

  bool Alive() const { return socket_.IsOpen(); }
  bool TimedOut() const { return false; }

  int Id() const { return socket_.fd(); }

  void Execute() {
    if (buffer_.size() < 2) return;
    const uint8_t *data = buffer_.data();
    size_t size = data[0];
    size <<= 8;
    size += data[1];
    if (buffer_.size() < size + 2) return;

    for (int i = 0; i < REPLY; ++i)
      ASSERT_TRUE(this->socket_.Write(data + 2, size));

    buffer_.Shift(size + 2);
  }

  io::network::StreamBuffer Allocate() { return buffer_.Allocate(); }

  void Written(size_t len) { buffer_.Written(len); }

  void Close() {
    DLOG(INFO) << "Close session!";
    this->socket_.Close();
  }

  Socket &socket() { return socket_; }

  void RefreshLastEventTime(
      const std::chrono::time_point<std::chrono::steady_clock>
          &last_event_time) {
    last_event_time_ = last_event_time;
  }

  communication::bolt::Buffer<SIZE * 2> buffer_;
  Socket socket_;
  io::network::Epoll::Event event_;
  std::chrono::time_point<std::chrono::steady_clock> last_event_time_;
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
