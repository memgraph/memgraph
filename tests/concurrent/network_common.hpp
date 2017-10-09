#pragma once

#include <array>
#include <cassert>
#include <cstring>
#include <iostream>
#include <random>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "communication/bolt/v1/decoder/buffer.hpp"
#include "communication/server.hpp"
#include "io/network/epoll.hpp"
#include "io/network/socket.hpp"

static constexpr const int SIZE = 60000;
static constexpr const int REPLY = 10;

using endpoint_t = io::network::NetworkEndpoint;
using socket_t = io::network::Socket;

class TestData {};

class TestSession {
 public:
  TestSession(socket_t &&socket, TestData &) : socket_(std::move(socket)) {
    event_.data.ptr = this;
  }

  bool Alive() { return socket_.IsOpen(); }

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

  communication::bolt::Buffer<SIZE * 2> buffer_;
  socket_t socket_;
  io::network::Epoll::Event event_;
};

using test_server_t = communication::Server<TestSession, socket_t, TestData>;

void server_start(void *serverptr, int num) {
  ((test_server_t *)serverptr)->Start(num);
}

void client_run(int num, const char *interface, const char *port,
                const unsigned char *data, int lo, int hi) {
  std::stringstream name;
  name << "Client " << num;
  unsigned char buffer[SIZE * REPLY], head[2];
  int have, read;
  endpoint_t endpoint(interface, port);
  socket_t socket;
  ASSERT_TRUE(socket.Connect(endpoint));
  ASSERT_TRUE(socket.SetTimeout(2, 0));
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
