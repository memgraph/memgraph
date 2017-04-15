#pragma once

#include <array>
#include <cassert>
#include <cstring>
#include <iostream>
#include <vector>

#include "gtest/gtest.h"

#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"

#include "communication/bolt/v1/decoder/buffer.hpp"
#include "communication/server.hpp"
#include "dbms/dbms.hpp"
#include "io/network/epoll.hpp"
#include "io/network/socket.hpp"
#include "query/engine.hpp"

static constexpr const int SIZE = 60000;
static constexpr const int REPLY = 10;

using endpoint_t = io::network::NetworkEndpoint;
using socket_t = io::network::Socket;

class TestOutputStream {};

class TestSession {
 public:
  TestSession(socket_t&& socket, Dbms& dbms,
              QueryEngine<TestOutputStream>& query_engine)
      : logger_(logging::log->logger("TestSession")),
        socket_(std::move(socket)) {
    event_.data.ptr = this;
  }

  bool Alive() { return socket_.IsOpen(); }

  int Id() const { return socket_.id(); }

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

  io::network::StreamBuffer Allocate() {
    return buffer_.Allocate();
  }

  void Written(size_t len) {
    buffer_.Written(len);
  }

  void Close() {
    logger_.trace("Close session!");
    this->socket_.Close();
  }

  communication::bolt::Buffer<SIZE * 2> buffer_;
  Logger logger_;
  socket_t socket_;
  io::network::Epoll::Event event_;
};

using test_server_t =
    communication::Server<TestSession, TestOutputStream, socket_t>;

void server_start(void* serverptr, int num) {
  ((test_server_t*)serverptr)->Start(num);
}

void client_run(int num, const char* interface, const char* port,
                const unsigned char* data, int lo, int hi) {
  std::stringstream name;
  name << "Client " << num;
  Logger logger = logging::log->logger(name.str());
  unsigned char buffer[SIZE * REPLY], head[2];
  int have, read;
  endpoint_t endpoint(interface, port);
  socket_t socket;
  ASSERT_TRUE(socket.Connect(endpoint));
  ASSERT_TRUE(socket.SetTimeout(2, 0));
  logger.trace("Socket create: {}", socket.id());
  for (int len = lo; len <= hi; len += 100) {
    have = 0;
    head[0] = (len >> 8) & 0xff;
    head[1] = len & 0xff;
    ASSERT_TRUE(socket.Write(head, 2));
    ASSERT_TRUE(socket.Write(data, len));
    logger.trace("Socket write: {}", socket.id());
    while (have < len * REPLY) {
      read = socket.Read(buffer + have, SIZE);
      logger.trace("Socket read: {}", socket.id());
      if (read == -1) break;
      have += read;
    }
    for (int i = 0; i < REPLY; ++i)
      for (int j = 0; j < len; ++j) ASSERT_EQ(buffer[i * len + j], data[j]);
  }
  logger.trace("Socket done: {}", socket.id());
  socket.Close();
}

void initialize_data(unsigned char* data, int size) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 255);
  for (int i = 0; i < size; ++i) {
    data[i] = dis(gen);
  }
}
