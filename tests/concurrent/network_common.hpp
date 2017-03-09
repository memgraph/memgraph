#pragma once

#include <array>
#include <cassert>
#include <cstring>
#include <iostream>
#include <vector>

#include "gtest/gtest.h"

#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"

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
  TestSession(socket_t &&socket, Dbms &dbms,
              QueryEngine<TestOutputStream> &query_engine)
      : socket(std::move(socket)), logger_(logging::log->logger("TestSession")) {
    event.data.ptr = this;
  }

  bool alive() { return socket.IsOpen(); }

  int id() const { return socket.id(); }

  void execute(const byte *data, size_t len) {
    if (size_ == 0) {
      size_ = data[0];
      size_ <<= 8;
      size_ += data[1];
      data += 2;
      len -= 2;
    }
    memcpy(buffer_ + have_, data, len);
    have_ += len;
    if (have_ < size_) return;

    for (int i = 0; i < REPLY; ++i)
      ASSERT_TRUE(this->socket.Write(buffer_, size_));

    have_ = 0;
    size_ = 0;
  }

  void close() {
    logger_.trace("Close session!");
    this->socket.Close();
  }

  char buffer_[SIZE * 2];
  uint32_t have_, size_;

  Logger logger_;
  socket_t socket;
  io::network::Epoll::Event event;
};

using test_server_t =
    communication::Server<TestSession, TestOutputStream, socket_t>;

void server_start(void* serverptr, int num) {
  ((test_server_t*)serverptr)->Start(num);
}

void client_run(int num, const char* interface, const char* port, const unsigned char* data, int lo, int hi) {
  std::stringstream name;
  name << "Client " << num;
  Logger logger = logging::log->logger(name.str());
  unsigned char buffer[SIZE * REPLY], head[2];
  int have, read;
  endpoint_t endpoint(interface, port);
  socket_t socket;
  ASSERT_TRUE(socket.Connect(endpoint));
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
