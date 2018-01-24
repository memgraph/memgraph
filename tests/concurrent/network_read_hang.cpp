#ifndef NDEBUG
#define NDEBUG
#endif

#include <array>
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "communication/bolt/v1/decoder/buffer.hpp"
#include "communication/server.hpp"
#include "database/graph_db_accessor.hpp"
#include "io/network/epoll.hpp"
#include "io/network/socket.hpp"

static constexpr const char interface[] = "127.0.0.1";

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

  void Execute() { this->socket_.Write(buffer_.data(), buffer_.size()); }

  io::network::StreamBuffer Allocate() { return buffer_.Allocate(); }

  void Written(size_t len) { buffer_.Written(len); }

  void Close() { this->socket_.Close(); }

  Socket &socket() { return socket_; }

  void RefreshLastEventTime(
      const std::chrono::time_point<std::chrono::steady_clock>
          &last_event_time) {
    last_event_time_ = last_event_time;
  }

  Socket socket_;
  communication::bolt::Buffer<> buffer_;
  io::network::Epoll::Event event_;
  std::chrono::time_point<std::chrono::steady_clock> last_event_time_;
};

std::atomic<bool> run{true};

void client_run(int num, const char *interface, uint16_t port) {
  Endpoint endpoint(interface, port);
  Socket socket;
  uint8_t data = 0x00;
  ASSERT_TRUE(socket.Connect(endpoint));
  socket.SetTimeout(1, 0);
  // set socket timeout to 1s
  ASSERT_TRUE(socket.Write((uint8_t *)"\xAA", 1));
  ASSERT_TRUE(socket.Read(&data, 1));
  fprintf(stderr, "CLIENT %d READ 0x%02X!\n", num, data);
  ASSERT_EQ(data, 0xAA);
  while (run) std::this_thread::sleep_for(std::chrono::milliseconds(100));
  socket.Close();
}

TEST(Network, SocketReadHangOnConcurrentConnections) {
  // initialize listen socket
  Endpoint endpoint(interface, 0);

  std::cout << endpoint << std::endl;

  // initialize server
  TestData data;
  int N = (std::thread::hardware_concurrency() + 1) / 2;
  int Nc = N * 3;
  communication::Server<TestSession, TestData> server(endpoint, data, N);

  const auto &ep = server.endpoint();
  // start clients
  std::vector<std::thread> clients;
  for (int i = 0; i < Nc; ++i)
    clients.push_back(std::thread(client_run, i, interface, ep.port()));

  // wait for 2s and stop clients
  std::this_thread::sleep_for(std::chrono::seconds(2));
  run = false;

  // cleanup clients
  for (int i = 0; i < Nc; ++i) clients[i].join();
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
