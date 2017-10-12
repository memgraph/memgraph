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
#include "io/network/epoll.hpp"
#include "io/network/socket.hpp"

static constexpr const char interface[] = "127.0.0.1";

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

  void Execute() { this->socket_.Write(buffer_.data(), buffer_.size()); }

  io::network::StreamBuffer Allocate() { return buffer_.Allocate(); }

  void Written(size_t len) { buffer_.Written(len); }

  void Close() { this->socket_.Close(); }

  socket_t socket_;
  communication::bolt::Buffer<> buffer_;
  io::network::Epoll::Event event_;
};

using test_server_t = communication::Server<TestSession, socket_t, TestData>;

test_server_t *serverptr;
std::atomic<bool> run{true};

void client_run(int num, const char *interface, const char *port) {
  endpoint_t endpoint(interface, port);
  socket_t socket;
  uint8_t data = 0x00;
  ASSERT_TRUE(socket.Connect(endpoint));
  ASSERT_TRUE(socket.SetTimeout(1, 0));
  // set socket timeout to 1s
  ASSERT_TRUE(socket.Write((uint8_t *)"\xAA", 1));
  ASSERT_TRUE(socket.Read(&data, 1));
  fprintf(stderr, "CLIENT %d READ 0x%02X!\n", num, data);
  ASSERT_EQ(data, 0xAA);
  while (run) std::this_thread::sleep_for(std::chrono::milliseconds(100));
  socket.Close();
}

void server_run(void *serverptr, int num) {
  ((test_server_t *)serverptr)->Start(num);
}

TEST(Network, SocketReadHangOnConcurrentConnections) {
  // initialize listen socket
  endpoint_t endpoint(interface, "0");
  socket_t socket;
  ASSERT_TRUE(socket.Bind(endpoint));
  ASSERT_TRUE(socket.SetNonBlocking());
  ASSERT_TRUE(socket.Listen(1024));

  // get bound address
  auto ep = socket.endpoint();
  printf("ADDRESS: %s, PORT: %d\n", ep.address(), ep.port());

  // initialize server
  TestData data;
  test_server_t server(std::move(socket), data);
  serverptr = &server;

  // start server
  int N = (std::thread::hardware_concurrency() + 1) / 2;
  int Nc = N * 3;
  std::thread server_thread(server_run, serverptr, N);

  // start clients
  std::vector<std::thread> clients;
  for (int i = 0; i < Nc; ++i)
    clients.push_back(std::thread(client_run, i, interface, ep.port_str()));

  // wait for 2s and stop clients
  std::this_thread::sleep_for(std::chrono::seconds(2));
  run = false;

  // cleanup clients
  for (int i = 0; i < Nc; ++i) clients[i].join();

  // stop server
  server.Shutdown();
  server_thread.join();
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
