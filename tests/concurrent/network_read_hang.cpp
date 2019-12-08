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

#include "communication/server.hpp"

static constexpr const char interface[] = "127.0.0.1";

using io::network::Endpoint;
using io::network::Socket;

class TestData {};

class TestSession {
 public:
  TestSession(TestData *, const io::network::Endpoint &,
              communication::InputStream *input_stream,
              communication::OutputStream *output_stream)
      : input_stream_(input_stream), output_stream_(output_stream) {}

  void Execute() {
    output_stream_->Write(input_stream_->data(), input_stream_->size());
  }

  communication::InputStream *input_stream_;
  communication::OutputStream *output_stream_;
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
  communication::ServerContext context;
  communication::Server<TestSession, TestData> server(endpoint, &data, &context,
                                                      -1, "Test", N);
  ASSERT_TRUE(server.Start());

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

  // shutdown server
  server.Shutdown();
  server.AwaitShutdown();
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
