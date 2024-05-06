// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#ifndef NDEBUG
#define NDEBUG
#endif

#include <array>
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "communication/server.hpp"

inline constexpr const char interface[] = "127.0.0.1";

using memgraph::io::network::Endpoint;
using memgraph::io::network::Socket;

class TestData {};

class TestSession {
 public:
  TestSession(TestData *, const memgraph::io::network::Endpoint &, memgraph::communication::InputStream *input_stream,
              memgraph::communication::OutputStream *output_stream)
      : input_stream_(input_stream), output_stream_(output_stream) {}

  void Execute() { output_stream_->Write(input_stream_->data(), input_stream_->size()); }

  memgraph::communication::InputStream *input_stream_;
  memgraph::communication::OutputStream *output_stream_;
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
  memgraph::communication::ServerContext context;
  memgraph::communication::Server<TestSession, TestData> server(endpoint, &data, &context, -1, "Test", N);
  ASSERT_TRUE(server.Start());

  const auto &ep = server.endpoint();
  // start clients
  std::vector<std::thread> clients;
  for (int i = 0; i < Nc; ++i) clients.push_back(std::thread(client_run, i, interface, ep.GetPort()));

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
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
