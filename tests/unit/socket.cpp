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

#include <chrono>
#include <csignal>
#include <thread>

#include <gtest/gtest.h>

#include "io/network/socket.hpp"
#include "utils/timer.hpp"

TEST(Socket, WaitForReadyRead) {
  memgraph::io::network::Socket server;
  ASSERT_TRUE(server.Bind({"127.0.0.1", 0}));
  ASSERT_TRUE(server.Listen(1024));

  std::thread thread([&server] {
    memgraph::io::network::Socket client;
    ASSERT_TRUE(client.Connect(server.endpoint()));
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    ASSERT_TRUE(client.Write("test"));
  });

  uint8_t buff[100];
  auto client = server.Accept();
  ASSERT_TRUE(client);

  client->SetNonBlocking();

  ASSERT_EQ(client->Read(buff, sizeof(buff)), -1);
  ASSERT_TRUE(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR);

  memgraph::utils::Timer timer;
  ASSERT_TRUE(client->WaitForReadyRead());
  ASSERT_GT(timer.Elapsed().count(), 1.0);

  ASSERT_GT(client->Read(buff, sizeof(buff)), 0);

  thread.join();
}

TEST(Socket, WaitForReadyWrite) {
  memgraph::io::network::Socket server;
  ASSERT_TRUE(server.Bind({"127.0.0.1", 0}));
  ASSERT_TRUE(server.Listen(1024));

  std::jthread thread([&server] {
    uint8_t buff[10000];
    memgraph::io::network::Socket client;
    ASSERT_TRUE(client.Connect(server.endpoint()));
    client.SetNonBlocking();

    // Wait for server to fill its buffer and hence would WaitForReadyWrite
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    while (true) {
      int ret = client.Read(buff, sizeof(buff));
      if (ret == -1 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
        std::raise(SIGPIPE);
      } else if (ret == 0) {
        break;
      } else if (ret == -1) {
        client.WaitForReadyRead();  // reduce CPU load
      }
    }
  });

  auto connection_with_client = server.Accept();
  ASSERT_TRUE(connection_with_client);

  connection_with_client->SetNonBlocking();

  // Decrease the TCP write buffer.
  int len = 1024;
  ASSERT_EQ(setsockopt(connection_with_client->fd(), SOL_SOCKET, SO_SNDBUF, &len, sizeof(len)), 0);

  auto const payload = std::string_view{"test"};
  memgraph::utils::Timer timer;
  for (int i = 0; i < 1'000'000; ++i) {
    ASSERT_TRUE(connection_with_client->Write(payload));
  }

  connection_with_client->Close();
}
