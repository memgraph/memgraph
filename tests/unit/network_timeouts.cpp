// Copyright 2022 Memgraph Ltd.
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
#include <iostream>
#include <thread>

#include <gflags/gflags.h>
#include "gtest/gtest.h"

#include "communication/server.hpp"
#include "io/network/socket.hpp"

using namespace std::chrono_literals;

class TestData {};

class TestSession {
 public:
  TestSession(TestData *, const memgraph::io::network::Endpoint &, memgraph::communication::InputStream *input_stream,
              memgraph::communication::OutputStream *output_stream)
      : input_stream_(input_stream), output_stream_(output_stream) {}

  void Execute() {
    spdlog::info("Received data: '{}'",
                 std::string(reinterpret_cast<const char *>(input_stream_->data()), input_stream_->size()));
    if (input_stream_->data()[0] == 'e') {
      std::this_thread::sleep_for(std::chrono::seconds(5));
    }
    output_stream_->Write(input_stream_->data(), input_stream_->size());
    input_stream_->Shift(input_stream_->size());
  }

 private:
  memgraph::communication::InputStream *input_stream_;
  memgraph::communication::OutputStream *output_stream_;
};

const std::string safe_query("tttt");
const std::string expensive_query("eeee");

bool QueryServer(memgraph::io::network::Socket &socket, const std::string &query) {
  if (!socket.Write(query)) return false;
  char response[105];
  int len = 0;
  while (len < query.size()) {
    int got = socket.Read(response + len, query.size() - len);
    if (got <= 0) return false;
    response[got] = 0;
    len += got;
  }
  if (std::string(response, strlen(response)) != query) return false;
  return true;
}

TEST(NetworkTimeouts, InactiveSession) {
  // Instantiate the server and set the session timeout to 2 seconds.
  TestData test_data;
  memgraph::communication::ServerContext context;
  memgraph::communication::Server<TestSession, TestData> server{{"127.0.0.1", 0}, &test_data, &context, 2, "Test", 1};
  ASSERT_TRUE(server.Start());

  // Create the client and connect to the server.
  memgraph::io::network::Socket client;
  ASSERT_TRUE(client.Connect(server.endpoint()));

  // Send some data to the server.
  ASSERT_TRUE(QueryServer(client, safe_query));

  for (int i = 0; i < 3; ++i) {
    // After this sleep the session should still be alive.
    std::this_thread::sleep_for(500ms);

    // Send some data to the server.
    ASSERT_TRUE(QueryServer(client, safe_query));
  }

  // After this sleep the session should have timed out.
  std::this_thread::sleep_for(3500ms);
  ASSERT_FALSE(QueryServer(client, safe_query));

  // Shutdown the server.
  server.Shutdown();
  server.AwaitShutdown();
}

TEST(NetworkTimeouts, ActiveSession) {
  // Instantiate the server and set the session timeout to 2 seconds.
  TestData test_data;
  memgraph::communication::ServerContext context;
  memgraph::communication::Server<TestSession, TestData> server{{"127.0.0.1", 0}, &test_data, &context, 2, "Test", 1};
  ASSERT_TRUE(server.Start());

  // Create the client and connect to the server.
  memgraph::io::network::Socket client;
  ASSERT_TRUE(client.Connect(server.endpoint()));

  // Send some data to the server.
  ASSERT_TRUE(QueryServer(client, expensive_query));

  for (int i = 0; i < 3; ++i) {
    // After this sleep the session should still be alive.
    std::this_thread::sleep_for(500ms);

    // Send some data to the server.
    ASSERT_TRUE(QueryServer(client, safe_query));
  }

  // After this sleep the session should have timed out.
  std::this_thread::sleep_for(3500ms);
  ASSERT_FALSE(QueryServer(client, safe_query));

  // Shutdown the server.
  server.Shutdown();
  server.AwaitShutdown();
}
