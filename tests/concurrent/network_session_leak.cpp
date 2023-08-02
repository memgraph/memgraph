// Copyright 2023 Memgraph Ltd.
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

#include <chrono>
#include <iostream>

#include "network_common.hpp"

inline constexpr const char interface[] = "127.0.0.1";

unsigned char data[SIZE];

using namespace std::chrono_literals;

TEST(Network, SessionLeak) {
  // initialize test data
  initialize_data(data, SIZE);

  // initialize listen socket
  Endpoint endpoint(interface, 0);

  // initialize server
  TestData session_context;
  ContextT context;
  ServerT server(endpoint, &session_context, &context, -1, "Test", 2);
  ASSERT_TRUE(server.Start());

  // start clients
  int N = 50;
  std::vector<std::thread> clients;

  const auto &ep = server.endpoint();
  int testlen = 3000;
  for (int i = 0; i < N; ++i) {
    clients.push_back(std::thread(client_run, i, interface, ep.port, data, testlen, testlen));
    std::this_thread::sleep_for(10ms);
  }

  // cleanup clients
  for (int i = 0; i < N; ++i) clients[i].join();

  std::this_thread::sleep_for(2s);

  // shutdown server
  server.Shutdown();
  server.AwaitShutdown();
}

// run with "valgrind --leak-check=full ./network_session_leak" to check for
// memory leaks
int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
