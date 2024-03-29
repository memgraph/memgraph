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

#include <iostream>

#include "network_common.hpp"

inline constexpr const char interface[] = "127.0.0.1";

unsigned char data[SIZE];

TEST(Network, Server) {
  // initialize test data
  initialize_data(data, SIZE);

  // initialize listen socket
  Endpoint endpoint(interface, 0);
  std::cout << endpoint << std::endl;

  // initialize server
  TestData session_context;
  int N = (std::thread::hardware_concurrency() + 1) / 2;
  ContextT context;
  ServerT server(endpoint, &session_context, &context, -1, "Test", N);
  ASSERT_TRUE(server.Start());

  const auto &ep = server.endpoint();
  // start clients
  std::vector<std::thread> clients;
  for (int i = 0; i < N; ++i) clients.push_back(std::thread(client_run, i, interface, ep.port, data, 30000, SIZE));

  // cleanup clients
  for (int i = 0; i < N; ++i) clients[i].join();

  // shutdown server
  server.Shutdown();
  server.AwaitShutdown();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
