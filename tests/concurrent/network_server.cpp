#ifndef NDEBUG
#define NDEBUG
#endif

#include <iostream>

#include "network_common.hpp"

static constexpr const char interface[] = "127.0.0.1";

unsigned char data[SIZE];

TEST(Network, Server) {
  // initialize test data
  initialize_data(data, SIZE);

  // initialize listen socket
  Endpoint endpoint(interface, 0);
  std::cout << endpoint << std::endl;

  // initialize server
  TestData session_data;
  int N = (std::thread::hardware_concurrency() + 1) / 2;
  ServerT server(endpoint, session_data, -1, "Test", N);

  const auto &ep = server.endpoint();
  // start clients
  std::vector<std::thread> clients;
  for (int i = 0; i < N; ++i)
    clients.push_back(
        std::thread(client_run, i, interface, ep.port(), data, 30000, SIZE));

  // cleanup clients
  for (int i = 0; i < N; ++i) clients[i].join();
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
