#ifndef NDEBUG
#define NDEBUG
#endif

#include <chrono>
#include <iostream>

#include "network_common.hpp"

static constexpr const char interface[] = "127.0.0.1";

unsigned char data[SIZE];

using namespace std::chrono_literals;

TEST(Network, SessionLeak) {
  // initialize test data
  initialize_data(data, SIZE);

  // initialize listen socket
  Endpoint endpoint(interface, 0);

  // initialize server
  TestData session_data;
  ContextT context;
  ServerT server(endpoint, session_data, &context, -1, "Test", 2);

  // start clients
  int N = 50;
  std::vector<std::thread> clients;

  const auto &ep = server.endpoint();
  int testlen = 3000;
  for (int i = 0; i < N; ++i) {
    clients.push_back(std::thread(client_run, i, interface, ep.port(), data,
                                  testlen, testlen));
    std::this_thread::sleep_for(10ms);
  }

  // cleanup clients
  for (int i = 0; i < N; ++i) clients[i].join();

  std::this_thread::sleep_for(2s);
}

// run with "valgrind --leak-check=full ./network_session_leak" to check for
// memory leaks
int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
