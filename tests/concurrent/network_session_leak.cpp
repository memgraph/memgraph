#ifndef NDEBUG
#define NDEBUG
#endif

#include <chrono>

#include "network_common.hpp"

static constexpr const char interface[] = "127.0.0.1";
static constexpr const char port[] = "40000";

unsigned char data[SIZE];

test_server_t *serverptr;

using namespace std::chrono_literals;

TEST(Network, SessionLeak) {
  // initialize test data
  initialize_data(data, SIZE);

  // initialize listen socket
  endpoint_t endpoint(interface, port);
  socket_t socket;
  ASSERT_TRUE(socket.Bind(endpoint));
  ASSERT_TRUE(socket.SetNonBlocking());
  ASSERT_TRUE(socket.Listen(1024));

  // initialize server
  Dbms dbms;
  QueryEngine<TestOutputStream> query_engine;
  test_server_t server(std::move(socket), dbms, query_engine);
  serverptr = &server;

  // start server
  std::thread server_thread(server_start, serverptr, 2);

  // start clients
  int N = 50;
  std::vector<std::thread> clients;

  int testlen = 3000;
  for (int i = 0; i < N; ++i) {
    clients.push_back(
        std::thread(client_run, i, interface, port, data, testlen, testlen));
    std::this_thread::sleep_for(10ms);
  }

  // cleanup clients
  for (int i = 0; i < N; ++i) clients[i].join();

  std::this_thread::sleep_for(2s);

  // stop server
  server.Shutdown();
  server_thread.join();
}

// run with "valgrind --leak-check=full ./network_session_leak" to check for
// memory leaks
int main(int argc, char **argv) {
  logging::init_sync();
  logging::log->pipe(std::make_unique<Stdout>());
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
