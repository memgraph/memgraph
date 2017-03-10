#ifndef NDEBUG
#define NDEBUG
#endif

#include "network_common.hpp"

static constexpr const char interface[] = "127.0.0.1";
static constexpr const char port[] = "30000";

unsigned char data[SIZE];

test_server_t *serverptr;

TEST(Network, Server) {
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
  int N = std::thread::hardware_concurrency() / 2;
  std::thread server_thread(server_start, serverptr, N);

  // start clients
  std::vector<std::thread> clients;
  for (int i = 0; i < N; ++i)
    clients.push_back(
        std::thread(client_run, i, interface, port, data, 30000, SIZE));

  // cleanup clients
  for (int i = 0; i < N; ++i) clients[i].join();

  // stop server
  server.Shutdown();
  server_thread.join();
}

int main(int argc, char **argv) {
  logging::init_async();
  logging::log->pipe(std::make_unique<Stdout>());
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
