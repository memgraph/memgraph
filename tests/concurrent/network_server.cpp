#ifndef NDEBUG
#define NDEBUG
#endif

#include "network_common.hpp"

static constexpr const char interface[] = "127.0.0.1";

unsigned char data[SIZE];

test_server_t *serverptr;

TEST(Network, Server) {
  // initialize test data
  initialize_data(data, SIZE);

  // initialize listen socket
  endpoint_t endpoint(interface, "0");
  socket_t socket;
  ASSERT_TRUE(socket.Bind(endpoint));
  ASSERT_TRUE(socket.SetNonBlocking());
  ASSERT_TRUE(socket.Listen(1024));

  // get bound address
  auto ep = socket.endpoint();
  printf("ADDRESS: %s, PORT: %d\n", ep.address(), ep.port());

  // initialize server
  Dbms dbms;
  QueryEngine<TestOutputStream> query_engine;
  test_server_t server(std::move(socket), dbms, query_engine);
  serverptr = &server;

  // start server
  int N = (std::thread::hardware_concurrency() + 1) / 2;
  std::thread server_thread(server_start, serverptr, N);

  // start clients
  std::vector<std::thread> clients;
  for (int i = 0; i < N; ++i)
    clients.push_back(std::thread(client_run, i, interface, ep.port_str(), data,
                                  30000, SIZE));

  // cleanup clients
  for (int i = 0; i < N; ++i) clients[i].join();

  // stop server
  server.Shutdown();
  server_thread.join();
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
