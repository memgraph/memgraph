#include <chrono>
#include <csignal>
#include <thread>

#include <gtest/gtest.h>

#include "io/network/socket.hpp"
#include "utils/timer.hpp"

TEST(Socket, WaitForReadyRead) {
  io::network::Socket server;
  ASSERT_TRUE(server.Bind({"127.0.0.1", 0}));
  ASSERT_TRUE(server.Listen(1024));

  std::thread thread([&server] {
    io::network::Socket client;
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

  utils::Timer timer;
  ASSERT_TRUE(client->WaitForReadyRead());
  ASSERT_GT(timer.Elapsed().count(), 1.0);

  ASSERT_GT(client->Read(buff, sizeof(buff)), 0);

  thread.join();
}

TEST(Socket, WaitForReadyWrite) {
  io::network::Socket server;
  ASSERT_TRUE(server.Bind({"127.0.0.1", 0}));
  ASSERT_TRUE(server.Listen(1024));

  std::thread thread([&server] {
    uint8_t buff[10000];
    io::network::Socket client;
    ASSERT_TRUE(client.Connect(server.endpoint()));
    client.SetNonBlocking();

    // Decrease the TCP read buffer.
    int len = 1024;
    ASSERT_EQ(setsockopt(client.fd(), SOL_SOCKET, SO_RCVBUF, &len, sizeof(len)),
              0);

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    while (true) {
      int ret = client.Read(buff, sizeof(buff));
      if (ret == -1 && errno != EAGAIN && errno != EWOULDBLOCK &&
          errno != EINTR) {
        std::raise(SIGPIPE);
      } else if (ret == 0) {
        break;
      }
    }
  });

  auto client = server.Accept();
  ASSERT_TRUE(client);

  client->SetNonBlocking();

  // Decrease the TCP write buffer.
  int len = 1024;
  ASSERT_EQ(setsockopt(client->fd(), SOL_SOCKET, SO_SNDBUF, &len, sizeof(len)),
            0);

  utils::Timer timer;
  for (int i = 0; i < 1000000; ++i) {
    ASSERT_TRUE(client->Write("test"));
  }
  ASSERT_GT(timer.Elapsed().count(), 1.0);

  client->Close();

  thread.join();
}
