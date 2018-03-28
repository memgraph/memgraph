#include <chrono>
#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include "gtest/gtest.h"

#include "communication/server.hpp"
#include "io/network/socket.hpp"

using namespace std::chrono_literals;

class TestData {};

class TestSession {
 public:
  TestSession(TestData &, communication::InputStream &input_stream,
              communication::OutputStream &output_stream)
      : input_stream_(input_stream), output_stream_(output_stream) {}

  void Execute() {
    LOG(INFO) << "Received data: '"
              << std::string(
                     reinterpret_cast<const char *>(input_stream_.data()),
                     input_stream_.size())
              << "'";
    output_stream_.Write(input_stream_.data(), input_stream_.size());
    input_stream_.Shift(input_stream_.size());
  }

 private:
  communication::InputStream &input_stream_;
  communication::OutputStream &output_stream_;
};

const std::string query("timeout test");

bool QueryServer(io::network::Socket &socket) {
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
  communication::Server<TestSession, TestData> server{
      {"127.0.0.1", 0}, test_data, 2, "Test", 1};

  // Create the client and connect to the server.
  io::network::Socket client;
  ASSERT_TRUE(client.Connect(server.endpoint()));

  // Send some data to the server.
  ASSERT_TRUE(QueryServer(client));

  for (int i = 0; i < 3; ++i) {
    // After this sleep the session should still be alive.
    std::this_thread::sleep_for(500ms);

    // Send some data to the server.
    ASSERT_TRUE(QueryServer(client));
  }

  // After this sleep the session should have timed out.
  std::this_thread::sleep_for(3500ms);
  ASSERT_FALSE(QueryServer(client));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}
