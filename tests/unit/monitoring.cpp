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

#include <atomic>
#include <cstddef>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <fmt/core.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <spdlog/common.h>
#include <spdlog/spdlog.h>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/websocket.hpp>

#include "communication/websocket/auth.hpp"
#include "communication/websocket/server.hpp"

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

inline constexpr auto kResponseSuccess{"success"};
inline constexpr auto kResponseMessage{"message"};

struct MockAuth : public memgraph::communication::websocket::AuthenticationInterface {
  bool Authenticate(const std::string & /*username*/, const std::string & /*password*/) const override {
    return authentication;
  }

  bool HasPermission(memgraph::auth::Permission /*permission*/) const override { return authorization; }

  bool AccessControlled() const override { return has_any_users; }

  bool authentication{true};
  bool authorization{true};
  bool has_any_users{true};
};

class MonitoringServerTest : public ::testing::Test {
 protected:
  void SetUp() override { ASSERT_NO_THROW(monitoring_server.Start()); }

  void TearDown() override {
    StopLogging();
    ASSERT_NO_THROW(monitoring_server.Shutdown());
    ASSERT_NO_THROW(monitoring_server.AwaitShutdown());
  }

  std::string ServerPort() const { return std::to_string(monitoring_server.GetEndpoint().port()); }

  std::string ServerAddress() const { return monitoring_server.GetEndpoint().address().to_string(); }

  void StartLogging(std::vector<std::pair<spdlog::level::level_enum, std::string>> messages) {
    messages_ = std::move(messages);
    logging_.store(true, std::memory_order_relaxed);
    bg_thread_ = std::jthread([this]() {
      while (logging_.load(std::memory_order_relaxed)) {
        for (const auto &[message_level, message_content] : messages_) {
          spdlog::log(message_level, message_content);
          spdlog::default_logger()->flush();
        }
      }
    });
  }

  void StopLogging() {
    if (!logging_.load(std::memory_order_relaxed)) {
      return;
    }
    logging_.store(false, std::memory_order_relaxed);
    ASSERT_TRUE(bg_thread_.joinable());
    bg_thread_.join();
  }

  MockAuth auth;
  memgraph::communication::ServerContext context;
  memgraph::communication::websocket::Server monitoring_server{{"0.0.0.0", 0}, &context, auth};

 private:
  std::jthread bg_thread_;
  std::vector<std::pair<spdlog::level::level_enum, std::string>> messages_;
  std::atomic<bool> logging_{false};
};

class Client {
 public:
  ~Client() { ws_.close(websocket::close_code::normal); }

  void Connect(const std::string &host, const std::string &port) {
    tcp::resolver resolver{ioc_};
    auto endpoint_ = resolver.resolve(host, port);
    auto ep = net::connect(ws_.next_layer(), endpoint_);
    const auto server = fmt::format("{}:{}", host, ep.port());
    ws_.set_option(websocket::stream_base::decorator([](websocket::request_type &req) {
      req.set(http::field::user_agent, std::string(BOOST_BEAST_VERSION_STRING) + " websocket-client-coro");
    }));

    // Perform the websocket handshake
    ws_.handshake(host, "/");
  }

  void Write(const std::string &msg) { ws_.write(net::buffer(msg)); }

  std::string Read() {
    ws_.read(buffer_);
    std::string response = beast::buffers_to_string(buffer_.data());
    buffer_.consume(buffer_.size());
    return response;
  }

 private:
  net::io_context ioc_;
  websocket::stream<tcp::socket> ws_{ioc_};
  beast::flat_buffer buffer_;
};

TEST(MonitoringServer, MonitoringWorkflow) {
  /**
   * Notice how there is no port management for the clients
   * and the servers, that is because when using "0.0.0.0" as address and
   * and 0 as port number we delegate port assignment to the OS
   * and it is the keeper of all available port numbers and
   * assigns them automatically.
   */
  MockAuth auth;
  memgraph::communication::ServerContext context;
  memgraph::communication::websocket::Server monitoring_server({"0.0.0.0", 0}, &context, auth);
  const auto port = monitoring_server.GetEndpoint().port();

  SCOPED_TRACE(fmt::format("Checking port number different then 0: {}", port));
  EXPECT_NE(port, 0);
  EXPECT_NO_THROW(monitoring_server.Start());
  EXPECT_TRUE(monitoring_server.IsRunning());

  EXPECT_NO_THROW(monitoring_server.Shutdown());
  EXPECT_FALSE(monitoring_server.IsRunning());

  EXPECT_NO_THROW(monitoring_server.AwaitShutdown());
  EXPECT_FALSE(monitoring_server.IsRunning());
}

TEST(MonitoringServer, Connection) {
  MockAuth auth;
  memgraph::communication::ServerContext context;
  memgraph::communication::websocket::Server monitoring_server({"0.0.0.0", 0}, &context, auth);
  ASSERT_NO_THROW(monitoring_server.Start());
  {
    Client client;
    EXPECT_NO_THROW(client.Connect("0.0.0.0", std::to_string(monitoring_server.GetEndpoint().port())));
  }

  ASSERT_NO_THROW(monitoring_server.Shutdown());
  ASSERT_NO_THROW(monitoring_server.AwaitShutdown());
  ASSERT_FALSE(monitoring_server.IsRunning());
}

TEST_F(MonitoringServerTest, Logging) {
  auth.has_any_users = false;
  // Set up the websocket logger as one of the defaults for spdlog
  {
    auto default_logger = spdlog::default_logger();
    auto sinks = default_logger->sinks();
    sinks.push_back(monitoring_server.GetLoggingSink());

    auto logger = std::make_shared<spdlog::logger>("memgraph_log", sinks.begin(), sinks.end());
    logger->set_level(default_logger->level());
    logger->flush_on(spdlog::level::trace);
    spdlog::set_default_logger(std::move(logger));
  }
  Client client;
  client.Connect(ServerAddress(), ServerPort());
  std::vector<std::pair<spdlog::level::level_enum, std::string>> messages{
      {spdlog::level::err, "Sending error message!"},
      {spdlog::level::warn, "Sending warn message!"},
      {spdlog::level::info, "Sending info message!"},
      {spdlog::level::trace, "Sending trace message!"},
  };

  StartLogging(messages);
  std::unordered_set<std::string> received_messages;
  // Worst case scenario we might need to read 100 messages to get all messages
  // that we expect, in case messages get scrambled in network
  for (size_t i{0}; i < 100; ++i) {
    const auto received_message = client.Read();
    received_messages.insert(received_message);
    if (received_messages.size() == 4) {
      break;
    }
  }
  ASSERT_EQ(received_messages.size(), 4);
  for (const auto &[message_level, message_content] : messages) {
    EXPECT_TRUE(
        received_messages.contains(fmt::format("{{\"event\": \"log\", \"level\": \"{}\", \"message\": \"{}\"}}\n",
                                               spdlog::level::to_string_view(message_level), message_content)));
  }
}

TEST_F(MonitoringServerTest, AuthenticationParsingError) {
  static constexpr auto auth_fail = "Cannot parse JSON for WebSocket authentication";

  {
    SCOPED_TRACE("Checking handling of first request parsing error.");
    Client client;
    EXPECT_NO_THROW(client.Connect(ServerAddress(), ServerPort()));
    EXPECT_NO_THROW(client.Write("Test"));
    const auto response = nlohmann::json::parse(client.Read());
    const auto message_header = response[kResponseMessage].get<std::string>();
    const auto message_first_part = message_header.substr(0, message_header.find(": "));

    EXPECT_FALSE(response[kResponseSuccess]);
    EXPECT_EQ(message_first_part, auth_fail);
  }
  {
    SCOPED_TRACE("Checking handling of JSON parsing error.");
    Client client;
    EXPECT_NO_THROW(client.Connect(ServerAddress(), ServerPort()));
    const std::string json_without_comma = R"({"username": "user" "password": "123"})";
    EXPECT_NO_THROW(client.Write(json_without_comma));
    const auto response = nlohmann::json::parse(client.Read());
    const auto message_header = response[kResponseMessage].get<std::string>();
    const auto message_first_part = message_header.substr(0, message_header.find(": "));

    EXPECT_FALSE(response[kResponseSuccess]);
    EXPECT_EQ(message_first_part, auth_fail);
  }
}

TEST_F(MonitoringServerTest, AuthenticationWhenAuthPasses) {
  static constexpr auto auth_success = R"({"message":"User has been successfully authenticated!","success":true})";

  {
    SCOPED_TRACE("Checking successful authentication response.");
    Client client;
    EXPECT_NO_THROW(client.Connect(ServerAddress(), ServerPort()));
    EXPECT_NO_THROW(client.Write(R"({"username": "user", "password": "123"})"));
    const auto response = client.Read();

    EXPECT_EQ(response, auth_success);
  }
}

TEST_F(MonitoringServerTest, AuthenticationWithMultipleAttempts) {
  static constexpr auto auth_success = R"({"message":"User has been successfully authenticated!","success":true})";
  static constexpr auto auth_fail = "Cannot parse JSON for WebSocket authentication";

  {
    SCOPED_TRACE("Checking multiple authentication tries from same client");
    Client client;
    EXPECT_NO_THROW(client.Connect(ServerAddress(), ServerPort()));
    EXPECT_NO_THROW(client.Write(R"({"username": "user" "password": "123"})"));

    {
      const auto response = nlohmann::json::parse(client.Read());
      const auto message_header = response[kResponseMessage].get<std::string>();
      const auto message_first_part = message_header.substr(0, message_header.find(": "));

      EXPECT_FALSE(response[kResponseSuccess]);
      EXPECT_EQ(message_first_part, auth_fail);
    }
    {
      EXPECT_NO_THROW(client.Connect(ServerAddress(), ServerPort()));
      EXPECT_NO_THROW(client.Write(R"({"username": "user", "password": "123"})"));
      const auto response = client.Read();
      EXPECT_EQ(response, auth_success);
    }
  }
  {
    SCOPED_TRACE("Checking multiple authentication tries from different clients");
    Client client1;
    Client client2;

    EXPECT_NO_THROW(client1.Connect(ServerAddress(), ServerPort()));
    EXPECT_NO_THROW(client2.Connect(ServerAddress(), ServerPort()));

    EXPECT_NO_THROW(client1.Write(R"({"username": "user" "password": "123"})"));
    EXPECT_NO_THROW(client2.Write(R"({"username": "user", "password": "123"})"));

    {
      const auto response = nlohmann::json::parse(client1.Read());
      const auto message_header = response[kResponseMessage].get<std::string>();
      const auto message_first_part = message_header.substr(0, message_header.find(": "));

      EXPECT_FALSE(response[kResponseSuccess]);
      EXPECT_EQ(message_first_part, auth_fail);
    }
    {
      const auto response = client2.Read();
      EXPECT_EQ(response, auth_success);
    }
  }
}

TEST_F(MonitoringServerTest, AuthenticationFails) {
  auth.authentication = false;

  static constexpr auto auth_fail = R"({"message":"Authentication failed!","success":false})";
  {
    Client client;
    EXPECT_NO_THROW(client.Connect(ServerAddress(), ServerPort()));
    EXPECT_NO_THROW(client.Write(R"({"username": "user", "password": "123"})"));

    const auto response = client.Read();
    EXPECT_EQ(response, auth_fail);
  }
}

#ifdef MG_ENTERPRISE
TEST_F(MonitoringServerTest, AuthorizationFails) {
  auth.authorization = false;
  static constexpr auto auth_fail = R"({"message":"Authorization failed!","success":false})";

  {
    Client client;
    EXPECT_NO_THROW(client.Connect(ServerAddress(), ServerPort()));
    EXPECT_NO_THROW(client.Write(R"({"username": "user", "password": "123"})"));

    const auto response = client.Read();
    EXPECT_EQ(response, auth_fail);
  }
}
#endif
