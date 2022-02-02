// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt."""

#define BOOST_ASIO_USE_TS_EXECUTOR_AS_DEFAULT

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <thread>

#include <fmt/core.h>
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

constexpr auto kResponseSuccess{"success"};
constexpr auto kResponseMessage{"message"};

struct MockAuth : public communication::websocket::AuthenticationInterface {
  MockAuth() = default;

  bool Authenticate(const std::string & /*username*/, const std::string & /*password*/) const override {
    return authentication;
  }

  bool HasUserPermission(const std::string & /*username*/, auth::Permission /*permission*/) const override {
    return authorization;
  }

  bool HasAnyUsers() const override { return has_any_users; }

  bool authentication{true};
  bool authorization{true};
  bool has_any_users{true};
};

class WebSocketServerTest : public ::testing::Test {
 public:
 protected:
  WebSocketServerTest() : websocket_server{{"0.0.0.0", 0}, &context, auth} {
    EXPECT_NO_THROW(websocket_server.Start());
  }

  void TearDown() override {
    EXPECT_NO_THROW(websocket_server.Shutdown());
    EXPECT_NO_THROW(websocket_server.AwaitShutdown());
  }

  std::string ServerPort() const { return std::to_string(websocket_server.GetEndpoint().port()); }

  std::string ServerAddress() const { return websocket_server.GetEndpoint().address().to_string(); }

  MockAuth auth;
  communication::ServerContext context{};
  communication::websocket::Server websocket_server;
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
    const std::string response = beast::buffers_to_string(buffer_.data());
    buffer_.consume(buffer_.size());
    return response;
  }

 private:
  net::io_context ioc_{};
  websocket::stream<tcp::socket> ws_{ioc_};
  beast::flat_buffer buffer_;
};

TEST(WebSocketServer, WebsocketWorkflow) {
  /**
   * Notice how there is no port management for the clients
   * and the servers, that is because when using "0.0.0.0" as address and
   * and 0 as port number we delegate port assignment to the OS
   * and it is the keeper of all available port numbers and
   * assigns them automatically.
   */
  MockAuth auth{};
  communication::ServerContext context{};
  communication::websocket::Server websocket_server({"0.0.0.0", 0}, &context, auth);
  const auto port = websocket_server.GetEndpoint().port();

  SCOPED_TRACE(fmt::format("Checking port number different then 0: {}", port));
  EXPECT_NE(port, 0);
  EXPECT_NO_THROW(websocket_server.Start());
  EXPECT_TRUE(websocket_server.IsRunning());

  EXPECT_NO_THROW(websocket_server.Shutdown());
  EXPECT_FALSE(websocket_server.IsRunning());

  EXPECT_NO_THROW(websocket_server.AwaitShutdown());
  EXPECT_FALSE(websocket_server.IsRunning());
}

TEST_F(WebSocketServerTest, WebsocketConnection) {
  {
    auto client = Client{};
    EXPECT_NO_THROW(client.Connect("0.0.0.0", ServerPort()));
  }

  websocket_server.Shutdown();
  websocket_server.AwaitShutdown();
}

TEST_F(WebSocketServerTest, WebsocketLogging) {
  auth.has_any_users = false;
  // Set up the websocket logger as one of the defaults for spdlog
  {
    auto default_logger = spdlog::default_logger();
    auto sinks = default_logger->sinks();
    sinks.push_back(websocket_server.GetLoggingSink());

    auto logger = std::make_shared<spdlog::logger>("memgraph_log", sinks.begin(), sinks.end());
    logger->set_level(default_logger->level());
    logger->flush_on(spdlog::level::trace);
    spdlog::set_default_logger(std::move(logger));
  }
  {
    auto client = Client();
    client.Connect(ServerAddress(), ServerPort());

    auto log_message = [](spdlog::level::level_enum level, std::string message) {
      spdlog::log(level, message);
      spdlog::default_logger()->flush();
    };
    auto log_and_check = [log_message, &client](spdlog::level::level_enum level, std::string message,
                                                std::string log_level_received) {
      std::thread(log_message, level, message).detach();
      const auto received_message = client.Read();
      EXPECT_EQ(received_message, fmt::format("{{\"event\": \"log\", \"level\": \"{}\", \"message\": \"{}\"}}\n",
                                              log_level_received, message));
    };

    log_and_check(spdlog::level::err, "Sending error message!", "error");
    log_and_check(spdlog::level::warn, "Sending warn message!", "warning");
    log_and_check(spdlog::level::info, "Sending info message!", "info");
    log_and_check(spdlog::level::trace, "Sending trace message!", "trace");
  }
}

TEST_F(WebSocketServerTest, WebsocketAuthenticationParsingError) {
  constexpr auto auth_fail = "Cannot parse JSON for WebSocket authentication";

  {
    SCOPED_TRACE("Checking handling of first request parsing error.");
    auto client = Client();
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
    auto client = Client();
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

TEST_F(WebSocketServerTest, WebsocketAuthenticationWhenAuthPasses) {
  constexpr auto auth_success = R"({"message":"User has been successfully authenticated!","success":true})";

  {
    SCOPED_TRACE("Checking successful authentication response.");
    auto client = Client();
    EXPECT_NO_THROW(client.Connect(ServerAddress(), ServerPort()));
    EXPECT_NO_THROW(client.Write(R"({"username": "user", "password": "123"})"));
    const auto response = client.Read();

    EXPECT_EQ(response, auth_success);
  }
}

TEST_F(WebSocketServerTest, WebsocketAuthenticationWithMultipleAttempts) {
  constexpr auto auth_success = R"({"message":"User has been successfully authenticated!","success":true})";
  constexpr auto auth_fail = "Cannot parse JSON for WebSocket authentication";

  {
    SCOPED_TRACE("Checking multiple authentication tries from same client");
    auto client = Client();
    EXPECT_NO_THROW(client.Connect(ServerAddress(), ServerPort()));
    EXPECT_NO_THROW(client.Write(R"({"username": "user" "password": "123"})"));
    const auto response1 = nlohmann::json::parse(client.Read());
    const auto message_header1 = response1[kResponseMessage].get<std::string>();
    const auto message_first_part1 = message_header1.substr(0, message_header1.find(": "));

    EXPECT_FALSE(response1[kResponseSuccess]);
    EXPECT_EQ(message_first_part1, auth_fail);

    EXPECT_NO_THROW(client.Connect(ServerAddress(), ServerPort()));
    EXPECT_NO_THROW(client.Write(R"({"username": "user", "password": "123"})"));
    const auto response2 = client.Read();
    EXPECT_EQ(response2, auth_success);
  }
  {
    SCOPED_TRACE("Checking multiple authentication tries from different clients");
    auto client1 = Client();
    auto client2 = Client();

    EXPECT_NO_THROW(client1.Connect(ServerAddress(), ServerPort()));
    EXPECT_NO_THROW(client2.Connect(ServerAddress(), ServerPort()));

    EXPECT_NO_THROW(client1.Write(R"({"username": "user" "password": "123"})"));
    EXPECT_NO_THROW(client2.Write(R"({"username": "user", "password": "123"})"));

    const auto response1 = nlohmann::json::parse(client1.Read());
    const auto message_header1 = response1[kResponseMessage].get<std::string>();
    const auto message_first_part1 = message_header1.substr(0, message_header1.find(": "));

    EXPECT_FALSE(response1[kResponseSuccess]);
    EXPECT_EQ(message_first_part1, auth_fail);

    const auto response2 = client2.Read();
    EXPECT_EQ(response2, auth_success);
  }
}

TEST_F(WebSocketServerTest, WebsocketAuthenticationFails) {
  auth.authentication = false;

  constexpr auto auth_fail = R"({"message":"Authentication failed!","success":false})";
  {
    auto client = Client();
    EXPECT_NO_THROW(client.Connect(ServerAddress(), ServerPort()));
    EXPECT_NO_THROW(client.Write(R"({"username": "user", "password": "123"})"));

    const auto response = client.Read();
    EXPECT_EQ(response, auth_fail);
  }
}

#ifdef MG_ENTERPRISE
TEST_F(WebSocketServerTest, WebsocketAuthorizationFails) {
  auth.authorization = false;
  constexpr auto auth_fail = R"({"message":"Authorization failed!","success":false})";

  {
    auto client = Client();
    EXPECT_NO_THROW(client.Connect(ServerAddress(), ServerPort()));
    EXPECT_NO_THROW(client.Write(R"({"username": "user", "password": "123"})"));

    const auto response = client.Read();
    EXPECT_EQ(response, auth_fail);
  }
}
#endif
