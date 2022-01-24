// Copyright 2021 Memgraph Ltd.
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
#include <string_view>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/websocket.hpp>
#include <json/json.hpp>

#include "communication/websocket/auth.hpp"
#include "communication/websocket/server.hpp"

class MockAuth : public communication::websocket::IAuthentication {
 public:
  MockAuth() = default;
  MockAuth(bool authentication, bool authorization, bool has_any_users)
      : authentication_{authentication}, authorization_{authorization}, has_any_users_{has_any_users} {}

  bool Authenticate(const std::string & /*username*/, const std::string & /*password*/) const override {
    return authentication_;
  }

  bool HasUserPermission(const std::string & /*username*/, auth::Permission /*permission*/) const override {
    return authorization_;
  }

  bool HasAnyUsers() const override { return has_any_users_; }

 private:
  bool authentication_{true};
  bool authorization_{true};
  bool has_any_users_{true};
};

namespace beast = boost::beast;          // from <boost/beast.hpp>
namespace http = beast::http;            // from <boost/beast/http.hpp>
namespace websocket = beast::websocket;  // from <boost/beast/websocket.hpp>
namespace net = boost::asio;             // from <boost/asio.hpp>
using tcp = boost::asio::ip::tcp;        // from <boost/asio/ip/tcp.hpp>

constexpr auto kResponseSuccess{"success"};
constexpr auto kResponseMessage{"message"};

class Client {
 public:
  Client() : ioc_{}, ws_{ioc_} {}

  ~Client() { ws_.close(websocket::close_code::normal); }

  void Connect(const std::string &host, const std::string &port) {
    tcp::resolver resolver{ioc_};
    auto endpoint_ = resolver.resolve(host, port);
    auto ep = net::connect(ws_.next_layer(), endpoint_);
    const auto server = host + ":" + std::to_string(ep.port());
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
  net::io_context ioc_;
  websocket::stream<tcp::socket> ws_;
  beast::flat_buffer buffer_;
};

TEST(WebSocketServer, WebSockerServerCreation) {
  MockAuth auth{};
  EXPECT_NO_THROW(communication::websocket::Server websocket_server({"0.0.0.0", 7444}, &auth));
}

TEST(WebSocketServer, WebsocketWorkflow) {
  MockAuth auth{};
  communication::websocket::Server websocket_server({"0.0.0.0", 7444}, &auth);

  EXPECT_NO_THROW(websocket_server.Start());
  EXPECT_TRUE(websocket_server.IsRunning());

  EXPECT_NO_THROW(websocket_server.Shutdown());
  EXPECT_FALSE(websocket_server.IsRunning());

  EXPECT_NO_THROW(websocket_server.AwaitShutdown());
  EXPECT_FALSE(websocket_server.IsRunning());
}

TEST(WebSocketServer, WebsocketLoggingSink) {
  MockAuth auth{};
  communication::websocket::Server websocket_server({"0.0.0.0", 7444}, &auth);

  EXPECT_NO_THROW(websocket_server.GetLoggingSink());
}

TEST(WebSocketServer, WebsocketConnection) {
  MockAuth auth{};
  communication::websocket::Server websocket_server({"0.0.0.0", 7444}, &auth);
  websocket_server.Start();

  {
    auto client = Client{};
    EXPECT_NO_THROW(client.Connect("0.0.0.0", "7444"));
  }

  websocket_server.Shutdown();
  websocket_server.AwaitShutdown();
}

TEST(WebSocketServer, WebsocketAuthenticationWhenAuthPasses) {
  MockAuth auth{true, true, true};
  communication::websocket::Server websocket_server({"0.0.0.0", 7444}, &auth);
  websocket_server.Start();
  constexpr auto host = "0.0.0.0";
  constexpr auto port = "7444";
  constexpr auto auth_fail = "Cannot parse JSON for WebSocket authentication";
  constexpr auto auth_success = R"({"message":"User has been successfully authenticated!","success":true})";

  {
    auto client = Client();
    EXPECT_NO_THROW(client.Connect(host, port));
    EXPECT_NO_THROW(client.Write("Test"));
    const auto response = nlohmann::json::parse(client.Read());
    const auto message_header = response[kResponseMessage].get<std::string>();
    const auto message_first_part = message_header.substr(0, message_header.find(": "));

    EXPECT_FALSE(response[kResponseSuccess]);
    EXPECT_EQ(message_first_part, auth_fail) << "Message was: " << message_header;
  }
  {
    auto client = Client();
    EXPECT_NO_THROW(client.Connect(host, port));
    EXPECT_NO_THROW(client.Write(R"({"username": "user" "password": "123"})"));
    const auto response = nlohmann::json::parse(client.Read());
    const auto message_header = response[kResponseMessage].get<std::string>();
    const auto message_first_part = message_header.substr(0, message_header.find(": "));

    EXPECT_FALSE(response[kResponseSuccess]);
    EXPECT_EQ(message_first_part, auth_fail) << "Message was: " << message_header;
  }
  {
    auto client = Client();
    EXPECT_NO_THROW(client.Connect(host, port));
    EXPECT_NO_THROW(client.Write(R"({"username": "user", "password": "123"})"));
    const auto response = client.Read();

    EXPECT_TRUE(response == auth_success) << "Response was: " << response;
  }
  {
    auto client = Client();
    EXPECT_NO_THROW(client.Connect(host, port));
    EXPECT_NO_THROW(client.Write(R"({"username": "user" "password": "123"})"));
    const auto response1 = nlohmann::json::parse(client.Read());
    const auto message_header1 = response1[kResponseMessage].get<std::string>();
    const auto message_first_part1 = message_header1.substr(0, message_header1.find(": "));

    EXPECT_FALSE(response1[kResponseSuccess]);
    EXPECT_EQ(message_first_part1, auth_fail) << "Message was: " << message_header1;

    EXPECT_NO_THROW(client.Connect(host, port));
    EXPECT_NO_THROW(client.Write(R"({"username": "user", "password": "123"})"));
    const auto response2 = client.Read();
    EXPECT_TRUE(response2 == auth_success) << "Response was: " << response2;
  }
  {
    auto client1 = Client();
    auto client2 = Client();

    EXPECT_NO_THROW(client1.Connect(host, port));
    EXPECT_NO_THROW(client2.Connect(host, port));

    EXPECT_NO_THROW(client1.Write(R"({"username": "user" "password": "123"})"));
    EXPECT_NO_THROW(client2.Write(R"({"username": "user", "password": "123"})"));

    const auto response1 = nlohmann::json::parse(client1.Read());
    const auto message_header1 = response1[kResponseMessage].get<std::string>();
    const auto message_first_part1 = message_header1.substr(0, message_header1.find(": "));
    EXPECT_FALSE(response1[kResponseSuccess]);
    EXPECT_EQ(message_first_part1, auth_fail) << "Message was: " << message_header1;

    const auto response2 = client2.Read();
    EXPECT_TRUE(response2 == auth_success) << "Response was: " << response2;
  }

  EXPECT_NO_THROW(websocket_server.Shutdown());
  EXPECT_NO_THROW(websocket_server.AwaitShutdown());
}

TEST(WebSocketServer, WebsocketAuthenticationFails) {
  constexpr auto host = "0.0.0.0";
  constexpr auto port = "7444";
  constexpr auto auth_fail = R"({"message":"Authentication failed!","success":false})";

  MockAuth auth{false, true, true};
  communication::websocket::Server websocket_server({"0.0.0.0", 7444}, &auth);
  websocket_server.Start();
  {
    auto client = Client();
    EXPECT_NO_THROW(client.Connect(host, port));
    EXPECT_NO_THROW(client.Write(R"({"username": "user", "password": "123"})"));

    const auto response = client.Read();
    EXPECT_EQ(response, auth_fail);
  }
  EXPECT_NO_THROW(websocket_server.Shutdown());
  EXPECT_NO_THROW(websocket_server.AwaitShutdown());
}

TEST(WebSocketServer, WebsocketAuthorizationWhenFails) {
  constexpr auto host = "0.0.0.0";
  constexpr auto port = "7444";
  constexpr auto auth_fail = R"({"message":"Authorization failed!","success":false})";

  MockAuth auth{true, false, true};
  communication::websocket::Server websocket_server({"0.0.0.0", 7444}, &auth);
  websocket_server.Start();
  {
    auto client = Client();
    EXPECT_NO_THROW(client.Connect(host, port));
    EXPECT_NO_THROW(client.Write(R"({"username": "user", "password": "123"})"));

    const auto response = client.Read();
    EXPECT_EQ(response, auth_fail);
  }
  EXPECT_NO_THROW(websocket_server.Shutdown());
  EXPECT_NO_THROW(websocket_server.AwaitShutdown());
}
