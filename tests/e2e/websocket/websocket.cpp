// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gflags/gflags.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <fmt/core.h>
#include <spdlog/spdlog.h>
#include <unistd.h>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/websocket.hpp>
#include <json/json.hpp>
#include <mgclient.hpp>

#include "utils/logging.hpp"

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

DEFINE_uint64(bolt_port, 7687, "Bolt port");

namespace {
constexpr std::array kSupportedLevels{"debug", "trace", "info", "warning", "error", "critical"};

struct Credentials {
  std::string_view username;
  std::string_view passsword;
};

std::string GetAuthenticationJSON(const Credentials &creds) {
  nlohmann::json json_creds;
  json_creds["username"] = creds.username;
  json_creds["password"] = creds.passsword;
  return json_creds.dump();
}

void Fail(beast::error_code ec, char const *what) { std::cerr << what << ": " << ec.message() << "\n"; }

}  // namespace

class Session : public std::enable_shared_from_this<Session> {
 public:
  explicit Session(net::io_context &ioc, std::vector<std::string> &expected_messages)
      : resolver_(net::make_strand(ioc)), ws_(net::make_strand(ioc)), received_messages_{expected_messages} {}

  explicit Session(net::io_context &ioc, std::vector<std::string> &expected_messages, Credentials creds)
      : resolver_(net::make_strand(ioc)),
        ws_(net::make_strand(ioc)),
        received_messages_{expected_messages},
        creds_{creds} {}

  void Run(std::string host, std::string port) {
    host_ = host;

    resolver_.async_resolve(host, port, beast::bind_front_handler(&Session::OnResolve, shared_from_this()));
  }

  void OnResolve(beast::error_code ec, tcp::resolver::results_type results) {
    if (ec) {
      return Fail(ec, "resolve");
    }

    beast::get_lowest_layer(ws_).expires_after(std::chrono::seconds(30));
    beast::get_lowest_layer(ws_).async_connect(results,
                                               beast::bind_front_handler(&Session::OnConnect, shared_from_this()));
  }

  void OnConnect(beast::error_code ec, tcp::resolver::results_type::endpoint_type ep) {
    if (ec) {
      return Fail(ec, "connect");
    }

    beast::get_lowest_layer(ws_).expires_never();

    ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));

    ws_.set_option(websocket::stream_base::decorator([](websocket::request_type &req) {
      req.set(http::field::user_agent, std::string(BOOST_BEAST_VERSION_STRING) + " websocket-client-async");
    }));

    host_ = fmt::format("{}:{}", host_, ep.port());

    ws_.async_handshake(host_, "/", beast::bind_front_handler(&Session::OnHandshake, shared_from_this()));
  }

  void OnHandshake(beast::error_code ec) {
    if (ec) {
      return Fail(ec, "handshake");
    }
    ws_.async_write(net::buffer(GetAuthenticationJSON(creds_)),
                    beast::bind_front_handler(&Session::OnWrite, shared_from_this()));
  }

  void OnWrite(beast::error_code ec, std::size_t bytes_transferred) {
    boost::ignore_unused(bytes_transferred);

    if (ec) {
      return Fail(ec, "write");
    }

    ws_.async_read(buffer_, beast::bind_front_handler(&Session::OnRead, shared_from_this()));
  }

  void OnRead(beast::error_code ec, std::size_t /*bytes_transferred*/) {
    if (ec) {
      return Fail(ec, "read");
    }
    received_messages_.push_back(boost::beast::buffers_to_string(buffer_.data()));
    buffer_.clear();

    ws_.async_read(buffer_, beast::bind_front_handler(&Session::OnRead, shared_from_this()));
  }

  void OnClose(beast::error_code ec) {
    if (ec) {
      return Fail(ec, "close");
    }
  }

 private:
  tcp::resolver resolver_;
  websocket::stream<beast::tcp_stream> ws_;
  beast::flat_buffer buffer_;
  std::string host_;
  std::vector<std::string> &received_messages_;
  Credentials creds_;
};

class WebsocketClient {
 public:
  WebsocketClient() : ioc_{}, session_{std::make_shared<Session>(ioc_, received_messages_)} {}

  WebsocketClient(Credentials creds) : ioc_{}, session_{std::make_shared<Session>(ioc_, received_messages_, creds)} {}

  void Connect(const std::string host, const std::string port) {
    session_->Run(host, port);
    bg_thread_ = std::thread([this]() { ioc_.run(); });
    bg_thread_.detach();
  }

  void Close() { ioc_.stop(); }

  std::vector<std::string> GetReceivedMessages() { return received_messages_; }

 private:
  std::vector<std::string> received_messages_{};
  net::io_context ioc_{};
  std::thread bg_thread_;
  std::shared_ptr<Session> session_;
};

auto GetBoltClient() {
  auto client =
      mg::Client::Connect({.host = "127.0.0.1", .port = static_cast<uint16_t>(FLAGS_bolt_port), .use_ssl = false});
  MG_ASSERT(client, "Failed to connect!");

  return client;
}

void CleanDatabase(auto &client) {
  MG_ASSERT(client->Execute("MATCH (n) DETACH DELETE n;"));
  client->DiscardAll();
}

void AddUser(auto &client) {
  MG_ASSERT(client->Execute("CREATE USER test IDENTIFIED BY 'testing';"));
  client->DiscardAll();
}

void AddVertex(auto &client) {
  MG_ASSERT(client->Execute("CREATE ();"));
  client->DiscardAll();
}
void AddConnectedVertices(auto &client) {
  MG_ASSERT(client->Execute("CREATE ()-[:TO]->();"));
  client->DiscardAll();
}

void AssertAuthMessage(auto &json_message, const bool success = true) {
  MG_ASSERT(json_message.at("message").is_string(), "Event is not a string!");
  MG_ASSERT(json_message.at("success").is_boolean(), "Success is not a boolean!");
  MG_ASSERT(json_message.at("success").template get<bool>() == success, "Success does not match expected!");
}

void AssertLogMessage(const std::string &log_message) {
  const auto json_message = nlohmann::json::parse(log_message);
  if (json_message.contains("success")) {
    AssertAuthMessage(json_message);
    return;
  }
  MG_ASSERT(json_message.at("event").is_string(), "Event is not a string!");
  MG_ASSERT(json_message.at("level").is_string(), "Level is not a string!");
  MG_ASSERT(std::ranges::count(kSupportedLevels, json_message.at("level")) > 0);
  MG_ASSERT(json_message.at("message").is_string(), "Message is not a string!");
}

void TestWebsocketWithoutAnyUsers() {
  auto mg_client = GetBoltClient();
  auto websocket_client = WebsocketClient();
  websocket_client.Connect("127.0.0.1", "7444");

  CleanDatabase(mg_client);
  AddVertex(mg_client);
  AddVertex(mg_client);
  AddVertex(mg_client);
  AddConnectedVertices(mg_client);
  CleanDatabase(mg_client);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  websocket_client.Close();
  const auto received_messages = websocket_client.GetReceivedMessages();
  MG_ASSERT(!received_messages.empty(), "There are no received messages!");
  for (const auto &log_message : received_messages) {
    AssertLogMessage(log_message);
  }
}

void TestWebsocketWithAuthentication() {
  auto mg_client = GetBoltClient();
  AddUser(mg_client);
  std::this_thread::sleep_for(std::chrono::seconds(1));
  auto websocket_client = WebsocketClient({"test", "testing"});
  websocket_client.Connect("127.0.0.1", "7444");

  CleanDatabase(mg_client);
  AddVertex(mg_client);
  AddVertex(mg_client);
  AddVertex(mg_client);
  AddConnectedVertices(mg_client);
  CleanDatabase(mg_client);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  websocket_client.Close();
  const auto received_messages = websocket_client.GetReceivedMessages();
  MG_ASSERT(!received_messages.empty(), "There are no received messages!");
  for (const auto &log_message : received_messages) {
    AssertLogMessage(log_message);
  }
}

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E websocket!");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  MG_ASSERT(FLAGS_bolt_port != 0);
  logging::RedirectToStderr();

  mg::Client::Init();

  TestWebsocketWithoutAnyUsers();
  TestWebsocketWithAuthentication();

  return 0;
}
