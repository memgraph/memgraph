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
#include <thread>
#include <vector>

#include <fmt/core.h>
#include <spdlog/spdlog.h>
#include <unistd.h>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/websocket.hpp>
#include <mgclient.hpp>

#include "utils/logging.hpp"

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

DEFINE_uint64(bolt_port, 7687, "Bolt port");

// Report a failure
void Fail(beast::error_code ec, char const *what) { std::cerr << what << ": " << ec.message() << "\n"; }

// Sends a WebSocket message and prints the response
class Session : public std::enable_shared_from_this<Session> {
 public:
  // Resolver and socket require an io_context
  explicit Session(net::io_context &ioc, std::vector<std::string> &expected_messages)
      : resolver_(net::make_strand(ioc)), ws_(net::make_strand(ioc)), received_messages_{expected_messages} {}

  // Start the asynchronous operation
  void Run(std::string host, std::string port) {
    // Save these for later
    host_ = host;

    // Look up the domain name
    resolver_.async_resolve(host, port, beast::bind_front_handler(&Session::OnResolve, shared_from_this()));
  }

  void OnResolve(beast::error_code ec, tcp::resolver::results_type results) {
    if (ec) return Fail(ec, "resolve");

    // Set the timeout for the operation
    beast::get_lowest_layer(ws_).expires_after(std::chrono::seconds(30));

    // Make the connection on the IP address we get from a lookup
    beast::get_lowest_layer(ws_).async_connect(results,
                                               beast::bind_front_handler(&Session::OnConnect, shared_from_this()));
  }

  void OnConnect(beast::error_code ec, tcp::resolver::results_type::endpoint_type ep) {
    if (ec) return Fail(ec, "connect");

    // Turn off the timeout on the tcp_stream, because
    // the websocket stream has its own timeout system.
    beast::get_lowest_layer(ws_).expires_never();

    // Set suggested timeout settings for the websocket
    ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));

    // Set a decorator to change the User-Agent of the handshake
    ws_.set_option(websocket::stream_base::decorator([](websocket::request_type &req) {
      req.set(http::field::user_agent, std::string(BOOST_BEAST_VERSION_STRING) + " websocket-client-async");
    }));

    // Update the host_ string. This will provide the value of the
    // Host HTTP header during the WebSocket handshake.
    // See https://tools.ietf.org/html/rfc7230#section-5.4
    host_ += ':' + std::to_string(ep.port());

    // Perform the websocket handshake
    ws_.async_handshake(host_, "/", beast::bind_front_handler(&Session::OnHandshake, shared_from_this()));
  }

  void OnHandshake(beast::error_code ec) {
    if (ec) return Fail(ec, "handshake");

    // Send the message
    ws_.async_write(net::buffer("Test client"), beast::bind_front_handler(&Session::OnWrite, shared_from_this()));
  }

  void OnWrite(beast::error_code ec, std::size_t bytes_transferred) {
    boost::ignore_unused(bytes_transferred);

    if (ec) return Fail(ec, "write");

    // Read a message into our buffer
    ws_.async_read(buffer_, beast::bind_front_handler(&Session::OnRead, shared_from_this()));
  }

  void OnRead(beast::error_code ec, std::size_t bytes_transferred) {
    if (ec) return Fail(ec, "read");
    received_messages_.push_back(boost::beast::buffers_to_string(buffer_.data()));
    buffer_.clear();

    // Close the WebSocket connection
    // ws_.async_close(websocket::close_code::normal, beast::bind_front_handler(&Session::OnClose, shared_from_this()));
    ws_.async_read(buffer_, beast::bind_front_handler(&Session::OnRead, shared_from_this()));
  }

  void OnClose(beast::error_code ec) {
    if (ec) return Fail(ec, "close");

    // If we get here then the connection is closed gracefully

    // The make_printable() function helps print a ConstBufferSequence
    std::cout << beast::make_printable(buffer_.data()) << std::endl;
  }

 private:
  tcp::resolver resolver_;
  websocket::stream<beast::tcp_stream> ws_;
  beast::flat_buffer buffer_;
  std::string host_;
  std::vector<std::string> &received_messages_;
};

class WebsocketClient {
 public:
  WebsocketClient() : ioc_{}, session_{std::make_shared<Session>(ioc_, received_messages_)} {}

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

void CleanDatabase() {
  auto client = GetBoltClient();
  MG_ASSERT(client->Execute("MATCH (n) DETACH DELETE n;"));
  client->DiscardAll();
}

void AddUser() {
  auto client = GetBoltClient();
  MG_ASSERT(client->Execute("CREATE USER test IDENTIFIED BY 'testing';"));
  client->DiscardAll();
}

void AddVertex() {
  auto client = GetBoltClient();
  MG_ASSERT(client->Execute("CREATE ();"));
  client->DiscardAll();
}
void AddConnectedVertices() {
  auto client = GetBoltClient();
  MG_ASSERT(client->Execute("CREATE ()-[:TO]->();"));
  client->DiscardAll();
}

void TestWebsocketWithoutAnyUsers() {
  constexpr auto create_vertex_log =
      "{\"event\": \"log\", \"level\": \"debug\", \"message\": \"[Run] 'CREATE ();'\"}\n";
  constexpr auto create_connected_vertex_log =
      "{\"event\": \"log\", \"level\": \"debug\", \"message\": \"[Run] 'CREATE ()-[:TO]->();'\"}\n";
  constexpr auto commit_transaction_log =
      "{\"event\": \"log\", \"level\": \"debug\", \"message\": \"Finished committing the transaction\"}\n";
  constexpr auto clean_everything =
      "{\"event\": \"log\", \"level\": \"debug\", \"message\": \"[Run] 'MATCH (n) DETACH DELETE n;'\"}\n";

  auto websocket_client = WebsocketClient();
  websocket_client.Connect("127.0.0.1", "7444");

  CleanDatabase();
  AddVertex();
  AddVertex();
  AddVertex();
  AddConnectedVertices();
  CleanDatabase();
  // Maybe some way of avoiding sleep, but since the communication is async
  // there so bulletproof way of avoiding this
  sleep(1);

  websocket_client.Close();
  const auto received_messages = websocket_client.GetReceivedMessages();

  //   for (auto &m : received_messages) {
  //     std::cerr << "Reading: " << m << "\n";
  //   }

  //   std::cerr << "Num of create_vertex_log: " << std::ranges::count(received_messages, create_vertex_log) <<
  //   std::endl; std::cerr << "Num of commit_transaction_log: " << std::ranges::count(received_messages,
  //   commit_transaction_log)
  //             << std::endl;
  //   std::cerr << "Num of create_connected_vertex_log: "
  //             << std::ranges::count(received_messages, create_connected_vertex_log) << std::endl;
  //   std::cerr << "Num of clean_everything: " << std::ranges::count(received_messages, clean_everything) << std::endl;
  MG_ASSERT(std::ranges::count(received_messages, create_vertex_log) > 0, "Created vertex logs has not been received!");
  MG_ASSERT(std::ranges::count(received_messages, commit_transaction_log) > 0,
            "Commit transaction logs has not been received!");
  MG_ASSERT(std::ranges::count(received_messages, create_connected_vertex_log) > 0,
            "Create connected vertex logs has not been received!");
  MG_ASSERT(std::ranges::count(received_messages, clean_everything) > 0, "Clean everything log has not been received!");
}

void TestWebsocketWithAuthentication() {
  constexpr auto create_vertex_log =
      "{\"event\": \"log\", \"level\": \"debug\", \"message\": \"[Run] 'CREATE ();'\"}\n";
  constexpr auto create_connected_vertex_log =
      "{\"event\": \"log\", \"level\": \"debug\", \"message\": \"[Run] 'CREATE ()-[:TO]->();'\"}\n";
  constexpr auto commit_transaction_log =
      "{\"event\": \"log\", \"level\": \"debug\", \"message\": \"Finished committing the transaction\"}\n";
  constexpr auto clean_everything =
      "{\"event\": \"log\", \"level\": \"debug\", \"message\": \"[Run] 'MATCH (n) DETACH DELETE n;'\"}\n";

  AddUser();
  auto websocket_client = WebsocketClient();
  websocket_client.Connect("127.0.0.1", "7444");

  CleanDatabase();
  AddVertex();
  AddVertex();
  AddVertex();
  AddConnectedVertices();
  CleanDatabase();
  std::this_thread::sleep_for(std::chrono::seconds(1));

  websocket_client.Close();
  const auto received_messages = websocket_client.GetReceivedMessages();

  MG_ASSERT(std::ranges::count(received_messages, create_vertex_log) > 0, "Created vertex logs has not been received!");
  MG_ASSERT(std::ranges::count(received_messages, commit_transaction_log) > 0,
            "Commit transaction logs has not been received!");
  MG_ASSERT(std::ranges::count(received_messages, create_connected_vertex_log) > 0,
            "Create connected vertex logs has not been received!");
  MG_ASSERT(std::ranges::count(received_messages, clean_everything) > 0, "Clean everything log has not been received!");
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
