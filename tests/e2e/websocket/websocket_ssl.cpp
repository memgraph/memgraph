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

#include "common.hpp"
#include "utils/logging.hpp"

DEFINE_uint64(bolt_port, 7687, "Bolt port");

class Session : public std::enable_shared_from_this<Session> {
 public:
  explicit Session(net::io_context &ioc, ssl::context &ctx, std::vector<std::string> &expected_messages)
      : resolver_(net::make_strand(ioc)), ws_(net::make_strand(ioc), ctx), received_messages_{expected_messages} {}

  explicit Session(net::io_context &ioc, ssl::context &ctx, std::vector<std::string> &expected_messages,
                   Credentials creds)
      : resolver_(net::make_strand(ioc)),
        ws_(net::make_strand(ioc), ctx),
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

    host_ = fmt::format("{}:{}", host_, ep.port());

    beast::get_lowest_layer(ws_).expires_after(std::chrono::seconds(30));

    if (!SSL_set_tlsext_host_name(ws_.next_layer().native_handle(), host_.c_str())) {
      ec = beast::error_code(static_cast<int>(::ERR_get_error()), net::error::get_ssl_category());
      return Fail(ec, "connect");
    }

    ws_.next_layer().async_handshake(ssl::stream_base::client,
                                     beast::bind_front_handler(&Session::OnSSLHandshake, shared_from_this()));
  }

  void OnSSLHandshake(beast::error_code ec) {
    if (ec) {
      return Fail(ec, "ssl_handshake");
    }

    beast::get_lowest_layer(ws_).expires_never();
    ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
    ws_.set_option(websocket::stream_base::decorator([](websocket::request_type &req) {
      req.set(http::field::user_agent, std::string(BOOST_BEAST_VERSION_STRING) + " websocket-client-async-ssl");
    }));

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

  void OnRead(beast::error_code ec, std::size_t bytes_transferred) {
    boost::ignore_unused(bytes_transferred);

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
  websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws_;
  beast::flat_buffer buffer_;
  std::string host_;
  std::vector<std::string> &received_messages_;
  Credentials creds_;
};

class WebsocketClient {
 public:
  WebsocketClient() { session_ = {std::make_shared<Session>(ioc_, ctx_, received_messages_)}; }

  explicit WebsocketClient(Credentials creds) {
    session_ = {std::make_shared<Session>(ioc_, ctx_, received_messages_, creds)};
  }

  void Connect(const std::string host, const std::string port) {
    session_->Run(host, port);
    bg_thread_ = std::thread([this]() { ioc_.run(); });
    bg_thread_.detach();
  }

  void Close() { ioc_.stop(); }

  std::vector<std::string> GetReceivedMessages() { return received_messages_; }

 private:
  std::vector<std::string> received_messages_{};
  ssl::context ctx_{ssl::context::tlsv12_client};
  net::io_context ioc_{};
  std::thread bg_thread_;
  std::shared_ptr<Session> session_;
};

void TestWebsocketWithoutAnyUsers(auto &mg_client) {
  spdlog::info("Starting websocket SSL connection without any users.");
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
  spdlog::info("Received {} messages.", received_messages.size());

  MG_ASSERT(!received_messages.empty(), "There are no received messages!");
  for (const auto &log_message : received_messages) {
    AssertLogMessage(log_message);
  }
  spdlog::info("Finishing websocket SSL connection without any users.");
}

void TestWebsocketWithAuthentication(auto &mg_client) {
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
  spdlog::info("Received {} messages.", received_messages.size());

  MG_ASSERT(!received_messages.empty(), "There are no received messages!");
  for (const auto &log_message : received_messages) {
    AssertLogMessage(log_message);
  }
  spdlog::info("Finishing websocket SSL connection with users.");
}

void TestWebsocketWithoutBeingAuthorized(auto &mg_client) {
  spdlog::info("Starting websocket connection with users but without being authenticated.");
  std::this_thread::sleep_for(std::chrono::seconds(1));
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
  spdlog::info("Received {} messages.", received_messages.size());

  MG_ASSERT(received_messages.size() < 2, "There is no more than one received message!");
  if (!received_messages.empty()) {
    auto json_message = nlohmann::json::parse(received_messages[0]);
    AssertAuthMessage(json_message, false);
  }
  spdlog::info("Finishing websocket connection with users but without being authenticated.");
}

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E websocket SSL!");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  MG_ASSERT(FLAGS_bolt_port != 0);
  logging::RedirectToStderr();

  auto mg_client = GetBoltClient(static_cast<uint16_t>(FLAGS_bolt_port), true);
  mg::Client::Init();

  TestWebsocketWithoutAnyUsers(mg_client);
  TestWebsocketWithAuthentication(mg_client);
  TestWebsocketWithoutBeingAuthorized(mg_client);

  return 0;
}
