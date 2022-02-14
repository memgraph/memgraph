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
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <thread>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <spdlog/spdlog.h>
#include <unistd.h>
#include <mgclient.hpp>

#include "common.hpp"
#include "utils/logging.hpp"

DEFINE_uint64(bolt_port, 7687, "Bolt port");

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
  WebsocketClient() : session_{std::make_shared<Session>(ioc_, received_messages_)} {}

  WebsocketClient(Credentials creds) : session_{std::make_shared<Session>(ioc_, received_messages_, creds)} {}

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

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E websocket!");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  MG_ASSERT(FLAGS_bolt_port != 0);
  logging::RedirectToStderr();

  mg::Client::Init();
  auto mg_client = GetBoltClient(static_cast<uint16_t>(FLAGS_bolt_port), false);

  TestWebsocketWithoutAnyUsers<WebsocketClient>(mg_client);
  TestWebsocketWithAuthentication<WebsocketClient>(mg_client);
  TestWebsocketWithoutBeingAuthorized<WebsocketClient>(mg_client);

  return 0;
}
