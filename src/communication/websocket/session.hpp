// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <deque>
#include <memory>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/websocket.hpp>

class WebSocketSession : public std::enable_shared_from_this<WebSocketSession> {
  using tcp = boost::asio::ip::tcp;

 public:
  template <typename... Args>
  static std::shared_ptr<WebSocketSession> CreateWebSocketSession(Args &&...args) {
    return std::shared_ptr<WebSocketSession>{new WebSocketSession{std::forward<Args>(args)...}};
  }

  void Run() {
    // run on the strand
    OnRun();
  }

  void Write(const std::string_view message) {
    if (!connected_.load(std::memory_order_relaxed)) {
      return;
    }
    const auto message_string = std::make_shared<std::string>(message);
    boost::asio::dispatch(ws_.get_executor(), [message_string, shared_this = shared_from_this()] {
      shared_this->messages_.push_back(message_string);

      if (shared_this->messages_.size() > 1) {
        return;
      }
      shared_this->DoWrite();
    });
  }

  bool Connected() { return connected_.load(std::memory_order_relaxed); }

 private:
  explicit WebSocketSession(tcp::socket &&socket) : ws_(std::move(socket)) {}

  void DoWrite() {
    const auto next_message = messages_.front();
    ws_.async_write(boost::asio::buffer(*next_message),
                    [message_string = next_message, shared_this = shared_from_this()](boost::beast::error_code ec,
                                                                                      std::size_t bytes_transferred) {
                      shared_this->OnWrite(ec, bytes_transferred);
                    });
  }
  void OnWrite(boost::beast::error_code ec, std::size_t bytest_transferred) {
    messages_.pop_front();

    if (ec) {
      return LogError(ec, "write");
    }

    if (!messages_.empty()) {
      DoWrite();
    }
  }

  void OnRun() {
    ws_.set_option(boost::beast::websocket::stream_base::timeout::suggested(boost::beast::role_type::server));

    ws_.set_option(boost::beast::websocket::stream_base::decorator([](boost::beast::websocket::response_type &res) {
      res.set(boost::beast::http::field::server, "Memgraph WS");
    }));

    // Accept the websocket handshake
    boost::beast::error_code ec;
    ws_.accept(ec);
    OnAccept(ec);
    // ws_.async_accept([shared_this = shared_from_this()](boost::beast::error_code ec) { shared_this->OnAccept(ec); });
  }

  void OnAccept(boost::beast::error_code ec) {
    if (ec) {
      return LogError(ec, "accept");
    }
    connected_.store(true, std::memory_order_relaxed);

    // run on the strand
    boost::asio::dispatch(ws_.get_executor(), [shared_this = shared_from_this()] { shared_this->DoRead(); });
  }

  void DoRead() {
    ws_.async_read(buffer_,
                   [shared_this = shared_from_this()](boost::beast::error_code ec, std::size_t bytes_transferred) {
                     shared_this->OnRead(ec, bytes_transferred);
                   });
  }

  void OnRead(boost::beast::error_code ec, std::size_t bytest_transferred) {
    if (ec == boost::beast::websocket::error::closed) {
      connected_.store(false, std::memory_order_relaxed);
      return;
    }

    buffer_.consume(buffer_.size());
    DoRead();
  }

  void LogError(boost::beast::error_code ec, const std::string_view what) {
    spdlog::warn("Websocket session failed on {}: {}", what, ec.message());
  }

  boost::beast::websocket::stream<boost::beast::tcp_stream> ws_;
  boost::beast::flat_buffer buffer_;
  std::deque<std::shared_ptr<std::string>> messages_;
  std::atomic<bool> connected_{false};
};
