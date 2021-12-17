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
#include <boost/asio/strand.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/websocket.hpp>

namespace communication::websocket {
class Session : public std::enable_shared_from_this<Session> {
  using tcp = boost::asio::ip::tcp;

 public:
  template <typename... Args>
  static std::shared_ptr<Session> Create(Args &&...args) {
    return std::shared_ptr<Session>{new Session{std::forward<Args>(args)...}};
  }

  void Run();
  void Write(std::shared_ptr<std::string> message);
  bool IsConnected();

 private:
  explicit Session(tcp::socket &&socket)
      : ws_(std::move(socket)), strand_{boost::asio::make_strand(ws_.get_executor())} {}

  void DoWrite();
  void OnWrite(boost::beast::error_code ec, size_t bytest_transferred);
  void DoRead();
  void OnRead(boost::beast::error_code ec, size_t bytest_transferred);

  boost::beast::websocket::stream<boost::beast::tcp_stream> ws_;
  boost::beast::flat_buffer buffer_;
  std::deque<std::shared_ptr<std::string>> messages_;
  boost::asio::strand<decltype(ws_)::executor_type> strand_;
  std::atomic<bool> connected_{false};
};
}  // namespace communication::websocket
