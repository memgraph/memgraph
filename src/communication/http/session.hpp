// Copyright 2023 Memgraph Ltd.
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
#include <optional>
#include <variant>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/version.hpp>
#include <json/json.hpp>

#include "communication/context.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::communication::http {

class Session : public std::enable_shared_from_this<Session> {
  using tcp = boost::asio::ip::tcp;

 public:
  template <typename... Args>
  static std::shared_ptr<Session> Create(Args &&...args) {
    return std::shared_ptr<Session>{new Session{std::forward<Args>(args)...}};
  }

  void Run();

 private:
  using PlainSocket = boost::beast::tcp_stream;
  using SSLSocket = boost::beast::ssl_stream<boost::beast::tcp_stream>;

  explicit Session(tcp::socket &&socket, ServerContext &context);

  std::variant<PlainSocket, SSLSocket> CreateSocket(tcp::socket &&socket, ServerContext &context);

  void OnWrite(boost::beast::error_code ec, size_t bytes_transferred);

  void DoRead();
  void OnRead(boost::beast::error_code ec, size_t bytes_transferred);

  void DoClose();
  void OnClose(boost::beast::error_code ec);

  auto GetExecutor() {
    return std::visit(utils::Overloaded{[](auto &&stream) { return stream.get_executor(); }}, stream_);
  }

  template <typename F>
  decltype(auto) ExecuteForStream(F &&fn) {
    return std::visit(utils::Overloaded{std::forward<F>(fn)}, stream_);
  }

  std::optional<std::reference_wrapper<boost::asio::ssl::context>> ssl_context_;
  std::variant<PlainSocket, SSLSocket> stream_;
  boost::beast::flat_buffer buffer_;
  boost::beast::http::request<boost::beast::http::string_body> req_;
  std::shared_ptr<void> res_;
  boost::asio::strand<boost::beast::tcp_stream::executor_type> strand_;
  bool close_{false};
};
}  // namespace memgraph::communication::http
