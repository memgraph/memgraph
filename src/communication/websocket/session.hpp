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

#pragma once

#include <deque>
#include <memory>
#include <optional>
#include <variant>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <json/json.hpp>

#include "communication/context.hpp"
#include "communication/websocket/auth.hpp"
#include "utils/result.hpp"
#include "utils/synchronized.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::communication::websocket {
class Session : public std::enable_shared_from_this<Session> {
  using tcp = boost::asio::ip::tcp;

 public:
  template <typename... Args>
  static std::shared_ptr<Session> Create(Args &&...args) {
    return std::shared_ptr<Session>{new Session{std::forward<Args>(args)...}};
  }

  bool Run();
  void Write(std::shared_ptr<std::string> message);
  bool IsConnected() const;

 private:
  using PlainWebSocket = boost::beast::websocket::stream<boost::beast::tcp_stream>;
  using SSLWebSocket = boost::beast::websocket::stream<boost::beast::ssl_stream<boost::beast::tcp_stream>>;

  explicit Session(tcp::socket &&socket, ServerContext &context, AuthenticationInterface &auth);

  void DoWrite();
  void OnWrite(boost::beast::error_code ec, size_t bytes_transferred);

  void DoRead();
  void OnRead(boost::beast::error_code ec, size_t bytes_transferred);

  void DoClose();
  void OnClose(boost::beast::error_code ec);

  bool IsAuthenticated() const;

  utils::BasicResult<std::string> Authorize(const nlohmann::json &creds);

  void DoShutdown();

  auto GetExecutor() {
    return std::visit(utils::Overloaded{[](auto &&ws) { return ws.get_executor(); }}, ws_);
  }

  template <typename F>
  decltype(auto) ExecuteForWebsocket(F &&fn) {
    return std::visit(utils::Overloaded{std::forward<F>(fn)}, ws_);
  }

  std::variant<PlainWebSocket, SSLWebSocket> CreateWebSocket(tcp::socket &&socket, ServerContext &context);

  std::optional<std::reference_wrapper<boost::asio::ssl::context>> ssl_context_;
  std::variant<PlainWebSocket, SSLWebSocket> ws_;
  boost::beast::flat_buffer buffer_;
  std::deque<std::shared_ptr<std::string>> messages_;
  boost::asio::strand<PlainWebSocket::executor_type> strand_;
  std::atomic<bool> connected_{false};
  bool authenticated_{false};
  bool close_{false};
  AuthenticationInterface &auth_;
};
}  // namespace memgraph::communication::websocket
