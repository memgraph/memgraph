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

#include <boost/asio/io_context.hpp>
#include <list>
#include <memory>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>

#include <spdlog/spdlog.h>

#include "communication/websocket/session.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace communication::websocket {
class WebSocketListener : public std::enable_shared_from_this<WebSocketListener> {
  using tcp = boost::asio::ip::tcp;

 public:
  template <typename... Args>
  static std::shared_ptr<WebSocketListener> CreateWebSocketListener(Args &&...args) {
    return std::shared_ptr<WebSocketListener>{new WebSocketListener(std::forward<Args>(args)...)};
  }

  // Start accepting incoming connections
  void Run() { DoAccept(); }

  void WriteToAll(const std::string_view message) {
    auto sessions_ptr = sessions_.Lock();
    for (auto &session : *sessions_ptr) {
      session->Write(message);
    }
  }

 private:
  WebSocketListener(boost::asio::io_context &ioc, tcp::endpoint endpoint) : ioc_(ioc), acceptor_(ioc) {
    boost::beast::error_code ec;

    // Open the acceptor
    acceptor_.open(endpoint.protocol(), ec);
    if (ec) {
      LogError(ec, "open");
      return;
    }

    // Allow address reuse
    acceptor_.set_option(boost::asio::socket_base::reuse_address(true), ec);
    if (ec) {
      LogError(ec, "set_option");
      return;
    }

    // Bind to the server address
    acceptor_.bind(endpoint, ec);
    if (ec) {
      LogError(ec, "bind");
      return;
    }

    acceptor_.listen(boost::asio::socket_base::max_listen_connections, ec);
    if (ec) {
      LogError(ec, "listen");
      return;
    }
  }

  void DoAccept() {
    // The new connection gets its own strand
    acceptor_.async_accept(boost::asio::make_strand(ioc_), [shared_this = shared_from_this()](auto ec, auto socket) {
      shared_this->OnAccept(ec, std::move(socket));
    });
  }

  void OnAccept(boost::beast::error_code ec, tcp::socket socket) {
    if (ec) {
      return LogError(ec, "accept");
    }

    {
      auto sessions_ptr = sessions_.Lock();
      sessions_ptr->emplace_back(WebSocketSession::CreateWebSocketSession(std::move(socket)))->Run();

      // Clean disconnected clients
      std::erase_if(*sessions_ptr, [](const auto &elem) { return !elem->Connected(); });
    }

    DoAccept();
  }

  static void LogError(boost::beast::error_code ec, const std::string_view what) {
    spdlog::warn("Websocket listener failed on {}: {}", what, ec.message());
  }

  boost::asio::io_context &ioc_;
  tcp::acceptor acceptor_;
  utils::Synchronized<std::list<std::shared_ptr<WebSocketSession>>, utils::SpinLock> sessions_;
};
}  // namespace communication::websocket
