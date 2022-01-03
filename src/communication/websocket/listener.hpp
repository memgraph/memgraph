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

#include <list>
#include <memory>

#include <spdlog/spdlog.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>

#include "communication/websocket/session.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace communication::websocket {
template <typename TSession, typename TSessionData>
class Listener : public std::enable_shared_from_this<Listener<TSession, TSessionData>> {
  using tcp = boost::asio::ip::tcp;

 public:
  template <typename... Args>
  static std::shared_ptr<Listener> Create(Args &&...args) {
    return std::shared_ptr<Listener>{new Listener(std::forward<Args>(args)...)};
  }

  // Start accepting incoming connections
  void Run() {
    // The new connection gets its own strand
    acceptor_.async_accept(ioc_, [shared_this = this->shared_from_this()](auto ec, auto socket) {
      shared_this->OnAccept(ec, std::move(socket));
    });
  }

 private:
  Listener(boost::asio::io_context &ioc, tcp::endpoint endpoint, TSessionData *data)
      : data_{data}, ioc_(ioc), acceptor_(ioc) {
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

    spdlog::info("WebSocket server is listening on {}:{}", endpoint.address(), endpoint.port());
  }

  void OnAccept(boost::beast::error_code ec, tcp::socket socket) {
    if (ec) {
      return LogError(ec, "accept");
    }

    Session<TSession, TSessionData>::Create(std::move(socket), data_)->Run();
    Run();
  }

  TSessionData *data_;
  boost::asio::io_context &ioc_;
  tcp::acceptor acceptor_;
};
}  // namespace communication::websocket
