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

#pragma once

#include <cstddef>
#include <list>
#include <memory>
#include <thread>

#include <spdlog/spdlog.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>

#include "communication/context.hpp"
#include "communication/v2/session.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::communication::v2 {
inline void LogError(boost::beast::error_code ec, const std::string_view what) {
  spdlog::warn("Listener failed on {}: {}", what, ec.message());
}

template <class TSession, class TSessionData>
class Listener final : public std::enable_shared_from_this<Listener<TSession, TSessionData>> {
  using tcp = boost::asio::ip::tcp;
  using SessionHandler = Session<TSession, TSessionData>;

 public:
  Listener(const Listener &) = delete;
  Listener(Listener &&) = delete;
  Listener &operator=(const Listener &) = delete;
  Listener &operator=(Listener &&) = delete;
  ~Listener() {}

  template <typename... Args>
  static std::shared_ptr<Listener> Create(Args &&...args) {
    return std::shared_ptr<Listener>{new Listener(std::forward<Args>(args)...)};
  }

  void Start() { DoAccept(); }

 private:
  Listener(boost::asio::io_context &ioc, TSessionData *data, ServerContext *server_context, tcp::endpoint &endpoint,
           size_t workers_count, const std::string &service_name)
      : ioc_(ioc),
        data_(data),
        server_context_(server_context),
        acceptor_(ioc),
        workers_count_(workers_count),
        endpoint_{endpoint},
        service_name_{service_name} {
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
      spdlog::error(
          utils::MessageWithLink("Cannot bind to socket on endpoint {}.", endpoint, "https://memgr.ph/socket"));
      LogError(ec, "bind");
      return;
    }

    acceptor_.listen(boost::asio::socket_base::max_listen_connections, ec);
    if (ec) {
      LogError(ec, "listen");
      return;
    }

    spdlog::info("{} server is fully armed and operational", service_name_);
    spdlog::info("{} listening on {}", service_name_, endpoint.address());
  }

  void DoAccept() {
    acceptor_.async_accept(ioc_, [shared_this = this->shared_from_this()](auto ec, auto socket) {
      shared_this->OnAccept(ec, std::move(socket));
    });
  }

  void OnAccept(boost::system::error_code ec, tcp::socket socket) {
    if (ec) {
      return LogError(ec, "accept");
    }

    spdlog::info("Accepted a connection from {}:", service_name_, socket.local_endpoint().address(),
                 socket.local_endpoint().port());

    auto session = SessionHandler::Create(std::move(socket), this->data_, *this->server_context_, this->endpoint_);
    sessions_.WithLock([session = session](auto &sessions) {
      // Clean disconnected clients
      std::erase_if(sessions, [](const auto &elem) { return !elem->IsConnected(); });
      sessions.push_back(std::move(session));
    });

    session->Start();
    DoAccept();
  }

  boost::asio::io_context &ioc_;
  TSessionData *data_;
  ServerContext *server_context_;
  tcp::acceptor acceptor_;

  size_t workers_count_;
  tcp::endpoint endpoint_;
  std::string service_name_;

  utils::Synchronized<std::list<std::shared_ptr<SessionHandler>>, utils::SpinLock> sessions_;
  std::atomic<bool> alive_;
};
}  // namespace memgraph::communication::v2
