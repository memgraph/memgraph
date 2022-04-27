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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <thread>
#include <vector>

#include <spdlog/spdlog.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/system/detail/error_code.hpp>

#include "communication/context.hpp"
#include "communication/v2/pool.hpp"
#include "communication/v2/session.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::communication::v2 {

template <class TSession, class TSessionData>
class Listener final : public std::enable_shared_from_this<Listener<TSession, TSessionData>> {
  using tcp = boost::asio::ip::tcp;
  using SessionHandler = Session<TSession, TSessionData>;
  using std::enable_shared_from_this<Listener<TSession, TSessionData>>::shared_from_this;

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

  bool IsRunning() const noexcept { return alive_.load(std::memory_order_relaxed); }

 private:
  Listener(boost::asio::io_context &io_context, TSessionData *data, ServerContext *server_context,
           tcp::endpoint &endpoint, const std::string_view service_name, const uint64_t inactivity_timeout_sec)
      : io_context_(io_context),
        data_(data),
        server_context_(server_context),
        acceptor_(io_context_),
        endpoint_{endpoint},
        service_name_{service_name},
        inactivity_timeout_{inactivity_timeout_sec} {
    boost::system::error_code ec;
    // Open the acceptor
    acceptor_.open(endpoint.protocol(), ec);
    if (ec) {
      OnError(ec, "open");
      return;
    }

    // Allow address reuse
    acceptor_.set_option(boost::asio::socket_base::reuse_address(true), ec);
    if (ec) {
      OnError(ec, "set_option");
      return;
    }

    // Bind to the server address
    acceptor_.bind(endpoint, ec);
    if (ec) {
      spdlog::error(
          utils::MessageWithLink("Cannot bind to socket on endpoint {}.", endpoint, "https://memgr.ph/socket"));
      OnError(ec, "bind");
      return;
    }

    acceptor_.listen(boost::asio::socket_base::max_listen_connections, ec);
    if (ec) {
      OnError(ec, "listen");
      return;
    }
  }

  void DoAccept() {
    acceptor_.async_accept(io_context_,
                           [shared_this = shared_from_this()](auto ec, boost::asio::ip::tcp::socket &&socket) {
                             shared_this->OnAccept(ec, std::move(socket));
                           });
  }

  void OnAccept(boost::system::error_code ec, tcp::socket socket) {
    if (ec) {
      return OnError(ec, "accept");
    }

    auto session = SessionHandler::Create(std::move(socket), data_, *server_context_, endpoint_, inactivity_timeout_,
                                          service_name_);
    session->Start();
    DoAccept();
  }

  void OnError(const boost::system::error_code &ec, const std::string_view what) {
    spdlog::error("Listener failed on {}: {}", what, ec.message());
    alive_.store(false, std::memory_order_relaxed);
  }

  boost::asio::io_context &io_context_;
  TSessionData *data_;
  ServerContext *server_context_;
  tcp::acceptor acceptor_;

  tcp::endpoint endpoint_;
  std::string_view service_name_;
  std::chrono::seconds inactivity_timeout_;

  std::atomic<bool> alive_;
};
}  // namespace memgraph::communication::v2
