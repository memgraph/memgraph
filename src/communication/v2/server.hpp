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

#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "communication/context.hpp"
#include "communication/fmt.hpp"
#include "communication/init.hpp"
#include "communication/v2/pool.hpp"
#include "communication/v2/session.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"
#include "utils/thread.hpp"

namespace memgraph::communication::v2 {

using Socket = boost::asio::ip::tcp::socket;
using ServerEndpoint = boost::asio::ip::tcp::endpoint;
/**
 * Communication server.
 *
 * Listens for incoming connections on the server port and assigns them to the
 * connection listener. The listener and session are implemented using asio
 * async model. Currently the implemented model is thread per core model
 * opposed to io_context per core. The reasoning for opting for the former model
 * is the robustness to the multiple resource demanding queries that can be split
 * across multiple threads, and then a single thread would not block io_context,
 * unlike in the latter model where it is possible that thread that accepts
 * request is being blocked by demanding query.
 * All logic is contained within handlers that are being dispatched
 * on a single strand per session. The only exception is write which is
 * synchronous since the nature of the clients conenction is synchronous as
 * well.
 *
 * Current Server architecture:
 * incoming connection -> server -> listener -> session

 *
 * @tparam TSession the server can handle different Sessions, each session
 *         represents a different protocol so the same network infrastructure
 *         can be used for handling different protocols
 * @tparam TSessionContext the class with objects that will be forwarded to the
 *         session
 */

template <typename TSession, typename TSessionContext>
class Server final {
  using tcp = boost::asio::ip::tcp;
  using SessionHandler = Session<TSession, TSessionContext>;

 public:
  /**
   * Constructs and binds server to endpoint, operates on session data and
   * invokes workers_count workers
   */

  Server(ServerEndpoint &endpoint, TSessionContext *session_context, ServerContext *server_context,
         std::string_view service_name, size_t workers_count = std::thread::hardware_concurrency());

  ~Server();

  Server(const Server &) = delete;
  Server(Server &&) = delete;
  Server &operator=(const Server &) = delete;
  Server &operator=(Server &&) = delete;

  const auto &Endpoint() const;

  bool Start();

  void Shutdown() {
    listener_thread_.Shutdown();
    context_thread_pool_.Shutdown();
    spdlog::info("{} shutdown.", service_name_);
  }

  void AwaitShutdown() {
    listener_thread_.AwaitShutdown();
    context_thread_pool_.AwaitShutdown();
  }

  bool IsRunning() const noexcept;

 private:
  void DoAccept() {
    acceptor_.async_accept([this](auto ec, boost::asio::ip::tcp::socket &&socket) { OnAccept(ec, std::move(socket)); });
  }

  void OnAccept(boost::system::error_code ec, tcp::socket socket);

  void OnError(const boost::system::error_code &ec, const std::string_view what) {
    spdlog::error("Listener failed on {}: {}", what, ec.message());
    // Shutdown();
    // TODO Recoverable?
  }

  ServerEndpoint endpoint_;
  std::string service_name_;

  TSessionContext *session_context_;
  ServerContext *server_context_;

  IOContextThreadPool context_thread_pool_;
  IOContextThreadPool listener_thread_{1};
  tcp::acceptor acceptor_{listener_thread_.GetIOContext()};
};

template <typename TSession, typename TSessionContext>
Server<TSession, TSessionContext>::~Server() {
  MG_ASSERT(!IsRunning(), "Server wasn't shutdown properly");
}

template <typename TSession, typename TSessionContext>
Server<TSession, TSessionContext>::Server(ServerEndpoint &endpoint, TSessionContext *session_context,
                                          ServerContext *server_context, const std::string_view service_name,
                                          size_t workers_count)
    : endpoint_{endpoint},
      service_name_{service_name},
      session_context_(session_context),
      server_context_(server_context),
      context_thread_pool_{workers_count} {
  boost::system::error_code ec;
  // Open the acceptor
  (void)acceptor_.open(endpoint_.protocol(), ec);
  if (ec) {
    OnError(ec, "open");
    MG_ASSERT(false, "Failed to open to socket.");
    return;
  }

  // Allow address reuse
  (void)acceptor_.set_option(boost::asio::socket_base::reuse_address(true), ec);
  if (ec) {
    OnError(ec, "set_option");
    MG_ASSERT(false, "Failed to set_option.");
    return;
  }

  // Bind to the server address
  (void)acceptor_.bind(endpoint_, ec);
  if (ec) {
    spdlog::error(
        utils::MessageWithLink("Cannot bind to socket on endpoint {}.", endpoint_, "https://memgr.ph/socket"));
    OnError(ec, "bind");
    MG_ASSERT(false, "Failed to bind.");
    return;
  }

  (void)acceptor_.listen(boost::asio::socket_base::max_listen_connections, ec);
  if (ec) {
    OnError(ec, "listen");
    MG_ASSERT(false, "Failed to listen.");
    return;
  }
}

template <typename TSession, typename TSessionContext>
bool Server<TSession, TSessionContext>::Start() {
  if (IsRunning()) {
    spdlog::error("The server is already running");
    return false;
  }

  context_thread_pool_.Run();
  listener_thread_.Run();
  DoAccept();

  spdlog::info("{} server is fully armed and operational", service_name_);
  spdlog::info("{} listening on {}", service_name_, endpoint_);
  return true;
}

template <typename TSession, typename TSessionContext>
inline void Server<TSession, TSessionContext>::OnAccept(boost::system::error_code ec, tcp::socket socket) {
  if (ec) {
    return OnError(ec, "accept");
  }

  // Move socket from the listener thread to worker pool and create a new session
  auto fd = socket.release();
  boost::asio::ip::tcp::socket connected_socket(context_thread_pool_.GetIOContext());
  connected_socket.assign(endpoint_.protocol(), fd);
  auto session = SessionHandler::Create(std::move(connected_socket), session_context_, *server_context_, service_name_);
  session->Start();

  DoAccept();
}

template <typename TSession, typename TSessionContext>
const auto &Server<TSession, TSessionContext>::Endpoint() const {
  MG_ASSERT(IsRunning(), "You can't get the server endpoint when it's not running!");
  return endpoint_;
}

template <typename TSession, typename TSessionContext>
bool Server<TSession, TSessionContext>::IsRunning() const noexcept {
  return context_thread_pool_.IsRunning() && listener_thread_.IsRunning();
}

}  // namespace memgraph::communication::v2
