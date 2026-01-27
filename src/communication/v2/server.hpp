// Copyright 2026 Memgraph Ltd.
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

#include <boost/system/detail/errc.hpp>
#include <memory>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "communication/context.hpp"
#include "communication/v2/pool.hpp"
#include "communication/v2/session.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"
#include "utils/numa.hpp"

namespace memgraph::communication::v2 {

using Socket = boost::asio::ip::tcp::socket;
using ServerEndpoint = boost::asio::ip::tcp::endpoint;

/**
 * Communication server using Sharded Acceptors for NUMA efficiency.
 *
 * This version implements one acceptor per NUMA node using SO_REUSEPORT.
 * This allows the kernel to dispatch incoming connections to the specific
 * I/O thread running on the NUMA node where the network interrupt occurred,
 * minimizing cross-socket traffic and context switching.
 */
template <typename TSession, typename TSessionContext>
class Server final {
  using tcp = boost::asio::ip::tcp;
  using SessionHandler = Session<TSession, TSessionContext>;

 public:
  Server(ServerEndpoint &endpoint, TSessionContext *session_context, ServerContext *server_context,
         std::string_view service_name, unsigned io_n_threads);

  // NUMA-aware constructor
  Server(ServerEndpoint &endpoint, TSessionContext *session_context, ServerContext *server_context,
         std::string_view service_name, const utils::numa::NUMATopology &topology);

  ~Server();

  Server(const Server &) = delete;
  Server(Server &&) = delete;
  Server &operator=(const Server &) = delete;
  Server &operator=(Server &&) = delete;

  const auto &Endpoint() const;

  bool Start();

  void Shutdown() {
    spdlog::info("{} io shutting down.", service_name_);
    io_thread_pool_.Shutdown();
    spdlog::info("{} shutdown.", service_name_);
  }

  void AwaitShutdown() { io_thread_pool_.AwaitShutdown(); }

  bool IsRunning() const noexcept;

 private:
  /**
   * Starts an asynchronous accept loop on a specific acceptor.
   */
  void DoAccept(tcp::acceptor &acceptor) {
    acceptor.async_accept([this, &acceptor](auto ec, boost::asio::ip::tcp::socket &&socket) {
      OnAccept(ec, std::move(socket), acceptor);
    });
  }

  void OnAccept(boost::system::error_code ec, tcp::socket socket, tcp::acceptor &acceptor);

  void InitializeAcceptors();

  void OnError(const boost::system::error_code &ec, const std::string_view what, tcp::acceptor *acceptor) {
    spdlog::error("Listener failed on {}: {}", what, ec.message());
    if (ec == boost::system::errc::too_many_files_open || ec == boost::system::errc::too_many_files_open_in_system) {
      spdlog::trace("too many open files... retrying");
      if (acceptor) DoAccept(*acceptor);
      return;
    }
    spdlog::trace("fatal communication error... shutting down");
    Shutdown();
  }

  ServerEndpoint endpoint_;
  std::string service_name_;

  TSessionContext *session_context_;
  ServerContext *server_context_;

  IOContextThreadPool io_thread_pool_;
  // One acceptor per I/O context (NUMA node)
  std::vector<std::unique_ptr<tcp::acceptor>> acceptors_;
};

template <typename TSession, typename TSessionContext>
Server<TSession, TSessionContext>::~Server() {
  MG_ASSERT(!IsRunning(), "Server wasn't shutdown properly");
}

template <typename TSession, typename TSessionContext>
Server<TSession, TSessionContext>::Server(ServerEndpoint &endpoint, TSessionContext *session_context,
                                          ServerContext *server_context, const std::string_view service_name,
                                          const unsigned io_n_threads)
    : endpoint_{endpoint},
      service_name_{service_name},
      session_context_(session_context),
      server_context_(server_context),
      io_thread_pool_{io_n_threads} {
  // Single legacy acceptor
  acceptors_.push_back(std::make_unique<tcp::acceptor>(io_thread_pool_.GetIOContext()));
  InitializeAcceptors();
}

template <typename TSession, typename TSessionContext>
Server<TSession, TSessionContext>::Server(ServerEndpoint &endpoint, TSessionContext *session_context,
                                          ServerContext *server_context, const std::string_view service_name,
                                          const utils::numa::NUMATopology &topology)
    : endpoint_{endpoint},
      service_name_{service_name},
      session_context_(session_context),
      server_context_(server_context),
      io_thread_pool_{topology} {
  // Create one acceptor per NUMA node context
  for (const auto &node : topology.nodes) {
    acceptors_.push_back(std::make_unique<tcp::acceptor>(io_thread_pool_.GetIOContextForNUMA(node.node_id)));
  }
  InitializeAcceptors();
}

template <typename TSession, typename TSessionContext>
void Server<TSession, TSessionContext>::InitializeAcceptors() {
  for (auto &acceptor : acceptors_) {
    boost::system::error_code ec;
    (void)acceptor->open(endpoint_.protocol(), ec);
    if (ec) {
      OnError(ec, "open", nullptr);
      MG_ASSERT(false, "Failed to open to socket.");
      return;
    }

    // Allow address reuse
    (void)acceptor->set_option(boost::asio::socket_base::reuse_address(true), ec);

    // Enable SO_REUSEPORT for NUMA sharding
    // This allows multiple acceptors to bind to the same port and lets the kernel
    // distribute connections based on NUMA locality.
    typedef boost::asio::detail::socket_option::integer<SOL_SOCKET, SO_REUSEPORT> reuse_port;
    (void)acceptor->set_option(reuse_port(1), ec);

    if (ec) {
      OnError(ec, "set_option (reuse_port)", nullptr);
      MG_ASSERT(false, "Failed to set SO_REUSEPORT.");
      return;
    }

    (void)acceptor->bind(endpoint_, ec);
    if (ec) {
      spdlog::error(
          utils::MessageWithLink("Cannot bind to socket on endpoint {}.", endpoint_, "https://memgr.ph/socket"));
      OnError(ec, "bind", nullptr);
      MG_ASSERT(false, "Failed to bind.");
      return;
    }

    (void)acceptor->listen(boost::asio::socket_base::max_listen_connections, ec);
    if (ec) {
      OnError(ec, "listen", nullptr);
      MG_ASSERT(false, "Failed to listen.");
      return;
    }
  }
}

template <typename TSession, typename TSessionContext>
bool Server<TSession, TSessionContext>::Start() {
  if (IsRunning()) {
    spdlog::error("The server is already running");
    return false;
  }

  io_thread_pool_.Run();

  // Start accept loop for every acceptor
  for (auto &acceptor : acceptors_) {
    DoAccept(*acceptor);
  }

  spdlog::info("{} server is fully armed and operational", service_name_);
  spdlog::info("{} listening on {} with {} sharded acceptor(s)", service_name_, endpoint_, acceptors_.size());
  return true;
}

template <typename TSession, typename TSessionContext>
inline void Server<TSession, TSessionContext>::OnAccept(boost::system::error_code ec, tcp::socket socket,
                                                        tcp::acceptor &acceptor) {
  if (ec) {
    return OnError(ec, "accept", &acceptor);
  }

  // Socket is already associated with the io_context of the acceptor that handled it.
  // With SO_REUSEPORT and NUMA IRQ affinity, this should naturally be the context
  // on the local NUMA node.
  auto session = SessionHandler::Create(std::move(socket), session_context_, *server_context_, service_name_);
  session->Start();

  // Continue the loop for this specific acceptor
  DoAccept(acceptor);
}

template <typename TSession, typename TSessionContext>
const auto &Server<TSession, TSessionContext>::Endpoint() const {
  MG_ASSERT(IsRunning(), "You can't get the server endpoint when it's not running!");
  return endpoint_;
}

template <typename TSession, typename TSessionContext>
bool Server<TSession, TSessionContext>::IsRunning() const noexcept {
  return io_thread_pool_.IsRunning();
}

}  // namespace memgraph::communication::v2
