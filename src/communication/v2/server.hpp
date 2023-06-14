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

#include <algorithm>
#include <cstdint>
#include <iostream>
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
#include "communication/init.hpp"
#include "communication/v2/listener.hpp"
#include "communication/v2/pool.hpp"
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
 * synchronous since the nature of the clients connection is synchronous as
 * well.
 *
 * Current Server architecture:
 * incoming connection -> server -> listener -> session

 *
 * @tparam TSession the server can handle different Sessions, each session
 *         represents a different protocol so the same network infrastructure
 *         can be used for handling different protocols
 * @tparam TSessionData the class with objects that will be forwarded to the
 *         session
 */
template <typename TSession, typename TSessionData>
class Server final {
  using ServerHandler = Server<TSession, TSessionData>;

 public:
  /**
   * Constructs and binds server to endpoint, operates on session data and
   * invokes workers_count workers
   */
  Server(ServerEndpoint &endpoint, TSessionData *session_data, ServerContext *server_context,
         const int inactivity_timeout_sec, const std::string_view service_name,
         size_t workers_count = std::thread::hardware_concurrency())
      : endpoint_{endpoint},
        service_name_{service_name},
        context_thread_pool_{workers_count},
        listener_{Listener<TSession, TSessionData>::Create(context_thread_pool_.GetIOContext(), session_data,
                                                           server_context, endpoint_, service_name_,
                                                           inactivity_timeout_sec)} {}

  ~Server() { MG_ASSERT(!IsRunning(), "Server wasn't shutdown properly"); }

  Server(const Server &) = delete;
  Server(Server &&) = delete;
  Server &operator=(const Server &) = delete;
  Server &operator=(Server &&) = delete;

  const auto &Endpoint() const {
    MG_ASSERT(IsRunning(), "You can't get the server endpoint when it's not running!");
    return endpoint_;
  }

  bool Start() {
    if (IsRunning()) {
      spdlog::error("The server is already running");
      return false;
    }
    listener_->Start();

    spdlog::info("{} server is fully armed and operational", service_name_);
    spdlog::info("{} listening on {}", service_name_, endpoint_.address());
    context_thread_pool_.Run();

    return true;
  }

  void Shutdown() {
    context_thread_pool_.Shutdown();
    spdlog::info("{} shutting down...", service_name_);
  }

  void AwaitShutdown() { context_thread_pool_.AwaitShutdown(); }

  bool IsRunning() const noexcept { return context_thread_pool_.IsRunning() && listener_->IsRunning(); }

 private:
  ServerEndpoint endpoint_;
  std::string service_name_;

  IOContextThreadPool context_thread_pool_;
  std::shared_ptr<Listener<TSession, TSessionData>> listener_;
};

}  // namespace memgraph::communication::v2
