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
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "communication/context.hpp"
#include "communication/init.hpp"
#include "communication/v2/listener.hpp"
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
 * connection listener. The listener processes the events with a thread pool
 * that has `num_workers` threads. It is started automatically on constructor,
 * and stopped at destructor.
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
        listener_{Listener<TSession, TSessionData>::Create(ioc_, session_data, server_context, endpoint_, workers_count,
                                                           service_name_, inactivity_timeout_sec)} {}

  ~Server() {
    MG_ASSERT(!background_thread_ || (ioc_.stopped() && !background_thread_->joinable()),
              "Server wasn't shutdown properly");
  }

  Server(const Server &) = delete;
  Server(Server &&) = delete;
  Server &operator=(const Server &) = delete;
  Server &operator=(Server &&) = delete;

  const auto &Endpoint() const {
    MG_ASSERT(IsRunning(), "You can't get the server endpoint when it's not running!");
    return endpoint_;
  }

  bool Start() {
    MG_ASSERT(!background_thread_, "The server was already started!");
    listener_->Start();

    background_thread_.emplace(std::thread([this]() {
      utils::ThreadSetName(fmt::format("{} server", service_name_));
      spdlog::info("{} server is fully armed and operational", service_name_);
      spdlog::info("{} listening on {}", service_name_, endpoint_.address());

      ioc_.run();
      spdlog::info("{} shutting down...", service_name_);
    }));

    return true;
  }

  void Shutdown() { ioc_.stop(); }

  void AwaitShutdown() {
    if (background_thread_ && background_thread_->joinable()) {
      background_thread_->join();
    }
  }

  bool IsRunning() const { return background_thread_ && !ioc_.stopped(); }

 private:
  boost::asio::io_context ioc_;
  std::optional<std::thread> background_thread_;

  ServerEndpoint endpoint_;
  std::string_view service_name_;
  std::shared_ptr<Listener<TSession, TSessionData>> listener_;
};

}  // namespace memgraph::communication::v2
