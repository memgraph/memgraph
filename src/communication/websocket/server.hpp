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

#define BOOST_ASIO_USE_TS_EXECUTOR_AS_DEFAULT

#include <thread>

#include <spdlog/sinks/base_sink.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "communication/websocket/listener.hpp"
#include "io/network/endpoint.hpp"

namespace communication::websocket {

template <typename TSession, typename TSessionData>
class Server final {
  using tcp = boost::asio::ip::tcp;

 public:
  explicit Server(io::network::Endpoint endpoint, TSessionData *data)
      : ioc_{},
        listener_{Listener<TSession, TSessionData>::Create(
            ioc_, tcp::endpoint{boost::asio::ip::make_address(endpoint.address), endpoint.port}, data)} {}

  Server(const Server &) = delete;
  Server(Server &&) = delete;
  Server &operator=(const Server &) = delete;
  Server &operator=(Server &&) = delete;

  ~Server() {
    MG_ASSERT(!background_thread_ || (ioc_.stopped() && !background_thread_->joinable()),
              "Server wasn't shutdown properly");
  }

  void Start() {
    MG_ASSERT(!background_thread_, "The server was already started!");
    listener_->Run();
    background_thread_.emplace([this] { ioc_.run(); });
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

  std::shared_ptr<Listener<TSession, TSessionData>> listener_;
  std::optional<std::thread> background_thread_;
};
}  // namespace communication::websocket
