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

#include <thread>

#include <spdlog/sinks/base_sink.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <utility>

#include "communication/websocket/listener.hpp"
#include "io/network/endpoint.hpp"

namespace memgraph::communication::websocket {

class Server final {
  using tcp = boost::asio::ip::tcp;

 public:
  explicit Server(io::network::Endpoint endpoint, ServerContext *context, AuthenticationInterface &auth)
      : listener_{Listener::Create(
            ioc_, context, tcp::endpoint{boost::asio::ip::make_address(endpoint.GetAddress()), endpoint.GetPort()},
            auth)} {}

  Server(const Server &) = delete;
  Server(Server &&) = delete;
  Server &operator=(const Server &) = delete;
  Server &operator=(Server &&) = delete;

  ~Server();

  void Start();
  void Shutdown();
  void AwaitShutdown();
  [[nodiscard]] bool IsRunning() const;
  [[nodiscard]] tcp::endpoint GetEndpoint() const;

  bool HasErrorHappened() const;

  class LoggingSink : public spdlog::sinks::base_sink<std::mutex> {
   public:
    explicit LoggingSink(std::weak_ptr<Listener> listener) : listener_(std::move(listener)) {}

   private:
    void sink_it_(const spdlog::details::log_msg &msg) override;

    void flush_() override {}

    std::weak_ptr<Listener> listener_;
  };

  std::shared_ptr<LoggingSink> GetLoggingSink();

 private:
  boost::asio::io_context ioc_;

  std::shared_ptr<Listener> listener_;
  std::optional<std::thread> background_thread_;
};
}  // namespace memgraph::communication::websocket
