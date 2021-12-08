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

#include <thread>

#include <spdlog/sinks/base_sink.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "communication/websocket/listener.hpp"

namespace communication::websocket {

class Server final {
  using tcp = boost::asio::ip::tcp;

 public:
  explicit Server(boost::asio::ip::tcp::endpoint endpoint)
      : ioc_{}, listener_{std::make_shared<WebSocketListener>(ioc_, std::move(endpoint))} {}

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

  bool IsRunning() { return !background_thread_ || ioc_.stopped(); }

  class LoggingSink : public spdlog::sinks::base_sink<std::mutex> {
   public:
    explicit LoggingSink(std::weak_ptr<WebSocketListener> listener) : listener_(listener) {}

   private:
    void sink_it_(const spdlog::details::log_msg &msg) override {
      const auto listener = listener_.lock();
      if (!listener) {
        return;
      }
      using memory_buf_t = fmt::basic_memory_buffer<char, 250>;
      memory_buf_t formatted;
      base_sink<std::mutex>::formatter_->format(msg, formatted);
      listener->WriteToAll(std::string_view{formatted.data(), formatted.size()});
    }

    void flush_() override {}

    std::weak_ptr<WebSocketListener> listener_;
  };

  std::shared_ptr<LoggingSink> GetLoggingSink() { return std::make_shared<LoggingSink>(listener_); }

 private:
  boost::asio::io_context ioc_;

  std::shared_ptr<WebSocketListener> listener_;
  std::optional<std::thread> background_thread_;
};
}  // namespace communication::websocket
