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

#include "communication/websocket/server.hpp"

namespace communication::websocket {

Server::~Server() {
  MG_ASSERT(!background_thread_ || (ioc_.stopped() && !background_thread_->joinable()),
            "Server wasn't shutdown properly");
}

void Server::Start() {
  MG_ASSERT(!background_thread_, "The server was already started!");
  listener_->Run();
  background_thread_.emplace([this] { ioc_.run(); });
}

void Server::Shutdown() { ioc_.stop(); }

void Server::AwaitShutdown() {
  if (background_thread_ && background_thread_->joinable()) {
    background_thread_->join();
  }
}

bool Server::IsRunning() { return background_thread_ && !ioc_.stopped(); }

void Server::LoggingSink::sink_it_(const spdlog::details::log_msg &msg) {
  const auto listener = listener_.lock();
  if (!listener) {
    return;
  }
  using memory_buf_t = fmt::basic_memory_buffer<char, 250>;
  memory_buf_t formatted;
  base_sink<std::mutex>::formatter_->format(msg, formatted);
  listener->WriteToAll(std::make_shared<std::string>(formatted.data(), formatted.size()));
}

std::shared_ptr<Server::LoggingSink> Server::GetLoggingSink() { return std::make_shared<LoggingSink>(listener_); }

}  // namespace communication::websocket
