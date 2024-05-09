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

#include "communication/websocket/server.hpp"

#include <spdlog/pattern_formatter.h>
#include <spdlog/spdlog.h>

namespace memgraph::communication::websocket {

Server::~Server() {
  MG_ASSERT(!background_thread_ || (ioc_.stopped() && !background_thread_->joinable()),
            "Server wasn't shutdown properly");
}

void Server::Start() {
  MG_ASSERT(!background_thread_, "The server was already started!");
  if (listener_->HasErrorHappened()) {
    spdlog::error("We have error on websocket listener already! Aborting server start.");
    return;
  }
  listener_->Run();
  background_thread_.emplace([this] { ioc_.run(); });
}

void Server::Shutdown() {
  if (ioc_.stopped()) {
    spdlog::trace("Websocket is already stopped!");
    return;
  }
  ioc_.stop();
}

void Server::AwaitShutdown() {
  if (background_thread_ && background_thread_->joinable()) {
    background_thread_->join();
  }
}

bool Server::IsRunning() const { return !listener_->HasErrorHappened() && background_thread_ && !ioc_.stopped(); }

bool Server::HasErrorHappened() const { return listener_->HasErrorHappened(); }

boost::asio::ip::tcp::endpoint Server::GetEndpoint() const { return listener_->GetEndpoint(); };

namespace {
class QuoteEscapeFormatter : public spdlog::custom_flag_formatter {
 public:
  void format(const spdlog::details::log_msg &msg, const std::tm & /*time*/, spdlog::memory_buf_t &dest) override {
    for (const auto c : msg.payload) {
      if (c == '"') {
        static constexpr std::string_view escaped_quote = "\\\"";
        dest.append(escaped_quote.data(), escaped_quote.data() + escaped_quote.size());
      } else if (c == '\n') {
        static constexpr std::string_view escaped_newline = "\\n";
        dest.append(escaped_newline.data(), escaped_newline.data() + escaped_newline.size());
      } else {
        dest.push_back(c);
      }
    }
  }
  std::unique_ptr<custom_flag_formatter> clone() const override {
    return spdlog::details::make_unique<QuoteEscapeFormatter>();
  }
};

};  // namespace

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

std::shared_ptr<Server::LoggingSink> Server::GetLoggingSink() {
  auto formatter = std::make_unique<spdlog::pattern_formatter>();
  formatter->add_flag<QuoteEscapeFormatter>('*').set_pattern(
      R"json({"event": "log", "level": "%l", "message": "%*"})json");
  auto sink = std::make_shared<LoggingSink>(listener_);
  sink->set_formatter(std::move(formatter));
  return sink;
}

}  // namespace memgraph::communication::websocket
