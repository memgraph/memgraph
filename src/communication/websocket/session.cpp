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

#include "communication/websocket/session.hpp"

#include "utils/logging.hpp"

namespace communication::websocket {
namespace {
void LogError(boost::beast::error_code ec, const std::string_view what) {
  spdlog::warn("Websocket session failed on {}: {}", what, ec.message());
}
}  // namespace

void Session::Run() {
  ws_.set_option(boost::beast::websocket::stream_base::timeout::suggested(boost::beast::role_type::server));

  ws_.set_option(boost::beast::websocket::stream_base::decorator(
      [](boost::beast::websocket::response_type &res) { res.set(boost::beast::http::field::server, "Memgraph WS"); }));

  // Accept the websocket handshake
  boost::beast::error_code ec;
  ws_.accept(ec);
  if (ec) {
    return LogError(ec, "accept");
  }
  connected_.store(true, std::memory_order_relaxed);

  // run on the strand
  boost::asio::dispatch(strand_, [shared_this = shared_from_this()] { shared_this->DoRead(); });
}

void Session::Write(std::shared_ptr<std::string> message) {
  if (!connected_.load(std::memory_order_relaxed)) {
    return;
  }
  boost::asio::dispatch(strand_, [message, shared_this = shared_from_this()] {
    shared_this->messages_.push_back(message);

    if (shared_this->messages_.size() > 1) {
      return;
    }
    shared_this->DoWrite();
  });
}

bool Session::IsConnected() { return connected_.load(std::memory_order_relaxed); }

void Session::DoWrite() {
  const auto next_message = messages_.front();
  ws_.async_write(boost::asio::buffer(*next_message), [message_string = next_message, shared_this = shared_from_this()](
                                                          boost::beast::error_code ec, const size_t bytes_transferred) {
    shared_this->OnWrite(ec, bytes_transferred);
  });
}

void Session::OnWrite(boost::beast::error_code ec, size_t /*bytest_transferred*/) {
  messages_.pop_front();

  if (ec) {
    return LogError(ec, "write");
  }

  if (!messages_.empty()) {
    DoWrite();
  }
}

void Session::DoRead() {
  ws_.async_read(buffer_,
                 [shared_this = shared_from_this()](boost::beast::error_code ec, const size_t bytes_transferred) {
                   shared_this->OnRead(ec, bytes_transferred);
                 });
}

void Session::OnRead(boost::beast::error_code ec, size_t /*bytest_transferred*/) {
  if (ec == boost::beast::websocket::error::closed) {
    messages_.clear();
    connected_.store(false, std::memory_order_relaxed);
    return;
  }

  buffer_.consume(buffer_.size());
  DoRead();
}

}  // namespace communication::websocket
