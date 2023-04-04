// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "communication/http/session.hpp"

#include <boost/beast/websocket/error.hpp>
#include <functional>
#include <memory>
#include <string>

#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <boost/asio/bind_executor.hpp>
#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/core/stream_traits.hpp>
#include <json/json.hpp>

#include "communication/context.hpp"
#include "communication/http/handler.hpp"
#include "utils/logging.hpp"

namespace memgraph::communication::http {
namespace {
void LogError(const boost::beast::error_code ec, const std::string_view what) {
  spdlog::warn("Websocket session failed on {}: {}", what, ec.message());
}
}  // namespace

Session::Session(tcp::socket &&socket, ServerContext &context)
    : stream_(std::move(socket)), strand_{boost::asio::make_strand(GetExecutor())} {}

void Session::Run() {
  // run on the strand
  boost::asio::dispatch(strand_, [shared_this = shared_from_this()] { shared_this->DoRead(); });
}

bool Session::IsConnected() const { return connected_.load(std::memory_order_relaxed); }

void Session::OnWrite(boost::beast::error_code ec, size_t bytes_transferred) {
  boost::ignore_unused(bytes_transferred);

  if (ec) {
    close_ = true;
    return LogError(ec, "write");
  }

  if (close_) {
    DoClose();
    return;
  }

  res_ = nullptr;

  DoRead();
}

void Session::DoRead() {
  req_ = {};

  stream_.expires_after(std::chrono::seconds(30));

  boost::beast::http::async_read(
      stream_, buffer_, req_,
      boost::asio::bind_executor(strand_, std::bind_front(&Session::OnRead, shared_from_this())));
}

void Session::OnRead(const boost::beast::error_code ec, const size_t bytes_transferred) {
  boost::ignore_unused(bytes_transferred);

  if (ec == boost::beast::http::error::end_of_stream) {
    DoClose();
    return;
  }

  if (ec) {
    return LogError(ec, "read");
  }

  auto async_write = [this](http::response<http::string_body> msg) {
    // The lifetime of the message has to extend
    // for the duration of the async operation so
    // we use a shared_ptr to manage it.
    auto sp = std::make_shared<http::response<http::string_body>>(std::move(msg));

    // Store a type-erased version of the shared
    // pointer in the class to keep it alive.
    res_ = sp;

    // Write the response
    boost::beast::http::async_write(
        stream_, *sp, boost::asio::bind_executor(strand_, std::bind_front(&Session::OnWrite, shared_from_this())));
  };

  // handle request
  HandleRequest(std::move(req_), async_write);
}

void Session::DoClose() {
  boost::beast::error_code ec;
  stream_.socket().shutdown(tcp::socket::shutdown_send, ec);

  // At this point the connection is closed gracefully
}
}  // namespace memgraph::communication::http
