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

std::variant<Session::PlainSocket, Session::SSLSocket> Session::CreateSocket(tcp::socket &&socket,
                                                                             ServerContext &context) {
  if (context.use_ssl()) {
    ssl_context_.emplace(context.context_clone());
    return Session::SSLSocket{std::move(socket), *ssl_context_};
  }

  return Session::PlainSocket{std::move(socket)};
}

Session::Session(tcp::socket &&socket, ServerContext &context)
    : stream_(CreateSocket(std::move(socket), context)), strand_{boost::asio::make_strand(GetExecutor())} {}

void Session::Run() {
  if (auto *ssl = std::get_if<SSLSocket>(&stream_); ssl != nullptr) {
    try {
      boost::beast::get_lowest_layer(*ssl).expires_after(std::chrono::seconds(30));
      ssl->handshake(boost::asio::ssl::stream_base::server);
    } catch (const boost::system::system_error &e) {
      spdlog::warn("Failed on SSL handshake: {}", e.what());
      return;
    }
  }

  // run on the strand
  boost::asio::dispatch(strand_, [shared_this = shared_from_this()] { shared_this->DoRead(); });
}

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

  ExecuteForStream([this](auto &&stream) {
    beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(30));

    boost::beast::http::async_read(
        stream, buffer_, req_,
        boost::asio::bind_executor(strand_, std::bind_front(&Session::OnRead, shared_from_this())));
  });
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
    ExecuteForStream([this, &msg](auto &&stream) {
      // The lifetime of the message has to extend
      // for the duration of the async operation so
      // we use a shared_ptr to manage it.
      auto sp = std::make_shared<http::response<http::string_body>>(std::move(msg));

      // Store a type-erased version of the shared
      // pointer in the class to keep it alive.
      res_ = sp;
      // Write the response
      boost::beast::http::async_write(
          stream, *sp, boost::asio::bind_executor(strand_, std::bind_front(&Session::OnWrite, shared_from_this())));
    });
  };

  // handle request
  HandleRequest(std::move(req_), async_write);
}

void Session::DoClose() {
  std::visit(
      utils::Overloaded{[this](SSLSocket &stream) {
                          boost::beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(30));

                          // Perform the SSL shutdown
                          stream.async_shutdown(beast::bind_front_handler(&Session::OnClose, shared_from_this()));
                        },
                        [](PlainSocket &stream) {
                          boost::beast::error_code ec;
                          stream.socket().shutdown(tcp::socket::shutdown_send, ec);
                        }},
      stream_);
}

void Session::OnClose(boost::beast::error_code ec) {
  if (ec) {
    LogError(ec, "close");
  }

  // At this point the connection is closed gracefully
}
}  // namespace memgraph::communication::http
