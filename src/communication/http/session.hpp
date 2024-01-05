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

#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <variant>

#include <spdlog/spdlog.h>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/core/stream_traits.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/version.hpp>
#include <json/json.hpp>

#include "communication/context.hpp"
#include "utils/logging.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::communication::http {

inline constexpr uint16_t kSSLExpirySeconds = 30;
inline void LogError(boost::beast::error_code ec, const std::string_view what) {
  spdlog::warn("HTTP session failed on {}: {}", what, ec.message());
}

template <class TRequestHandler, typename TSessionContext>
class Session : public std::enable_shared_from_this<Session<TRequestHandler, TSessionContext>> {
  using tcp = boost::asio::ip::tcp;
  using std::enable_shared_from_this<Session<TRequestHandler, TSessionContext>>::shared_from_this;

 public:
  template <typename... Args>
  static std::shared_ptr<Session> Create(Args &&...args) {
    return std::shared_ptr<Session>{new Session{std::forward<Args>(args)...}};
  }

  void Run() {
    if (auto *ssl = std::get_if<SSLSocket>(&stream_); ssl != nullptr) {
      try {
        boost::beast::get_lowest_layer(*ssl).expires_after(std::chrono::seconds(kSSLExpirySeconds));
        ssl->handshake(boost::asio::ssl::stream_base::server);
      } catch (const boost::system::system_error &e) {
        spdlog::warn("Failed on SSL handshake: {}", e.what());
        return;
      }
    }

    // run on the strand
    boost::asio::dispatch(strand_, [shared_this = shared_from_this()] { shared_this->DoRead(); });
  }

 private:
  using PlainSocket = boost::beast::tcp_stream;
  using SSLSocket = boost::beast::ssl_stream<boost::beast::tcp_stream>;

  explicit Session(tcp::socket &&socket, TSessionContext *data, ServerContext &context)
      : stream_(CreateSocket(std::move(socket), context)),
        handler_(data),
        strand_{boost::asio::make_strand(GetExecutor())} {}

  std::variant<PlainSocket, SSLSocket> CreateSocket(tcp::socket &&socket, ServerContext &context) {
    if (context.use_ssl()) {
      ssl_context_.emplace(context.context_clone());
      return Session::SSLSocket{std::move(socket), *ssl_context_};
    }

    return Session::PlainSocket{std::move(socket)};
  }

  void OnWrite(boost::beast::error_code ec, size_t bytes_transferred) {
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

  void DoRead() {
    req_ = {};

    ExecuteForStream([this](auto &stream) {
      boost::beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(kSSLExpirySeconds));

      boost::beast::http::async_read(
          stream, buffer_, req_,
          boost::asio::bind_executor(strand_, std::bind_front(&Session::OnRead, shared_from_this())));
    });
  }

  void OnRead(boost::beast::error_code ec, size_t bytes_transferred) {
    boost::ignore_unused(bytes_transferred);

    if (ec == boost::beast::http::error::end_of_stream) {
      DoClose();
      return;
    }

    if (ec) {
      return LogError(ec, "read");
    }

    auto async_write = [this](boost::beast::http::response<boost::beast::http::string_body> msg) {
      ExecuteForStream([this, &msg](auto &stream) {
        // The lifetime of the message has to extend
        // for the duration of the async operation so
        // we use a shared_ptr to manage it.
        auto sp = std::make_shared<boost::beast::http::response<boost::beast::http::string_body>>(std::move(msg));

        // Store a type-erased version of the shared
        // pointer in the class to keep it alive.
        res_ = sp;
        // Write the response
        boost::beast::http::async_write(
            stream, *sp, boost::asio::bind_executor(strand_, std::bind_front(&Session::OnWrite, shared_from_this())));
      });
    };

    // handle request
    handler_.HandleRequest(std::move(req_), async_write);
  }

  void DoClose() {
    std::visit(utils::Overloaded{[this](SSLSocket &stream) {
                                   boost::beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(30));

                                   // Perform the SSL shutdown
                                   stream.async_shutdown(
                                       boost::beast::bind_front_handler(&Session::OnClose, shared_from_this()));
                                 },
                                 [](PlainSocket &stream) {
                                   boost::beast::error_code ec;
                                   stream.socket().shutdown(tcp::socket::shutdown_send, ec);
                                 }},
               stream_);
  }
  void OnClose(boost::beast::error_code ec) {
    if (ec) {
      LogError(ec, "close");
    }

    // At this point the connection is closed gracefully
  }

  auto GetExecutor() {
    return std::visit(utils::Overloaded{[](auto &stream) { return stream.get_executor(); }}, stream_);
  }

  template <typename F>
  decltype(auto) ExecuteForStream(F &&fn) {
    return std::visit(utils::Overloaded{std::forward<F>(fn)}, stream_);
  }

  std::optional<std::reference_wrapper<boost::asio::ssl::context>> ssl_context_;
  std::variant<PlainSocket, SSLSocket> stream_;
  boost::beast::flat_buffer buffer_;

  TRequestHandler handler_;
  boost::beast::http::request<boost::beast::http::string_body> req_;
  std::shared_ptr<void> res_;

  boost::asio::strand<boost::beast::tcp_stream::executor_type> strand_;
  bool close_{false};
};
}  // namespace memgraph::communication::http
