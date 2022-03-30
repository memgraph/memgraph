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

#include <algorithm>
#include <boost/asio/socket_base.hpp>
#include <boost/asio/ssl/stream_base.hpp>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <variant>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/registered_buffer.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/system_context.hpp>
#include <boost/asio/write.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/system/detail/error_code.hpp>

#include "communication/buffer.hpp"
#include "communication/context.hpp"
#include "communication/exceptions.hpp"
#include "communication/helpers.hpp"
#include "io/network/stream_buffer.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::communication::v2 {

/**
 * This is used to provide input to user Sessions. All Sessions used with the
 * network stack should use this class as their input stream.
 */
using InputStream = communication::Buffer::ReadEnd;
using tcp = boost::asio::ip::tcp;

/**
 * This is used to provide output from user Sessions. All Sessions used with the
 * network stack should use this class for their output stream.
 */
class OutputStream final {
 public:
  explicit OutputStream(std::function<bool(const uint8_t *, size_t, bool)> write_function)
      : write_function_(write_function) {}

  OutputStream(const OutputStream &) = delete;
  OutputStream(OutputStream &&) = delete;
  OutputStream &operator=(const OutputStream &) = delete;
  OutputStream &operator=(OutputStream &&) = delete;

  bool Write(const uint8_t *data, size_t len, bool have_more = false) { return write_function_(data, len, have_more); }

  bool Write(const std::string &str, bool have_more = false) {
    return Write(reinterpret_cast<const uint8_t *>(str.data()), str.size(), have_more);
  }

 private:
  std::function<bool(const uint8_t *, size_t, bool)> write_function_;
};

/**
 * This class is used internally in the communication stack to handle all user
 * Sessions. It handles socket ownership, inactivity timeout and protocol
 * wrapping.
 */
template <typename TSession, typename TSessionData>
class Session final : public std::enable_shared_from_this<Session<TSession, TSessionData>> {
  using TCPSocket = boost::beast::tcp_stream;
  using SSLSocket = boost::asio::ssl::stream<TCPSocket>;

 public:
  template <typename... Args>
  static std::shared_ptr<Session> Create(Args &&...args) {
    return std::shared_ptr<Session>(new Session(std::forward<Args>(args)...));
  }

  Session(const Session &) = delete;
  Session(Session &&) = delete;
  Session &operator=(const Session &) = delete;
  Session &operator=(Session &&) = delete;

  ~Session() {
    if (execution_active_) {
      spdlog::error("Session: Destructor called while execution is active");
    }
  }

  bool Start() {
    if (execution_active_) {
      return false;
    }
    execution_active_ = true;
    if (auto *socket = std::get_if<SSLSocket>(&socket_); socket) {
      boost::asio::dispatch(this->shared_from_this()->strand_,
                            [shared_this = this->shared_from_this()] { shared_this->DoHandshake(); });
    } else {
      boost::asio::dispatch(this->shared_from_this()->strand_,
                            [shared_this = this->shared_from_this()] { shared_this->DoRead(); });
    }
    return true;
  }

  bool Write(const uint8_t *data, size_t len, bool have_more = false) {
    boost::asio::dispatch(this->strand_, [shared_this = this->shared_from_this(), data, len, have_more] {
      shared_this->DoWrite(data, len, have_more);
    });
    return true;
  }

  bool IsConnected() const noexcept { return execution_active_; }

 private:
  explicit Session(tcp::socket &&socket, TSessionData *data, ServerContext &server_context, tcp::endpoint endpoint)
      : socket_(CreateWebSocket(std::move(socket), server_context)),
        strand_{boost::asio::make_strand(GetExecutor())},
        output_stream_([this](const uint8_t *data, size_t len, bool have_more) { return Write(data, len, have_more); }),
        session_(data, endpoint, input_buffer_.read_end(), &output_stream_) {
    std::visit(
        utils::Overloaded{
            [](SSLSocket &socket) {
              socket.lowest_layer().set_option(boost::asio::ip::tcp::no_delay(true));        // enable PSH
              socket.lowest_layer().set_option(boost::asio::socket_base::keep_alive(true));  // enable SO_KEEPALIVE
              socket.lowest_layer().non_blocking(false);
            },
            [](TCPSocket &socket) {
              socket.socket().set_option(boost::asio::ip::tcp::no_delay(true));        // enable PSH
              socket.socket().set_option(boost::asio::socket_base::keep_alive(true));  // enable SO_KEEPALIVE
              socket.socket().non_blocking(false);
            }},
        socket_);
  }

  void DoWrite(const uint8_t *data, size_t len, bool /*have_more*/) {
    std::visit(utils::Overloaded{[shared_this = this->shared_from_this(), data, len](TCPSocket &socket) {
                                   if (!socket.socket().is_open()) return;
                                   boost::asio::write(socket, boost::asio::buffer(data, len));
                                 },
                                 [shared_this = this->shared_from_this(), data, len](SSLSocket &socket) {
                                   if (!socket.next_layer().socket().is_open()) return;
                                   boost::asio::write(socket, boost::asio::buffer(data, len));
                                 }},
               socket_);
  }

  void DoRead() {
    if (!execution_active_) {
      return;
    }
    ExecuteForSocket([this](auto &&socket) {
      auto buffer = input_buffer_.write_end()->Allocate();
      socket.async_read_some(
          boost::asio::buffer(buffer.data, buffer.len),
          boost::asio::bind_executor(strand_, std::bind_front(&Session::OnRead, this->shared_from_this())));
    });
  }

  void OnRead(const boost::system::error_code &ec, const size_t bytes_transferred) {
    if (ec) {
      OnError(ec);
      return;
    }

    input_buffer_.write_end()->Written(bytes_transferred);
    session_.Execute();
    DoRead();
  }

  void OnError(const boost::system::error_code &ec) {
    if (ec == boost::asio::error::operation_aborted) {
      return;
    }

    if (ec == boost::asio::error::eof) {
      spdlog::info("Session closed by peer");
    } else {
      spdlog::error("Session error: {}", ec.message());
    }

    DoShutdown();
  }

  void DoShutdown() {
    execution_active_ = false;
    std::visit(utils::Overloaded{[](TCPSocket &socket) {
                                   boost::system::error_code ec;
                                   socket.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
                                   if (ec) {
                                     spdlog::error("Session shutdown failed: {}", ec.what());
                                   }
                                   socket.close();
                                 },
                                 [](SSLSocket &ssl_socket) {
                                   boost::system::error_code ec;
                                   ssl_socket.lowest_layer().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
                                   if (ec) {
                                     spdlog::error("Session shutdown failed: {}", ec.what());
                                   }
                                   ssl_socket.lowest_layer().close();
                                 }},
               socket_);
  }

  void DoHandshake() {
    if (auto *socket = std::get_if<SSLSocket>(&socket_); socket) {
      socket->async_handshake(
          boost::asio::ssl::stream_base::server,
          boost::asio::bind_executor(strand_, std::bind_front(&Session::OnHandshake, this->shared_from_this())));
    }
  }

  void OnHandshake(const boost::system::error_code &ec) {
    DoRead();
    if (ec) {
      OnError(ec);
      return;
    }
    if (!execution_active_) {
      return;
    }
  }

  std::variant<TCPSocket, SSLSocket> CreateWebSocket(tcp::socket &&socket, ServerContext &context) {
    if (context.use_ssl()) {
      ssl_context_.emplace(context.context_clone());
      return SSLSocket{std::move(socket), *ssl_context_};
    }

    return TCPSocket{std::move(socket)};
  }

  auto GetExecutor() {
    return std::visit(utils::Overloaded{[](auto &&socket) { return socket.get_executor(); }}, socket_);
  }

  template <typename F>
  decltype(auto) ExecuteForSocket(F &&fun) {
    return std::visit(utils::Overloaded{std::forward<F>(fun)}, socket_);
  }

  std::variant<TCPSocket, SSLSocket> socket_;
  std::optional<std::reference_wrapper<boost::asio::ssl::context>> ssl_context_;
  boost::asio::strand<tcp::socket::executor_type> strand_;

  communication::Buffer input_buffer_;
  OutputStream output_stream_;
  TSession session_;
  bool execution_active_{false};
};
}  // namespace memgraph::communication::v2
