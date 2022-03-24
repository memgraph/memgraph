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

#define BOOST_ASIO_USE_TS_EXECUTOR_AS_DEFAULT 1

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <variant>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/registered_buffer.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/system_context.hpp>
#include <boost/system/detail/error_code.hpp>

#include "communication/buffer.hpp"
#include "communication/context.hpp"
#include "communication/exceptions.hpp"
#include "communication/helpers.hpp"
#include "io/network/socket.hpp"
#include "io/network/stream_buffer.hpp"
#include "spdlog/spdlog.h"
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
    boost::asio::dispatch(strand_, [shared_this = this->shared_from_this()] { shared_this->DoRead(); });
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
    if (auto *ssl_socket = std::get_if<SSLSocket>(&socket_); ssl_socket != nullptr) {
    }
    ExecuteForSocket([](auto &&socket) {
      // socket.set_option(boost::asio::ip::tcp::no_delay(true));        // enable PSH
      // socket.set_option(boost::asio::socket_base::keep_alive(true));  // enable SO_KEEPALIVE
      // socket_.set_option(boost::asio::detail::socket_option::integer<SOL_SOCKET, SO_RCVTIMEO>{ 200 });
      // socket_.set_option(rcv_timeout_option{1000});                    // set rcv timeout for seession
    });
  }

  void DoWrite(const uint8_t *data, size_t len, bool /*have_more*/) {
    ExecuteForSocket([this, data, len](auto &&socket) {
      if (execution_active_) {
        socket.async_write_some(boost::asio::buffer(data, len),
                                boost::asio::bind_executor(strand_, [shared_this = this->shared_from_this()](
                                                                        const boost::system::error_code &ec,
                                                                        std::size_t /*bytes_transferred*/) {
                                  if (ec) {
                                    shared_this->OnError(ec);
                                  }
                                }));
      }
    });
  }

  void DoRead() {
    if (!execution_active_) {
      return;
    }
    ExecuteForSocket([this](auto &&socket) mutable {
      auto buffer = input_buffer_.write_end()->Allocate();
      socket.async_read_some(
          boost::asio::buffer(buffer.data, buffer.len),
          boost::asio::bind_executor(strand_, [shared_this = this->shared_from_this()](
                                                  const boost::system::error_code &ec, std::size_t bytes_transferred) {
            if (ec) {
              shared_this->OnError(ec);
              return;
            }
            shared_this->input_buffer_.write_end()->Written(bytes_transferred);
            shared_this->session_.Execute();
            shared_this->DoRead();
          }));
    });
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
    boost::asio::dispatch(strand_, [shared_this = this->shared_from_this()] {
      shared_this->execution_active_ = false;
      shared_this->OnShutdown();
    });
  }

  void OnShutdown() {
    std::visit(
        utils::Overloaded{[](TCPSocket &socket) { socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both); },
                          [](SSLSocket &ssl_socket) {
                            ssl_socket.lowest_layer().shutdown(boost::asio::ip::tcp::socket::shutdown_both);
                          }},
        socket_);
  }

  void OnWrite() {
    if (!execution_active_) {
      DoShutdown();
      return;
    }
  }

  using TCPSocket = tcp::socket;
  using SSLSocket = boost::asio::ssl::stream<tcp::socket>;

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
    std::visit(utils::Overloaded{std::forward<F>(fun)}, socket_);
  }

  std::variant<TCPSocket, SSLSocket> socket_;
  std::optional<std::reference_wrapper<boost::asio::ssl::context>> ssl_context_;
  boost::asio::strand<tcp::socket::executor_type> strand_;

  communication::Buffer input_buffer_;
  OutputStream output_stream_;
  TSession session_;
  std::deque<std::shared_ptr<std::string>> messages_;
  bool execution_active_{false};
};
}  // namespace memgraph::communication::v2
