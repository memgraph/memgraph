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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>

#include <spdlog/spdlog.h>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/socket_base.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/ssl/stream_base.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/system_context.hpp>
#include <boost/asio/write.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/rfc6455.hpp>
#include <boost/system/detail/error_code.hpp>

#include "communication/bolt/v1/session.hpp"
#include "communication/buffer.hpp"
#include "communication/context.hpp"
#include "communication/exceptions.hpp"
#include "communication/fmt.hpp"
#include "dbms/global.hpp"
#include "utils/event_counter.hpp"
#include "utils/logging.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::metrics {
extern const Event ActiveSessions;
extern const Event ActiveTCPSessions;
extern const Event ActiveSSLSessions;
extern const Event ActiveWebSocketSessions;
}  // namespace memgraph::metrics

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
      : write_function_(std::move(write_function)) {}

  OutputStream(const OutputStream &) = delete;
  OutputStream(OutputStream &&) = delete;
  OutputStream &operator=(const OutputStream &) = delete;
  OutputStream &operator=(OutputStream &&) = delete;
  ~OutputStream() = default;

  bool Write(const uint8_t *data, size_t len, bool have_more = false) { return write_function_(data, len, have_more); }

  bool Write(const std::string &str, bool have_more = false) {
    return Write(reinterpret_cast<const uint8_t *>(str.data()), str.size(), have_more);
  }

 private:
  std::function<bool(const uint8_t *, size_t, bool)> write_function_;
};

/**
 * This class is used internally in the communication stack to handle all user
 * Websocket Sessions. It handles socket ownership, inactivity timeout and protocol
 * wrapping.
 */
template <typename TSession, typename TSessionContext>
class WebsocketSession : public std::enable_shared_from_this<WebsocketSession<TSession, TSessionContext>> {
  using WebSocket = boost::beast::websocket::stream<boost::beast::tcp_stream>;
  using std::enable_shared_from_this<WebsocketSession<TSession, TSessionContext>>::shared_from_this;

 public:
  template <typename... Args>
  static std::shared_ptr<WebsocketSession> Create(Args &&...args) {
    return std::shared_ptr<WebsocketSession>(new WebsocketSession(std::forward<Args>(args)...));
  }

  ~WebsocketSession() = default;

  WebsocketSession(const WebsocketSession &) = delete;
  WebsocketSession &operator=(const WebsocketSession &) = delete;
  WebsocketSession(WebsocketSession &&) noexcept = delete;
  WebsocketSession &operator=(WebsocketSession &&) noexcept = delete;

  // Start the asynchronous accept operation
  template <class Body, class Allocator>
  void DoAccept(boost::beast::http::request<Body, boost::beast::http::basic_fields<Allocator>> req) {
    memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveWebSocketSessions);

    execution_active_ = true;
    // Set suggested timeout settings for the websocket
    ws_.set_option(boost::beast::websocket::stream_base::timeout::suggested(boost::beast::role_type::server));
    boost::asio::socket_base::keep_alive option(true);

    // Set a decorator to change the Server of the handshake
    ws_.set_option(boost::beast::websocket::stream_base::decorator([&req](boost::beast::websocket::response_type &res) {
      res.set(boost::beast::http::field::server, std::string("Memgraph Bolt WS"));

      // We need to do this to support WASM clients, which explicitly send this flag
      // in their upgrade request
      // Neo4j client breaks when this flag is sent
      if (const auto secondary_protocol = req.base().find(boost::beast::http::field::sec_websocket_protocol);
          secondary_protocol != res.base().end() && secondary_protocol->value() == "binary") {
        res.set(boost::beast::http::field::sec_websocket_protocol, "binary");
      }
    }));
    ws_.binary(true);

    // Accept the websocket handshake
    ws_.async_accept(
        req, boost::asio::bind_executor(strand_, std::bind_front(&WebsocketSession::OnAccept, shared_from_this())));
  }

  bool Write(const uint8_t *data, size_t len) {
    if (!IsConnected()) {
      return false;
    }

    boost::system::error_code ec;
    ws_.write(boost::asio::buffer(data, len), ec);
    if (ec) {
      OnError(ec, "write");
      return false;
    }
    return true;
  }

 private:
  // Take ownership of the socket
  explicit WebsocketSession(tcp::socket &&socket, TSessionContext *session_context, tcp::endpoint endpoint,
                            std::string_view service_name)
      : ws_(std::move(socket)),
        strand_{boost::asio::make_strand(ws_.get_executor())},
        output_stream_([this](const uint8_t *data, size_t len, bool /*have_more*/) { return Write(data, len); }),
        session_{session_context->ic,       endpoint, input_buffer_.read_end(), &output_stream_, session_context->auth,
#ifdef MG_ENTERPRISE
                 session_context->audit_log
#endif
        },
        session_context_{session_context},
        endpoint_{endpoint},
        remote_endpoint_{ws_.next_layer().socket().remote_endpoint()},
        service_name_{service_name} {
  }

  void OnAccept(boost::beast::error_code ec) {
    if (ec) {
      return OnError(ec, "accept");
    }

    // Read a message
    DoRead();
  }

  void DoRead() {
    if (!IsConnected()) {
      return;
    }
    // Read a message into our buffer
    auto buffer = input_buffer_.write_end()->Allocate();
    ws_.async_read_some(
        boost::asio::buffer(buffer.data, buffer.len),
        boost::asio::bind_executor(strand_, std::bind_front(&WebsocketSession::OnRead, shared_from_this())));
  }

  void OnRead(const boost::system::error_code &ec, const size_t bytes_transferred) {
    // This indicates that the WebsocketSession was closed
    if (ec == boost::beast::websocket::error::closed) {
      return;
    }
    if (ec) {
      OnError(ec, "read");
    }
    input_buffer_.write_end()->Written(bytes_transferred);

    try {
      session_.Execute();
      DoRead();
    } catch (const SessionClosedException &e) {
      spdlog::info("{} client {} closed the connection.", service_name_, remote_endpoint_);
      DoClose();
    } catch (const std::exception &e) {
      spdlog::error("Exception was thrown while processing event in {} session associated with {}", service_name_,
                    remote_endpoint_);
      spdlog::debug("Exception message: {}", e.what());
      DoClose();
    }
  }

  void OnError(const boost::system::error_code &ec, const std::string_view action) {
    spdlog::error("Websocket Bolt session error: {} on {}", ec.message(), action);

    DoClose();
  }

  void DoClose() {
    ws_.async_close(
        boost::beast::websocket::close_code::normal,
        boost::asio::bind_executor(
            strand_, [shared_this = shared_from_this()](boost::beast::error_code ec) { shared_this->OnClose(ec); }));
  }

  void OnClose(const boost::system::error_code &ec) {
    if (!IsConnected()) {
      return;
    }

    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveSessions);
    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveWebSocketSessions);

    if (ec) {
      return OnError(ec, "close");
    }
  }

  bool IsConnected() const { return ws_.is_open() && execution_active_; }

  WebSocket ws_;
  boost::asio::strand<WebSocket::executor_type> strand_;

  communication::Buffer input_buffer_;
  OutputStream output_stream_;
  TSession session_;
  TSessionContext *session_context_;
  tcp::endpoint endpoint_;
  tcp::endpoint remote_endpoint_;
  std::string_view service_name_;
  bool execution_active_{false};
};

/**
 * This class is used internally in the communication stack to handle all user
 * Sessions. It handles socket ownership, inactivity timeout and protocol
 * wrapping.
 */
template <typename TSession, typename TSessionContext>
class Session final : public std::enable_shared_from_this<Session<TSession, TSessionContext>> {
  using TCPSocket = tcp::socket;
  using SSLSocket = boost::asio::ssl::stream<TCPSocket>;
  using std::enable_shared_from_this<Session<TSession, TSessionContext>>::shared_from_this;

 public:
  template <typename... Args>
  static std::shared_ptr<Session> Create(Args &&...args) {
    return std::shared_ptr<Session>(new Session(std::forward<Args>(args)...));
  }

  ~Session() = default;

  Session(const Session &) = delete;
  Session(Session &&) = delete;
  Session &operator=(const Session &) = delete;
  Session &operator=(Session &&) = delete;

  bool Start() {
    if (execution_active_) {
      return false;
    }

    memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveSessions);

    execution_active_ = true;
    timeout_timer_.async_wait(boost::asio::bind_executor(strand_, std::bind(&Session::OnTimeout, shared_from_this())));

    if (std::holds_alternative<SSLSocket>(socket_)) {
      utils::OnScopeExit increment_counter(
          [] { memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveSSLSessions); });
      boost::asio::dispatch(strand_, [shared_this = shared_from_this()] { shared_this->DoHandshake(); });
    } else {
      utils::OnScopeExit increment_counter(
          [] { memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveTCPSessions); });
      boost::asio::dispatch(strand_, [shared_this = shared_from_this()] { shared_this->DoRead(); });
    }
    return true;
  }

  bool Write(const uint8_t *data, size_t len, bool have_more = false) {
    if (!IsConnected()) {
      return false;
    }
    return std::visit(
        utils::Overloaded{[shared_this = shared_from_this(), data, len, have_more](TCPSocket &socket) mutable {
                            boost::system::error_code ec;
                            while (len > 0) {
                              const auto sent = socket.send(boost::asio::buffer(data, len),
                                                            MSG_NOSIGNAL | (have_more ? MSG_MORE : 0), ec);
                              if (ec) {
                                shared_this->OnError(ec);
                                return false;
                              }
                              data += sent;
                              len -= sent;
                            }
                            return true;
                          },
                          [shared_this = shared_from_this(), data, len](SSLSocket &socket) mutable {
                            boost::system::error_code ec;
                            while (len > 0) {
                              const auto sent = socket.write_some(boost::asio::buffer(data, len), ec);
                              if (ec) {
                                shared_this->OnError(ec);
                                return false;
                              }
                              data += sent;
                              len -= sent;
                            }
                            return true;
                          }},
        socket_);
  }

  bool IsConnected() const {
    return std::visit([this](const auto &socket) { return execution_active_ && socket.lowest_layer().is_open(); },
                      socket_);
  }

 private:
  explicit Session(tcp::socket &&socket, TSessionContext *session_context, ServerContext &server_context,
                   tcp::endpoint endpoint, const std::chrono::seconds inactivity_timeout_sec,
                   std::string_view service_name)
      : socket_(CreateSocket(std::move(socket), server_context)),
        strand_{boost::asio::make_strand(GetExecutor())},
        output_stream_([this](const uint8_t *data, size_t len, bool have_more) { return Write(data, len, have_more); }),
        session_{session_context->ic,       endpoint, input_buffer_.read_end(), &output_stream_, session_context->auth,
#ifdef MG_ENTERPRISE
                 session_context->audit_log
#endif
        },
        session_context_{session_context},
        endpoint_{endpoint},
        remote_endpoint_{GetRemoteEndpoint()},
        service_name_{service_name},
        timeout_seconds_(inactivity_timeout_sec),
        timeout_timer_(GetExecutor()) {
    ExecuteForSocket([](auto &&socket) {
      socket.lowest_layer().set_option(tcp::no_delay(true));                         // enable PSH
      socket.lowest_layer().set_option(boost::asio::socket_base::keep_alive(true));  // enable SO_KEEPALIVE
      socket.lowest_layer().non_blocking(false);
    });
    timeout_timer_.expires_at(boost::asio::steady_timer::time_point::max());
    spdlog::info("Accepted a connection from {}: {}", service_name_, remote_endpoint_);
  }

  void DoRead() {
    if (!IsConnected()) {
      return;
    }
    timeout_timer_.expires_after(timeout_seconds_);
    ExecuteForSocket([this](auto &&socket) {
      auto buffer = input_buffer_.write_end()->Allocate();
      socket.async_read_some(
          boost::asio::buffer(buffer.data, buffer.len),
          boost::asio::bind_executor(strand_, std::bind_front(&Session::OnRead, shared_from_this())));
    });
  }

  bool IsWebsocketUpgrade(boost::beast::http::request_parser<boost::beast::http::string_body> &parser) {
    boost::system::error_code error_code_parsing;
    parser.put(boost::asio::buffer(input_buffer_.read_end()->data(), input_buffer_.read_end()->size()),
               error_code_parsing);
    if (error_code_parsing) {
      return false;
    }

    return boost::beast::websocket::is_upgrade(parser.get());
  }

  void OnRead(const boost::system::error_code &ec, const size_t bytes_transferred) {
    if (ec) {
      // TODO Check if client disconnected
      session_.HandleError();
      return OnError(ec);
    }
    input_buffer_.write_end()->Written(bytes_transferred);

    // Can be a websocket connection only on the first read, since it is not
    // expected from clients to upgrade from tcp to websocket
    if (!has_received_msg_) {
      has_received_msg_ = true;
      boost::beast::http::request_parser<boost::beast::http::string_body> parser;

      if (IsWebsocketUpgrade(parser)) {
        spdlog::info("Switching {} to websocket connection", remote_endpoint_);
        if (std::holds_alternative<TCPSocket>(socket_)) {
          auto sock = std::get<TCPSocket>(std::move(socket_));
          WebsocketSession<TSession, TSessionContext>::Create(std::move(sock), session_context_, endpoint_,
                                                              service_name_)
              ->DoAccept(parser.release());
          execution_active_ = false;
          return;
        }
        spdlog::error("Error while upgrading connection to websocket");
        DoShutdown();
      }
    }

    try {
      session_.Execute();
      DoRead();
    } catch (const SessionClosedException &e) {
      spdlog::info("{} client {} closed the connection.", service_name_, remote_endpoint_);
      DoShutdown();
    } catch (const std::exception &e) {
      spdlog::error("Exception was thrown while processing event in {} session associated with {}", service_name_,
                    remote_endpoint_);
      spdlog::debug("Exception message: {}", e.what());
      DoShutdown();
    }
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
    if (!IsConnected()) {
      return;
    }
    execution_active_ = false;
    timeout_timer_.cancel();
    ExecuteForSocket([](auto &socket) {
      boost::system::error_code ec;
      auto &lowest_layer = socket.lowest_layer();
      lowest_layer.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
      if (ec) {
        spdlog::error("Session shutdown failed: {}", ec.what());
      }
      lowest_layer.close();
    });
  }

  void DoHandshake() {
    if (!IsConnected()) {
      return;
    }
    if (auto *socket = std::get_if<SSLSocket>(&socket_); socket) {
      socket->async_handshake(
          boost::asio::ssl::stream_base::server,
          boost::asio::bind_executor(strand_, std::bind_front(&Session::OnHandshake, shared_from_this())));
    }
  }

  void OnHandshake(const boost::system::error_code &ec) {
    if (ec) {
      return OnError(ec);
    }
    DoRead();
  }

  void OnClose(const boost::system::error_code &ec) {
    if (ssl_context_.has_value()) {
      memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveSSLSessions);
    } else {
      memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveTCPSessions);
    }

    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveSessions);

    if (ec) {
      return OnError(ec);
    }
  }

  void OnTimeout() {
    if (!IsConnected()) {
      return;
    }
    // Check whether the deadline has passed. We compare the deadline against
    // the current time since a new asynchronous operation may have moved the
    // deadline before this actor had a chance to run.
    if (timeout_timer_.expiry() <= boost::asio::steady_timer::clock_type::now()) {
      // The deadline has passed. Stop the session. The other actors will
      // terminate as soon as possible.
      spdlog::info("Shutting down session after {} seconds of inactivity", timeout_seconds_.count());
      DoShutdown();
    } else {
      // Put the actor back to sleep.
      timeout_timer_.async_wait(
          boost::asio::bind_executor(strand_, std::bind(&Session::OnTimeout, shared_from_this())));
    }
  }

  std::variant<TCPSocket, SSLSocket> CreateSocket(tcp::socket &&socket, ServerContext &context) {
    if (context.use_ssl()) {
      ssl_context_.emplace(context.context_clone());
      return SSLSocket{std::move(socket), *ssl_context_};
    }

    return TCPSocket{std::move(socket)};
  }

  auto GetExecutor() {
    return std::visit(utils::Overloaded{[](auto &&socket) { return socket.get_executor(); }}, socket_);
  }

  auto GetRemoteEndpoint() const {
    return std::visit(utils::Overloaded{[](const auto &socket) { return socket.lowest_layer().remote_endpoint(); }},
                      socket_);
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
  TSessionContext *session_context_;
  tcp::endpoint endpoint_;
  tcp::endpoint remote_endpoint_;
  std::string_view service_name_;
  std::chrono::seconds timeout_seconds_;
  boost::asio::steady_timer timeout_timer_;
  bool execution_active_{false};
  bool has_received_msg_{false};
};
}  // namespace memgraph::communication::v2
