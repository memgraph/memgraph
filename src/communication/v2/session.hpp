// Copyright 2026 Memgraph Ltd.
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

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
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

#include "communication/buffer.hpp"
#include "communication/context.hpp"
#include "communication/exceptions.hpp"
#include "communication/fmt.hpp"
#include "communication/v2/epoll_poller.hpp"
#include "communication/v2/session_registry.hpp"
#include "utils/logging.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/priorities.hpp"
#include "utils/priority_thread_pool.hpp"
#include "utils/spin_lock.hpp"
#include "utils/variant_helpers.hpp"

#include "flags/scheduler.hpp"
#include "metrics/prometheus_metrics.hpp"

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

  bool Write(std::span<const uint8_t> data, bool have_more = false) {
    return Write(data.data(), data.size(), have_more);
  }

  bool Write(std::string_view str, bool have_more = false) {
    return Write(reinterpret_cast<const uint8_t *>(str.data()), str.size(), have_more);
  }

 private:
  std::function<bool(const uint8_t *, size_t, bool)> write_function_;
};

/**
 * This class is used internally in the communication stack to handle all user
 * Sessions. It handles socket ownership and protocol wrapping.
 */
template <typename TSession, typename TSessionContext>
class Session final : public std::enable_shared_from_this<Session<TSession, TSessionContext>>,
                      public TerminableSession,
                      public PollTarget {
  using TCPSocket = tcp::socket;
  using SSLSocket = boost::asio::ssl::stream<TCPSocket>;
  using WebSocket = boost::beast::websocket::stream<boost::beast::tcp_stream>;
  using std::enable_shared_from_this<Session<TSession, TSessionContext>>::shared_from_this;

 public:
  template <typename... Args>
  static std::shared_ptr<Session> Create(Args &&...args) {
    return std::shared_ptr<Session>(new Session(std::forward<Args>(args)...));
  }

  ~Session() override {
    if constexpr (requires { session_.UUID(); }) {
      SessionRegistry::Instance().Deregister(session_.UUID(), this);
    }
  }

  Session(const Session &) = delete;
  Session(Session &&) = delete;
  Session &operator=(const Session &) = delete;
  Session &operator=(Session &&) = delete;

  bool Start() {
    if (execution_active_) {
      return false;
    }

    metrics::Metrics().global.active_sessions->Increment();

    execution_active_ = true;

    if constexpr (requires { session_.UUID(); }) {
      SessionRegistry::Instance().Register(session_.UUID(), std::weak_ptr<TerminableSession>{shared_from_this()});
    }

    if (std::holds_alternative<SSLSocket>(socket_)) {
      utils::OnScopeExit increment_counter([] { metrics::Metrics().global.active_ssl_sessions->Increment(); });
      boost::asio::dispatch(strand_, [shared_this = shared_from_this()] { shared_this->DoSSLHandshake(); });
    } else {
      utils::OnScopeExit increment_counter([] { metrics::Metrics().global.active_tcp_sessions->Increment(); });
      boost::asio::dispatch(strand_, [shared_this = shared_from_this()] { shared_this->DoFirstRead(); });
    }
    return true;
  }

  bool Write(const uint8_t *data, size_t len, bool have_more = false) {
    if (!IsConnected()) {
      return false;
    }
    if (raw_.load(std::memory_order_acquire)) {
      return RawWrite_(data, len, have_more);
    }
    return std::visit(
        utils::Overloaded{[shared_this = shared_from_this(), data, len, have_more](TCPSocket &socket) mutable {
                            boost::system::error_code ec;
                            while (len > 0) {
                              const auto sent = socket.send(
                                  boost::asio::buffer(data, len), MSG_NOSIGNAL | (have_more ? MSG_MORE : 0U), ec);
                              if (ec) {
                                spdlog::trace("Failed to write to TCP socket: {}", ec.message());
                                shared_this->OnError(ec);
                                return false;
                              }
                              data += sent;
                              len -= sent;
                            }
                            std::this_thread::yield();
                            return true;
                          },
                          [shared_this = shared_from_this(), data, len](SSLSocket &socket) mutable {
                            boost::system::error_code ec;
                            while (len > 0) {
                              const auto sent = socket.write_some(boost::asio::buffer(data, len), ec);
                              if (ec) {
                                spdlog::trace("Failed to write to SSL socket: {}", ec.message());
                                shared_this->OnError(ec);
                                return false;
                              }
                              data += sent;
                              len -= sent;
                            }
                            std::this_thread::yield();
                            return true;
                          },
                          [shared_this = shared_from_this(), data, len](WebSocket &ws) mutable {
                            boost::system::error_code ec;
                            ws.write(boost::asio::buffer(data, len), ec);
                            if (ec) {
                              spdlog::trace("Failed to write to Web socket: {}", ec.message());
                              shared_this->OnError(ec);
                              return false;
                            }
                            std::this_thread::yield();
                            return true;
                          }},
        socket_);
  }

  bool IsConnected() const {
    if (!execution_active_) return false;
    if (raw_.load(std::memory_order_acquire)) return raw_fd_.load(std::memory_order_acquire) >= 0;
    return std::visit(utils::Overloaded{[](const WebSocket &ws) { return ws.is_open(); },
                                        [](const auto &socket) { return socket.lowest_layer().is_open(); }},
                      socket_);
  }

  // PollTarget: the caller claimed this session's readiness and so owns it. Runs on the calling thread.
  void RunInline(const utils::Priority thread_priority) override { RunReady_(thread_priority, true); }

  // PollTarget: the caller claimed this session's readiness; hand ownership to a pool task.
  void Dispatch() override { DispatchWork_(true); }

  // Callable from any thread. post, not dispatch: the caller here is foreign to the session (an
  // admin command thread), so it must never run session code inline on its own stack.
  void RequestTermination() override {
    terminate_requested_.store(true, std::memory_order_release);
    if (raw_.load(std::memory_order_acquire)) {
      RawTerminate_();
      return;
    }
    boost::asio::post(strand_, [shared_this = shared_from_this()] { shared_this->TerminateIfIdle_(); });
  }

 private:
  explicit Session(tcp::socket &&socket, TSessionContext *session_context, ServerContext &server_context,
                   std::string_view service_name, EpollPoller *poller)
      : socket_(CreateSocket(std::move(socket), server_context)),
        strand_{boost::asio::make_strand(GetExecutor())},
        output_stream_([this](const uint8_t *data, size_t len, bool have_more) { return Write(data, len, have_more); }),
        session_{*session_context, input_buffer_.read_end(), &output_stream_},
        session_context_{session_context},
        remote_endpoint_{GetRemoteEndpoint()},
        service_name_{service_name},
        poller_{poller} {
    std::visit(utils::Overloaded{[](WebSocket & /* unused */) { DMG_ASSERT(false, "Shouldn't get here..."); },
                                 [](auto &socket) {
                                   socket.lowest_layer().set_option(tcp::no_delay(true));  // enable PSH
                                   socket.lowest_layer().set_option(
                                       boost::asio::socket_base::keep_alive(true));  // enable SO_KEEPALIVE
                                   socket.lowest_layer().non_blocking(false);
                                 }},
               socket_);
    spdlog::info("Accepted a connection from {}: {}", service_name_, remote_endpoint_);
  }

  // Start the asynchronous accept operation
  template <class Body, class Allocator>
  void DoAccept(boost::beast::http::request<Body, boost::beast::http::basic_fields<Allocator>> req) {
    DMG_ASSERT(std::holds_alternative<WebSocket>(socket_), "DoAccept is only for WebSocket communication");
    metrics::Metrics().global.active_websocket_sessions->Increment();
    auto &ws = std::get<WebSocket>(socket_);

    // Set suggested timeout settings for the websocket
    ws.set_option(boost::beast::websocket::stream_base::timeout::suggested(boost::beast::role_type::server));
    boost::asio::socket_base::keep_alive option(true);

    // Set a decorator to change the Server of the handshake
    ws.set_option(boost::beast::websocket::stream_base::decorator([&req](boost::beast::websocket::response_type &res) {
      res.set(boost::beast::http::field::server, std::string("Memgraph Bolt WS"));

      // We need to do this to support WASM clients, which explicitly send this flag
      // in their upgrade request
      // Neo4j client breaks when this flag is sent
      if (const auto secondary_protocol = req.base().find(boost::beast::http::field::sec_websocket_protocol);
          secondary_protocol != res.base().end() && secondary_protocol->value() == "binary") {
        res.set(boost::beast::http::field::sec_websocket_protocol, "binary");
      }
    }));
    ws.binary(true);

    // Accept the websocket handshake
    read_armed_ = true;
    ws.async_accept(req, boost::asio::bind_executor(strand_, [self = shared_from_this()](boost::beast::error_code ec) {
                      self->read_armed_ = false;
                      if (self->terminate_requested_.load(std::memory_order_acquire)) {
                        self->DoShutdown();
                        return;
                      }
                      if (ec) {
                        return self->OnError(ec);
                      }
                      // Start branch based on the selected scheduler. Each function is self-calling, no need for
                      // further checks.
                      switch (GetSchedulerType()) {
                        using enum SchedulerType;
                        case ASIO:
                          self->DoReadAsio();
                          break;
                        case PRIORITY_QUEUE_WITH_SIDECAR:
                          self->DoRead();
                          break;
                      }
                    }));
  }

  // Must run on strand_: the sole async_read_some site (DoRead/DoReadAsio/DoFirstRead funnel here),
  // so honouring terminate_requested_ instead of arming the read keeps the socket single-owner.
  template <typename OnReadFn>
  void ArmRead_(OnReadFn on_read) {
    if (!IsConnected()) {
      return;
    }
    if (terminate_requested_.load(std::memory_order_acquire)) {
      DoShutdown();
      return;
    }
    read_armed_ = true;
    ExecuteForSocket([&](auto &socket) {
      auto buffer = input_buffer_.write_end()->GetBuffer();
      socket.async_read_some(boost::asio::buffer(buffer.data, buffer.len),
                             boost::asio::bind_executor(strand_, std::move(on_read)));
    });
  }

  void DoRead() {
    // dispatch, not post: DoRead runs on a priority worker thread (after Execute()/Write()), so this
    // moves async_read_some's arming onto the strand; the other two call sites are already on it.
    boost::asio::dispatch(strand_,
                          [self = shared_from_this()] { self->ArmRead_(std::bind_front(&Session::OnRead, self)); });
  }

  void DoReadAsio() {
    boost::asio::dispatch(strand_,
                          [self = shared_from_this()] { self->ArmRead_(std::bind_front(&Session::OnReadAsio, self)); });
  }

  void DoFirstRead() {
    boost::asio::dispatch(
        strand_, [self = shared_from_this()] { self->ArmRead_(std::bind_front(&Session::OnFirstRead, self)); });
  }

  std::optional<boost::beast::http::request<boost::beast::http::string_body>> IsWebsocketUpgrade(uint8_t *data,
                                                                                                 size_t size) {
    boost::beast::http::request_parser<boost::beast::http::string_body> parser;
    boost::system::error_code error_code_parsing;
    parser.put(boost::asio::buffer(data, size), error_code_parsing);
    if (error_code_parsing) {
      return std::nullopt;
    }

    if (boost::beast::websocket::is_upgrade(parser.get())) return parser.release();
    return std::nullopt;
  }

  void OnFirstRead(const boost::system::error_code &ec, const size_t bytes_transferred) {
    read_armed_ = false;
    if (ec) {
      session_.HandleError();
      return OnError(ec);
    }

    // Can be a websocket connection only on the first read, since it is not
    // expected from clients to upgrade from tcp to websocket

    if (auto req = IsWebsocketUpgrade(input_buffer_.read_end()->data(), bytes_transferred); req) {
      spdlog::info("Switching {} to websocket connection", remote_endpoint_);
      if (std::holds_alternative<TCPSocket>(socket_)) {
        WebSocket ws{std::get<TCPSocket>(std::move(socket_))};
        socket_.emplace<WebSocket>(std::move(ws));
        DoAccept(std::move(*req));
        return;
      }
      spdlog::error("Error while upgrading connection to websocket");
      DoShutdown();
    }

    // Start branch based on the selected scheduler. Each function is self-calling, no need for further checks.
    switch (GetSchedulerType()) {
      using enum SchedulerType;
      case ASIO:
        OnReadAsio(ec, bytes_transferred);
        break;
      case PRIORITY_QUEUE_WITH_SIDECAR:
        if (poller_ && TryAdopt_(bytes_transferred)) return;
        OnRead(ec, bytes_transferred);
        break;
    }
  }

  // Strand, plain TCP, right after the first read (no asio op outstanding). Moves the fd out of asio into
  // the poller and hands the buffered bytes to a pool task. Returns false if asio still owns the session.
  bool TryAdopt_(const size_t bytes_transferred) {
    auto *tcp_socket = std::get_if<TCPSocket>(&socket_);
    if (!tcp_socket) return false;
    boost::system::error_code ec;
    const int fd = tcp_socket->release(ec);
    if (ec) return false;

    const int flags = ::fcntl(fd, F_GETFL, 0);
    const auto slot = (flags >= 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0)
                          ? poller_->Adopt(fd, shared_from_this())
                          : EpollPoller::kInvalid;
    if (slot == EpollPoller::kInvalid) {
      spdlog::error("Failed to hand {} over to the poller; closing", remote_endpoint_);
      ::shutdown(fd, SHUT_RDWR);
      ::close(fd);
      execution_active_ = false;
      metrics::Metrics().global.active_tcp_sessions->Decrement();
      metrics::Metrics().global.active_sessions->Decrement();
      return true;
    }
    slot_ = slot;
    raw_fd_.store(fd, std::memory_order_release);
    raw_.store(true, std::memory_order_release);
    input_buffer_.write_end()->Written(bytes_transferred);
    DispatchWork_(false);
    return true;
  }

  // Submits the session's next run to the pool. The caller is the owner and this is its last touch of the stream.
  void DispatchWork_(const bool read_first) {
    ClearOwner_();
    session_context_->AddTask(
        [self = shared_from_this(), read_first](const utils::Priority thread_priority) {
          self->RunReady_(thread_priority, read_first);
        },
        session_.ApproximateQueryPriority());
  }

  // Runs on the owning thread (a pool worker); nothing else may touch the stream until RawArm_/close/re-submit.
  void RunReady_(const utils::Priority thread_priority, const bool read_first) {
    SetOwner_();
    try {
      bool filled = false;
      if (read_first && !ReadAvailable_(filled)) {
        return;
      }
      while (true) {
        if (session_.Execute()) {
          if (thread_priority > session_.ApproximateQueryPriority()) {
            DispatchWork_(false);
            return;
          }
        } else if (filled) {
          // Last recv filled the buffer; more is likely queued.
          filled = false;
          if (!ReadAvailable_(filled)) {
            return;
          }
        } else {
          RawArm_();
          return;
        }
      }
    } catch (const std::exception & /* unused */) {
      HandleException(std::current_exception());
    }
  }

  // Owner only. Returns true if bytes were appended and the caller should Execute(); false if the read was
  // handled here (re-armed, or routed to the error path). filled: the recv filled the whole buffer.
  bool ReadAvailable_(bool &filled) {
    if (!IsConnected()) return false;
    AssertOwner_();
    auto buffer = input_buffer_.write_end()->GetBuffer();
    DMG_ASSERT(buffer.len > 0, "recv into an empty buffer would be misread as EOF");
    ssize_t n;
    do {
      n = ::recv(raw_fd_.load(std::memory_order_relaxed), buffer.data, buffer.len, MSG_DONTWAIT);
    } while (n < 0 && errno == EINTR);
    const int err = errno;

    if (n > 0) {
      input_buffer_.write_end()->Written(static_cast<size_t>(n));
      filled = static_cast<size_t>(n) == buffer.len;
      return true;
    }
    if (n < 0 && (err == EAGAIN || err == EWOULDBLOCK)) {
      RawArm_();
      return false;
    }
    const auto ec = n == 0 ? boost::system::error_code{boost::asio::error::eof}
                           : boost::system::error_code{err, boost::system::system_category()};
    session_.HandleError();
    OnError(ec);
    return false;
  }

  // Owner only. Blocking send: the fd stays O_NONBLOCK, so wait for POLLOUT on EAGAIN.
  bool RawWrite_(const uint8_t *data, size_t len, const bool have_more) {
    AssertOwner_();
    const int fd = raw_fd_.load(std::memory_order_relaxed);
    while (len > 0) {
      const auto sent = ::send(fd, data, len, MSG_NOSIGNAL | (have_more ? MSG_MORE : 0));
      if (sent < 0) {
        int err = errno;
        if (err == EINTR) continue;
        if (err == EAGAIN || err == EWOULDBLOCK) {
          pollfd pfd{.fd = fd, .events = POLLOUT, .revents = 0};
          const int r = ::poll(&pfd, 1, -1);
          if (r >= 0 || errno == EINTR) continue;
          err = errno;
        }
        const boost::system::error_code ec{err, boost::system::system_category()};
        spdlog::trace("Failed to write to TCP socket: {}", ec.message());
        OnError(ec);
        return false;
      }
      data += sent;
      len -= static_cast<size_t>(sent);
    }
    std::this_thread::yield();
    return true;
  }

  // Owner only. Re-arms readiness under arm_lock_ (so a foreign RequestTermination cannot be lost), or closes
  // if termination was requested. After the arm the owner must not touch the stream again.
  void RawArm_() {
    ArmGuard guard{arm_lock_};
    if (!IsConnected()) return;
    if (terminate_requested_.load(std::memory_order_acquire)) {
      CloseRawLocked_();
      return;
    }
    ClearOwner_();
    if (!poller_->Arm(slot_)) {
      spdlog::error("Failed to re-arm {} in the poller; closing", remote_endpoint_);
      CloseRawLocked_();
    }
  }

  // Requires arm_lock_ and IsConnected(); the caller is the owner (RUNNING) or won TryBeginClose (CLOSING).
  std::shared_ptr<PollTarget> CloseRawLocked_() {
    execution_active_ = false;
    raw_fd_.store(-1, std::memory_order_release);
    ClearOwner_();
    metrics::Metrics().global.active_tcp_sessions->Decrement();
    metrics::Metrics().global.active_sessions->Decrement();
    return poller_->Close(slot_);
  }

  // Any thread. Closes only if the session is ARMED; a RUNNING owner honours terminate_requested_ at RawArm_.
  void RawTerminate_() {
    std::shared_ptr<PollTarget> keep;
    {
      ArmGuard guard{arm_lock_};
      if (!IsConnected() || !poller_->TryBeginClose(slot_)) return;
      keep = CloseRawLocked_();
    }
    // Keeps ~Session off the terminating thread's stack. During shutdown ScheduledAddTask drops the task, so there
    // ~Session can still run on the caller.
    session_context_->AddTask([keep = std::move(keep)](const utils::Priority /*unused*/) {}, utils::Priority::LOW);
  }

#ifndef NDEBUG
  void SetOwner_() { owner_.store(std::this_thread::get_id(), std::memory_order_relaxed); }

  void ClearOwner_() { owner_.store(std::thread::id{}, std::memory_order_relaxed); }

  void AssertOwner_() const {
    DMG_ASSERT(owner_.load(std::memory_order_relaxed) == std::this_thread::get_id(),
               "Raw TCP stream touched by a thread that does not own the session");
  }
#else
  void SetOwner_() {}

  void ClearOwner_() {}

  void AssertOwner_() const {}
#endif

  void HandleException(const std::exception_ptr eptr) {
    DMG_ASSERT(eptr, "No exception to handle");
    try {
      std::rethrow_exception(eptr);
    } catch (const SessionClosedException &e) {
      spdlog::info("{} client {} closed the connection.", service_name_, remote_endpoint_);
      DoShutdown();
    } catch (const std::exception &e) {
      spdlog::error("Exception was thrown while processing event in {} session associated with {}",
                    service_name_,
                    remote_endpoint_);
      spdlog::debug("Exception message: {}", e.what());
      DoShutdown();
    }
  }

  void OnRead(const boost::system::error_code &ec, const size_t bytes_transferred) {
    read_armed_ = false;
    if (ec) {
      spdlog::trace("OnRead error: {}", ec.message());
      session_.HandleError();
      return OnError(ec);
    }

    input_buffer_.write_end()->Written(bytes_transferred);
    DoWork();
  }

  void OnReadAsio(const boost::system::error_code &ec, const size_t bytes_transferred) {
    read_armed_ = false;
    if (ec) {
      spdlog::trace("OnRead error: {}", ec.message());
      session_.HandleError();
      return OnError(ec);
    }

    input_buffer_.write_end()->Written(bytes_transferred);

    try {
      // Execute until all data has been read
      while (session_.Execute()) {
      }
      // Handled all data,  async wait for new incoming data
      DoReadAsio();
    } catch (const std::exception & /* unused */) {
      HandleException(std::current_exception());
    }
  }

  void DoWork() {
    session_context_->AddTask(
        [shared_this = shared_from_this()](const auto thread_priority) {
          try {
            while (true) {
              if (shared_this->session_.Execute()) {
                // Check if we can just steal this task (loop through)
                if (thread_priority > shared_this->session_.ApproximateQueryPriority()) {
                  // Task priority lower; reschedule
                  shared_this->DoWork();
                  return;
                }
              } else {
                // Handled all data,  async wait for new incoming data
                shared_this->DoRead();
                return;
              }
            }
          } catch (const std::exception & /* unused */) {
            boost::asio::post(shared_this->strand_,
                              [shared_this, eptr = std::current_exception()]() { shared_this->HandleException(eptr); });
          }
        },
        session_.ApproximateQueryPriority());
  }

  void OnError(const boost::system::error_code &ec) {
    if (ec == boost::asio::error::operation_aborted) {
      return;
    }
    // This indicates that the WebsocketSession was closed
    if (ec == boost::beast::websocket::error::closed) {
      return;
    }

    if (ec == boost::asio::error::eof) {
      spdlog::info("Session closed by peer {}", remote_endpoint_);
    } else {
      spdlog::error("Session error: {}", ec.message());
    }

    DoShutdown();
  }

  // Runs on strand_.
  void TerminateIfIdle_() {
    if (raw_.load(std::memory_order_acquire)) {
      RawTerminate_();
      return;
    }
    // Deferred: read_armed_ == false means a worker may own the socket (Execute()/Write()); leave
    // terminate_requested_ set and let ArmRead_ close it on the next read-arm instead of racing here.
    if (!read_armed_) {
      return;
    }
    read_armed_ = false;
    DoShutdown();
  }

  void DoShutdown() {
    if (raw_.load(std::memory_order_acquire)) {
      // Owner only (foreign closes go through RawTerminate_).
      ArmGuard guard{arm_lock_};
      if (IsConnected()) CloseRawLocked_();
      return;
    }
    if (!IsConnected()) {
      return;
    }
    execution_active_ = false;

    std::visit(utils::Overloaded{[this](WebSocket &ws) {
                                   ws.async_close(
                                       boost::beast::websocket::close_code::normal,
                                       boost::asio::bind_executor(
                                           strand_, [shared_this = shared_from_this()](boost::beast::error_code ec) {
                                             memgraph::metrics::Metrics().global.active_websocket_sessions->Decrement();
                                             if (ec) {
                                               shared_this->OnError(ec);
                                             }
                                           }));
                                 },
                                 [](auto &socket) {
                                   boost::system::error_code ec;
                                   socket.lowest_layer().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
                                   if (ec) {
                                     spdlog::error("Session shutdown failed: {}", ec.what());
                                   }
                                   socket.lowest_layer().close(ec);
                                   if (ec) {
                                     spdlog::error("Session close failed: {}", ec.what());
                                   }
                                 }},
               socket_);

    // Update metrics
    if (ssl_context_) {
      metrics::Metrics().global.active_ssl_sessions->Decrement();
    } else {
      metrics::Metrics().global.active_tcp_sessions->Decrement();
    }

    metrics::Metrics().global.active_sessions->Decrement();
  }

  void DoSSLHandshake() {
    if (!IsConnected()) {
      return;
    }
    if (auto *socket = std::get_if<SSLSocket>(&socket_); socket) {
      // Mirror ArmRead_: honour a terminate requested during the SSL handshake before arming. The handshake
      // wait is otherwise unbounded (no application-level timer), so a session terminated mid-handshake would
      // only close on the OS TCP timeout. IsConnected() above guarantees DoShutdown() acts on the SSL socket.
      if (terminate_requested_.load(std::memory_order_acquire)) {
        DoShutdown();
        return;
      }
      read_armed_ = true;
      socket->async_handshake(
          boost::asio::ssl::stream_base::server,
          boost::asio::bind_executor(strand_, std::bind_front(&Session::OnSSLHandshake, shared_from_this())));
    }
  }

  void OnSSLHandshake(const boost::system::error_code &ec) {
    read_armed_ = false;
    if (terminate_requested_.load(std::memory_order_acquire)) {
      DoShutdown();
      return;
    }
    if (ec) {
      return OnError(ec);
    }
    DoFirstRead();
  }

  std::variant<TCPSocket, SSLSocket, WebSocket> CreateSocket(tcp::socket &&socket, ServerContext &context) {
    if (context.use_ssl()) {
      ssl_context_ = context.context_clone();
      return SSLSocket{std::move(socket), *ssl_context_};
    }

    return TCPSocket{std::move(socket)};
  }

  auto GetExecutor() {
    return std::visit(utils::Overloaded{[](auto &&socket) { return socket.get_executor(); }}, socket_);
  }

  std::optional<tcp::endpoint> GetRemoteEndpoint() const {
    try {
      return std::visit(
          utils::Overloaded{[](const WebSocket &ws) { return ws.next_layer().socket().remote_endpoint(); },
                            [](const auto &socket) { return socket.lowest_layer().remote_endpoint(); }},
          socket_);
    } catch (const boost::system::system_error &e) {
      return std::nullopt;
    }
  }

  template <typename F>
  decltype(auto) ExecuteForSocket(F &&fun) {
    return std::visit(utils::Overloaded{std::forward<F>(fun)}, socket_);
  }

  std::shared_ptr<boost::asio::ssl::context> ssl_context_;  // must be destroyed after socket_
  std::variant<TCPSocket, SSLSocket, WebSocket> socket_;
  boost::asio::strand<tcp::socket::executor_type> strand_;

  communication::Buffer input_buffer_;
  OutputStream output_stream_;
  TSession session_;
  TSessionContext *session_context_;
  std::optional<tcp::endpoint> remote_endpoint_;
  std::string_view service_name_;
  EpollPoller *poller_;  // null: the session stays on asio
  std::atomic_bool execution_active_{false};
  // Set by any thread via RequestTermination; only ever set, never cleared. Checked on the strand
  // at every read-arm (ArmRead_), so a request made while a worker owns the socket can't be lost.
  std::atomic_bool terminate_requested_{false};
  // STRAND-CONFINED. true: an async op (handshake/upgrade/read) is pending and the strand solely
  // owns the socket; false: a worker may be inside Execute()/Write(), so don't touch it elsewhere.
  bool read_armed_{false};

  using ArmGuard = std::lock_guard<utils::SpinLock>;
  // Raw-TCP mode (fd owned by poller_, set once on the strand by TryAdopt_). slot_ is written before raw_ is
  // published and never changes after; raw_fd_ turns -1 when the session closes.
  std::atomic_bool raw_{false};
  std::atomic<int> raw_fd_{-1};
  EpollPoller::Slot slot_{EpollPoller::kInvalid};
  // Serializes the owner's re-arm/close against a foreign RawTerminate_ (leaf lock).
  utils::SpinLock arm_lock_;
#ifndef NDEBUG
  std::atomic<std::thread::id> owner_{};
#endif
};
}  // namespace memgraph::communication::v2
