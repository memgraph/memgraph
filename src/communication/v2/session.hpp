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
#include "utils/logging.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/priority_thread_pool.hpp"
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
class Session final : public std::enable_shared_from_this<Session<TSession, TSessionContext>> {
  using TCPSocket = tcp::socket;
  using SSLSocket = boost::asio::ssl::stream<TCPSocket>;
  using WebSocket = boost::beast::websocket::stream<boost::beast::tcp_stream>;
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

    metrics::Metrics().global.active_sessions->Increment();

    execution_active_ = true;

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
    return execution_active_ &&
           std::visit(utils::Overloaded{[](const WebSocket &ws) { return ws.is_open(); },
                                        [](const auto &socket) { return socket.lowest_layer().is_open(); }},
                      socket_);
  }

 private:
  explicit Session(tcp::socket &&socket, TSessionContext *session_context, ServerContext &server_context,
                   std::string_view service_name)
      : socket_(CreateSocket(std::move(socket), server_context)),
        strand_{boost::asio::make_strand(GetExecutor())},
        output_stream_([this](const uint8_t *data, size_t len, bool have_more) { return Write(data, len, have_more); }),
        session_{*session_context, input_buffer_.read_end(), &output_stream_},
        session_context_{session_context},
        remote_endpoint_{GetRemoteEndpoint()},
        service_name_{service_name} {
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
    ws.async_accept(req, boost::asio::bind_executor(strand_, [self = shared_from_this()](boost::beast::error_code ec) {
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

  void DoRead() {
    if (!IsConnected()) {
      return;
    }
    ExecuteForSocket([this](auto &socket) {
      auto buffer = input_buffer_.write_end()->GetBuffer();
      socket.async_read_some(
          boost::asio::buffer(buffer.data, buffer.len),
          boost::asio::bind_executor(strand_, std::bind_front(&Session::OnRead, shared_from_this())));
    });
  }

  void DoReadAsio() {
    if (!IsConnected()) {
      return;
    }
    ExecuteForSocket([this](auto &socket) {
      auto buffer = input_buffer_.write_end()->GetBuffer();
      socket.async_read_some(
          boost::asio::buffer(buffer.data, buffer.len),
          boost::asio::bind_executor(strand_, std::bind_front(&Session::OnReadAsio, shared_from_this())));
    });
  }

  void DoFirstRead() {
    if (!IsConnected()) {
      return;
    }
    ExecuteForSocket([this](auto &socket) {
      auto buffer = input_buffer_.write_end()->GetBuffer();
      socket.async_read_some(
          boost::asio::buffer(buffer.data, buffer.len),
          boost::asio::bind_executor(strand_, std::bind_front(&Session::OnFirstRead, shared_from_this())));
    });
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
        OnRead(ec, bytes_transferred);
        break;
    }
  }

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
    if (ec) {
      spdlog::trace("OnRead error: {}", ec.message());
      session_.HandleError();
      return OnError(ec);
    }

    input_buffer_.write_end()->Written(bytes_transferred);

    // d1b: if a coro pull-task is in flight, suppress the DoWork dispatch — the pull-task
    // owns the session until it re-arms.  OnRead is strand-bound; pull_in_flight_ is acquire.
    if (pull_in_flight_.load(std::memory_order_acquire)) {
      return;  // pull-task's re-arm (DoWork call) will drain the newly arrived bytes
    }

    DoWork();
  }

  void OnReadAsio(const boost::system::error_code &ec, const size_t bytes_transferred) {
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
                // d1b: HandlePullDiscard may have stashed a coro pull and returned early.
                // Check BEFORE the priority-reschedule branch so we never arm a second task or
                // DoRead while a pull-task is being dispatched.
                if (shared_this->session_.PullTaskDispatched()) {
                  auto pend = shared_this->session_.TakePendingPull();

                  // Serialization assert: pull_in_flight_ must be false here (one task at a time).
                  // We do the CAS 0→1; if it fails the invariant is broken — terminate loudly.
                  bool expected = false;
                  const bool swapped = shared_this->pull_in_flight_.compare_exchange_strong(
                      expected, true, std::memory_order_release, std::memory_order_relaxed);
                  MG_ASSERT(swapped, "BUG: pull_in_flight_ was already set — two concurrent pull-tasks!");

                  // Dispatch the resumable pull-task.  It captures shared_this (session lifetime)
                  // and the stashed pend.  The task runs on a LOW pool worker (pinned by the
                  // ScheduleResumableTask wrapper on park/yield).
                  shared_this->session_context_->ScheduleResumableTask(
                      [shared_this, pend]() mutable -> bool {
                        // Pull-task body.  Re-enter via SessionHL::Pull on EVERY invocation
                        // (fresh + park-resume) so DbArenaScope + StartTrackingCurrentThread are
                        // re-established each call.
                        // Use std::optional to hold the summary without a default constructor
                        // dependency on the concrete bolt map type (template-safe).
                        bool parked = false;
                        // c3.0: event_parked distinguishes event-kind park (do NOT self-reschedule;
                        // NotifyProgress will re-enqueue) from yield-kind park (self-reschedule).
                        bool event_parked = false;
                        std::optional<decltype(shared_this->session_.Pull(pend.n, pend.qid))> maybe_summary;
                        try {
                          if (pend.is_pull) {
                            maybe_summary = shared_this->session_.Pull(pend.n, pend.qid, &parked, &event_parked);
                          } else {
                            // DISCARD is never coro-driven, but wire for symmetry.
                            maybe_summary = shared_this->session_.Discard(pend.n, pend.qid);
                          }
                        } catch (const std::exception & /* unused */) {
                          // Exception from the pull. Clear pull_in_flight_ + handle the exception ON THE
                          // STRAND, so they serialize with OnRead: while pull_in_flight_ is still true (until
                          // the strand lambda runs) OnRead early-returns, so no second task can start in the
                          // half-thrown Bolt state. (This is why the bolt state_ left at Result by HandlePull
                          // is harmless — HandleException → DoShutdown runs strand-serialized, after which no
                          // Execute() runs.)
                          boost::asio::post(shared_this->strand_, [shared_this, eptr = std::current_exception()]() {
                            shared_this->pull_in_flight_.store(false, std::memory_order_release);
                            shared_this->HandleException(eptr);
                          });
                          return false;  // task done (error path)
                        }

                        if (parked) {
                          if (event_parked) {
                            // c3.0 EVENT-KIND park: ProgressAwaitable suspended on an external
                            // progress event. The continuation is already registered with
                            // CollectionScheduler/NotifyProgress which will re-enqueue this task
                            // (pinned) when the last branch finishes.  We must NOT self-reschedule
                            // (return true) — that would race with NotifyProgress and potentially
                            // double-enqueue.
                            //
                            // Returning false signals the ResumableWrapper that the task is done for
                            // this invocation.  The wrapper's WasParked() check (set by
                            // RegisterProgressWaiter → SetParked) suppresses the normal "finished,
                            // re-arm decode loop" path.  NotifyProgress calls RescheduleTaskOnWorker
                            // to re-enqueue pinned on the same worker; that next invocation hits
                            // this body again (fresh entry, parked/event_parked cleared to false).
                            return false;  // no self-reschedule; NotifyProgress re-enqueues
                          }
                          // d1: parks are DORMANT — nothing sets yield_requested, so this branch
                          // never fires.  Wire it anyway for d2 (park return → wrapper reschedules
                          // pinned on same worker; continuation re-enters this body on resume).
                          return true;  // yield-kind: resumable wrapper re-queues pinned
                        }

                        // Pull completed (BatchContinues or Finished).  Send the summary and
                        // update the Bolt state machine.  FinishPull sets session_.state_ directly
                        // (avoids naming bolt::State in the generic template) and returns false on
                        // send failure.
                        const bool send_ok = shared_this->session_.FinishPull(std::move(*maybe_summary), pend.is_pull);

                        if (!send_ok) [[unlikely]] {
                          // FinishPull couldn't send — shut down (clear in-flight + shut down ON THE STRAND,
                          // serialized with OnRead — see the re-arm note below).
                          boost::asio::post(shared_this->strand_, [shared_this]() {
                            shared_this->pull_in_flight_.store(false, std::memory_order_release);
                            shared_this->DoShutdown();
                          });
                          return false;
                        }

                        // Re-arm ON THE STRAND. Clearing pull_in_flight_ and posting the next DoWork both
                        // run on strand_, which is the SAME executor as OnRead — so they are serialized and
                        // a concurrent OnRead can NEVER observe pull_in_flight_==false in a clear→DoWork
                        // window and enqueue a duplicate pool task. (Calling DoWork() directly off the pull
                        // worker was safe in d1 only by the fragile invariant "no async read is armed during
                        // the pull-task"; routing through the strand makes it unconditionally race-free for
                        // both the d1 no-park case and the d2 park-resume case. DoWork() internally drains
                        // buffered messages or arms DoRead — we never call DoRead directly here.)
                        boost::asio::post(shared_this->strand_, [shared_this]() {
                          shared_this->pull_in_flight_.store(false, std::memory_order_release);
                          shared_this->DoWork();
                        });
                        return false;  // task done
                      },
                      utils::Priority::LOW);

                  return;  // decode task done; pull-task owns continuation
                }

                // Normal priority-reschedule path (no pull-task dispatched).
                if (thread_priority > shared_this->session_.ApproximateQueryPriority()) {
                  // d1b safety: never reschedule while a pull-task is in flight.
                  // pull_in_flight_ was cleared above (or never set for non-coro), so this
                  // check is purely defensive.
                  if (!shared_this->pull_in_flight_.load(std::memory_order_acquire)) {
                    // Task priority lower; reschedule
                    shared_this->DoWork();
                    return;
                  }
                  // pull_in_flight_ is set — should not reach here given the dispatch branch above,
                  // but be safe: just return and let the pull-task re-arm.
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

  void DoShutdown() {
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
      socket->async_handshake(
          boost::asio::ssl::stream_base::server,
          boost::asio::bind_executor(strand_, std::bind_front(&Session::OnSSLHandshake, shared_from_this())));
    }
  }

  void OnSSLHandshake(const boost::system::error_code &ec) {
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
  std::atomic_bool execution_active_{false};

  // d1b: serialization guard for the coro pull-task dispatch.
  //
  // Invariant: at most ONE pool task touches this Session at any instant.
  //   - Set (release) by DoWork before dispatching the pull-task.
  //   - Cleared (release) by the pull-task's re-arm, sequenced BEFORE DoWork() re-arm.
  //   - Checked (acquire) by OnRead: if set, suppress the DoWork dispatch.
  //   - Checked (acquire) by the priority-reschedule branch in DoWork: never reschedule
  //     while a pull-task is live.
  // A CAS 0→1 on dispatch acts as a release-build assert (MG_ASSERT on failure).
  // alignas(64) prevents false sharing with execution_active_ on the same cache line.
  alignas(64) std::atomic<bool> pull_in_flight_{false};
};
}  // namespace memgraph::communication::v2
