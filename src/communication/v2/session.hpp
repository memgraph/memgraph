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

#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/states/handlers.hpp"
#include "communication/buffer.hpp"
#include "communication/context.hpp"
#include "communication/exceptions.hpp"
#include "communication/fmt.hpp"
#include "utils/coro_task.hpp"
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
      while (true) {
        const auto outcome = session_.Execute();
        if (outcome == bolt::ExecuteResult::kNeedsCoroPrepare) [[unlikely]] {
          // Unreachable: this outcome requires GetCurrentWorkerId() to be set (Execute_'s own gate),
          // which only a pool LP worker publishes, and the ASIO scheduler runs on the connection's
          // strand. A hard failure rather than a silent loop, because state_ would otherwise never
          // leave State::Parsed.
          //
          // Deliberately NOT "drain the chain here" via SyncWait(HandlePrepareCoro(...)), which an
          // earlier version did on the strength of AcquireAccessorCoro's DMG_ASSERT firing first. That
          // assert is compiled out under NDEBUG, so a release build could genuinely park; and SyncWait
          // binds the Task as a TEMPORARY destroyed at end of full-expression, while Task<void>::Run()
          // deliberately tolerates a park and returns with frames suspended -- so the destructor would
          // handle_.destroy() a frame still registered and unclaimed in both registries, and the next
          // wake would resume freed memory. Converting an impossible state into a use-after-free is
          // worse than the hang it was written to avoid.
          throw utils::BasicException(
              "Execute_ returned kNeedsCoroPrepare on the ASIO scheduler path, which requires a pool "
              "worker id that this thread cannot have. This is a bug in the scheduler wiring.");
        }
        if (outcome == bolt::ExecuteResult::kNoMoreData) break;
        // kMoreData: keep draining.
      }
      // Handled all data,  async wait for new incoming data
      DoReadAsio();
    } catch (const std::exception & /* unused */) {
      HandleException(std::current_exception());
    }
  }

  // Drives a single connection iteration exactly like DoWork's original inline loop did: repeatedly
  // calls session_.Execute() until it signals kNoMoreData (arm a fresh read) or kNeedsCoroPrepare
  // (drive the coroutine Prepare chain via DrivePreparedRun, parking this connection's slot if it
  // suspends). Shared by DoWork's initial dispatch and DrivePreparedRun's post-park continuation
  // (re-entry after a resume) so both a fresh dispatch and a resumed-after-park connection are
  // driven by IDENTICAL logic -- this is the "single in-flight slot" invariant (B3): whichever path
  // is currently driving RunLoop is the only one allowed to touch this connection's Execute()/state_
  // until it returns.
  void RunLoop(std::shared_ptr<Session> shared_this, utils::Priority thread_priority) {
    try {
      while (true) {
        const auto outcome = shared_this->session_.Execute();
        if (outcome == bolt::ExecuteResult::kNeedsCoroPrepare) {
          // shared_this is forwarded down as an opaque std::function, and that closure is what keeps the
          // session alive across the park.
          //
          // NB there IS a refcount cycle while parked: session -> parked_prepare_ -> frames ->
          // std::function -> shared_ptr<Session>. Type erasure hides it from a reader, not from the
          // refcount -- PrepareCoro takes the std::function by value and copies it again into
          // AcquireAccessorCoro, so two live frames reference the session that transitively owns them.
          // It is broken by hand, by parked_prepare_.reset() on every resume path, which is exactly why
          // any path that drops a resume leaks the Session and its DatabaseAccess and stalls shutdown.
          // Do not "simplify" the reset away.
          //
          // parked_prepare_ MUST hold the Task before Resume() is called, and more generally NOTHING
          // below may race a resume -- the `if (!Done())` check and this member are touched by both this
          // thread and the resume closure. The delivery gate is what guarantees it: a claim taken while
          // we are still inside Resume() cannot be DELIVERED until this pool task ends, strictly after
          // this function returns. The resume then runs on whatever worker services the post, so the
          // exclusion is the gate's, not the scheduler's.
          shared_this->parked_prepare_.emplace(shared_this->DrivePreparedRun([shared_this] {
            auto &parked = shared_this->parked_prepare_;
            if (!parked || !parked->Done()) {
              // Still genuinely parked (this resume's re-probe re-parked on a fresh ParkState) --
              // nothing to do yet; a later resume will find Done() == true and take the branch below.
              return;
            }
            // Break the cycle FIRST, before anything that can throw, and only then look at the result.
            // Moving the Task out and resetting immediately is what makes the reset unskippable -- the
            // same shape the synchronous-completion branch below uses.
            //
            // Safe here, unlike inside DrivePreparedRun's own body: this closure runs OUTSIDE any
            // coroutine frame's execution, strictly after h.resume() returned control to the posted
            // resume closure, so the frame is merely sitting at its final_suspend.
            auto finished = std::move(*parked);
            parked.reset();

            // catch(...) as well as catch(std::exception), and the catch(...) is the load-bearing one.
            // `Promise::unhandled_exception` stores whatever was thrown and `TakeValue()` rethrows it
            // verbatim, so a non-std-derived exception is representable here. With only the narrow
            // handler it did not match and escaped this closure, skipping the reset above -- and that
            // reset is the ONLY thing breaking the cycle documented above, so the Session survived with
            // its DatabaseAccess and Gatekeeper<Database>::count_ never returned to zero. It also
            // escaped into the posted resume task and out through the worker's Phase 1A task body (no
            // handler there, by design -- master's task bodies do not throw), terminating the process.
            try {
              finished.TakeValue();  // rethrow, if DrivePreparedRun itself somehow escaped an exception
            } catch (const std::exception &e) {
              spdlog::error("Parked Prepare coroutine finished with an unexpected exception: {}", e.what());
            } catch (...) {
              spdlog::error(
                  "Parked Prepare coroutine finished with a non-standard exception. The query is abandoned; the "
                  "session is released normally.");
            }
            // Re-drive exactly like a fresh DoWork iteration. Priority::LOW is what a resumed pool
            // task runs at, and PostResumeTask's closure signature discards the resuming worker's own
            // ambient priority anyway, so there is nothing more accurate available to pass.
            //
            // Usually -- not always -- an LP worker. PostResumeTask only ever posts to those, but its
            // all-workers-stopped fallback runs the closure inline on whichever thread claimed the
            // park, which during teardown can be the main shutdown thread or a thread releasing
            // main_lock_. LOW remains the right value there too; it is the "always" that was wrong.
            shared_this->RunLoop(shared_this, utils::Priority::LOW);
          }));
          shared_this->parked_prepare_->Resume();
          if (!shared_this->parked_prepare_->Done()) {
            // PARKED (B3/R3.3): retain this connection's single in-flight slot. Do NOT arm DoRead,
            // do NOT dispatch/re-post -- the ONLY thing that re-drives this connection is the posted
            // resume (PostResumeTask), which re-enters via this same coroutine chain and, once it
            // fully completes, calls back into RunLoop itself (see DrivePreparedRun). That resume
            // cannot start before this function returns -- see the gate note at the emplace above.
            return;
          }
          // Completed synchronously: Resume()'s single call above ran the WHOLE chain to completion
          // without ever suspending -- i.e. no ParkState was ever created or exposed to another
          // thread for this attempt (had it suspended even once, the `if (!Done())` check above
          // would already have returned). Nothing else can be concurrently touching parked_prepare_
          // here; this branch is unreachable from any racing resume.
          auto finished = std::move(*shared_this->parked_prepare_);
          shared_this->parked_prepare_.reset();
          finished.TakeValue();  // rethrow, if DrivePreparedRun itself somehow escaped an exception
          continue;              // state_ advanced (Result/Error/Close); keep draining decoder_buffer_
        }
        if (outcome == bolt::ExecuteResult::kMoreData) {
          // Check if we can just steal this task (loop through)
          if (thread_priority > shared_this->session_.ApproximateQueryPriority()) {
            // Task priority lower; reschedule
            shared_this->DoWork();
            return;
          }
          continue;
        }
        // kNoMoreData: handled all data, async wait for new incoming data
        shared_this->DoRead();
        return;
      }
    } catch (const std::exception & /* unused */) {
      boost::asio::post(shared_this->strand_,
                        [shared_this, eptr = std::current_exception()]() { shared_this->HandleException(eptr); });
    }
  }

  // Top-level drive coroutine. Its body is deliberately JUST the co_await -- no post-await bookkeeping
  // may live here, because this frame is owned by `parked_prepare_`: touching, let alone resetting, that
  // member from within this frame's own execution would destroy the coroutine currently running. The
  // "what happens once the chain is done" logic lives in the caller-supplied `on_park_resumed` closure
  // instead, which runs from the posted resume strictly AFTER the parked handle is resumed, i.e. outside
  // this frame entirely.
  //
  // Takes `on_park_resumed` as an opaque std::function rather than a named shared_ptr<Session> purely
  // for uniformity with every other frame in the chain -- NOT as cycle avoidance. The closure holds a
  // shared_ptr<Session> whatever static type wraps it, so the session -> parked_prepare_ -> frame ->
  // session cycle is real while parked; see the note at RunLoop's kNeedsCoroPrepare site.
  utils::Task<void> DrivePreparedRun(std::function<void()> on_park_resumed) {
    co_await bolt::HandlePrepareCoro(session_, std::move(on_park_resumed));
  }

  void DoWork() {
    session_context_->AddTask([shared_this = shared_from_this()](
                                  const auto thread_priority) { shared_this->RunLoop(shared_this, thread_priority); },
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

  // Session-surgery Stage B: owns the parked top-level Prepare coroutine, if any. Only ever engaged
  // when flags::run_time::CoroPrepareAccessorYieldEnabled() AND this connection is being driven via
  // DoWork (PRIORITY_QUEUE_WITH_SIDECAR scheduler, i.e. on a PriorityThreadPool worker) -- see the
  // ExecuteResult::kNeedsCoroPrepare doc comment. While engaged, this session's single in-flight-task
  // slot is retained by the parked frame: no DoRead, no further dispatch, until the posted resume
  // drives it back through RunLoop.
  //
  // DECLARED LAST, deliberately, and it must stay last. Members are destroyed in reverse declaration
  // order, and ~Task destroys a still-suspended coroutine frame -- whose locals include a
  // PendingHandle registered on the storage's main_lock_, a ScopedSessionLog, and references into
  // session_. Declared before session_/socket_ (where it originally sat), those would already be gone
  // by the time the frame unwound. Unreachable today, because a suspended frame holds a
  // shared_ptr<Session> and so ~Session cannot run while this is engaged -- but the entire point of
  // this design is that that cycle gets broken by hand, so do not rely on the cycle for ordering
  // safety when declaration order gives it for free.
  std::optional<utils::Task<void>> parked_prepare_;
};
}  // namespace memgraph::communication::v2
