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
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>

#include <gflags/gflags.h>

#include "communication/session.hpp"
#include "io/network/epoll.hpp"
#include "io/network/socket.hpp"
#include "utils/logging.hpp"
#include "utils/signals.hpp"
#include "utils/spin_lock.hpp"
#include "utils/thread.hpp"

namespace memgraph::communication {

/**
 * This class listens to events on an epoll object and processes them.
 * When a new connection is added a `TSession` object is created to handle the
 * connection. When the `TSession` handler raises an exception or an error
 * occurs the `TSession` object is deleted and the corresponding socket is
 * closed. Also, this class has a background thread that periodically, every
 * second, checks all sessions for expiration and shuts them down if they have
 * expired.
 */
template <class TSession, class TSessionData>
class Listener final {
 private:
  // The maximum number of events handled per execution thread is 1. This is
  // because each event represents the start of a network request and it doesn't
  // make sense to take more than one event because the processing of an event
  // can take a long time.
  static const int kMaxEvents = 1;

  using SessionHandler = Session<TSession, TSessionData>;

 public:
  Listener(TSessionData *data, ServerContext *context, int inactivity_timeout_sec, const std::string &service_name,
           size_t workers_count)
      : data_(data),
        alive_(false),
        context_(context),
        inactivity_timeout_sec_(inactivity_timeout_sec),
        service_name_(service_name),
        workers_count_(workers_count) {}

  ~Listener() {
    bool worker_alive = false;
    for (auto &thread : worker_threads_) {
      if (thread.joinable()) worker_alive = true;
    }
    MG_ASSERT(!alive_ && !worker_alive && !timeout_thread_.joinable(),
              "You should call Shutdown and AwaitShutdown on "
              "communication::Listener!");
  }

  Listener(const Listener &) = delete;
  Listener(Listener &&) = delete;
  Listener &operator=(const Listener &) = delete;
  Listener &operator=(Listener &&) = delete;

  /**
   * This function adds a socket to the listening event pool.
   *
   * @param connection socket which should be added to the event pool
   */
  void AddConnection(io::network::Socket &&connection) {
    std::lock_guard<utils::SpinLock> guard(lock_);

    // Remember fd before moving connection into Session.
    int fd = connection.fd();

    // Create a new Session for the connection.
    sessions_.push_back(
        std::make_unique<SessionHandler>(std::move(connection), data_, context_, inactivity_timeout_sec_));

    // Register the connection in Epoll.
    // We want to listen to an incoming event which is edge triggered and
    // we also want to listen on the hangup event. Epoll is hard to use
    // concurrently and that is why we use `EPOLLONESHOT`, for a detailed
    // description what are the problems and why this is correct see:
    // https://idea.popcount.org/2017-02-20-epoll-is-fundamentally-broken-12/
    epoll_.Add(fd, EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLONESHOT, sessions_.back().get());
  }

  /**
   * This function starts the listener
   */
  void Start() {
    MG_ASSERT(!alive_, "The listener is already started!");
    alive_.store(true);

    spdlog::info("Starting {} {} workers", workers_count_, service_name_);

    std::string service_name(service_name_);
    for (size_t i = 0; i < workers_count_; ++i) {
      worker_threads_.emplace_back([this, service_name, i]() {
        utils::ThreadSetName(fmt::format("{} worker {}", service_name, i + 1));
        while (alive_) {
          WaitAndProcessEvents();
        }
      });
    }

    if (inactivity_timeout_sec_ > 0) {
      timeout_thread_ = std::thread([this, service_name]() {
        utils::ThreadSetName(fmt::format("{} timeout", service_name));
        while (alive_) {
          {
            std::lock_guard<utils::SpinLock> guard(lock_);
            for (auto &session : sessions_) {
              if (session->TimedOut()) {
                spdlog::warn("{} session associated with {} timed out", service_name, session->socket().endpoint());
                // Here we shutdown the socket to terminate any leftover
                // blocking `Write` calls and to signal an event that the
                // session is closed. Session cleanup will be done in the event
                // process function.
                session->socket().Shutdown();
              }
            }
          }
          // TODO (mferencevic): Should this be configurable?
          std::this_thread::sleep_for(std::chrono::seconds(1));
        }
      });
    }
  }

  /**
   * This function starts a graceful shutdown of the listener.
   */
  void Shutdown() { alive_.store(false); }

  /**
   * This function blocks the calling thread until the listener shutdown is
   * complete.
   */
  void AwaitShutdown() {
    if (timeout_thread_.joinable()) timeout_thread_.join();
    for (auto &worker_thread : worker_threads_) {
      if (worker_thread.joinable()) worker_thread.join();
    }
    // Here we free all active connections to close them and notify the other
    // end that we won't process them because we stopped all worker threads.
    std::lock_guard<utils::SpinLock> guard(lock_);
    sessions_.clear();
  }

 private:
  /**
   * This function polls the event queue and processes incoming data.
   * It is thread safe and is intended to be called from multiple threads and
   * doesn't block the calling threads.
   */
  void WaitAndProcessEvents() {
    // This array can't be global because this function can be called from
    // multiple threads, therefore, it must be on the stack.
    io::network::Epoll::Event events[kMaxEvents];

    // Waits for an events and returns a maximum of max_events (1)
    // and stores them in the events array. It waits for wait_timeout
    // milliseconds. If wait_timeout is achieved, returns 0.
    int n = epoll_.Wait(events, kMaxEvents, 200);
    if (n <= 0) return;

    // Process the event.
    auto &event = events[0];

    // We get the currently associated Session pointer and immediately
    // dereference it here. It is safe to dereference the pointer because
    // this design guarantees that there will never be an event that has
    // a stale Session pointer.
    SessionHandler &session = *reinterpret_cast<SessionHandler *>(event.data.ptr);

    // Process epoll events. We use epoll in edge-triggered mode so we process
    // all events here. Only one of the `if` statements must be executed
    // because each of them can call `CloseSession` which destroys the session
    // and calling a function on that session after that would cause a
    // segfault.
    if (event.events & EPOLLIN) {
      // Read and process all incoming data.
      while (ExecuteSession(session))
        ;
    } else if (event.events & EPOLLRDHUP) {
      // The client closed the connection.
      spdlog::info("{} client {} closed the connection.", service_name_, session.socket().endpoint());
      CloseSession(session);
    } else if (!(event.events & EPOLLIN) || event.events & (EPOLLHUP | EPOLLERR)) {
      // There was an error on the server side.
      spdlog::error("Error occurred in {} session associated with {}", service_name_, session.socket().endpoint());
      CloseSession(session);
    } else {
      // Unhandled epoll event.
      spdlog::error("Unhandled event occurred in {} session associated with {} events: {}", service_name_,
                    session.socket().endpoint(), event.events);
      CloseSession(session);
    }
  }

  bool ExecuteSession(SessionHandler &session) {
    try {
      if (session.Execute()) {
        // Session execution done, rearm epoll to send events for this
        // socket.
        epoll_.Modify(session.socket().fd(), EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLONESHOT, &session);
        return false;
      }
    } catch (const SessionClosedException &e) {
      spdlog::info("{} client {} closed the connection.", service_name_, session.socket().endpoint());
      CloseSession(session);
      return false;
    } catch (const std::exception &e) {
      // Catch all exceptions.
      spdlog::error(
          "Exception was thrown while processing event in {} session "
          "associated with {}",
          service_name_, session.socket().endpoint());
      spdlog::debug("Exception message: {}", e.what());
      CloseSession(session);
      return false;
    }
    return true;
  }

  void CloseSession(SessionHandler &session) {
    // Deregister the Session's socket from epoll to disable further events. For
    // a detailed description why this is necessary before destroying (closing)
    // the socket, see:
    // https://idea.popcount.org/2017-03-20-epoll-is-fundamentally-broken-22/
    epoll_.Delete(session.socket().fd());

    std::lock_guard<utils::SpinLock> guard(lock_);
    auto it = std::find_if(sessions_.begin(), sessions_.end(), [&](const auto &l) { return l.get() == &session; });

    MG_ASSERT(it != sessions_.end(), "Trying to remove session that is not found in sessions!");
    int i = it - sessions_.begin();
    swap(sessions_[i], sessions_.back());

    // This will call all destructors on the Session. Consequently, it will call
    // the destructor on the Socket and close the socket.
    sessions_.pop_back();
  }

  io::network::Epoll epoll_;

  TSessionData *data_;

  utils::SpinLock lock_;
  std::vector<std::unique_ptr<SessionHandler>> sessions_;

  std::thread timeout_thread_;
  std::vector<std::thread> worker_threads_;
  std::atomic<bool> alive_;

  ServerContext *context_;
  const int inactivity_timeout_sec_;
  const std::string service_name_;
  const size_t workers_count_;
};
}  // namespace memgraph::communication
