#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <iomanip>
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "io/network/network_error.hpp"
#include "io/network/socket.hpp"
#include "io/network/socket_event_dispatcher.hpp"
#include "io/network/stream_buffer.hpp"
#include "threading/sync/spinlock.hpp"

namespace communication {

/**
 * TODO (mferencevic): document methods
 */

/**
 * Communication worker.
 * Listens for incomming data on connections and accepts new connections.
 * Also, executes sessions on incomming data.
 *
 * @tparam TSession the worker can handle different Sessions, each session
 *         represents a different protocol so the same network infrastructure
 *         can be used for handling different protocols
 * @tparam TSessionData the class with objects that will be forwarded to the
 *         session
 */
template <typename TSession, typename TSessionData>
class Worker {
  using Socket = io::network::Socket;

 public:
  void AddConnection(Socket &&connection) {
    std::unique_lock<SpinLock> guard(lock_);
    // Remember fd before moving connection into SessionListener.
    int fd = connection.fd();
    session_listeners_.push_back(
        std::make_unique<SessionSocketListener>(std::move(connection), *this));
    // We want to listen to an incoming event which is edge triggered and
    // we also want to listen on the hangup event.
    dispatcher_.AddListener(fd, *session_listeners_.back(),
                            EPOLLIN | EPOLLRDHUP);
  }

  explicit Worker(TSessionData &session_data) : session_data_(session_data) {}

  void Start(std::atomic<bool> &alive) {
    while (alive) {
      dispatcher_.WaitAndProcessEvents();

      bool check_sessions_for_timeouts = true;
      while (check_sessions_for_timeouts) {
        check_sessions_for_timeouts = false;

        std::unique_lock<SpinLock> guard(lock_);
        for (auto &session_listener : session_listeners_) {
          if (session_listener->session().TimedOut()) {
            // We need to unlock here, because OnSessionAndTxTimeout will need
            // to acquire same lock.
            guard.unlock();
            session_listener->OnSessionAndTxTimeout();
            // Since we released lock we can't continue iteration so we need to
            // break. There could still be more sessions that timed out so we
            // set check_sessions_for_timeout back to true.
            check_sessions_for_timeouts = true;
            break;
          }
        }
      }
    }
  }

 private:
  // TODO: Think about ownership. Who should own socket session,
  // SessionSocketListener or Worker?
  class SessionSocketListener {
   public:
    SessionSocketListener(Socket &&socket,
                          Worker<TSession, TSessionData> &worker)
        : session_(std::move(socket), worker.session_data_), worker_(worker) {}

    auto &session() { return session_; }
    const auto &session() const { return session_; }
    const auto &TimedOut() const { return session_.TimedOut(); }

    void OnData() {
      session_.RefreshLastEventTime(std::chrono::steady_clock::now());
      DLOG(INFO) << "On data";
      // allocate the buffer to fill the data
      auto buf = session_.Allocate();
      // read from the buffer at most buf.len bytes
      int len = session_.socket().Read(buf.data, buf.len);

      // check for read errors
      if (len == -1) {
        // This means read would block or read was interrupted by signal.
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
          return;
        }
        // Some other error occurred, check errno.
        OnError();
        return;
      }
      // The client has closed the connection.
      if (len == 0) {
        DLOG(WARNING) << "Calling OnClose because the socket is closed!";
        OnClose();
        return;
      }

      // Notify the stream that it has new data.
      session_.Written(len);
      DLOG(INFO) << "OnRead";
      try {
        session_.Execute();
      } catch (const std::exception &e) {
        LOG(ERROR) << "Error occured while executing statement with message: "
                   << e.what();
        OnError();
      }
      session_.RefreshLastEventTime(std::chrono::steady_clock::now());
    }

    // TODO: Remove duplication in next three functions.
    void OnError() {
      LOG(ERROR) << fmt::format(
          "Error occured in session associated with {}:{}",
          session_.socket().endpoint().address(),
          session_.socket().endpoint().port());
      CloseSession();
    }

    void OnException(const std::exception &e) {
      LOG(ERROR) << fmt::format(
          "Exception was thrown while processing event in session associated "
          "with {}:{} with message: {}",
          session_.socket().endpoint().address(),
          session_.socket().endpoint().port(), e.what());
      CloseSession();
    }

    void OnSessionAndTxTimeout() {
      LOG(WARNING) << fmt::format(
          "Session or transaction associated with {}:{} timed out.",
          session_.socket().endpoint().address(),
          session_.socket().endpoint().port());
      // TODO: report to client what happend.
      CloseSession();
    }

    void OnClose() {
      LOG(INFO) << fmt::format("Client {}:{} closed the connection.",
                               session_.socket().endpoint().address(),
                               session_.socket().endpoint().port());
      CloseSession();
    }

   private:
    void CloseSession() {
      session_.Close();

      std::unique_lock<SpinLock> guard(worker_.lock_);
      auto it = std::find_if(
          worker_.session_listeners_.begin(), worker_.session_listeners_.end(),
          [&](const auto &l) { return l->session_.Id() == session_.Id(); });

      CHECK(it != worker_.session_listeners_.end())
          << "Trying to remove session that is not found in worker's sessions";
      int i = it - worker_.session_listeners_.begin();
      swap(worker_.session_listeners_[i], worker_.session_listeners_.back());
      worker_.session_listeners_.pop_back();
    }

    TSession session_;
    Worker &worker_;
  };

  SpinLock lock_;
  TSessionData &session_data_;
  io::network::SocketEventDispatcher<SessionSocketListener> dispatcher_;
  std::vector<std::unique_ptr<SessionSocketListener>> session_listeners_;
};
}  // namespace communication
