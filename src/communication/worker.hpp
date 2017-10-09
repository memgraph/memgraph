#pragma once

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <iomanip>
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>

#include <glog/logging.h>

#include "io/network/network_error.hpp"
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
 * @tparam Session the worker can handle different Sessions, each session
 *         represents a different protocol so the same network infrastructure
 *         can be used for handling different protocols
 * @tparam Socket the input/output socket that should be used
 * @tparam SessionData the class with objects that will be forwarded to the
 *         session
 */
template <typename Session, typename Socket, typename SessionData>
class Worker {
  using StreamBuffer = io::network::StreamBuffer;

 public:
  void AddConnection(Socket &&connection) {
    std::unique_lock<SpinLock> gurad(lock_);
    // Remember fd before moving connection into SessionListener.
    int fd = connection.fd();
    session_listeners_.push_back(
        std::make_unique<SessionSocketListener>(std::move(connection), *this));
    // We want to listen to an incoming event which is edge triggered and
    // we also want to listen on the hangup event.
    dispatcher_.AddListener(fd, *session_listeners_.back(),
                            EPOLLIN | EPOLLRDHUP);
  }

  Worker(SessionData &session_data) : session_data_(session_data) {}

  void Start(std::atomic<bool> &alive) {
    while (alive) {
      dispatcher_.WaitAndProcessEvents();
    }
  }

 private:
  // TODO: Think about ownership. Who should own socket session,
  // SessionSocketListener or Worker?
  class SessionSocketListener : public io::network::BaseListener {
   public:
    SessionSocketListener(Socket &&socket,
                          Worker<Session, Socket, SessionData> &worker)
        : BaseListener(session_.socket_),
          session_(std::move(socket), worker.session_data_),
          worker_(worker) {}

    void OnError() {
      LOG(ERROR) << "Error occured in this session";
      OnClose();
    }

    void OnData() {
      DLOG(INFO) << "On data";

      if (UNLIKELY(!session_.Alive())) {
        DLOG(WARNING) << "Calling OnClose because the stream isn't alive!";
        OnClose();
        return;
      }

      // allocate the buffer to fill the data
      auto buf = session_.Allocate();

      // read from the buffer at most buf.len bytes
      int len = session_.socket_.Read(buf.data, buf.len);

      // check for read errors
      if (len == -1) {
        // this means we have read all available data
        if (LIKELY(errno == EAGAIN || errno == EWOULDBLOCK)) {
          return;
        }

        // some other error occurred, check errno
        OnError();
        return;
      }

      // end of file, the client has closed the connection
      if (UNLIKELY(len == 0)) {
        DLOG(WARNING) << "Calling OnClose because the socket is closed!";
        OnClose();
        return;
      }

      // notify the stream that it has new data
      session_.Written(len);

      DLOG(INFO) << "OnRead";

      try {
        session_.Execute();
      } catch (const std::exception &e) {
        LOG(ERROR) << "Error occured while executing statement. " << std::endl
                   << e.what();
        // TODO: report to client
      }
      // TODO: Should we even continue with this session if error occurs while
      // reading.
    }

    void OnClose() {
      LOG(INFO) << fmt::format("Client {}:{} closed the connection.",
                               session_.socket_.endpoint().address(),
                               session_.socket_.endpoint().port())
                << std::endl;
      session_.Close();
      std::unique_lock<SpinLock> gurad(worker_.lock_);
      auto it = std::find_if(
          worker_.session_listeners_.begin(), worker_.session_listeners_.end(),
          [&](const auto &l) { return l->session_.Id() == session_.Id(); });
      CHECK(it != worker_.session_listeners_.end())
          << "Trying to remove session that is not found in worker's sessions";
      int i = it - worker_.session_listeners_.begin();
      swap(worker_.session_listeners_[i], worker_.session_listeners_.back());
      worker_.session_listeners_.pop_back();
    }

   private:
    Session session_;
    Worker &worker_;
  };

  SpinLock lock_;
  SessionData &session_data_;
  io::network::SocketEventDispatcher<SessionSocketListener> dispatcher_;
  std::vector<std::unique_ptr<SessionSocketListener>> session_listeners_;
};
}
