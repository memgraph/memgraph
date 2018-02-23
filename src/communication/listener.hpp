#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "io/network/epoll.hpp"
#include "io/network/socket.hpp"
#include "threading/sync/spinlock.hpp"
#include "utils/thread.hpp"

namespace communication {

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
class Listener {
 private:
  // The maximum number of events handled per execution thread is 1. This is
  // because each event represents the start of a network request and it doesn't
  // make sense to take more than one event because the processing of an event
  // can take a long time.
  static const int kMaxEvents = 1;

 public:
  Listener(TSessionData &data, bool check_for_timeouts,
           const std::string &service_name)
      : data_(data), alive_(true) {
    if (check_for_timeouts) {
      thread_ = std::thread([this, service_name]() {
        utils::ThreadSetName(fmt::format("{} timeout", service_name));
        while (alive_) {
          {
            std::unique_lock<SpinLock> guard(lock_);
            for (auto &session : sessions_) {
              if (session->TimedOut()) {
                LOG(WARNING) << "Session associated with "
                             << session->socket().endpoint() << " timed out.";
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

  ~Listener() {
    alive_.store(false);
    if (thread_.joinable()) thread_.join();
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
    std::unique_lock<SpinLock> guard(lock_);

    // Set connection options.
    // The socket is left to be a blocking socket, but when `Read` is called
    // then a flag is manually set to enable non-blocking read that is used in
    // conjunction with `EPOLLET`. That means that the socket is used in a
    // non-blocking fashion for reads and a blocking fashion for writes.
    connection.SetKeepAlive();
    connection.SetNoDelay();

    // Remember fd before moving connection into Session.
    int fd = connection.fd();

    // Create a new Session for the connection.
    sessions_.push_back(
        std::make_unique<TSession>(std::move(connection), data_));

    // Register the connection in Epoll.
    // We want to listen to an incoming event which is edge triggered and
    // we also want to listen on the hangup event. Epoll is hard to use
    // concurrently and that is why we use `EPOLLONESHOT`, for a detailed
    // description what are the problems and why this is correct see:
    // https://idea.popcount.org/2017-02-20-epoll-is-fundamentally-broken-12/
    epoll_.Add(fd, EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLONESHOT,
               sessions_.back().get());
  }

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
    TSession &session = *reinterpret_cast<TSession *>(event.data.ptr);

    // Process epoll events. We use epoll in edge-triggered mode so we process
    // all events here. Only one of the `if` statements must be executed
    // because each of them can call `CloseSession` which destroys the session
    // and calling a function on that session after that would cause a
    // segfault.
    if (event.events & EPOLLIN) {
      // Read and process all incoming data.
      while (ReadAndProcessSession(session))
        ;
    } else if (event.events & EPOLLRDHUP) {
      // The client closed the connection.
      LOG(INFO) << "Client " << session.socket().endpoint()
                << " closed the connection.";
      CloseSession(session);
    } else if (!(event.events & EPOLLIN) ||
               event.events & (EPOLLHUP | EPOLLERR)) {
      // There was an error on the server side.
      LOG(ERROR) << "Error occured in session associated with "
                 << session.socket().endpoint();
      CloseSession(session);
    } else {
      // Unhandled epoll event.
      LOG(ERROR) << "Unhandled event occured in session associated with "
                 << session.socket().endpoint() << " events: " << event.events;
      CloseSession(session);
    }
  }

 private:
  bool ReadAndProcessSession(TSession &session) {
    // Refresh the last event time in the session.
    // This function must be implemented thread safe.
    session.RefreshLastEventTime(std::chrono::steady_clock::now());

    // Allocate the buffer to fill the data.
    auto buf = session.Allocate();
    // Read from the buffer at most buf.len bytes in a non-blocking fashion.
    int len = session.socket().Read(buf.data, buf.len, true);

    // Check for read errors.
    if (len == -1) {
      // This means read would block or read was interrupted by signal, we
      // return `false` to stop reading of data.
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        // Rearm epoll to send events from this socket.
        epoll_.Modify(session.socket().fd(),
                      EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLONESHOT, &session);
        return false;
      }
      // Some other error occurred, close the session.
      CloseSession(session);
      return false;
    }

    // The client has closed the connection.
    if (len == 0) {
      LOG(INFO) << "Client " << session.socket().endpoint()
                << " closed the connection.";
      CloseSession(session);
      return false;
    }

    // Notify the session that it has new data.
    session.Written(len);

    // Execute the session.
    try {
      session.Execute();
      session.RefreshLastEventTime(std::chrono::steady_clock::now());
    } catch (const std::exception &e) {
      // Catch all exceptions.
      LOG(ERROR) << "Exception was thrown while processing event in session "
                    "associated with "
                 << session.socket().endpoint()
                 << " with message: " << e.what();
      CloseSession(session);
      return false;
    }

    return true;
  }

  void CloseSession(TSession &session) {
    // Deregister the Session's socket from epoll to disable further events. For
    // a detailed description why this is necessary before destroying (closing)
    // the socket, see:
    // https://idea.popcount.org/2017-03-20-epoll-is-fundamentally-broken-22/
    epoll_.Delete(session.socket().fd());

    std::unique_lock<SpinLock> guard(lock_);
    auto it =
        std::find_if(sessions_.begin(), sessions_.end(),
                     [&](const auto &l) { return l->Id() == session.Id(); });

    CHECK(it != sessions_.end())
        << "Trying to remove session that is not found in sessions!";
    int i = it - sessions_.begin();
    swap(sessions_[i], sessions_.back());

    // This will call all destructors on the Session. Consequently, it will call
    // the destructor on the Socket and close the socket.
    sessions_.pop_back();
  }

  io::network::Epoll epoll_;

  TSessionData &data_;

  SpinLock lock_;
  std::vector<std::unique_ptr<TSession>> sessions_;

  std::thread thread_;
  std::atomic<bool> alive_;
};
}  // namespace communication
