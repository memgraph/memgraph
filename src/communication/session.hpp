#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>

#include <glog/logging.h>

#include "communication/buffer.hpp"
#include "io/network/socket.hpp"
#include "io/network/stream_buffer.hpp"
#include "threading/sync/spinlock.hpp"
#include "utils/exceptions.hpp"

namespace communication {

/**
 * This exception is thrown to indicate to the communication stack that the
 * session is closed and that cleanup should be performed.
 */
class SessionClosedException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

/**
 * This is used to provide input to user sessions. All sessions used with the
 * network stack should use this class as their input stream.
 */
using InputStream = Buffer::ReadEnd;

/**
 * This is used to provide output from user sessions. All sessions used with the
 * network stack should use this class for their output stream.
 */
class OutputStream {
 public:
  OutputStream(io::network::Socket &socket) : socket_(socket) {}

  bool Write(const uint8_t *data, size_t len, bool have_more = false) {
    return socket_.Write(data, len, have_more);
  }

  bool Write(const std::string &str, bool have_more = false) {
    return Write(reinterpret_cast<const uint8_t *>(str.data()), str.size(),
                 have_more);
  }

 private:
  io::network::Socket &socket_;
};

/**
 * This class is used internally in the communication stack to handle all user
 * sessions. It handles socket ownership, inactivity timeout and protocol
 * wrapping.
 */
template <class TSession, class TSessionData>
class Session {
 public:
  Session(io::network::Socket &&socket, TSessionData &data,
          int inactivity_timeout_sec)
      : socket_(std::move(socket)),
        output_stream_(socket_),
        session_(data, input_buffer_.read_end(), output_stream_),
        inactivity_timeout_sec_(inactivity_timeout_sec) {}

  Session(const Session &) = delete;
  Session(Session &&) = delete;
  Session &operator=(const Session &) = delete;
  Session &operator=(Session &&) = delete;

  /**
   * This function is called from the communication stack when an event occurs
   * indicating that there is data waiting to be read. This function calls the
   * `Execute` method from the supplied `TSession` and handles all things
   * necessary before the execution (eg. reading data from network, protocol
   * encapsulation, etc.). This function returns `true` if the session is done
   * with execution (when all data is read and all processing is done). It
   * returns `false` when there is more data that should be read and processed.
   */
  bool Execute() {
    // Refresh the last event time in the session.
    RefreshLastEventTime();

    // Allocate the buffer to fill the data.
    auto buf = input_buffer_.write_end().Allocate();
    // Read from the buffer at most buf.len bytes in a non-blocking fashion.
    int len = socket_.Read(buf.data, buf.len, true);

    // Check for read errors.
    if (len == -1) {
      // This means read would block or read was interrupted by signal, we
      // return `true` to indicate that all data is processad and to stop
      // reading of data.
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        return true;
      }
      // Some other error occurred, throw an exception to start session cleanup.
      throw utils::BasicException("Couldn't read data from socket!");
    }

    // The client has closed the connection.
    if (len == 0) {
      throw SessionClosedException("Session was closed by client.");
    }

    // Notify the input buffer that it has new data.
    input_buffer_.write_end().Written(len);

    // Execute the session.
    session_.Execute();

    // Refresh the last event time.
    RefreshLastEventTime();

    return false;
  }

  /**
   * Returns true if session has timed out. Session times out if there was no
   * activity in inactivity_timeout_sec seconds. This function must be thread
   * safe because this function and `RefreshLastEventTime` are called from
   * different threads in the network stack.
   */
  bool TimedOut() {
    std::unique_lock<SpinLock> guard(lock_);
    return last_event_time_ + std::chrono::seconds(inactivity_timeout_sec_) <
           std::chrono::steady_clock::now();
  }

  /**
   * Returns a reference to the internal socket.
   */
  io::network::Socket &socket() { return socket_; }

 private:
  void RefreshLastEventTime() {
    std::unique_lock<SpinLock> guard(lock_);
    last_event_time_ = std::chrono::steady_clock::now();
  }

  // We own the socket.
  io::network::Socket socket_;

  // Input and output buffers/streams.
  Buffer input_buffer_;
  OutputStream output_stream_;

  // Session that will be executed.
  TSession session_;

  // Time of the last event and associated lock.
  std::chrono::time_point<std::chrono::steady_clock> last_event_time_{
      std::chrono::steady_clock::now()};
  SpinLock lock_;
  const int inactivity_timeout_sec_;
};
}  // namespace communication
