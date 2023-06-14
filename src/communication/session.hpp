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

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include "communication/buffer.hpp"
#include "communication/context.hpp"
#include "communication/exceptions.hpp"
#include "communication/helpers.hpp"
#include "io/network/socket.hpp"
#include "io/network/stream_buffer.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/spin_lock.hpp"

namespace memgraph::communication {

/**
 * This is used to provide input to user sessions. All sessions used with the
 * network stack should use this class as their input stream.
 */
using InputStream = Buffer::ReadEnd;

/**
 * This is used to provide output from user sessions. All sessions used with the
 * network stack should use this class for their output stream.
 */
class OutputStream final {
 public:
  OutputStream(std::function<bool(const uint8_t *, size_t, bool)> write_function) : write_function_(write_function) {}

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
 * sessions. It handles socket ownership, inactivity timeout and protocol
 * wrapping.
 */
template <class TSession, class TSessionData>
class Session final {
 public:
  Session(io::network::Socket &&socket, TSessionData *data, ServerContext *context, int inactivity_timeout_sec)
      : socket_(std::move(socket)),
        output_stream_([this](const uint8_t *data, size_t len, bool have_more) { return Write(data, len, have_more); }),
        session_(data, socket_.endpoint(), input_buffer_.read_end(), &output_stream_),
        inactivity_timeout_sec_(inactivity_timeout_sec) {
    // Set socket options.
    // The socket is set to be a non-blocking socket. We use the socket in a
    // non-blocking fashion for reads and manually simulate a blocking socket
    // type for writes. This manual handling of writes is necessary because
    // OpenSSL doesn't provide a way to add `recv` parameters to the `SSL_read`
    // call so we can't have a blocking socket and use it in a non-blocking way
    // only for reads.
    // Keep alive is enabled so that the Kernel's TCP stack notifies us if a
    // connection is broken and shouldn't be used anymore.
    // Because we manually always set the `have_more` flag to the socket
    // `Write` call we can disable the Nagle algorithm because we know that we
    // are always sending optimal packets. Even if we don't send optimal
    // packets, there will be no delay between packets and throughput won't
    // suffer.
    socket_.SetNonBlocking();
    socket_.SetKeepAlive();
    socket_.SetNoDelay();

    // Prepare SSL if we should be using it.
    if (context->use_ssl()) {
      // Create a new SSL object that will be used for SSL communication.
      ssl_ = SSL_new(context->context());
      MG_ASSERT(ssl_ != nullptr, "Couldn't create server SSL object!");

      // Create a new BIO (block I/O) SSL object so that OpenSSL can communicate
      // using our socket. We specify `BIO_NOCLOSE` to indicate to OpenSSL that
      // it doesn't need to close the socket when destructing all objects (we
      // handle that in our socket destructor).
      bio_ = BIO_new_socket(socket_.fd(), BIO_NOCLOSE);
      MG_ASSERT(bio_ != nullptr, "Couldn't create server BIO object!");

      // Connect the BIO object to the SSL object so that OpenSSL knows which
      // stream it should use for communication. We use the same object for both
      // the read and write end. This function cannot fail.
      SSL_set_bio(ssl_, bio_, bio_);

      // Indicate to OpenSSL that this connection is a server. The TLS handshake
      // will be performed in the first `SSL_read` or `SSL_write` call. This
      // function cannot fail.
      SSL_set_accept_state(ssl_);
    }
  }

  Session(const Session &) = delete;
  Session(Session &&) = delete;
  Session &operator=(const Session &) = delete;
  Session &operator=(Session &&) = delete;

  ~Session() {
    // If we are using SSL we need to free the allocated objects. Here we only
    // free the SSL object because the `SSL_free` function also automatically
    // frees the BIO object.
    if (ssl_) {
      SSL_free(ssl_);
    }
  }

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
    RefreshLastEventTime(true);
    utils::OnScopeExit on_exit([this] { RefreshLastEventTime(false); });

    // Allocate the buffer to fill the data.
    auto buf = input_buffer_.write_end()->Allocate();

    if (ssl_) {
      // We clear errors here to prevent errors piling up in the internal
      // OpenSSL error queue. To see when could that be an issue read this:
      // https://www.arangodb.com/2014/07/started-hate-openssl/
      ERR_clear_error();

      // Read data from the socket using the OpenSSL API.
      auto len = SSL_read(ssl_, buf.data, buf.len);

      // Check for read errors.
      if (len < 0) {
        auto err = SSL_get_error(ssl_, len);
        if (err == SSL_ERROR_WANT_READ) {
          // OpenSSL want's to read more data from the socket. We return `true`
          // to stop execution of the session to wait for more data to be
          // received.
          return true;
        } else if (err == SSL_ERROR_WANT_WRITE) {
          // The OpenSSL library wants to perform some kind of handshake so we
          // wait for the socket to become ready for a write and call the read
          // again. We return `false` so that the listener calls this function
          // again.
          socket_.WaitForReadyWrite();
          return false;
        } else if (err == SSL_ERROR_SYSCALL) {
          // OpenSSL returns this error when you open a connection to the server
          // but you don't send any data. We do this often when we check whether
          // the server is alive by trying to open a connection to it using
          // `nc -z -w 1 127.0.0.1 7687`.
          // When this error occurs we need to check the `errno` to find out
          // what really happened.
          // See: https://www.openssl.org/docs/man1.1.0/ssl/SSL_get_error.html
          if (errno == 0) {
            // The client closed the connection.
            throw SessionClosedException("Session was closed by the client.");
          }
          // If `errno` isn't 0 then something bad happened.
          throw utils::BasicException(SslGetLastError());
        } else {
          // This is a fatal error.
          spdlog::error(utils::MessageWithLink(
              "An unknown error occurred while processing SSL messages. "
              "Please make sure that you have SSL properly configured on the server and the client.",
              "https://memgr.ph/ssl"));
          throw utils::BasicException(SslGetLastError());
        }
      } else if (len == 0) {
        // The client closed the connection.
        throw SessionClosedException("Session was closed by the client.");
        return false;
      } else {
        // Notify the input buffer that it has new data.
        input_buffer_.write_end()->Written(len);
      }
    } else {
      // Read from the buffer at most buf.len bytes in a non-blocking fashion.
      // Note, the `true` parameter for non-blocking here is redundant because
      // the socket already is non-blocking.
      auto len = socket_.Read(buf.data, buf.len, true);

      // Check for read errors.
      if (len == -1) {
        // This means read would block or read was interrupted by signal, we
        // return `true` to indicate that all data is processad and to stop
        // reading of data.
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
          return true;
        }
        // Some other error occurred, throw an exception to start session
        // cleanup.
        throw utils::BasicException("Couldn't read data from the socket!");
      } else if (len == 0) {
        // The client has closed the connection.
        throw SessionClosedException("Session was closed by client.");
      } else {
        // Notify the input buffer that it has new data.
        input_buffer_.write_end()->Written(len);
      }
    }

    // Execute the session.
    session_.Execute();

    return false;
  }

  /**
   * Returns true if session has timed out. Session times out if there was no
   * activity in inactivity_timeout_sec seconds. This function must be thread
   * safe because this function and `RefreshLastEventTime` are called from
   * different threads in the network stack.
   */
  bool TimedOut() {
    std::unique_lock<utils::SpinLock> guard(lock_);
    if (execution_active_) return false;
    return last_event_time_ + std::chrono::seconds(inactivity_timeout_sec_) < std::chrono::steady_clock::now();
  }

  /**
   * Returns a reference to the internal socket.
   */
  io::network::Socket &socket() { return socket_; }

 private:
  void RefreshLastEventTime(bool active) {
    std::unique_lock<utils::SpinLock> guard(lock_);
    execution_active_ = active;
    last_event_time_ = std::chrono::steady_clock::now();
  }

  // TODO (mferencevic): the `have_more` flag currently isn't supported
  // when using OpenSSL
  bool Write(const uint8_t *data, size_t len, bool have_more = false) {
    if (ssl_) {
      // `SSL_write` has the interface of a normal `write` call. Because of that
      // we need to ensure that all data is written to the socket manually.
      while (len > 0) {
        // We clear errors here to prevent errors piling up in the internal
        // OpenSSL error queue. To see when could that be an issue read this:
        // https://www.arangodb.com/2014/07/started-hate-openssl/
        ERR_clear_error();

        // Write data to the socket using OpenSSL.
        auto written = SSL_write(ssl_, data, len);
        if (written < 0) {
          auto err = SSL_get_error(ssl_, written);
          if (err == SSL_ERROR_WANT_READ) {
            // OpenSSL wants to perform some kind of handshake, we need to
            // ensure that there is data available for the next call to
            // `SSL_write`.
            socket_.WaitForReadyRead();
          } else if (err == SSL_ERROR_WANT_WRITE) {
            // The socket probably returned WOULDBLOCK and we need to wait for
            // the output buffers to clear and reattempt the send.
            socket_.WaitForReadyWrite();
          } else {
            // This is a fatal error.
            return false;
          }
        } else if (written == 0) {
          // The client closed the connection.
          return false;
        } else {
          len -= written;
          data += written;
        }
      }
      return true;
    } else {
      // This function guarantees that all data will be written to the socket
      // even if the socket is non-blocking. It will use a non-busy wait to send
      // all data.
      return socket_.Write(data, len, have_more);
    }
  }

  // We own the socket.
  io::network::Socket socket_;

  // Input and output buffers/streams.
  Buffer input_buffer_;
  OutputStream output_stream_;

  // Session that will be executed.
  TSession session_;

  // Time of the last event and associated lock.
  std::chrono::time_point<std::chrono::steady_clock> last_event_time_{std::chrono::steady_clock::now()};
  bool execution_active_{false};
  utils::SpinLock lock_;
  const int inactivity_timeout_sec_;

  // SSL objects.
  SSL *ssl_{nullptr};
  BIO *bio_{nullptr};
};  // namespace communication
}  // namespace memgraph::communication
