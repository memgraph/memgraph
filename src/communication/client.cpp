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

#include "communication/client.hpp"

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <spdlog/spdlog.h>

#include "buffer.hpp"
#include "communication/helpers.hpp"
#include "context.hpp"
#include "io/network/stream_buffer.hpp"
#include "utils/on_scope_exit.hpp"

namespace {

auto SslWantToPollEvents(int const ssl_err) -> std::optional<short> {
  if (ssl_err == SSL_ERROR_WANT_READ) {
    return POLLIN;
  }
  if (ssl_err == SSL_ERROR_WANT_WRITE) {
    return POLLOUT;
  }
  return std::nullopt;
}

}  // namespace

namespace memgraph::communication {

Client::Client(ClientContext *context, std::chrono::milliseconds const connect_timeout_ms)
    : context_(context), connect_timeout_ms_(connect_timeout_ms) {}

Client::~Client() {
  Close();
  ReleaseSslObjects();
}

bool Client::Connect(const io::network::Endpoint &endpoint) {
  // Try to establish a socket connection. If SSL should be used, socket should not restore socket to the blocking mode,
  // we will do it here.
  if (!socket_.Connect(endpoint, connect_timeout_ms_, context_->use_ssl())) {
    return false;
  }

  bool success{false};
  auto cleanup = utils::OnScopeExit{[&]() {
    if (!success) {
      socket_.Close();
      ReleaseSslObjects();
    }
  }};

  if (context_->use_ssl()) {
    if (auto r = SetupSslObjects(); !r) {
      spdlog::error("Couldn't set up client SSL objects: {}", r.error());
      return false;
    }

    ERR_clear_error();
    spdlog::trace("Trying to do SSL_connect");

    if (auto r = DriveSslHandshake(); !r) {
      spdlog::warn("Couldn't complete SSL handshake: {}", r.error());
      return false;
    }
    // Socket intentionally stays non-blocking after the handshake. Read()/Write() poll for readiness
    // and loop on SSL_ERROR_WANT_READ/WRITE so the supplied timeout actually bounds the operation;
    // a blocking socket would let SSL_read sit in the kernel past the timeout when a TLS record
    // arrives but no application data is decryptable yet (e.g., TLS 1.3 post-handshake messages).
  }

  // Enable TCP keep alive for all connections.
  // Because we manually always set the `have_more` flag to the socket
  // `Write` call we can disable the Nagle algorithm because we know that we
  // are always sending optimal packets. Even if we don't send optimal
  // packets, there will be no delay between packets and throughput won't
  // suffer.
  socket_.SetKeepAlive();
  socket_.SetNoDelay();
  socket_.SetUserTimeout();

  success = true;
  return true;
}

auto Client::SetupSslObjects() -> std::expected<void, std::string> {
  // Release SSL objects left over from any prior Connect call on this Client.
  ReleaseSslObjects();

  // Pin the SSL context for the lifetime of this connection — without this, a
  // concurrent cluster TLS reload could drop the previous shared_ptr from the
  // singleton and free the SSL_CTX between context() returning and SSL_new
  // up-ref'ing it.
  ssl_context_ = context_->context();

  // Create a new SSL object that will be used for SSL communication.
  ssl_ = SSL_new(ssl_context_->native_handle());
  if (ssl_ == nullptr) {
    return std::unexpected{"SSL_new returned nullptr"};
  }

  // Create a new BIO (block I/O) SSL object so that OpenSSL can communicate
  // using our socket. We specify `BIO_NOCLOSE` to indicate to OpenSSL that it
  // doesn't need to close the socket when destructing all objects (we handle
  // that in our socket destructor).
  bio_ = BIO_new_socket(socket_.fd(), BIO_NOCLOSE);
  if (bio_ == nullptr) {
    return std::unexpected{"BIO_new_socket returned nullptr"};
  }

  // Bind the BIO to the SSL object (same BIO for read and write). Cannot fail.
  SSL_set_bio(ssl_, bio_, bio_);
  return {};
}

auto Client::DriveSslHandshake() -> std::expected<void, std::string> {
  auto const fd = socket_.fd();
  auto const deadline = std::chrono::steady_clock::now() + connect_timeout_ms_;

  while (true) {
    ERR_clear_error();
    auto const ret = SSL_connect(ssl_);
    if (ret == 1) return {};

    auto const events = SslWantToPollEvents(SSL_get_error(ssl_, ret));
    if (!events.has_value()) {
      return std::unexpected{SslGetLastError()};
    }

    auto const now = std::chrono::steady_clock::now();
    if (now >= deadline) return std::unexpected{"SSL handshake timed out"};

    auto const ms_remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
    pollfd pfd{.fd = fd, .events = *events, .revents = 0};

    auto const rc = poll(&pfd, 1, static_cast<int>(ms_remaining));
    if (rc == 0) return std::unexpected{"SSL handshake timed out"};

    if (rc < 0 && errno != EINTR) {
      return std::unexpected{fmt::format("poll() during SSL handshake failed: {}", std::strerror(errno))};
    }
  }
}

bool Client::ErrorStatus() const { return socket_.ErrorStatus(); }

bool Client::IsConnected() const { return socket_.IsOpen(); }

void Client::Shutdown() { socket_.Shutdown(); }

void Client::Close() {
  if (ssl_) {
    // Perform an unidirectional SSL shutdown. That just means that we send
    // the shutdown message and don't wait or care for the result.
    SSL_shutdown(ssl_);
  }
  socket_.Close();
}

auto Client::Read(size_t len, bool exactly_len, const std::optional<int> timeout_ms)
    -> std::expected<void, io::network::ClientCommunicationError> {
  if (len == 0) return std::unexpected{io::network::ClientCommunicationError::GENERIC_ERROR};
  size_t received = 0;
  buffer_.write_end()->Resize(buffer_.read_end()->size() + len);
  auto const deadline = std::invoke([&timeout_ms]() -> std::optional<std::chrono::steady_clock::time_point> {
    if (timeout_ms) {
      return std::chrono::steady_clock::now() + std::chrono::milliseconds(*timeout_ms);
    }
    return std::nullopt;
  });

  do {
    auto buff = buffer_.write_end()->GetBuffer();
    if (ssl_) {
      // We clear errors here to prevent errors piling up in the internal
      // OpenSSL error queue. To see when could that be an issue read this:
      // https://www.arangodb.com/2014/07/started-hate-openssl/
      ERR_clear_error();
      auto got = SSL_read(ssl_, buff.data, static_cast<int>(len - received));

      if (got == 0) {
        // The server closed the connection.
        return std::unexpected{io::network::ClientCommunicationError::GENERIC_ERROR};
      }

      // Handle errors that might have occurred.
      if (got < 0) {
        auto err = SSL_get_error(ssl_, got);
        auto const remaining_timeout_ms = std::invoke([&deadline]() -> std::optional<int> {
          if (!deadline) return std::nullopt;
          return std::chrono::duration_cast<std::chrono::milliseconds>(*deadline - std::chrono::steady_clock::now())
              .count();
        });
        if (remaining_timeout_ms && *remaining_timeout_ms <= 0) {
          return std::unexpected{io::network::ClientCommunicationError::TIMEOUT_ERROR};
        }

        if (err == SSL_ERROR_WANT_READ) {
          // POLLIN was set but no app data was decryptable (e.g., a TLS 1.3 NewSessionTicket
          // consumed internally).
          // Wait for the underlying socket to have data before SSL_read. With a non-blocking socket,
          // polling unconditionally (timeout_ms == nullopt -> poll(-1)) avoids a busy loop when
          // SSL_read returns WANT_READ on the retry path below.
          if (!socket_.WaitForReadyRead(remaining_timeout_ms)) {
            return std::unexpected{io::network::ClientCommunicationError::TIMEOUT_ERROR};
          }
          continue;
        } else if (err == SSL_ERROR_WANT_WRITE) {
          // OpenSSL needs to write (renegotiation). Wait for write readiness with the same timeout.
          if (!socket_.WaitForReadyWrite(remaining_timeout_ms)) {
            return std::unexpected{io::network::ClientCommunicationError::TIMEOUT_ERROR};
          }
          continue;
        } else {
          // This is a fatal error.
          spdlog::error("Received an unexpected SSL error: {}", err);
          return std::unexpected{io::network::ClientCommunicationError::GENERIC_ERROR};
        }
      }

      // Notify the buffer that it has new data.
      buffer_.write_end()->Written(got);
      received += got;
    } else {
      auto const remaining_timeout_ms = std::invoke([&deadline]() -> std::optional<int> {
        if (!deadline) return std::nullopt;
        return std::chrono::duration_cast<std::chrono::milliseconds>(*deadline - std::chrono::steady_clock::now())
            .count();
      });
      if (remaining_timeout_ms && *remaining_timeout_ms < 0) {
        return std::unexpected{io::network::ClientCommunicationError::TIMEOUT_ERROR};
      }
      // Read raw data from the socket.
      if (remaining_timeout_ms && !socket_.WaitForReadyRead(remaining_timeout_ms)) {
        return std::unexpected{io::network::ClientCommunicationError::TIMEOUT_ERROR};
      }
      auto got = socket_.Read(buff.data, len - received);

      if (got <= 0) {
        // If `read` returns 0 the server has closed the connection. If `read`
        // returns -1 all of the errors that could be found in `errno` are
        // fatal errors (because we are using a blocking socket) so return a
        // read failure.
        return std::unexpected{io::network::ClientCommunicationError::GENERIC_ERROR};
      }

      // Notify the buffer that it has new data.
      buffer_.write_end()->Written(got);
      received += got;
    }
  } while (received < len && exactly_len);
  return {};
}

uint8_t *Client::GetData() { return buffer_.read_end()->data(); }

size_t Client::GetDataSize() const { return buffer_.read_end()->size(); }

void Client::ShiftData(size_t len) { buffer_.read_end()->Shift(len); }

void Client::ClearData() { buffer_.read_end()->Clear(); }

auto Client::Write(const uint8_t *data, size_t len, bool have_more, const std::optional<int> timeout_ms)
    -> std::expected<void, io::network::ClientCommunicationError> {
  if (ssl_) {
    auto const deadline = std::invoke([&timeout_ms]() -> std::optional<std::chrono::steady_clock::time_point> {
      if (timeout_ms) {
        return std::chrono::steady_clock::now() + std::chrono::milliseconds(*timeout_ms);
      }
      return std::nullopt;
    });

    // `SSL_write` has the interface of a normal `write` call. Because of that
    // we need to ensure that all data is written to the socket manually.
    while (len > 0) {
      // We clear errors here to prevent errors piling up in the internal
      // OpenSSL error queue. To see when could that be an issue read this:
      // https://www.arangodb.com/2014/07/started-hate-openssl/
      ERR_clear_error();

      // Write data to the socket using OpenSSL.
      auto written = SSL_write(ssl_, data, static_cast<int>(len));
      if (written < 0) {
        auto err = SSL_get_error(ssl_, written);
        auto const remaining_timeout_ms = std::invoke([&deadline]() -> std::optional<int> {
          if (!deadline) return std::nullopt;
          return std::chrono::duration_cast<std::chrono::milliseconds>(*deadline - std::chrono::steady_clock::now())
              .count();
        });
        if (remaining_timeout_ms && *remaining_timeout_ms < 0) {
          return std::unexpected{io::network::ClientCommunicationError::TIMEOUT_ERROR};
        }

        if (err == SSL_ERROR_WANT_READ) {
          // OpenSSL wants to read (renegotiation). Wait for read readiness with the supplied timeout;
          // nullopt -> poll(-1) blocks indefinitely, matching pre-non-blocking behavior.
          if (!socket_.WaitForReadyRead(remaining_timeout_ms)) {
            return std::unexpected{io::network::ClientCommunicationError::TIMEOUT_ERROR};
          }
        } else if (err == SSL_ERROR_WANT_WRITE) {
          // The OS send buffer is full on a non-blocking socket; wait for capacity.
          if (!socket_.WaitForReadyWrite(remaining_timeout_ms)) {
            return std::unexpected{io::network::ClientCommunicationError::TIMEOUT_ERROR};
          }
        } else {
          // This is a fatal error.
          return std::unexpected{io::network::ClientCommunicationError::GENERIC_ERROR};
        }
      } else if (written == 0) {
        // The client closed the connection.
        return std::unexpected{io::network::ClientCommunicationError::GENERIC_ERROR};
      } else {
        len -= written;
        data += written;
      }
    }
    return {};
  }
  // Non-ssl
  return socket_.Write(data, len, have_more, timeout_ms);
}

auto Client::Write(std::string_view str, bool have_more, const std::optional<int> timeout_ms)
    -> std::expected<void, io::network::ClientCommunicationError> {
  return Write(reinterpret_cast<const uint8_t *>(str.data()), str.size(), have_more, timeout_ms);
}

const io::network::Endpoint &Client::endpoint() const { return socket_.endpoint(); }

void Client::ReleaseSslObjects() {
  // If we are using SSL we need to free the allocated objects. Here we only
  // free the SSL object because the `SSL_free` function also automatically
  // frees the BIO object.
  if (ssl_) {
    SSL_free(ssl_);
    ssl_ = nullptr;
    bio_ = nullptr;
  }
  ssl_context_.reset();
}

ClientInputStream::ClientInputStream(Client &client) : client_(client) {}

uint8_t *ClientInputStream::data() { return client_.GetData(); }

size_t ClientInputStream::size() const { return client_.GetDataSize(); }

void ClientInputStream::Shift(size_t len) { client_.ShiftData(len); }

void ClientInputStream::Clear() { client_.ClearData(); }

ClientOutputStream::ClientOutputStream(Client &client) : client_(client) {}

bool ClientOutputStream::Write(const uint8_t *data, size_t len, bool have_more) {
  return client_.Write(data, len, have_more).has_value();
}

bool ClientOutputStream::Write(std::string_view str, bool have_more) {
  return client_.Write(str, have_more).has_value();
}

}  // namespace memgraph::communication
