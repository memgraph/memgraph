// Copyright 2024 Memgraph Ltd.
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

#include "communication/helpers.hpp"
#include "io/network/network_error.hpp"
#include "utils/logging.hpp"

namespace memgraph::communication {

Client::Client(ClientContext *context) : context_(context) {}

Client::~Client() {
  Close();
  ReleaseSslObjects();
}

bool Client::Connect(const io::network::Endpoint &endpoint) {
  // Try to establish a socket connection.
  try {
    socket_.Connect(endpoint);
  } catch (const io::network::NetworkError &e) {
    return false;
  }

  // Enable TCP keep alive for all connections.
  // Because we manually always set the `have_more` flag to the socket
  // `Write` call we can disable the Nagle algorithm because we know that we
  // are always sending optimal packets. Even if we don't send optimal
  // packets, there will be no delay between packets and throughput won't
  // suffer.
  socket_.SetKeepAlive();
  socket_.SetNoDelay();

  if (context_->use_ssl()) {
    // Release leftover SSL objects.
    ReleaseSslObjects();

    // Create a new SSL object that will be used for SSL communication.
    ssl_ = SSL_new(context_->context());
    if (ssl_ == nullptr) {
      SPDLOG_ERROR("Couldn't create client SSL object!");
      socket_.Close();
      return false;
    }

    // Create a new BIO (block I/O) SSL object so that OpenSSL can communicate
    // using our socket. We specify `BIO_NOCLOSE` to indicate to OpenSSL that
    // it doesn't need to close the socket when destructing all objects (we
    // handle that in our socket destructor).
    bio_ = BIO_new_socket(socket_.fd(), BIO_NOCLOSE);
    if (bio_ == nullptr) {
      SPDLOG_ERROR("Couldn't create client BIO object!");
      socket_.Close();
      return false;
    }

    // Connect the BIO object to the SSL object so that OpenSSL knows which
    // stream it should use for communication. We use the same object for both
    // the read and write end. This function cannot fail.
    SSL_set_bio(ssl_, bio_, bio_);

    // Clear all leftover errors.
    ERR_clear_error();

    // Perform the TLS handshake.
    auto ret = SSL_connect(ssl_);
    if (ret != 1) {
      SPDLOG_WARN("Couldn't connect to SSL server: {}", SslGetLastError());
      socket_.Close();
      return false;
    }
  }

  return true;
}

bool Client::ErrorStatus() { return socket_.ErrorStatus(); }

bool Client::IsConnected() { return socket_.IsOpen(); }

void Client::Shutdown() { socket_.Shutdown(); }

void Client::Close() {
  if (ssl_) {
    // Perform an unidirectional SSL shutdown. That just means that we send
    // the shutdown message and don't wait or care for the result.
    SSL_shutdown(ssl_);
  }
  socket_.Close();
}

bool Client::Read(size_t len, bool exactly_len) {
  if (len == 0) return false;
  size_t received = 0;
  buffer_.write_end()->Resize(buffer_.read_end()->size() + len);
  do {
    auto buff = buffer_.write_end()->Allocate();
    if (ssl_) {
      // We clear errors here to prevent errors piling up in the internal
      // OpenSSL error queue. To see when could that be an issue read this:
      // https://www.arangodb.com/2014/07/started-hate-openssl/
      ERR_clear_error();

      // Read encrypted data from the socket using OpenSSL.
      auto got = SSL_read(ssl_, buff.data, len - received);

      // Handle errors that might have occurred.
      if (got < 0) {
        auto err = SSL_get_error(ssl_, got);
        if (err == SSL_ERROR_WANT_READ) {
          // OpenSSL want's to read more data from the socket. We wait for
          // more data to be ready and retry the call.
          socket_.WaitForReadyRead();
          continue;
        } else if (err == SSL_ERROR_WANT_WRITE) {
          // The OpenSSL library probably wants to perform some kind of
          // handshake so we wait for the socket to become ready for a write
          // and call the read again.
          socket_.WaitForReadyWrite();
          continue;
        } else {
          // This is a fatal error.
          SPDLOG_ERROR("Received an unexpected SSL error: {}", err);
          return false;
        }
      } else if (got == 0) {
        // The server closed the connection.
        return false;
      }

      // Notify the buffer that it has new data.
      buffer_.write_end()->Written(got);
      received += got;
    } else {
      // Read raw data from the socket.
      auto got = socket_.Read(buff.data, len - received);

      if (got <= 0) {
        // If `read` returns 0 the server has closed the connection. If `read`
        // returns -1 all of the errors that could be found in `errno` are
        // fatal errors (because we are using a blocking socket) so return a
        // read failure.
        return false;
      }

      // Notify the buffer that it has new data.
      buffer_.write_end()->Written(got);
      received += got;
    }
  } while (received < len && exactly_len);
  return true;
}

uint8_t *Client::GetData() { return buffer_.read_end()->data(); }

size_t Client::GetDataSize() { return buffer_.read_end()->size(); }

void Client::ShiftData(size_t len) { buffer_.read_end()->Shift(len); }

void Client::ClearData() { buffer_.read_end()->Clear(); }

bool Client::Write(const uint8_t *data, size_t len, bool have_more) {
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
    return socket_.Write(data, len, have_more);
  }
}

bool Client::Write(const std::string &str, bool have_more) {
  return Write(reinterpret_cast<const uint8_t *>(str.data()), str.size(), have_more);
}

const io::network::Endpoint &Client::endpoint() { return socket_.endpoint(); }

void Client::ReleaseSslObjects() {
  // If we are using SSL we need to free the allocated objects. Here we only
  // free the SSL object because the `SSL_free` function also automatically
  // frees the BIO object.
  if (ssl_) {
    SSL_free(ssl_);
    ssl_ = nullptr;
    bio_ = nullptr;
  }
}

ClientInputStream::ClientInputStream(Client &client) : client_(client) {}

uint8_t *ClientInputStream::data() { return client_.GetData(); }

size_t ClientInputStream::size() const { return client_.GetDataSize(); }

void ClientInputStream::Shift(size_t len) { client_.ShiftData(len); }

void ClientInputStream::Clear() { client_.ClearData(); }

ClientOutputStream::ClientOutputStream(Client &client) : client_(client) {}

bool ClientOutputStream::Write(const uint8_t *data, size_t len, bool have_more) {
  return client_.Write(data, len, have_more);
}
bool ClientOutputStream::Write(const std::string &str, bool have_more) { return client_.Write(str, have_more); }

}  // namespace memgraph::communication
