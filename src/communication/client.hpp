// Copyright 2023 Memgraph Ltd.
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

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

// Centos 7 OpenSSL includes libkrb5 which has brings in macros TRUE and FALSE. undef to prevent issues.
#undef TRUE
#undef FALSE

#include "communication/buffer.hpp"
#include "communication/context.hpp"
#include "communication/init.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/socket.hpp"

namespace memgraph::communication {

/**
 * This class implements a generic network Client.
 * It uses blocking sockets and provides an API that can be used to receive/send
 * data over the network connection.
 *
 * NOTE: If you use this client you **must** create `memgraph::communication::SSLInit`
 * from the `main` function before using the client!
 */
class Client final {
 public:
  explicit Client(ClientContext *context);

  ~Client();

  Client(const Client &) = delete;
  Client(Client &&) = delete;
  Client &operator=(const Client &) = delete;
  Client &operator=(Client &&) = delete;

  /**
   * This function connects to a remote server and returns whether the connect
   * succeeded.
   */
  bool Connect(const io::network::Endpoint &endpoint);

  /**
   * This function returns `true` if the socket is in an error state.
   */
  bool ErrorStatus();

  /**
   * This function returns `true` if the socket is connected to a remote host.
   */
  bool IsConnected();

  /**
   * This function shuts down the socket.
   */
  void Shutdown();

  /**
   * This function closes the socket.
   */
  void Close();

  /**
   * This function is used to receive exactly `len` bytes from the socket and
   * stores it in an internal buffer. If `exactly_len` is set to `false` then
   * less than `len` bytes can be received. It returns `true` if the read
   * succeeded and `false` if it didn't.
   */
  bool Read(size_t len, bool exactly_len = true);

  /**
   * This function returns a pointer to the read data that is currently stored
   * in the client.
   */
  uint8_t *GetData();

  /**
   * This function returns the size of the read data that is currently stored in
   * the client.
   */
  size_t GetDataSize();

  /**
   * This function removes first `len` bytes from the data buffer.
   */
  void ShiftData(size_t len);

  /**
   * This function clears the data buffer.
   */
  void ClearData();

  /**
   * This function writes data to the socket.
   * TODO (mferencevic): the `have_more` flag currently isn't supported when
   * using OpenSSL
   */
  bool Write(const uint8_t *data, size_t len, bool have_more = false);

  /**
   * This function writes data to the socket.
   */
  bool Write(const std::string &str, bool have_more = false);

  const io::network::Endpoint &endpoint();

 private:
  void ReleaseSslObjects();

  io::network::Socket socket_;
  Buffer buffer_;

  ClientContext *context_;
  SSL *ssl_{nullptr};
  BIO *bio_{nullptr};
};

/**
 * This class provides a stream-like input side object to the client.
 */
class ClientInputStream final {
 public:
  ClientInputStream(Client &client);

  ClientInputStream(const ClientInputStream &) = delete;
  ClientInputStream(ClientInputStream &&) = delete;
  ClientInputStream &operator=(const ClientInputStream &) = delete;
  ClientInputStream &operator=(ClientInputStream &&) = delete;

  uint8_t *data();

  size_t size() const;

  void Shift(size_t len);

  void Clear();

 private:
  Client &client_;
};

/**
 * This class provides a stream-like output side object to the client.
 */
class ClientOutputStream final {
 public:
  ClientOutputStream(Client &client);

  ClientOutputStream(const ClientOutputStream &) = delete;
  ClientOutputStream(ClientOutputStream &&) = delete;
  ClientOutputStream &operator=(const ClientOutputStream &) = delete;
  ClientOutputStream &operator=(ClientOutputStream &&) = delete;

  bool Write(const uint8_t *data, size_t len, bool have_more = false);

  bool Write(const std::string &str, bool have_more = false);

 private:
  Client &client_;
};

}  // namespace memgraph::communication
