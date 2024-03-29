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

#pragma once

#include <optional>
#include <string>

#include <openssl/ssl.h>
#include <boost/asio/ssl/context.hpp>

// Centos 7 OpenSSL includes libkrb5 which has brings in macros TRUE and FALSE. undef to prevent issues.
#undef TRUE
#undef FALSE

namespace memgraph::communication {

/**
 * This class represents a context that should be used with network clients. One
 * context can be reused between multiple clients (note: this mainly depends on
 * the underlying OpenSSL implementation [see `SSL_new` in
 * `openssl/ssl/ssl_lib.c`]).
 */
class ClientContext final {
 public:
  /**
   * This constructor constructs a ClientContext that can either not use SSL
   * (`use_ssl` is `false` by default), or it constructs a ClientContext that
   * doesn't use a client certificate when `use_ssl` is set to `true`.
   */
  explicit ClientContext(bool use_ssl = false);

  /**
   * This constructor constructs a ClientContext that uses SSL and uses the
   * specific client private key and certificate combination. If the parameters
   * `key_file` and `cert_file` are equal to "" then the constructor falls back
   * to the above constructor that uses SSL without certificates.
   */
  ClientContext(const std::string &key_file, const std::string &cert_file);

  // This object can't be copied because the underlying SSL implementation is
  // messy and ownership can't be handled correctly.
  ClientContext(const ClientContext &) = delete;
  ClientContext &operator=(const ClientContext &) = delete;

  // Move constructor/assignment that handle ownership change correctly.
  ClientContext(ClientContext &&other) noexcept;
  ClientContext &operator=(ClientContext &&other) noexcept;

  // Destructor that handles ownership of the SSL object.
  ~ClientContext();

  SSL_CTX *context();

  bool use_ssl();

 private:
  bool use_ssl_;
  SSL_CTX *ctx_;
};

/**
 * This class represents a context that should be used with network servers. One
 * context can be reused between multiple servers (note: this mainly depends on
 * the underlying OpenSSL implementation [see `SSL_new` in
 * `openssl/ssl/ssl_lib.c`]).
 */
class ServerContext final {
 public:
  ServerContext() = default;
  /**
   * This constructor constructs a ServerContext that uses SSL. The parameters
   * `key_file` and `cert_file` can't be "" because when setting up a server it
   * is mandatory to supply a private key and certificate. The parameter
   * `ca_file` can be "" because SSL doesn't necessarily need to check that the
   * client has a valid certificate. If you specify `verify_peer` to be `true`
   * to check that the client certificate is valid, then you need to supply a
   * valid `ca_file` as well.
   */
  ServerContext(const std::string &key_file, const std::string &cert_file, const std::string &ca_file = "",
                bool verify_peer = false);

  // This object can't be copied because the underlying SSL implementation is
  // messy and ownership can't be handled correctly.
  ServerContext(const ServerContext &) = delete;
  ServerContext &operator=(const ServerContext &) = delete;

  // Move constructor/assignment that handle ownership change correctly.
  ServerContext(ServerContext &&other) noexcept;
  ServerContext &operator=(ServerContext &&other) noexcept;

  ~ServerContext();

  SSL_CTX *context();
  boost::asio::ssl::context &context_clone();

  bool use_ssl() const;

 private:
  std::optional<boost::asio::ssl::context> ctx_;
};

}  // namespace memgraph::communication
