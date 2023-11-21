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

#include "communication/context.hpp"
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/verify_mode.hpp>
#include <boost/system/detail/error_code.hpp>

#include "utils/logging.hpp"

namespace memgraph::communication {

ClientContext::ClientContext(bool use_ssl) : use_ssl_(use_ssl), ctx_(nullptr) {
  if (use_ssl_) {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    ctx_ = SSL_CTX_new(SSLv23_client_method());
#else
    ctx_ = SSL_CTX_new(TLS_client_method());
#endif
    MG_ASSERT(ctx_ != nullptr, "Couldn't create client SSL_CTX object!");

    // Disable legacy SSL support. Other options can be seen here:
    // https://www.openssl.org/docs/man1.0.2/ssl/SSL_CTX_set_options.html
    SSL_CTX_set_options(ctx_, SSL_OP_NO_SSLv3);
  }
}

ClientContext::ClientContext(const std::string &key_file, const std::string &cert_file) : ClientContext(true) {
  if (!key_file.empty() && !cert_file.empty()) {
    MG_ASSERT(SSL_CTX_use_certificate_file(ctx_, cert_file.c_str(), SSL_FILETYPE_PEM) == 1,
              "Couldn't load client certificate from file: {}", cert_file);
    MG_ASSERT(SSL_CTX_use_PrivateKey_file(ctx_, key_file.c_str(), SSL_FILETYPE_PEM) == 1,
              "Couldn't load client private key from file: ", key_file);
  }
}

ClientContext::ClientContext(ClientContext &&other) noexcept : use_ssl_(other.use_ssl_), ctx_(other.ctx_) {
  other.use_ssl_ = false;
  other.ctx_ = nullptr;
}

ClientContext &ClientContext::operator=(ClientContext &&other) noexcept {
  if (this == &other) return *this;

  // destroy my objects
  if (use_ssl_) {
    SSL_CTX_free(ctx_);
  }

  // move other objects to self
  use_ssl_ = other.use_ssl_;
  ctx_ = other.ctx_;

  // reset other objects
  other.use_ssl_ = false;
  other.ctx_ = nullptr;

  return *this;
}

ClientContext::~ClientContext() {
  if (use_ssl_) {
    SSL_CTX_free(ctx_);
  }
}

SSL_CTX *ClientContext::context() { return ctx_; }

bool ClientContext::use_ssl() { return use_ssl_; }

ServerContext::ServerContext(const std::string &key_file, const std::string &cert_file, const std::string &ca_file,
                             bool verify_peer) {
  namespace ssl = boost::asio::ssl;
  ctx_.emplace(ssl::context::tls_server);
  // NOLINTNEXTLINE(hicpp-signed-bitwise)
  ctx_->set_options(ssl::context::default_workarounds | ssl::context::no_sslv2 | ssl::context::no_sslv3 |
                    ssl::context::single_dh_use);
  ctx_->set_default_verify_paths();
  // TODO: add support for encrypted private keys
  // TODO: add certificate revocation list (CRL)
  boost::system::error_code ec;
  ctx_->use_certificate_chain_file(cert_file, ec);
  MG_ASSERT(!ec, "Couldn't load server certificate from file: {}", cert_file);
  ctx_->use_private_key_file(key_file, ssl::context::pem, ec);
  MG_ASSERT(!ec, "Couldn't load server private key from file: {}", key_file);

  ctx_->set_options(SSL_OP_NO_SSLv3, ec);
  MG_ASSERT(!ec, "Setting options to SSL context failed!");

  if (!ca_file.empty()) {
    // Load the certificate authority file.
    boost::system::error_code ec;
    ctx_->load_verify_file(ca_file, ec);
    MG_ASSERT(!ec, "Couldn't load certificate authority from file: {}", ca_file);

    if (verify_peer) {
      // Enable verification of the client certificate.
      // NOLINTNEXTLINE(hicpp-signed-bitwise)
      ctx_->set_verify_mode(ssl::verify_peer | ssl::verify_fail_if_no_peer_cert, ec);
      MG_ASSERT(!ec, "Setting SSL verification mode failed!");
    }
  }
}

ServerContext::ServerContext(ServerContext &&other) noexcept { std::swap(ctx_, other.ctx_); }

ServerContext &ServerContext::operator=(ServerContext &&other) noexcept {
  if (this == &other) return *this;

  // move other objects to self
  ctx_ = std::move(other.ctx_);

  // reset other objects
  other.ctx_.reset();

  return *this;
}

ServerContext::~ServerContext() {}

SSL_CTX *ServerContext::context() {
  MG_ASSERT(ctx_);
  return ctx_->native_handle();
}

boost::asio::ssl::context &ServerContext::context_clone() {
  MG_ASSERT(ctx_);
  return *ctx_;
}

bool ServerContext::use_ssl() const { return ctx_.has_value(); }

}  // namespace memgraph::communication
