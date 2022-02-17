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

#include "communication/context.hpp"

#include "utils/logging.hpp"

namespace communication {

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
  if (key_file != "" && cert_file != "") {
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
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  auto *ctx = SSL_CTX_new(SSLv23_server_method());
#else
  auto *ctx = SSL_CTX_new(TLS_server_method());
#endif
  // TODO (mferencevic): add support for encrypted private keys
  // TODO (mferencevic): add certificate revocation list (CRL)
  MG_ASSERT(SSL_CTX_use_certificate_file(ctx, cert_file.c_str(), SSL_FILETYPE_PEM) == 1,
            "Couldn't load server certificate from file: {}", cert_file);
  MG_ASSERT(SSL_CTX_use_PrivateKey_file(ctx, key_file.c_str(), SSL_FILETYPE_PEM) == 1,
            "Couldn't load server private key from file: {}", key_file);

  // Disable legacy SSL support. Other options can be seen here:
  // https://www.openssl.org/docs/man1.0.2/ssl/SSL_CTX_set_options.html
  SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv3);

  if (ca_file != "") {
    // Load the certificate authority file.
    MG_ASSERT(SSL_CTX_load_verify_locations(ctx, ca_file.c_str(), nullptr) == 1,
              "Couldn't load certificate authority from file: {}", ca_file);

    if (verify_peer) {
      // Add the CA to list of accepted CAs that is sent to the client.
      STACK_OF(X509_NAME) *ca_names = SSL_load_client_CA_file(ca_file.c_str());
      MG_ASSERT(ca_names != nullptr, "Couldn't load certificate authority from file: {}", ca_file);
      // `ca_names` doesn' need to be free'd because we pass it to
      // `SSL_CTX_set_client_CA_list`:
      // https://mta.openssl.org/pipermail/openssl-users/2015-May/001363.html
      SSL_CTX_set_client_CA_list(ctx, ca_names);

      // Enable verification of the client certificate.
      // NOLINTNEXTLINE(hicpp-signed-bitwise)
      SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
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

}  // namespace communication
