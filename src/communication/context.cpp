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

#include "communication/context.hpp"

#include <openssl/opensslv.h>
#include <openssl/ssl.h>
#include <boost/asio/ssl.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/verify_mode.hpp>
#include <boost/system/detail/error_code.hpp>
#include <utility>

#include "communication/cluster_tls.hpp"
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
    SSL_CTX_set_options(ctx_, SSL_OP_NO_SSLv3 | SSL_OP_NO_SSLv2);
  }
}

ClientContext::ClientContext(const std::string &key_file, const std::string &cert_file) : ClientContext(true) {
  if (!key_file.empty() && !cert_file.empty()) {
    MG_ASSERT(SSL_CTX_use_certificate_file(ctx_, cert_file.c_str(), SSL_FILETYPE_PEM) == 1,
              "Couldn't load client certificate from file: {}",
              cert_file);
    MG_ASSERT(SSL_CTX_use_PrivateKey_file(ctx_, key_file.c_str(), SSL_FILETYPE_PEM) == 1,
              "Couldn't load client private key from file: {}",
              key_file);
  }
}

ClientContext::ClientContext(const std::string &key_file, const std::string &cert_file, const std::string &ca_file)
    : ClientContext(key_file, cert_file) {
  if (!ca_file.empty()) {
    MG_ASSERT(
        SSL_CTX_load_verify_locations(ctx_, ca_file.c_str(), nullptr) == 1, "Couldn't load CA from file {}", ca_file);
    SSL_CTX_set_verify(ctx_, SSL_VERIFY_PEER, nullptr);
  }
}

ClientContext ClientContext::FromClusterSingleton() { return ClientContext{Mode::ClusterView}; }

ClientContext::ClientContext(ClientContext &&other) noexcept
    : mode_(other.mode_), use_ssl_(other.use_ssl_), ctx_(other.ctx_) {
  other.mode_ = Mode::Standalone;
  other.use_ssl_ = false;
  other.ctx_ = nullptr;
}

ClientContext &ClientContext::operator=(ClientContext &&other) noexcept {
  if (this == &other) return *this;

  // destroy my objects (Standalone only — ClusterView doesn't own ctx_)
  if (mode_ == Mode::Standalone && use_ssl_) {
    SSL_CTX_free(ctx_);
  }

  // move other objects to self
  mode_ = other.mode_;
  use_ssl_ = other.use_ssl_;
  ctx_ = other.ctx_;

  // reset other objects
  other.mode_ = Mode::Standalone;
  other.use_ssl_ = false;
  other.ctx_ = nullptr;

  return *this;
}

ClientContext::~ClientContext() {
  if (mode_ == Mode::Standalone && use_ssl_) {
    SSL_CTX_free(ctx_);
  }
}

SSL_CTX *ClientContext::context() {
  if (mode_ == Mode::ClusterView) {
    auto ptr = ClusterClientSsl::Instance().CurrentContext();
    MG_ASSERT(ptr, "ClusterView ClientContext used before ClusterClientSsl::Init");
    return ptr->native_handle();
  }
  return ctx_;
}

auto ClientContext::use_ssl() const -> bool {
  if (mode_ == Mode::ClusterView) {
    return ClusterClientSsl::Instance().CurrentContext() != nullptr;
  }
  return use_ssl_;
}

ServerContext::ServerContext(std::string key_file, std::string cert_file, std::string ca_file, bool const verify_peer)
    : mode_(Mode::Standalone),
      key_file_(std::move(key_file)),
      cert_file_(std::move(cert_file)),
      ca_file_(std::move(ca_file)),
      verify_peer_(verify_peer) {
  if (auto const res = reload(); !res.has_value()) {
    LOG_FATAL(res.error().msg);
  }
}

ServerContext ServerContext::FromClusterSingleton() { return ServerContext{Mode::ClusterView}; }

ServerContext::~ServerContext() = default;

SSL_CTX *ServerContext::context() {
  if (mode_ == Mode::ClusterView) {
    auto ptr = ClusterServerSsl::Instance().CurrentContext();
    MG_ASSERT(ptr, "ClusterView ServerContext used before ClusterServerSsl::Init");
    return ptr->native_handle();
  }
  auto ptr = ctx_.load(std::memory_order_acquire);
  MG_ASSERT(ptr, "Trying to use uninitialized SSL context");
  return ptr->native_handle();
}

std::shared_ptr<boost::asio::ssl::context> ServerContext::context_clone() {
  auto ptr =
      mode_ == Mode::ClusterView ? ClusterServerSsl::Instance().CurrentContext() : ctx_.load(std::memory_order_acquire);
  MG_ASSERT(ptr, "Trying to use uninitialized SSL context");
  return ptr;
}

bool ServerContext::use_ssl() const {
  if (mode_ == Mode::ClusterView) {
    return ClusterServerSsl::Instance().CurrentContext() != nullptr;
  }
  return ctx_.load(std::memory_order_acquire) != nullptr;
}

auto ServerContext::reload() -> std::expected<void, utils::SSL_CTX_Error> {
  // ClusterView contexts are read-only views of the cluster TLS singleton.
  // Callers must reload via `ClusterServerSsl::Instance().{Prepare,Commit}`
  // directly; calling reload() on a view is a programming error.
  MG_ASSERT(mode_ == Mode::Standalone,
            "ServerContext::reload() called on a ClusterView context — use ClusterServerSsl directly");
  if (key_file_.empty() || cert_file_.empty()) {
    return std::unexpected{
        utils::SSL_CTX_Error{.err_type = utils::SSL_CTX_ERR_TYPE::FLAGS_NOT_CONFIGURED,
                             .msg = "Unable to reload SSL configuration because key or certificate file aren't set."}};
  }

  namespace ssl = boost::asio::ssl;

  auto new_ctx = std::make_shared<boost::asio::ssl::context>(ssl::context::tls_server);

  // NOLINTNEXTLINE(hicpp-signed-bitwise)
  new_ctx->set_options(ssl::context::default_workarounds | ssl::context::no_sslv2 | ssl::context::no_sslv3 |
                       ssl::context::single_dh_use);

  // We deliberately do NOT call `set_default_verify_paths()` here. The trust
  // store is consulted only when `verify_peer_` is true (i.e. on the
  // intra-cluster mTLS path), and there the operator-supplied `ca_file_` must
  // be the SOLE trust anchor — unioning it with the OS root store would let
  // any publicly-issued cert authenticate against the cluster, which defeats
  // the purpose of the private CA. For Bolt's server-only TLS (verify_peer
  // is false) this changes nothing because the trust store is never consulted.

  // TODO: add support for encrypted private keys
  // TODO: add certificate revocation list (CRL)
  boost::system::error_code ec;
  // NOLINTNEXTLINE(bugprone-unused-return-value)
  new_ctx->use_certificate_chain_file(cert_file_, ec);
  if (ec) {
    auto err_msg = fmt::format("Couldn't load server certificate from file {}. Error: {}", cert_file_, ec.message());
    spdlog::error(err_msg);
    return std::unexpected{
        utils::SSL_CTX_Error{.err_type = utils::SSL_CTX_ERR_TYPE::FAIL_CERT_FILE, .msg = std::move(err_msg)}};
  }
  // NOLINTNEXTLINE(bugprone-unused-return-value)
  new_ctx->use_private_key_file(key_file_, ssl::context::pem, ec);
  if (ec) {
    auto err_msg = fmt::format("Couldn't load server private key from file {}. Error: {}", key_file_, ec.message());
    spdlog::error(err_msg);
    return std::unexpected{
        utils::SSL_CTX_Error{.err_type = utils::SSL_CTX_ERR_TYPE::FAIL_KEY_FILE, .msg = std::move(err_msg)}};
  }

  if (!ca_file_.empty()) {
    // Load the certificate authority file.
    boost::system::error_code ec;
    // NOLINTNEXTLINE(bugprone-unused-return-value)
    new_ctx->load_verify_file(ca_file_, ec);
    if (ec) {
      auto err_msg = fmt::format("Couldn't load certificate authority from file {}. Error: {}", ca_file_, ec.message());
      spdlog::error(err_msg);
      return std::unexpected{
          utils::SSL_CTX_Error{.err_type = utils::SSL_CTX_ERR_TYPE::FAIL_LOAD_CA, .msg = std::move(err_msg)}};
    }

    if (verify_peer_) {
      // Enable verification of the client certificate.
      // NOLINTNEXTLINE(hicpp-signed-bitwise, bugprone-unused-return-value)
      new_ctx->set_verify_mode(ssl::verify_peer | ssl::verify_fail_if_no_peer_cert, ec);
      if (ec) {
        auto err_msg = fmt::format("Setting SSL verification mode failed! Error: {}", ec.message());
        spdlog::error(err_msg);
        return std::unexpected{utils::SSL_CTX_Error{.err_type = utils::SSL_CTX_ERR_TYPE::FAIL_SET_SSL_VERIFICATION_MODE,
                                                    .msg = std::move(err_msg)}};
      }
    }
  }
  ctx_.store(std::move(new_ctx), std::memory_order_release);
  return {};
}

}  // namespace memgraph::communication
