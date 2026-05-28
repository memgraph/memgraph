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

#include "communication/cluster_tls.hpp"

#include <fmt/format.h>
#include <openssl/ssl.h>
#include <spdlog/spdlog.h>
#include <boost/asio/ssl.hpp>
#include <boost/system/error_code.hpp>

#include <cstdint>
#include <utility>

namespace memgraph::communication {

namespace {

enum class ClusterTlsRole : std::uint8_t { kServer, kClient };

// Shared `Build()` body. The two cluster-TLS singletons differ in only four
// places: the SSL_CTX method (tls_server vs tls_client), the option set, the
// verify mode, and the log/error-message prefix — everything else (key/cert/CA
// loading, error handling, CA-required-for-mTLS check) is identical.
auto BuildClusterSslContext(utils::TlsConfig const &cfg, ClusterTlsRole role)
    -> std::expected<std::shared_ptr<boost::asio::ssl::context>, utils::SSL_CTX_Error> {
  namespace ssl = boost::asio::ssl;
  char const *const label = role == ClusterTlsRole::kServer ? "Cluster server TLS" : "Cluster client TLS";

  if (cfg.key_file.empty() || cfg.cert_file.empty()) {
    return std::unexpected{utils::SSL_CTX_Error{.err_type = utils::SSL_CTX_ERR_TYPE::FLAGS_NOT_CONFIGURED,
                                                .msg = fmt::format("{}: key or certificate file is empty.", label)}};
  }

  auto new_ctx = std::make_shared<ssl::context>(role == ClusterTlsRole::kServer ? ssl::context::tls_server
                                                                                : ssl::context::tls_client);

  if (role == ClusterTlsRole::kServer) {
    // NOLINTNEXTLINE(hicpp-signed-bitwise)
    new_ctx->set_options(ssl::context::default_workarounds | ssl::context::no_sslv2 | ssl::context::no_sslv3 |
                         ssl::context::single_dh_use);
  } else {
    // NOLINTNEXTLINE(bugprone-unused-return-value)
    new_ctx->set_options(SSL_OP_NO_SSLv3 | SSL_OP_NO_SSLv2);
  }

  // We deliberately do NOT call `set_default_verify_paths()`. Intra-cluster
  // mTLS uses the operator-supplied CA as the SOLE trust anchor; unioning
  // with the OS root store would let any publicly-issued cert authenticate
  // against the cluster.

  boost::system::error_code ec;
  // NOLINTNEXTLINE(bugprone-unused-return-value)
  new_ctx->use_certificate_chain_file(cfg.cert_file, ec);
  if (ec) {
    auto msg = fmt::format("{}: couldn't load certificate from {}: {}", label, cfg.cert_file, ec.message());
    spdlog::error(msg);
    return std::unexpected{
        utils::SSL_CTX_Error{.err_type = utils::SSL_CTX_ERR_TYPE::FAIL_CERT_FILE, .msg = std::move(msg)}};
  }
  // NOLINTNEXTLINE(bugprone-unused-return-value)
  new_ctx->use_private_key_file(cfg.key_file, ssl::context::pem, ec);
  if (ec) {
    auto msg = fmt::format("{}: couldn't load private key from {}: {}", label, cfg.key_file, ec.message());
    spdlog::error(msg);
    return std::unexpected{
        utils::SSL_CTX_Error{.err_type = utils::SSL_CTX_ERR_TYPE::FAIL_KEY_FILE, .msg = std::move(msg)}};
  }

  if (cfg.ca_file.empty()) {
    return std::unexpected{
        utils::SSL_CTX_Error{.err_type = utils::SSL_CTX_ERR_TYPE::FAIL_LOAD_CA,
                             .msg = fmt::format("{} requires a CA file (mTLS). cfg.ca_file is empty.", label)}};
  }

  // NOLINTNEXTLINE(bugprone-unused-return-value)
  new_ctx->load_verify_file(cfg.ca_file, ec);
  if (ec) {
    auto msg = fmt::format("{}: couldn't load CA from {}: {}", label, cfg.ca_file, ec.message());
    spdlog::error(msg);
    return std::unexpected{
        utils::SSL_CTX_Error{.err_type = utils::SSL_CTX_ERR_TYPE::FAIL_LOAD_CA, .msg = std::move(msg)}};
  }

  // NOLINTNEXTLINE(hicpp-signed-bitwise, bugprone-unused-return-value)
  new_ctx->set_verify_mode(
      role == ClusterTlsRole::kServer ? (ssl::verify_peer | ssl::verify_fail_if_no_peer_cert) : ssl::verify_peer, ec);
  if (ec) {
    auto msg = fmt::format("{}: set_verify_mode failed: {}", label, ec.message());
    spdlog::error(msg);
    return std::unexpected{utils::SSL_CTX_Error{.err_type = utils::SSL_CTX_ERR_TYPE::FAIL_SET_SSL_VERIFICATION_MODE,
                                                .msg = std::move(msg)}};
  }

  return new_ctx;
}

}  // namespace

ClusterServerSsl &ClusterServerSsl::Instance() {
  static ClusterServerSsl instance;
  return instance;
}

auto ClusterServerSsl::Init(utils::TlsConfig cfg) -> std::expected<void, utils::SSL_CTX_Error> {
  cfg_ = std::move(cfg);
  auto built = Build();
  if (!built.has_value()) return std::unexpected{built.error()};
  ctx_.store(*built, std::memory_order_release);
  spdlog::info("Cluster server TLS initialized (cert={}, key={}, ca={}).", cfg_.cert_file, cfg_.key_file, cfg_.ca_file);
  return {};
}

auto ClusterServerSsl::Prepare() -> std::expected<ClusterServerReloadPrepared, utils::SSL_CTX_Error> {
  auto built = Build();
  if (!built.has_value()) return std::unexpected{built.error()};
  return ClusterServerReloadPrepared{.next_ctx = std::move(*built)};
}

void ClusterServerSsl::Commit(ClusterServerReloadPrepared prepared) noexcept {
  ctx_.store(std::move(prepared.next_ctx), std::memory_order_release);
  spdlog::info("Cluster server TLS reloaded (cert={}, key={}, ca={}).", cfg_.cert_file, cfg_.key_file, cfg_.ca_file);
}

std::shared_ptr<boost::asio::ssl::context> ClusterServerSsl::CurrentContext() const {
  return ctx_.load(std::memory_order_acquire);
}

auto ClusterServerSsl::Build() -> std::expected<std::shared_ptr<boost::asio::ssl::context>, utils::SSL_CTX_Error> {
  return BuildClusterSslContext(cfg_, ClusterTlsRole::kServer);
}

ClusterClientSsl &ClusterClientSsl::Instance() {
  static ClusterClientSsl instance;
  return instance;
}

auto ClusterClientSsl::Init(utils::TlsConfig cfg) -> std::expected<void, utils::SSL_CTX_Error> {
  cfg_ = std::move(cfg);
  auto built = Build();
  if (!built.has_value()) return std::unexpected{built.error()};
  ctx_.store(*built, std::memory_order_release);
  spdlog::info("Cluster client TLS initialized (cert={}, key={}, ca={}).", cfg_.cert_file, cfg_.key_file, cfg_.ca_file);
  return {};
}

auto ClusterClientSsl::Prepare() -> std::expected<ClusterClientReloadPrepared, utils::SSL_CTX_Error> {
  auto built = Build();
  if (!built.has_value()) return std::unexpected{built.error()};
  return ClusterClientReloadPrepared{.next_ctx = std::move(*built)};
}

void ClusterClientSsl::Commit(ClusterClientReloadPrepared prepared) noexcept {
  ctx_.store(std::move(prepared.next_ctx), std::memory_order_release);
  spdlog::info("Cluster client TLS reloaded (cert={}, key={}, ca={}).", cfg_.cert_file, cfg_.key_file, cfg_.ca_file);
}

std::shared_ptr<boost::asio::ssl::context> ClusterClientSsl::CurrentContext() const {
  return ctx_.load(std::memory_order_acquire);
}

auto ClusterClientSsl::Build() -> std::expected<std::shared_ptr<boost::asio::ssl::context>, utils::SSL_CTX_Error> {
  return BuildClusterSslContext(cfg_, ClusterTlsRole::kClient);
}

}  // namespace memgraph::communication
