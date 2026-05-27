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

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_cert_reloader.hpp"
#include "utils/on_scope_exit.hpp"

#include <fmt/format.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <spdlog/spdlog.h>

#include <system_error>

namespace memgraph::coordination {

namespace {

auto OpenSslErrorString() -> std::string {
  unsigned long const err = ERR_peek_last_error();
  if (err == 0) return "no OpenSSL error queued";
  std::array<char, 256> buf{};
  ERR_error_string_n(err, buf.data(), buf.size());
  return std::string{buf.data()};
}

auto SafeMtime(std::string const &path) -> std::filesystem::file_time_type {
  std::error_code ec;
  auto t = std::filesystem::last_write_time(path, ec);
  if (ec) return {};
  return t;
}

}  // namespace

CoordinatorCertReloader &CoordinatorCertReloader::Instance() {
  static CoordinatorCertReloader instance;
  return instance;
}

auto CoordinatorCertReloader::Register(SSL_CTX *server_ctx, SSL_CTX *client_ctx, utils::TlsConfig cfg)
    -> std::expected<void, utils::SSL_CTX_Error> {
  std::lock_guard const guard{reload_mu_};
  server_ctx_ = server_ctx;
  client_ctx_ = client_ctx;
  cfg_ = std::move(cfg);

  auto loaded = LoadFromDisk();
  if (!loaded.has_value()) return std::unexpected{loaded.error()};
  current_.store(*loaded, std::memory_order_release);

  // Install SSL_CTX_set_cert_cb on BOTH server and client contexts. The cb
  // fires on every new handshake (inbound on server_ctx_, outbound on
  // client_ctx_) and applies the current snapshot's leaf + chain + key + CA
  // verify store to the per-connection SSL object (SSL_use_certificate /
  // SSL_use_PrivateKey / SSL_set1_verify_cert_store). Everything is applied
  // per-connection, so a reload never mutates these shared SSL_CTX*s and can't
  // race a concurrent handshake. Without the cb on client_ctx_, outbound
  // NuRaft handshakes would keep presenting whatever cert NuRaft loaded once
  // at init, leaving outbound rotation stuck on the old cert.
  SSL_CTX_set_cert_cb(server_ctx_, &CoordinatorCertReloader::CertCb, this);
  SSL_CTX_set_cert_cb(client_ctx_, &CoordinatorCertReloader::CertCb, this);

  spdlog::info(
      "Coordinator cert reloader registered (cert={}, key={}, ca={}).", cfg_.cert_file, cfg_.key_file, cfg_.ca_file);
  return {};
}

auto CoordinatorCertReloader::Prepare() -> std::expected<ReloadPrepared, utils::SSL_CTX_Error> {
  // Reload not active (Register was never called — no NuRaft / no TLS).
  // A successful Prepare with a null candidate is the right answer: Commit
  // will see a null candidate and do nothing.
  if (server_ctx_ == nullptr) return ReloadPrepared{.next = nullptr};

  auto loaded = LoadFromDisk();
  if (!loaded.has_value()) return std::unexpected{loaded.error()};
  return ReloadPrepared{.next = std::move(*loaded)};
}

void CoordinatorCertReloader::Commit(ReloadPrepared prepared) noexcept {
  if (!prepared.next) return;
  // Pure atomic store — the new snapshot already carries the parsed cert, key,
  // chain and verify store (built in Prepare/LoadFromDisk). Nothing on the
  // shared SSL_CTX is mutated, so this cannot fail and new handshakes pick up
  // the snapshot via the atomic load in CertCb.
  current_.store(std::move(prepared.next), std::memory_order_release);
  spdlog::info("Coordinator TLS committed (cert={}, key={}, ca={}).", cfg_.cert_file, cfg_.key_file, cfg_.ca_file);
}

auto CoordinatorCertReloader::LoadFromDisk() -> std::expected<std::shared_ptr<CertData>, utils::SSL_CTX_Error> {
  auto data = std::make_shared<CertData>();

  // --- Load the leaf cert + chain from cfg_.cert_file (PEM, may have chain).
  FILE *cert_fp = std::fopen(cfg_.cert_file.c_str(), "r");
  if (cert_fp == nullptr) {
    return std::unexpected{utils::SSL_CTX_Error{
        .err_type = utils::SSL_CTX_ERR_TYPE::FAIL_CERT_FILE,
        .msg = fmt::format("Couldn't open cert file '{}': {}", cfg_.cert_file, std::strerror(errno))}};
  }
  auto cert_fp_guard = utils::OnScopeExit{[cert_fp] { (void)std::fclose(cert_fp); }};

  X509 *leaf_raw = PEM_read_X509(cert_fp, nullptr, nullptr, nullptr);
  if (leaf_raw == nullptr) {
    return std::unexpected{utils::SSL_CTX_Error{
        .err_type = utils::SSL_CTX_ERR_TYPE::FAIL_CERT_FILE,
        .msg = fmt::format("Couldn't parse leaf cert from '{}': {}", cfg_.cert_file, OpenSslErrorString())}};
  }
  data->leaf.reset(leaf_raw);

  // Drain any intermediate certs that follow the leaf in the same PEM file.
  while (true) {
    X509 *next = PEM_read_X509(cert_fp, nullptr, nullptr, nullptr);
    if (next == nullptr) {
      // Treat EOF as success; clear the error queue so a stale "no start line"
      // doesn't get picked up later.
      ERR_clear_error();
      break;
    }
    data->chain.emplace_back(next);
  }

  // --- Load the private key from cfg_.key_file.
  FILE *key_fp = std::fopen(cfg_.key_file.c_str(), "r");
  if (key_fp == nullptr) {
    return std::unexpected{utils::SSL_CTX_Error{
        .err_type = utils::SSL_CTX_ERR_TYPE::FAIL_KEY_FILE,
        .msg = fmt::format("Couldn't open key file '{}': {}", cfg_.key_file, std::strerror(errno))}};
  }
  auto key_fp_guard = utils::OnScopeExit{[key_fp] { (void)std::fclose(key_fp); }};

  EVP_PKEY *key_raw = PEM_read_PrivateKey(key_fp, nullptr, nullptr, nullptr);
  if (key_raw == nullptr) {
    return std::unexpected{utils::SSL_CTX_Error{
        .err_type = utils::SSL_CTX_ERR_TYPE::FAIL_KEY_FILE,
        .msg = fmt::format("Couldn't parse private key from '{}': {}", cfg_.key_file, OpenSslErrorString())}};
  }
  data->key.reset(key_raw);

  // --- Build the CA verify store from cfg_.ca_file. This store is applied to
  // each per-connection SSL in CertCb (SSL_set1_verify_cert_store), so the CA
  // is never loaded onto the shared SSL_CTX — no in-place mutation, no race.
  X509_STORE *store_raw = X509_STORE_new();
  if (store_raw == nullptr) {
    return std::unexpected{utils::SSL_CTX_Error{.err_type = utils::SSL_CTX_ERR_TYPE::FAIL_LOAD_CA,
                                                .msg = "X509_STORE_new returned nullptr"}};
  }
  data->verify_store.reset(store_raw);
  if (X509_STORE_load_locations(store_raw, cfg_.ca_file.c_str(), nullptr) != 1) {
    return std::unexpected{
        utils::SSL_CTX_Error{.err_type = utils::SSL_CTX_ERR_TYPE::FAIL_LOAD_CA,
                             .msg = fmt::format("Couldn't load CA from '{}': {}", cfg_.ca_file, OpenSslErrorString())}};
  }

  data->cert_mtime = SafeMtime(cfg_.cert_file);
  data->key_mtime = SafeMtime(cfg_.key_file);
  data->ca_mtime = SafeMtime(cfg_.ca_file);

  return data;
}

auto CoordinatorCertReloader::RefreshIfStale() -> std::expected<void, utils::SSL_CTX_Error> {
  auto snapshot = current_.load(std::memory_order_acquire);

  auto const cert_mtime_now = SafeMtime(cfg_.cert_file);
  auto const key_mtime_now = SafeMtime(cfg_.key_file);
  auto const ca_mtime_now = SafeMtime(cfg_.ca_file);

  bool const stale = !snapshot ||                               //
                     cert_mtime_now != snapshot->cert_mtime ||  //
                     key_mtime_now != snapshot->key_mtime ||    //
                     ca_mtime_now != snapshot->ca_mtime;
  if (!stale) return {};

  std::lock_guard const guard{reload_mu_};
  // Double-check under the lock — another thread may have just reloaded.
  snapshot = current_.load(std::memory_order_acquire);
  if (snapshot && cert_mtime_now == snapshot->cert_mtime && key_mtime_now == snapshot->key_mtime &&
      ca_mtime_now == snapshot->ca_mtime) {
    return {};
  }

  auto loaded = LoadFromDisk();
  if (!loaded.has_value()) return std::unexpected{loaded.error()};

  current_.store(*loaded, std::memory_order_release);
  spdlog::info("Coordinator TLS reloaded via lazy mtime check (cert={}, key={}, ca={}).",
               cfg_.cert_file,
               cfg_.key_file,
               cfg_.ca_file);
  return {};
}

int CoordinatorCertReloader::CertCb(SSL *ssl, void *arg) {
  auto *self = static_cast<CoordinatorCertReloader *>(arg);

  if (auto r = self->RefreshIfStale(); !r.has_value()) {
    spdlog::error("Coordinator cert reload failed during handshake: {}", r.error().msg);
    // Don't abort the handshake just because reload failed — fall back to the
    // last good snapshot so the cluster stays up.
  }

  auto snapshot = self->current_.load(std::memory_order_acquire);
  if (!snapshot) {
    spdlog::error("Coordinator cert reloader has no snapshot during handshake — aborting.");
    return 0;
  }
  return ApplyToSsl(ssl, *snapshot);
}

int CoordinatorCertReloader::ApplyToSsl(SSL *ssl, CertData const &data) {
  if (SSL_clear_chain_certs(ssl) != 1) {
    spdlog::error("SSL_clear_chain_certs failed: {}", OpenSslErrorString());
    return 0;
  }
  if (SSL_use_certificate(ssl, data.leaf.get()) != 1) {
    spdlog::error("SSL_use_certificate failed: {}", OpenSslErrorString());
    return 0;
  }
  for (auto const &intermediate : data.chain) {
    // SSL_add1_chain_cert bumps the X509 refcount, so our unique_ptr keeps
    // ownership.
    if (SSL_add1_chain_cert(ssl, intermediate.get()) != 1) {
      spdlog::error("SSL_add1_chain_cert failed: {}", OpenSslErrorString());
      return 0;
    }
  }
  if (SSL_use_PrivateKey(ssl, data.key.get()) != 1) {
    spdlog::error("SSL_use_PrivateKey failed: {}", OpenSslErrorString());
    return 0;
  }
  // Install the CA trust store on this per-connection SSL (the "1" variant
  // bumps the X509_STORE refcount, so the snapshot's unique_ptr keeps ownership
  // and this SSL holds its own reference for the handshake's lifetime). This
  // replaces what would otherwise be a mutation of the shared SSL_CTX trust
  // store — keeping the whole apply per-connection and race-free.
  if (data.verify_store && SSL_set1_verify_cert_store(ssl, data.verify_store.get()) != 1) {
    spdlog::error("SSL_set1_verify_cert_store failed: {}", OpenSslErrorString());
    return 0;
  }
  return 1;
}

}  // namespace memgraph::coordination

#endif  // MG_ENTERPRISE
