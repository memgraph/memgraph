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

#pragma once

#ifdef MG_ENTERPRISE

#include "utils/tls.hpp"

#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <atomic>
#include <expected>
#include <filesystem>
#include <memory>
#include <mutex>
#include <vector>

namespace memgraph::coordination {

// Process-wide singleton that hot-reloads the cert/key/CA files backing NuRaft's
// SSL_CTX. Patterned after ClickHouse's CertificateReloader: SSL_CTX_set_cert_cb
// fires on every new server-side TLS handshake, the callback stats the on-disk
// cert/key/CA files, and reloads + republishes the in-memory X509+EVP_PKEY if any
// mtime advanced. CA changes are applied by re-calling SSL_CTX_load_verify_locations
// on both contexts (server and client) under reload_mu_.
//
// Reads from the cert_cb hot path are lock-free (atomic shared_ptr swap of an
// immutable CertData snapshot). The mutex serializes the rare reload-from-disk
// path.
class CoordinatorCertReloader {
 public:
  static CoordinatorCertReloader &Instance();

  CoordinatorCertReloader(CoordinatorCertReloader const &) = delete;
  CoordinatorCertReloader &operator=(CoordinatorCertReloader const &) = delete;
  CoordinatorCertReloader(CoordinatorCertReloader &&) = delete;
  CoordinatorCertReloader &operator=(CoordinatorCertReloader &&) = delete;

  // Called once from raft_state.cpp via asio_opts.ssl_ctx_init_. Records both
  // SSL_CTX*s, snapshots the initial CertData from cfg's paths, installs
  // SSL_CTX_set_cert_cb on the server ctx.
  auto Register(SSL_CTX *server_ctx, SSL_CTX *client_ctx, utils::TlsConfig cfg)
      -> std::expected<void, utils::SSL_CTX_Error>;

  // Forces an mtime check + reload. Called by CoordinatorInstance::ReloadTls()
  // so `RELOAD INTRA_CLUSTER TLS` has immediate effect; the per-handshake check
  // is the lazy backstop.
  auto MaybeReload() -> std::expected<void, utils::SSL_CTX_Error>;

 private:
  CoordinatorCertReloader() = default;
  ~CoordinatorCertReloader() = default;

  struct X509Deleter {
    void operator()(X509 *p) const noexcept { X509_free(p); }
  };

  struct EvpKeyDeleter {
    void operator()(EVP_PKEY *p) const noexcept { EVP_PKEY_free(p); }
  };

  using X509Ptr = std::unique_ptr<X509, X509Deleter>;
  using EvpKeyPtr = std::unique_ptr<EVP_PKEY, EvpKeyDeleter>;

  struct CertData {
    X509Ptr leaf;
    std::vector<X509Ptr> chain;  // intermediates, may be empty
    EvpKeyPtr key;
    std::filesystem::file_time_type cert_mtime{};
    std::filesystem::file_time_type key_mtime{};
    std::filesystem::file_time_type ca_mtime{};
  };

  // SSL_CTX_set_cert_cb entry. arg is `this`. Returns 1 on success, 0 on
  // failure (handshake aborts).
  static int CertCb(SSL *ssl, void *arg);

  // Loads cert+key from cfg_'s paths into a fresh CertData. Records the
  // observed mtimes of all three files.
  auto LoadFromDisk() -> std::expected<std::shared_ptr<CertData>, utils::SSL_CTX_Error>;

  // Stat-checks the three files; if any mtime > the cached snapshot's, reloads
  // from disk under reload_mu_, refreshes CA on both SSL_CTX*s, publishes the
  // new snapshot. No-op if mtimes are unchanged.
  auto RefreshIfStale() -> std::expected<void, utils::SSL_CTX_Error>;

  // Hot path — applies a CertData to a per-connection SSL*.
  static int ApplyToSsl(SSL *ssl, CertData const &data);

  std::atomic<std::shared_ptr<CertData>> current_{};
  std::mutex reload_mu_;
  utils::TlsConfig cfg_;
  SSL_CTX *server_ctx_{nullptr};
  SSL_CTX *client_ctx_{nullptr};
};

}  // namespace memgraph::coordination

#endif  // MG_ENTERPRISE
