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
  // SSL_CTX_set_cert_cb on BOTH the server and client contexts so outbound
  // NuRaft handshakes also pick up rotations per-handshake.
  auto Register(SSL_CTX *server_ctx, SSL_CTX *client_ctx, utils::TlsConfig cfg)
      -> std::expected<void, utils::SSL_CTX_Error>;

  struct X509Deleter {
    void operator()(X509 *p) const noexcept { X509_free(p); }
  };

  struct EvpKeyDeleter {
    void operator()(EVP_PKEY *p) const noexcept { EVP_PKEY_free(p); }
  };

  using X509Ptr = std::unique_ptr<X509, X509Deleter>;
  using EvpKeyPtr = std::unique_ptr<EVP_PKEY, EvpKeyDeleter>;

  // Parsed snapshot of the on-disk cert/key/CA at a specific point in time.
  // Exposed publicly only so `ReloadPrepared` can name it; callers never
  // construct one manually.
  struct CertData {
    X509Ptr leaf;
    std::vector<X509Ptr> chain;  // intermediates, may be empty
    EvpKeyPtr key;
    std::filesystem::file_time_type cert_mtime{};
    std::filesystem::file_time_type key_mtime{};
    std::filesystem::file_time_type ca_mtime{};
  };

  // Two-phase reload entry points. `Prepare` reads cert/key/CA from disk into
  // a fresh CertData candidate; no live state changes. `Commit` refreshes the
  // CA trust store on both SSL_CTX*s and then atomic-stores the candidate.
  //
  // Unlike `ClusterServerSsl::Commit` / `ClusterClientSsl::Commit` (which are
  // pure atomic stores), this Commit *can* fail: NuRaft owns the SSL_CTX and
  // we have to mutate it in place via `SSL_CTX_load_verify_locations`. We do
  // the mutation first; if it fails, no snapshot is published and the cluster
  // stays on the previous good state. Therefore the query handler must run
  // this Commit FIRST among the three TLS kingdoms — its failure is the only
  // one that can leave state un-mutated, so committing the others before it
  // would create a half-rotated cluster on failure.
  struct ReloadPrepared {
    std::shared_ptr<CertData> next;
  };

  [[nodiscard]] auto Prepare() -> std::expected<ReloadPrepared, utils::SSL_CTX_Error>;
  [[nodiscard]] auto Commit(ReloadPrepared prepared) noexcept -> std::expected<void, utils::SSL_CTX_Error>;

 private:
  CoordinatorCertReloader() = default;
  ~CoordinatorCertReloader() = default;

  // SSL_CTX_set_cert_cb entry. arg is `this`. Returns 1 on success, 0 on
  // failure (handshake aborts). Installed on BOTH server_ctx_ and client_ctx_
  // so both inbound and outbound NuRaft handshakes pick up rotation.
  static int CertCb(SSL *ssl, void *arg);

  // Loads cert+key from cfg_'s paths into a fresh CertData. Records the
  // observed mtimes of all three files.
  auto LoadFromDisk() -> std::expected<std::shared_ptr<CertData>, utils::SSL_CTX_Error>;

  // Lazy backstop path used by CertCb: stat-checks the three files; if any
  // mtime > the cached snapshot's, reloads from disk under reload_mu_,
  // refreshes CA on both SSL_CTX*s, publishes the new snapshot. No-op if
  // mtimes are unchanged. Separate from the explicit Prepare/Commit path
  // used by `RELOAD INTRA_CLUSTER TLS`.
  auto RefreshIfStale() -> std::expected<void, utils::SSL_CTX_Error>;

  // Refreshes the CA trust store on both contexts using the current cfg_.
  // Returns an error if SSL_CTX_load_verify_locations fails on either ctx.
  // Called under reload_mu_.
  auto RefreshCa() -> std::expected<void, utils::SSL_CTX_Error>;

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
