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
#include <openssl/x509_vfy.h>  // X509_STORE, X509_STORE_free

#include <atomic>
#include <expected>
#include <filesystem>
#include <memory>
#include <mutex>
#include <vector>

namespace memgraph::coordination {

// Process-wide singleton that hot-reloads the cert/key/CA files backing NuRaft's
// SSL_CTX. Patterned after ClickHouse's CertificateReloader: SSL_CTX_set_cert_cb
// fires on every new TLS handshake (server- and client-side), the callback stats
// the on-disk cert/key/CA files, and reloads + republishes an immutable snapshot
// (X509 leaf + chain, EVP_PKEY, and a CA X509_STORE) if any mtime advanced.
//
// Everything is applied to the per-connection SSL — cert/key/chain via
// SSL_use_certificate/SSL_use_PrivateKey and the CA via SSL_set1_verify_cert_store.
// The shared SSL_CTX*s are never mutated after Register, so reload can't race a
// concurrent handshake. Reads from the cert_cb hot path are lock-free (atomic
// shared_ptr load); the mutex only serializes the rare reload-from-disk path so
// concurrent stale-detections don't redundantly re-parse the files.
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

  struct X509StoreDeleter {
    void operator()(X509_STORE *p) const noexcept { X509_STORE_free(p); }
  };

  using X509Ptr = std::unique_ptr<X509, X509Deleter>;
  using EvpKeyPtr = std::unique_ptr<EVP_PKEY, EvpKeyDeleter>;
  using X509StorePtr = std::unique_ptr<X509_STORE, X509StoreDeleter>;

  // Parsed, immutable snapshot of the on-disk cert/key/CA at a specific point
  // in time. Everything a handshake needs is here — including the verify store
  // built from the CA file — so `CertCb` applies it all to the per-connection
  // SSL and never mutates a shared SSL_CTX. Exposed publicly only so
  // `ReloadPrepared` can name it; callers never construct one manually.
  struct CertData {
    X509Ptr leaf;
    std::vector<X509Ptr> chain;  // intermediates, may be empty
    EvpKeyPtr key;
    X509StorePtr verify_store;  // trust store built from the CA file
    std::filesystem::file_time_type cert_mtime{};
    std::filesystem::file_time_type key_mtime{};
    std::filesystem::file_time_type ca_mtime{};
  };

  // Two-phase reload entry points. `Prepare` reads cert/key/CA from disk into a
  // fresh CertData candidate (including building the verify store) — all the
  // fallible work happens here; no live state changes. `Commit` is a pure
  // atomic store of the candidate snapshot and cannot fail, exactly like
  // `ClusterServerSsl::Commit` / `ClusterClientSsl::Commit`. New handshakes
  // pick the new snapshot up via the atomic load in `CertCb`; nothing on the
  // shared SSL_CTX is mutated, so there's no race with concurrent handshakes.
  struct ReloadPrepared {
    std::shared_ptr<CertData> next;
  };

  [[nodiscard]] auto Prepare() -> std::expected<ReloadPrepared, utils::SSL_CTX_Error>;
  void Commit(ReloadPrepared prepared) noexcept;

 private:
  CoordinatorCertReloader() = default;
  ~CoordinatorCertReloader() = default;

  // SSL_CTX_set_cert_cb entry. arg is `this`. Returns 1 on success, 0 on
  // failure (handshake aborts). Installed on BOTH server_ctx_ and client_ctx_
  // so both inbound and outbound NuRaft handshakes pick up rotation.
  static int CertCb(SSL *ssl, void *arg);

  // Loads cert+key from cfg_'s paths and builds the CA verify store into a
  // fresh CertData. Records the observed mtimes of all three files. This is
  // where all parse failures (bad cert, key, or CA file) surface.
  auto LoadFromDisk() -> std::expected<std::shared_ptr<CertData>, utils::SSL_CTX_Error>;

  // Lazy backstop path used by CertCb: stat-checks the three files; if any
  // mtime advanced vs the cached snapshot, reloads from disk under reload_mu_
  // and atomic-stores the new snapshot. No-op if mtimes are unchanged.
  // Separate from the explicit Prepare/Commit path used by
  // `RELOAD INTRA_CLUSTER TLS`.
  auto RefreshIfStale() -> std::expected<void, utils::SSL_CTX_Error>;

  // Hot path — applies a CertData (leaf, chain, key, AND verify store) to a
  // per-connection SSL*. Touches only the per-connection SSL, never the shared
  // SSL_CTX, so it's safe under concurrent handshakes.
  static int ApplyToSsl(SSL *ssl, CertData const &data);

  std::atomic<std::shared_ptr<CertData>> current_{};
  std::mutex reload_mu_;
  // Written once in Register() at startup before NuRaft accepts any
  // connections and before the RELOAD INTRA_CLUSTER TLS query handler is
  // reachable. After that, cfg_ is effectively const and all readers
  // (Prepare, Commit, RefreshIfStale, LoadFromDisk) access it lock-free.
  // registered_ enforces the "written once" invariant via DMG_ASSERT.
  std::atomic<bool> registered_{false};
  utils::TlsConfig cfg_;
  SSL_CTX *server_ctx_{nullptr};
  SSL_CTX *client_ctx_{nullptr};
};

}  // namespace memgraph::coordination

#endif  // MG_ENTERPRISE
