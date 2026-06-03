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

#include "utils/tls.hpp"

#include <boost/asio/ssl/context.hpp>

#include <atomic>
#include <expected>
#include <memory>

namespace memgraph::communication {

// Process-wide singletons that own the cluster-TLS server- and client-side
// SSL_CTX. Every cluster-TLS `ServerContext`/`ClientContext` constructed in
// `ClusterView` mode reads through the singleton's atomic snapshot, so reload
// is a single atomic swap that's observed by every cluster-TLS endpoint on the
// node.
//
// `Init` runs once at startup from `memgraph.cpp` after the cluster TLS flags
// are validated. Subsequent rotations go through the two-phase `Prepare` +
// `Commit` API exposed to the `RELOAD INTRA_CLUSTER TLS` query handler.
// `Prepare` builds a candidate `boost::asio::ssl::context` from the on-disk
// files without touching live state; `Commit` is a pure atomic store and
// cannot fail. The query handler runs `Prepare` for every TLS "kingdom"
// (server singleton, client singleton, NuRaft) before committing any of them,
// so an unparseable cert leaves the cluster on the previous good state.

struct ClusterServerReloadPrepared {
  std::shared_ptr<boost::asio::ssl::context> next_ctx;
};

class ClusterServerSsl {
 public:
  static ClusterServerSsl &Instance();

  ClusterServerSsl(ClusterServerSsl const &) = delete;
  ClusterServerSsl &operator=(ClusterServerSsl const &) = delete;
  ClusterServerSsl(ClusterServerSsl &&) = delete;
  ClusterServerSsl &operator=(ClusterServerSsl &&) = delete;

  // Called once at startup. Records cfg, builds the initial SSL_CTX, and
  // atomic-stores it. After this call returns successfully, `CurrentContext`
  // returns a non-null pointer and stays non-null for the rest of the process
  // lifetime.
  [[nodiscard]] auto Init(utils::TlsConfig cfg) -> std::expected<void, utils::SSL_CTX_Error>;

  // Phase 1 of reload: reread cert/key/CA from disk into a fresh
  // `boost::asio::ssl::context`. Live state is unchanged on success or
  // failure. The candidate is returned to the caller, who is expected to
  // gather candidates from every TLS subsystem before any Commit runs.
  [[nodiscard]] auto Prepare() -> std::expected<ClusterServerReloadPrepared, utils::SSL_CTX_Error>;

  // Phase 2 of reload: atomic-store the prepared candidate as the live
  // context. Pure, cannot fail. The previously-live context survives in
  // any `shared_ptr` held by an in-flight handshake until that handshake
  // releases it.
  void Commit(ClusterServerReloadPrepared prepared) noexcept;

  // Hot path: returns the live `boost::asio::ssl::context` via atomic load.
  // Null when `Init` was never called (no cluster TLS configured).
  std::shared_ptr<boost::asio::ssl::context> CurrentContext() const;

 private:
  ClusterServerSsl() = default;
  ~ClusterServerSsl() = default;

  // Shared build logic used by both Init and Prepare. Always reads from cfg_.
  auto Build() -> std::expected<std::shared_ptr<boost::asio::ssl::context>, utils::SSL_CTX_Error>;

  std::atomic<std::shared_ptr<boost::asio::ssl::context>> ctx_;
  utils::TlsConfig cfg_;
};

struct ClusterClientReloadPrepared {
  std::shared_ptr<boost::asio::ssl::context> next_ctx;
};

class ClusterClientSsl {
 public:
  static ClusterClientSsl &Instance();

  ClusterClientSsl(ClusterClientSsl const &) = delete;
  ClusterClientSsl &operator=(ClusterClientSsl const &) = delete;
  ClusterClientSsl(ClusterClientSsl &&) = delete;
  ClusterClientSsl &operator=(ClusterClientSsl &&) = delete;

  [[nodiscard]] auto Init(utils::TlsConfig cfg) -> std::expected<void, utils::SSL_CTX_Error>;
  [[nodiscard]] auto Prepare() -> std::expected<ClusterClientReloadPrepared, utils::SSL_CTX_Error>;
  void Commit(ClusterClientReloadPrepared prepared) noexcept;
  std::shared_ptr<boost::asio::ssl::context> CurrentContext() const;

 private:
  ClusterClientSsl() = default;
  ~ClusterClientSsl() = default;
  auto Build() -> std::expected<std::shared_ptr<boost::asio::ssl::context>, utils::SSL_CTX_Error>;

  std::atomic<std::shared_ptr<boost::asio::ssl::context>> ctx_;
  utils::TlsConfig cfg_;
};

}  // namespace memgraph::communication
