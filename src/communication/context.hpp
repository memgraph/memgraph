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

#include <openssl/ssl.h>
#include <openssl/types.h>
#include <atomic>
#include <boost/asio/ssl/context.hpp>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>

#include "utils/tls.hpp"

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
  // ClientContext has two operating modes:
  //   * Standalone — owns its own SSL_CTX, built from constructor args. Used
  //     by Bolt clients and tests with explicit cert/key/ca paths.
  //   * ClusterView — delegates to the process-wide `ClusterClientSsl`
  //     singleton. Used by every intra-cluster client (replication, peer
  //     coordinator, peer data instance) so they all pick up cert/key/CA
  //     reload via a single atomic store on the singleton.
  enum class Mode : std::uint8_t { Standalone, ClusterView };

  /**
   * Standalone, no SSL (or SSL without a client cert if `use_ssl` is true).
   */
  explicit ClientContext(bool use_ssl = false);

  /**
   * Standalone with client cert+key but no CA verification.
   */
  ClientContext(const std::string &key_file, const std::string &cert_file);

  /**
   * Standalone with cert+key+CA (full mTLS shape).
   */
  ClientContext(const std::string &key_file, const std::string &cert_file, const std::string &ca_file);

  // Produces a ClusterView-mode ClientContext. `ClusterClientSsl::Instance()`
  // must already be initialized (via memgraph.cpp startup wiring) — otherwise
  // `context()` will assert.
  static ClientContext FromClusterSingleton();

  // This object can't be copied because the underlying SSL implementation is
  // messy and ownership can't be handled correctly.
  ClientContext(const ClientContext &) = delete;
  ClientContext &operator=(const ClientContext &) = delete;

  // Move constructor/assignment that handle ownership change correctly.
  ClientContext(ClientContext &&other) noexcept;
  ClientContext &operator=(ClientContext &&other) noexcept;

  // Destructor that handles ownership of the SSL object.
  ~ClientContext();

  // Returns the live SSL_CTX. In Standalone mode this is the owned `ctx_`;
  // in ClusterView it's an atomic load from the singleton (so the result
  // reflects any reload that happened since the last call).
  SSL_CTX *context();

  auto use_ssl() const -> bool;

 private:
  // Private mode-taking constructor. Only used internally by
  // FromClusterSingleton to produce a ClusterView. Standalone construction
  // goes through the public constructors above.
  explicit ClientContext(Mode mode) : mode_(mode) {}

  Mode mode_{Mode::Standalone};
  // Standalone-mode state. Unused (but harmless) in ClusterView.
  bool use_ssl_{false};
  SSL_CTX *ctx_{nullptr};
};

/**
 * This class represents a context that should be used with network servers. One
 * context can be reused between multiple servers (note: this mainly depends on
 * the underlying OpenSSL implementation [see `SSL_new` in
 * `openssl/ssl/ssl_lib.c`]).
 */
class ServerContext final {
 public:
  // ServerContext mirrors ClientContext's two-mode shape:
  //   * Standalone — owns its own SSL_CTX, built from constructor args.
  //     Used by Bolt's server and by tests.
  //   * ClusterView — delegates to the process-wide `ClusterServerSsl`
  //     singleton. Used by every intra-cluster server (replication,
  //     data-instance management, coordinator-instance management) so they
  //     all pick up cert/key/CA reload via a single atomic store on the
  //     singleton.
  enum class Mode : std::uint8_t { Standalone, ClusterView };

  ServerContext() = default;
  /**
   * This constructor constructs a Standalone ServerContext that uses SSL.
   * The parameters `key_file` and `cert_file` can't be "" because when
   * setting up a server it is mandatory to supply a private key and
   * certificate. The parameter `ca_file` can be "" because SSL doesn't
   * necessarily need to check that the client has a valid certificate. If
   * you specify `verify_peer` to be `true` to check that the client
   * certificate is valid, then you need to supply a valid `ca_file` as well.
   */
  ServerContext(std::string key_file, std::string cert_file, std::string ca_file = "", bool verify_peer = false);

  // Produces a ClusterView-mode ServerContext. `ClusterServerSsl::Instance()`
  // must already be initialized (via memgraph.cpp startup wiring) — otherwise
  // `context()` will assert.
  static ServerContext FromClusterSingleton();

  // This object can't be copied because the underlying SSL implementation is
  // messy and ownership can't be handled correctly.
  ServerContext(const ServerContext &) = delete;
  ServerContext &operator=(const ServerContext &) = delete;
  ServerContext(ServerContext &&other) = delete;
  ServerContext &operator=(ServerContext &&other) = delete;

  ~ServerContext();

  // Returns the live SSL_CTX. In ClusterView mode this is an atomic load
  // from the singleton, so the result reflects any reload that happened
  // since the last call.
  SSL_CTX *context();
  std::shared_ptr<boost::asio::ssl::context> context_clone();

  bool use_ssl() const;

  // Reload only makes sense in Standalone mode. In ClusterView mode reload
  // must go through `ClusterServerSsl::Instance().{Prepare,Commit}` directly;
  // calling this asserts (the per-class `ReloadTls()` plumbing that used to
  // call this on cluster contexts is deleted).
  [[nodiscard]] auto reload() -> std::expected<void, utils::SSL_CTX_Error>;

 private:
  // Private mode-taking constructor. Only used internally by
  // FromClusterSingleton to produce a ClusterView. Standalone construction
  // goes through the default ctor or the public cert-file constructor above.
  explicit ServerContext(Mode mode) : mode_(mode) {}

  Mode mode_{Mode::Standalone};
  // Standalone-mode state. Unused (but harmless) in ClusterView.
  std::string key_file_;
  std::string cert_file_;
  std::string ca_file_;
  bool verify_peer_{false};
  std::atomic<std::shared_ptr<boost::asio::ssl::context>> ctx_;
};

// Helpers that pick the right Context shape for a given cluster TLS config.
// With `tls_config` set, both helpers produce a `ClusterView` Context that
// delegates to the process-wide cluster TLS singleton; reload happens once
// at the singleton level. Without `tls_config`, the helpers return a
// default-constructed Standalone Context (no SSL).
inline auto CreateServerContext(std::optional<utils::TlsConfig> const &tls_config) -> communication::ServerContext {
  return tls_config.has_value() ? communication::ServerContext::FromClusterSingleton() : communication::ServerContext{};
}

inline auto CreateClientContext(std::optional<utils::TlsConfig> const &tls_config) -> communication::ClientContext {
  return tls_config.has_value() ? communication::ClientContext::FromClusterSingleton() : communication::ClientContext{};
}

}  // namespace memgraph::communication
