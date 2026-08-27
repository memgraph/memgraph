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

// Asserts that every SSL_CTX Memgraph builds carries a TLS 1.2 floor in FIPS
// approved mode (SP 800-52r2; RFC 8996 deprecates TLS 1.0/1.1), and that
// nothing changes outside it.
//
// Both directions are tested deliberately: the floor is gated on FIPS mode
// precisely so existing non-FIPS deployments keep negotiating what they always
// did, so "no floor when disabled" is as much a requirement as the floor
// itself.
//
// Observation strategy: read the floor back off the built context with
// SSL_CTX_get_min_proto_version rather than attempting a TLS 1.1 handshake. A
// negative handshake test would need a client that can still *speak* TLS 1.1,
// which OpenSSL 3.x at its default security level generally cannot -- so such
// a test would pass whether or not the floor was set, making it worthless as a
// regression guard.

#include "gtest/gtest.h"

#include <openssl/ssl.h>
#include <boost/dll/runtime_symbol_info.hpp>

#include <filesystem>
#include <ios>
#include <string>

#include "communication/cluster_tls.hpp"
#include "communication/context.hpp"
#include "communication/init.hpp"
#include "utils/logging.hpp"
#include "utils/tls.hpp"

namespace fs = std::filesystem;

namespace {

memgraph::communication::SSLInit ssl_init;

auto TestTlsConfig() -> memgraph::utils::TlsConfig {
  auto const certs = fs::path{boost::dll::program_location().parent_path().string()} / "tls_certs";
  return {.key_file = (certs / "instance1.key").string(),
          .cert_file = (certs / "instance1.crt").string(),
          .ca_file = (certs / "ca.crt").string()};
}

/// The floor a bare context reports, i.e. whatever openssl.cnf imposes before
/// Memgraph touches anything. Used only to decide whether these tests can
/// prove anything.
auto BareContextFloor() -> int {
  auto *raw = SSL_CTX_new(TLS_client_method());
  MG_ASSERT(raw != nullptr);
  auto const floor = static_cast<int>(SSL_CTX_get_min_proto_version(raw));
  SSL_CTX_free(raw);
  return floor;
}

// FIPS mode is process-global, so it must be reset even when a test fails.
class TlsMinVersion : public ::testing::Test {
 protected:
  void TearDown() override { memgraph::communication::SetFipsMode(false); }
};

/// For tests that read the floor off a context Memgraph built and compare it
/// against what the environment would have supplied anyway. With a 1.2+ floor
/// already imposed, the approved-mode expectation would hold whether or not
/// Memgraph set anything and the disabled-mode one would be indistinguishable
/// from it, so those tests skip rather than pass vacuously; the container-level
/// FIPS tests cover behaviour in that environment.
class TlsMinVersionVsEnvironment : public TlsMinVersion {
 protected:
  void SetUp() override {
    auto const floor = BareContextFloor();
    if (floor >= TLS1_2_VERSION) {
      GTEST_SKIP() << "openssl.cnf already imposes a TLS floor of 0x" << std::hex << floor
                   << ", so this test cannot show that Memgraph sets one.";
    }
  }
};

}  // namespace

TEST_F(TlsMinVersionVsEnvironment, ClientContext) {
  {
    auto client = memgraph::communication::ClientContext{true};
    ASSERT_TRUE(client.use_ssl());
    EXPECT_LT(SSL_CTX_get_min_proto_version(client.context()->native_handle()), TLS1_2_VERSION)
        << "non-FIPS deployments must keep whatever TLS version range they already had";
  }

  memgraph::communication::SetFipsMode(true);
  auto client = memgraph::communication::ClientContext{true};
  ASSERT_TRUE(client.use_ssl());
  EXPECT_EQ(SSL_CTX_get_min_proto_version(client.context()->native_handle()), TLS1_2_VERSION);
}

TEST_F(TlsMinVersionVsEnvironment, ServerContext) {
  auto const cfg = TestTlsConfig();
  {
    auto server = memgraph::communication::ServerContext{cfg.key_file, cfg.cert_file, cfg.ca_file, false};
    ASSERT_TRUE(server.use_ssl());
    EXPECT_LT(SSL_CTX_get_min_proto_version(server.context_clone()->native_handle()), TLS1_2_VERSION)
        << "non-FIPS deployments must keep whatever TLS version range they already had";
  }

  memgraph::communication::SetFipsMode(true);
  auto server = memgraph::communication::ServerContext{cfg.key_file, cfg.cert_file, cfg.ca_file, false};
  ASSERT_TRUE(server.use_ssl());
  EXPECT_EQ(SSL_CTX_get_min_proto_version(server.context_clone()->native_handle()), TLS1_2_VERSION);
}

TEST_F(TlsMinVersionVsEnvironment, ClusterServerContext) {
  auto &instance = memgraph::communication::ClusterServerSsl::Instance();

  auto res = instance.Init(TestTlsConfig());
  ASSERT_TRUE(res.has_value()) << res.error().msg;
  EXPECT_LT(SSL_CTX_get_min_proto_version(instance.CurrentContext()->native_handle()), TLS1_2_VERSION);

  memgraph::communication::SetFipsMode(true);
  res = instance.Init(TestTlsConfig());
  ASSERT_TRUE(res.has_value()) << res.error().msg;
  EXPECT_EQ(SSL_CTX_get_min_proto_version(instance.CurrentContext()->native_handle()), TLS1_2_VERSION);
}

TEST_F(TlsMinVersionVsEnvironment, ClusterClientContext) {
  auto &instance = memgraph::communication::ClusterClientSsl::Instance();

  auto res = instance.Init(TestTlsConfig());
  ASSERT_TRUE(res.has_value()) << res.error().msg;
  EXPECT_LT(SSL_CTX_get_min_proto_version(instance.CurrentContext()->native_handle()), TLS1_2_VERSION);

  memgraph::communication::SetFipsMode(true);
  res = instance.Init(TestTlsConfig());
  ASSERT_TRUE(res.has_value()) << res.error().msg;
  EXPECT_EQ(SSL_CTX_get_min_proto_version(instance.CurrentContext()->native_handle()), TLS1_2_VERSION);
}

// Approved mode must not relax a stricter local policy. Simulates an
// openssl.cnf that demands TLS 1.3 by pre-setting the floor, then checks that
// the policy leaves it alone.
TEST_F(TlsMinVersion, DoesNotLowerAStricterFloor) {
  memgraph::communication::SetFipsMode(true);

  auto *raw = SSL_CTX_new(TLS_client_method());
  ASSERT_NE(raw, nullptr);
  ASSERT_EQ(SSL_CTX_set_min_proto_version(raw, TLS1_3_VERSION), 1);

  EXPECT_TRUE(memgraph::communication::ApplyTlsVersionPolicy(raw));
  EXPECT_EQ(SSL_CTX_get_min_proto_version(raw), TLS1_3_VERSION);

  SSL_CTX_free(raw);
}
