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

#include "boost/dll/runtime_symbol_info.hpp"
#include "gtest/gtest.h"

#include "communication/cluster_tls.hpp"
#include "replication/config.hpp"
#include "replication/replication_server.hpp"
#include "replication_coordination_glue/messages.hpp"
#include "rpc/client.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/tls.hpp"

#include <openssl/bn.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <fmt/format.h>

#include <array>
#include <cstdio>
#include <optional>
#include <unordered_map>

namespace fs = std::filesystem;

using memgraph::communication::ClientContext;
using memgraph::io::network::Endpoint;
using memgraph::replication::ReplicationServer;
using memgraph::replication::ReplicationServerConfig;
using memgraph::replication_coordination_glue::FrequentHeartbeatRpc;
using memgraph::rpc::Client;
using memgraph::rpc::RpcFailedException;
using memgraph::rpc::RpcFailedToConnectException;
using memgraph::utils::TlsConfig;

using namespace std::string_view_literals;

constexpr int port{8186};

namespace {

// Initializes the process-wide cluster server SSL singleton from the given
// TlsConfig and returns whether it succeeded. Cluster-TLS servers built via
// `CreateServerContext(tls_config)` read through the singleton, so any test
// that constructs a ReplicationServer with `tls_config` set must call this
// first; otherwise the singleton stays uninitialized and the server runs in
// plain mode.
[[nodiscard]] bool InitClusterServerSslForTest(memgraph::utils::TlsConfig const &cfg) {
  auto r = memgraph::communication::ClusterServerSsl::Instance().Init(cfg);
  if (!r.has_value()) {
    std::cerr << "ClusterServerSsl::Init failed in test setup: " << r.error().msg << std::endl;
    return false;
  }
  return true;
}

// Hex-encoded uppercase serial number of an X509 cert. We use the serial as
// the cert discriminator (rather than CN) because OpenSSL guarantees it's
// unique per cert issued by a CA, whereas CN is a human-readable label that
// could in principle collide.
std::optional<std::string> X509SerialHex(X509 *cert) {
  ASN1_INTEGER *asn1 = X509_get_serialNumber(cert);
  if (!asn1) return std::nullopt;
  BIGNUM *big_num = ASN1_INTEGER_to_BN(asn1, nullptr);
  if (!big_num) return std::nullopt;
  auto bn_guard = memgraph::utils::OnScopeExit{[big_num] { BN_free(big_num); }};
  char *hex = BN_bn2hex(big_num);
  if (!hex) return std::nullopt;
  auto hex_guard = memgraph::utils::OnScopeExit{[hex] { OPENSSL_free(hex); }};
  return std::string{hex};
}

// Reads the serial of a PEM-encoded X509 cert directly from disk. Used by the
// tests to compute the expected serial without hardcoding hex values.
std::optional<std::string> ReadCertSerialFromFile(std::string const &cert_file) {
  FILE *file_ptr = std::fopen(cert_file.c_str(), "r");
  if (!file_ptr) return std::nullopt;
  auto file_guard = memgraph::utils::OnScopeExit{[file_ptr] { (void)std::fclose(file_ptr); }};
  X509 *cert = PEM_read_X509(file_ptr, nullptr, nullptr, nullptr);
  if (!cert) return std::nullopt;
  auto cert_guard = memgraph::utils::OnScopeExit{[cert] { X509_free(cert); }};
  return X509SerialHex(cert);
}

// Opens a TLS connection to 127.0.0.1:port via `openssl s_client` (subprocess),
// pipes the server's cert through `openssl x509 -serial -noout`, and returns
// the hex-encoded serial. `timeout 5` bounds the probe at 5 seconds so a stuck
// handshake doesn't hang the test. We deliberately use a subprocess rather
// than in-process OpenSSL to (a) avoid duplicating Memgraph's non-blocking
// handshake setup, (b) get a clean kill on stuck handshakes, and (c) mirror
// ClickHouse's pattern (tests/integration/test_keeper_raft_cert_reload/test.py:164-177).
std::optional<std::string> ProbeServerCertSerial(uint16_t port_, std::string const &ca_file,
                                                 std::string const &client_cert, std::string const &client_key) {
  // NOTE: do NOT pass `-quiet`. In some OpenSSL versions `-quiet` implicitly enables
  // `-ign_eof`, which (a) keeps s_client running until `timeout` kills it (5 s wasted
  // per probe) and (b) suppresses the certificate output we need to pipe into
  // `openssl x509`. Default verbosity prints the server cert chain in PEM form;
  // `openssl x509 -serial -noout` reads the first PEM block and prints its serial.
  auto const cmd = fmt::format(
      "echo | timeout 5 openssl s_client -connect 127.0.0.1:{} -cert {} -key {} -CAfile {} "
      "2>/dev/null | openssl x509 -serial -noout 2>/dev/null",
      port_,
      client_cert,
      client_key,
      ca_file);

  // NOLINTNEXTLINE(cert-env33-c): cmd is built from test-controlled fixture paths, not user input.
  FILE *pipe = popen(cmd.c_str(), "r");
  if (pipe == nullptr) return std::nullopt;
  auto pipe_guard = memgraph::utils::OnScopeExit{[pipe] { (void)pclose(pipe); }};

  std::array<char, 256> line{};
  if (std::fgets(line.data(), static_cast<int>(line.size()), pipe) == nullptr) {
    return std::nullopt;
  }
  // Output line shape: "serial=<UPPERCASE_HEX>\n"
  constexpr std::string_view kSerialPrefix{"serial="};
  std::string_view out{line.data()};
  auto const pos = out.find(kSerialPrefix);
  if (pos == std::string_view::npos) return std::nullopt;
  out.remove_prefix(pos + kSerialPrefix.size());
  while (!out.empty() && (out.back() == '\n' || out.back() == '\r')) {
    out.remove_suffix(1);
  }
  return std::string{out};
}

}  // namespace

TEST(ReplicationServer, NoTlsConnection) {
  ReplicationServerConfig cfg{.repl_server = Endpoint("0.0.0.0", port)};
  ReplicationServer server(cfg);
  ASSERT_TRUE(server.Start());

  // Configured without TLS
  ClientContext client_conntext;
  Client client(Endpoint("0.0.0.0", port), &client_conntext);
  auto stream = client.Stream<FrequentHeartbeatRpc>();
  EXPECT_NO_THROW(stream.SendAndWait());
}

TEST(ReplicationServer, TlsClientToNoTlsServer) {
  ReplicationServerConfig cfg{.repl_server = Endpoint("0.0.0.0", port)};
  ReplicationServer server(cfg);
  ASSERT_TRUE(server.Start());

  // Configure TLS client
  auto const certs_dir = fs::path{boost::dll::program_location().parent_path().string()} / "tls_certs";
  auto const key_file = (certs_dir / "instance1.key").string();
  auto const cert_file = (certs_dir / "instance1.crt").string();
  auto const ca_file = (certs_dir / "ca.crt").string();
  ClientContext client_conntext(key_file, cert_file, ca_file);
  auto const rpc_timeouts = std::unordered_map{std::make_pair("FrequentHeartbeatReq"sv, 500)};
  Client client(Endpoint("0.0.0.0", port), &client_conntext, rpc_timeouts, std::chrono::milliseconds{500});
  // Fails after 5s because the timeout is 5s on socket connect
  EXPECT_THROW(client.Stream<FrequentHeartbeatRpc>(), RpcFailedToConnectException);
}

TEST(ReplicationServer, TlsClientTlsServer) {
  // Test that instance1 can connect to instance2
  auto const certs_dir = fs::path{boost::dll::program_location().parent_path().string()} / "tls_certs";
  auto const key2_file = (certs_dir / "instance2.key").string();
  auto const cert2_file = (certs_dir / "instance2.crt").string();
  auto const ca_file = (certs_dir / "ca.crt").string();
  TlsConfig const tls_config{.key_file = key2_file, .cert_file = cert2_file, .ca_file = ca_file};
  ASSERT_TRUE(InitClusterServerSslForTest(tls_config));
  ReplicationServerConfig cfg{.repl_server = Endpoint("0.0.0.0", port), .tls_config = tls_config};
  ReplicationServer server(cfg);
  ASSERT_TRUE(server.Start());

  // Configure TLS client
  auto const key1_file = (certs_dir / "instance1.key").string();
  auto const cert1_file = (certs_dir / "instance1.crt").string();
  ClientContext client_conntext(key1_file, cert1_file, ca_file);
  auto const rpc_timeouts = std::unordered_map{std::make_pair("FrequentHeartbeatReq"sv, 500)};
  Client client(Endpoint("0.0.0.0", port), &client_conntext, rpc_timeouts);
  auto stream = client.Stream<FrequentHeartbeatRpc>();
  EXPECT_NO_THROW(stream.SendAndWait());
}

TEST(ReplicationServer, TlsServerNoTlsClient) {
  auto const certs_dir = fs::path{boost::dll::program_location().parent_path().string()} / "tls_certs";
  auto const key2_file = (certs_dir / "instance2.key").string();
  auto const cert2_file = (certs_dir / "instance2.crt").string();
  auto const ca_file = (certs_dir / "ca.crt").string();
  TlsConfig const tls_config{.key_file = key2_file, .cert_file = cert2_file, .ca_file = ca_file};
  ASSERT_TRUE(InitClusterServerSslForTest(tls_config));
  ReplicationServerConfig cfg{.repl_server = Endpoint("0.0.0.0", port), .tls_config = tls_config};
  ReplicationServer server(cfg);
  ASSERT_TRUE(server.Start());

  // Configure TLS client
  // Configured without TLS
  ClientContext client_conntext;
  Client client(Endpoint("0.0.0.0", port), &client_conntext);
  auto stream = client.Stream<FrequentHeartbeatRpc>();
  EXPECT_THROW(stream.SendAndWait(), RpcFailedException);
}

// Server starts with instance2's cert+key copied into a temp path. The peer
// cert presented to a TLS client is asserted to have instance2's serial. The
// cert+key files on disk are then overwritten with instance1's content; after
// driving the cluster-server singleton through Prepare+Commit (the same path
// the production RELOAD INTRA_CLUSTER TLS handler takes), the peer cert
// presented to a new TLS handshake must have instance1's serial.
TEST(ReplicationServer, TlsReload) {
  auto const certs_dir = fs::path{boost::dll::program_location().parent_path().string()} / "tls_certs";
  auto const ca_file = (certs_dir / "ca.crt").string();
  auto const tmp_dir = fs::temp_directory_path() / "mg_test_replication_server_tls_reload";
  fs::create_directories(tmp_dir);
  auto const tmp_crt = (tmp_dir / "server.crt").string();
  auto const tmp_key = (tmp_dir / "server.key").string();

  // Initial server identity: instance2.
  fs::copy_file(certs_dir / "instance2.crt", tmp_crt, fs::copy_options::overwrite_existing);
  fs::copy_file(certs_dir / "instance2.key", tmp_key, fs::copy_options::overwrite_existing);

  // Re-initialize the cluster server singleton so this test starts from a
  // known state regardless of which other tests in the suite ran before it.
  TlsConfig const tls_config{.key_file = tmp_key, .cert_file = tmp_crt, .ca_file = ca_file};
  ASSERT_TRUE(InitClusterServerSslForTest(tls_config));

  ReplicationServerConfig cfg{.repl_server = Endpoint("0.0.0.0", port), .tls_config = tls_config};
  ReplicationServer server(cfg);
  ASSERT_TRUE(server.Start());

  // Pre-compute the expected serials from the cert files on disk. We compare
  // against these rather than hardcoded hex values so the test stays correct
  // if certs are regenerated.
  auto const instance2_serial = ReadCertSerialFromFile((certs_dir / "instance2.crt").string());
  auto const instance1_serial = ReadCertSerialFromFile((certs_dir / "instance1.crt").string());
  ASSERT_TRUE(instance2_serial.has_value());
  ASSERT_TRUE(instance1_serial.has_value());
  ASSERT_NE(instance2_serial, instance1_serial)
      << "Test fixture broken: instance1 and instance2 have identical serials";

  // Client identity (instance1) — used for both the probe and the RPC client.
  auto const key1_file = (certs_dir / "instance1.key").string();
  auto const cert1_file = (certs_dir / "instance1.crt").string();

  // 1) Probe the cert the server is currently presenting (should be instance2).
  auto const serial_before = ProbeServerCertSerial(port, ca_file, cert1_file, key1_file);
  ASSERT_TRUE(serial_before.has_value()) << "Probe before reload failed — could not establish TLS to the server";
  ASSERT_EQ(*serial_before, *instance2_serial)
      << "Initial probe returned the wrong cert. Expected instance2 serial=" << *instance2_serial << " but got "
      << *serial_before;

  // Sanity: a real RPC round-trip also works.
  ClientContext client_conntext(key1_file, cert1_file, ca_file);
  auto const rpc_timeouts = std::unordered_map{std::make_pair("FrequentHeartbeatReq"sv, 500)};
  {
    Client client(Endpoint("0.0.0.0", port), &client_conntext, rpc_timeouts);
    EXPECT_NO_THROW(client.Stream<FrequentHeartbeatRpc>().SendAndWait());
  }

  // 2) Rotate the on-disk cert+key (same paths, different content — instance1's pair).
  fs::copy_file(certs_dir / "instance1.crt", tmp_crt, fs::copy_options::overwrite_existing);
  fs::copy_file(certs_dir / "instance1.key", tmp_key, fs::copy_options::overwrite_existing);

  // 3) Drive the production 2PC path: Prepare must succeed, Commit is pure.
  auto prepared = memgraph::communication::ClusterServerSsl::Instance().Prepare();
  ASSERT_TRUE(prepared.has_value()) << "Prepare failed: " << prepared.error().msg;
  memgraph::communication::ClusterServerSsl::Instance().Commit(std::move(*prepared));

  // 4) Probe again. The strong assertions:
  //    a) the served cert MUST have changed (catches no-op reloads),
  //    b) the new cert MUST be instance1's (catches reloading the wrong file).
  auto const serial_after = ProbeServerCertSerial(port, ca_file, cert1_file, key1_file);
  ASSERT_TRUE(serial_after.has_value()) << "Probe after reload failed — could not establish TLS to the server";
  EXPECT_EQ(*serial_after, *instance1_serial)
      << "Served cert after reload is not instance1's. Expected " << *instance1_serial << " got " << *serial_after;

  // Sanity: a fresh RPC round-trip after reload also works.
  {
    Client client(Endpoint("0.0.0.0", port), &client_conntext, rpc_timeouts);
    EXPECT_NO_THROW(client.Stream<FrequentHeartbeatRpc>().SendAndWait());
  }

  fs::remove_all(tmp_dir);
}
