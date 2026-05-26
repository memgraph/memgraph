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

#ifdef MG_ENTERPRISE

#include "communication/cluster_tls.hpp"
#include "coordination/coordinator_instance_management_server.hpp"
#include "coordination/data_instance_management_server.hpp"
#include "replication_coordination_glue/messages.hpp"
#include "rpc/client.hpp"
#include "rpc/file_replication_handler.hpp"
#include "rpc/utils.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/tls.hpp"

#include <openssl/bn.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <fmt/format.h>

#include <array>
#include <chrono>
#include <cstdio>
#include <optional>
#include <unordered_map>

namespace fs = std::filesystem;

using memgraph::communication::ClientContext;
using memgraph::coordination::CoordinatorInstanceManagementServer;
using memgraph::coordination::DataInstanceManagementServer;
using memgraph::coordination::ManagementServerConfig;
using memgraph::io::network::Endpoint;
using memgraph::replication_coordination_glue::SwapMainUUIDReq;
using memgraph::replication_coordination_glue::SwapMainUUIDRes;
using memgraph::replication_coordination_glue::SwapMainUUIDRpc;
using memgraph::rpc::Client;
using memgraph::rpc::RpcFailedException;
using memgraph::rpc::RpcFailedToConnectException;
using memgraph::utils::TlsConfig;
using memgraph::utils::UUID;

using namespace std::string_view_literals;

namespace {

// Per-server-type port (avoids EADDRINUSE between consecutive cases under TIME_WAIT).
template <typename T>
struct ServerPort;

template <>
struct ServerPort<DataInstanceManagementServer> {
  static constexpr int value = 8187;
};

template <>
struct ServerPort<CoordinatorInstanceManagementServer> {
  static constexpr int value = 8188;
};

auto MakeSwapHandler() {
  return [](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
            uint64_t const request_version,
            auto *req_reader,
            auto *res_builder) {
    SwapMainUUIDReq req;
    memgraph::slk::Load(&req, req_reader);
    SwapMainUUIDRes res{true};
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  };
}

fs::path CertsDir() { return fs::path{boost::dll::program_location().parent_path().string()} / "tls_certs"; }

TlsConfig MakeTlsConfig(std::string const &instance) {
  auto const dir = CertsDir();
  return TlsConfig{.key_file = (dir / (instance + ".key")).string(),
                   .cert_file = (dir / (instance + ".crt")).string(),
                   .ca_file = (dir / "ca.crt").string()};
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

// Opens a fresh TLS connection to 127.0.0.1:port using the supplied client
// cert/key + CA, completes the handshake, and returns the hex-encoded serial
// number of the server's certificate. Returns nullopt on any failure (connect,
// handshake, or missing peer cert). Used to verify *which* cert the server is
// currently presenting — distinct from a successful RPC round-trip, which
// only proves "some cert signed by the trusted CA was presented".
std::optional<std::string> ProbeServerCertSerial(uint16_t port, std::string const &ca_file,
                                                 std::string const &client_cert, std::string const &client_key) {
  // NOTE: do NOT pass `-quiet`. In some OpenSSL versions `-quiet` implicitly enables
  // `-ign_eof`, which (a) keeps s_client running until `timeout` kills it (5 s wasted
  // per probe) and (b) suppresses the certificate output we need to pipe into
  // `openssl x509`. Default verbosity prints the server cert chain in PEM form;
  // `openssl x509 -serial -noout` reads the first PEM block and prints its serial.
  auto const cmd = fmt::format(
      "echo | timeout 5 openssl s_client -connect 127.0.0.1:{} -cert {} -key {} -CAfile {} "
      "2>/dev/null | openssl x509 -serial -noout 2>/dev/null",
      port,
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

template <typename T>
class ManagementServerTest : public ::testing::Test {
 protected:
  static constexpr int port = ServerPort<T>::value;

  // Cluster-TLS management servers built via `CreateServerContext(tls_config)`
  // read through the process-wide singleton. Tests that construct a server
  // with `tls_config` must call this first; otherwise the singleton stays
  // uninitialized and the server runs in plain mode.
  [[nodiscard]] static bool InitClusterServerSsl(memgraph::utils::TlsConfig const &cfg) {
    auto r = memgraph::communication::ClusterServerSsl::Instance().Init(cfg);
    if (!r.has_value()) {
      std::cerr << "ClusterServerSsl::Init failed in test setup: " << r.error().msg << std::endl;
      return false;
    }
    return true;
  }
};

using ServerTypes = ::testing::Types<DataInstanceManagementServer, CoordinatorInstanceManagementServer>;
TYPED_TEST_SUITE(ManagementServerTest, ServerTypes);

TYPED_TEST(ManagementServerTest, NoTlsConnection) {
  ManagementServerConfig cfg{Endpoint("0.0.0.0", this->port)};
  TypeParam server(cfg, std::nullopt);
  server.template Register<SwapMainUUIDRpc>(MakeSwapHandler());
  ASSERT_TRUE(server.Start());

  ClientContext client_context;
  Client client(Endpoint("0.0.0.0", this->port), &client_context);
  auto stream = client.template Stream<SwapMainUUIDRpc>(UUID{});
  EXPECT_NO_THROW(stream.SendAndWait());
}

TYPED_TEST(ManagementServerTest, TlsClientToNoTlsServer) {
  // Server uses instance2, client uses instance1 — both signed by the shared CA.
  ManagementServerConfig cfg{Endpoint("0.0.0.0", this->port)};
  TypeParam server(cfg, std::nullopt);
  server.template Register<SwapMainUUIDRpc>(MakeSwapHandler());
  ASSERT_TRUE(server.Start());

  auto const client_tls = MakeTlsConfig("instance1");
  ClientContext client_context(client_tls.key_file, client_tls.cert_file, client_tls.ca_file);
  auto const rpc_timeouts = std::unordered_map{std::make_pair("SwapMainUUIDReq"sv, 500)};
  Client client(Endpoint("0.0.0.0", this->port), &client_context, rpc_timeouts, std::chrono::milliseconds{500});
  EXPECT_THROW(client.template Stream<SwapMainUUIDRpc>(UUID{}), RpcFailedToConnectException);
}

TYPED_TEST(ManagementServerTest, TlsClientTlsServer) {
  // Server uses instance2, client uses instance1 — both signed by the shared CA.
  auto const server_tls = MakeTlsConfig("instance2");
  ASSERT_TRUE(this->InitClusterServerSsl(server_tls));
  ManagementServerConfig cfg{Endpoint("0.0.0.0", this->port)};
  TypeParam server(cfg, server_tls);
  server.template Register<SwapMainUUIDRpc>(MakeSwapHandler());
  ASSERT_TRUE(server.Start());

  auto const client_tls = MakeTlsConfig("instance1");
  ClientContext client_context(client_tls.key_file, client_tls.cert_file, client_tls.ca_file);
  auto const rpc_timeouts = std::unordered_map{std::make_pair("SwapMainUUIDReq"sv, 500)};
  Client client(Endpoint("0.0.0.0", this->port), &client_context, rpc_timeouts);
  auto stream = client.template Stream<SwapMainUUIDRpc>(UUID{});
  EXPECT_NO_THROW(stream.SendAndWait());
}

TYPED_TEST(ManagementServerTest, TlsServerNoTlsClient) {
  auto const server_tls = MakeTlsConfig("instance2");
  // Management server with tls_config now backs onto the cluster server SSL
  // singleton; initialize it so the server actually serves TLS.
  ASSERT_TRUE(this->InitClusterServerSsl(server_tls));
  ManagementServerConfig cfg{Endpoint("0.0.0.0", this->port)};
  TypeParam server(cfg, server_tls);
  server.template Register<SwapMainUUIDRpc>(MakeSwapHandler());
  ASSERT_TRUE(server.Start());

  // Plain client — TLS server should reject the raw SLK bytes.
  ClientContext client_context;
  Client client(Endpoint("0.0.0.0", this->port), &client_context);
  auto stream = client.template Stream<SwapMainUUIDRpc>(UUID{});
  EXPECT_THROW(stream.SendAndWait(), RpcFailedException);
}

// Server starts with instance2's cert+key copied into a temp path. The peer
// cert presented to a TLS client is asserted to have instance2's serial. The
// cert+key files on disk are then overwritten with instance1's content; after
// driving the cluster-server singleton through Prepare+Commit (the same path
// the production RELOAD INTRA_CLUSTER TLS handler takes), the peer cert
// presented to a new TLS handshake must have instance1's serial.
TYPED_TEST(ManagementServerTest, TlsReload) {
  auto const dir = CertsDir();
  auto const ca_file = (dir / "ca.crt").string();
  auto const tmp_dir =
      fs::temp_directory_path() / ("mg_test_management_server_tls_reload_" + std::to_string(this->port));
  fs::create_directories(tmp_dir);
  auto const tmp_crt = (tmp_dir / "server.crt").string();
  auto const tmp_key = (tmp_dir / "server.key").string();

  // Initial server identity: instance2.
  fs::copy_file(dir / "instance2.crt", tmp_crt, fs::copy_options::overwrite_existing);
  fs::copy_file(dir / "instance2.key", tmp_key, fs::copy_options::overwrite_existing);

  // Re-initialize the cluster server singleton so this test starts from a
  // known state regardless of which other tests ran before it.
  TlsConfig const tls_config{.key_file = tmp_key, .cert_file = tmp_crt, .ca_file = ca_file};
  ASSERT_TRUE(this->InitClusterServerSsl(tls_config));

  ManagementServerConfig cfg{Endpoint("0.0.0.0", this->port)};
  TypeParam server(cfg, tls_config);
  server.template Register<SwapMainUUIDRpc>(MakeSwapHandler());
  ASSERT_TRUE(server.Start());

  // Pre-compute the expected serials from the cert files on disk.
  auto const instance2_serial = ReadCertSerialFromFile((dir / "instance2.crt").string());
  auto const instance1_serial = ReadCertSerialFromFile((dir / "instance1.crt").string());
  ASSERT_TRUE(instance2_serial.has_value());
  ASSERT_TRUE(instance1_serial.has_value());
  ASSERT_NE(instance2_serial, instance1_serial)
      << "Test fixture broken: instance1 and instance2 have identical serials";

  // Client uses instance1's identity (different from server's).
  auto const client_tls = MakeTlsConfig("instance1");

  // 1) Probe the cert the server is currently presenting (should be instance2).
  auto const serial_before = ProbeServerCertSerial(this->port, ca_file, client_tls.cert_file, client_tls.key_file);
  ASSERT_TRUE(serial_before.has_value()) << "Probe before reload failed — could not establish TLS to the server";
  ASSERT_EQ(*serial_before, *instance2_serial)
      << "Initial probe returned the wrong cert. Expected instance2 serial=" << *instance2_serial << " but got "
      << *serial_before;

  // Sanity: a real RPC round-trip also works.
  ClientContext client_context(client_tls.key_file, client_tls.cert_file, client_tls.ca_file);
  auto const rpc_timeouts = std::unordered_map{std::make_pair("SwapMainUUIDReq"sv, 500)};
  {
    Client client(Endpoint("0.0.0.0", this->port), &client_context, rpc_timeouts);
    EXPECT_NO_THROW(client.template Stream<SwapMainUUIDRpc>(UUID{}).SendAndWait());
  }

  // 2) Rotate the on-disk cert+key.
  fs::copy_file(dir / "instance1.crt", tmp_crt, fs::copy_options::overwrite_existing);
  fs::copy_file(dir / "instance1.key", tmp_key, fs::copy_options::overwrite_existing);

  // 3) Drive the production 2PC path: Prepare must succeed, Commit is pure.
  auto prepared = memgraph::communication::ClusterServerSsl::Instance().Prepare();
  ASSERT_TRUE(prepared.has_value()) << "Prepare failed: " << prepared.error().msg;
  memgraph::communication::ClusterServerSsl::Instance().Commit(std::move(*prepared));

  // 4) Probe again. Strong assertions:
  //    a) the served cert MUST have changed (catches no-op reloads),
  //    b) the new cert MUST be instance1's (catches reloading the wrong file).
  auto const serial_after = ProbeServerCertSerial(this->port, ca_file, client_tls.cert_file, client_tls.key_file);
  ASSERT_TRUE(serial_after.has_value()) << "Probe after reload failed — could not establish TLS to the server";
  EXPECT_EQ(*serial_after, *instance1_serial)
      << "Served cert after reload is not instance1's. Expected " << *instance1_serial << " got " << *serial_after;

  // Sanity: a fresh RPC round-trip after reload also works.
  {
    Client client(Endpoint("0.0.0.0", this->port), &client_context, rpc_timeouts);
    EXPECT_NO_THROW(client.template Stream<SwapMainUUIDRpc>(UUID{}).SendAndWait());
  }

  fs::remove_all(tmp_dir);
}

#endif  // MG_ENTERPRISE
