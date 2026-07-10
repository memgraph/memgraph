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

// Tests for the cluster-client TLS singleton's hot reload path. Observation
// strategy: spawn `openssl s_server -Verify 1 -naccept 1` as a subprocess and
// parse its verify-callback output for `depth=0 CN = X`. That tells us which
// leaf cert the Memgraph client presented during the mTLS handshake — i.e.
// whether the client picked up the new identity after `Prepare`+`Commit` on
// `ClusterClientSsl`.
//
// We match on CN (not serial) because `openssl s_server` doesn't dump the
// peer cert in PEM. The test fixture certs have human-readable, unique CNs
// (`instance1`, `instance2`, ...), which is enough to distinguish identities
// here.

#include "boost/dll/runtime_symbol_info.hpp"
#include "gtest/gtest.h"

#include "communication/client.hpp"
#include "communication/cluster_tls.hpp"
#include "communication/context.hpp"
#include "communication/init.hpp"
#include "io/network/endpoint.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/tls.hpp"

#include <fmt/format.h>

#include <array>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <future>
#include <optional>
#include <string>
#include <string_view>
#include <thread>

namespace fs = std::filesystem;

namespace {

memgraph::communication::SSLInit ssl_init;

// Distinct from the other unit tests' ports (8186-8188) to avoid TIME_WAIT
// collisions when running the whole suite.
constexpr uint16_t kPort{8199};

// Spawn `openssl s_server` in a worker thread, return the future that
// resolves to its full captured output (stderr merged with stdout). The
// subprocess is configured with `-naccept 1`, so it exits after one
// connection — popen returns once that happens.
//
// `sleep 10 | openssl s_server ...` keeps s_server's stdin open with no
// data for 10 seconds. We can NOT use `</dev/null` here: s_server's main
// loop polls stdin via select() and treats immediate EOF as "user wants to
// quit", tearing down the listening socket before the test client even
// connects. The `sleep` pipe gives a stdin that's "open but empty," which
// s_server is happy to leave alone until the connection finishes.
[[nodiscard]] std::future<std::string> SpawnSServer(uint16_t port, std::string const &server_cert,
                                                    std::string const &server_key, std::string const &ca_file) {
  // We pipe `sleep N` into s_server purely to keep its stdin open without
  // ever delivering EOF. The sleep duration is an upper bound on how long
  // pclose will block after s_server exits (some shells wait for the full
  // pipeline). 2s is plenty for one connection's worth of work and bounded
  // enough to not balloon the test runtime.
  auto const cmd = fmt::format(
      "sleep 2 | openssl s_server -accept {} -cert {} -key {} -CAfile {} -Verify 1 -naccept 1 "
      "2>&1",
      port,
      server_cert,
      server_key,
      ca_file);
  return std::async(std::launch::async, [cmd] {
    // NOLINTNEXTLINE(cert-env33-c): cmd is assembled from test-controlled fixture paths.
    FILE *pipe = popen(cmd.c_str(), "r");
    if (pipe == nullptr) return std::string{};
    auto guard = memgraph::utils::OnScopeExit{[pipe] { (void)pclose(pipe); }};
    std::string out;
    std::array<char, 1024> buf{};
    while (std::fgets(buf.data(), static_cast<int>(buf.size()), pipe) != nullptr) {
      out += buf.data();
    }
    return out;
  });
}

// Extract the leaf-cert CN from s_server's verify-callback output. Lines look
// like e.g. `depth=0 CN = instance1`. We find the first `depth=0` line and
// return the value after `CN = `.
[[nodiscard]] std::optional<std::string> ExtractDepth0CN(std::string_view output) {
  constexpr std::string_view kDepthZero{"depth=0"};
  auto const depth_pos = output.find(kDepthZero);
  if (depth_pos == std::string_view::npos) return std::nullopt;
  // The CN on the same line as `depth=0`.
  auto const eol = output.find('\n', depth_pos);
  auto const line = output.substr(depth_pos, eol == std::string_view::npos ? std::string_view::npos : eol - depth_pos);
  constexpr std::string_view kCnMarker{"CN = "};
  auto const cn_pos = line.find(kCnMarker);
  if (cn_pos == std::string_view::npos) return std::nullopt;
  auto cn = line.substr(cn_pos + kCnMarker.size());
  // Strip trailing whitespace / comma in case the subject has more fields.
  auto const end = cn.find_first_of(", \r\n");
  if (end != std::string_view::npos) cn = cn.substr(0, end);
  return std::string{cn};
}

// Drives one outbound mTLS handshake by constructing a ClusterView-backed
// communication::Client and calling Connect. Returns whether the handshake
// succeeded. The Client is destroyed on return, which triggers a clean
// SSL_shutdown + socket close — that's what causes s_server's accept loop
// to exit (`-naccept 1`).
bool DoOneOutboundHandshake(uint16_t port) {
  auto client_ctx = memgraph::communication::ClientContext::FromClusterSingleton();
  memgraph::communication::Client client(&client_ctx);
  return client.Connect(memgraph::io::network::Endpoint("127.0.0.1", port));
}

}  // namespace

// Asserts that `RELOAD INTRA_CLUSTER TLS` on the client side rotates the cert
// presented in OUTBOUND mTLS handshakes. Pre-refactor, every cluster-TLS
// ClientContext owned its own SSL_CTX built at construction and never
// reloaded; this test would have failed because the second handshake would
// have presented instance1's cert despite the rotation.
TEST(ClusterClientSsl, ReloadRotatesOutboundCert) {
  auto const certs_dir = fs::path{boost::dll::program_location().parent_path().string()} / "tls_certs";
  auto const ca_file = (certs_dir / "ca.crt").string();
  // Server identity for s_server — fixed across the test (we only rotate the client).
  // Use instance1's cert+key for the server because it's a CA-signed cert that
  // any client trusts; the test isn't sensitive to which server identity is used.
  auto const server_cert = (certs_dir / "instance1.crt").string();
  auto const server_key = (certs_dir / "instance1.key").string();

  auto const tmp_dir = fs::temp_directory_path() / "mg_test_cluster_client_ssl_reload";
  fs::create_directories(tmp_dir);
  auto const tmp_crt = (tmp_dir / "client.crt").string();
  auto const tmp_key = (tmp_dir / "client.key").string();

  // Initial client identity: instance1.
  fs::copy_file(certs_dir / "instance1.crt", tmp_crt, fs::copy_options::overwrite_existing);
  fs::copy_file(certs_dir / "instance1.key", tmp_key, fs::copy_options::overwrite_existing);

  memgraph::utils::TlsConfig const client_cfg{.key_file = tmp_key, .cert_file = tmp_crt, .ca_file = ca_file};
  auto init_r = memgraph::communication::ClusterClientSsl::Instance().Init(client_cfg);
  ASSERT_TRUE(init_r.has_value()) << "ClusterClientSsl::Init failed: " << init_r.error().msg;

  // ---- Probe 1: outbound with instance1 cert.
  auto receiver1 = SpawnSServer(kPort, server_cert, server_key, ca_file);
  // Give s_server time to bind + start listening before the client connects.
  // s_server prints "ACCEPT" to its output when ready, but the simpler fixed
  // sleep is sufficient and matches the existing test style for subprocess
  // probes.
  std::this_thread::sleep_for(std::chrono::milliseconds{300});

  EXPECT_TRUE(DoOneOutboundHandshake(kPort)) << "Initial mTLS handshake (instance1 outbound) failed";

  auto const output1 = receiver1.get();
  auto const cn1 = ExtractDepth0CN(output1);
  ASSERT_TRUE(cn1.has_value()) << "Couldn't find a `depth=0 CN = ...` line in s_server output. Full output:\n"
                               << output1;
  EXPECT_EQ(*cn1, "instance1") << "s_server saw the wrong client cert. Full output:\n" << output1;

  // ---- Rotate the on-disk client cert/key to instance2.
  fs::copy_file(certs_dir / "instance2.crt", tmp_crt, fs::copy_options::overwrite_existing);
  fs::copy_file(certs_dir / "instance2.key", tmp_key, fs::copy_options::overwrite_existing);

  // Drive the production 2PC path through the singleton.
  auto prep = memgraph::communication::ClusterClientSsl::Instance().Prepare();
  ASSERT_TRUE(prep.has_value()) << "ClusterClientSsl::Prepare failed: " << prep.error().msg;
  memgraph::communication::ClusterClientSsl::Instance().Commit(std::move(*prep));

  // ---- Probe 2: outbound after reload — must present instance2's cert.
  auto receiver2 = SpawnSServer(kPort, server_cert, server_key, ca_file);
  std::this_thread::sleep_for(std::chrono::milliseconds{300});

  EXPECT_TRUE(DoOneOutboundHandshake(kPort)) << "Post-reload mTLS handshake (instance2 outbound) failed";

  auto const output2 = receiver2.get();
  auto const cn2 = ExtractDepth0CN(output2);
  ASSERT_TRUE(cn2.has_value()) << "Couldn't find a `depth=0 CN = ...` line in s_server output. Full output:\n"
                               << output2;
  EXPECT_EQ(*cn2, "instance2") << "s_server saw the wrong client cert after reload. Full output:\n" << output2;
  EXPECT_NE(*cn1, *cn2) << "Client cert didn't change across the reload — singleton swap is a no-op";

  fs::remove_all(tmp_dir);
}
