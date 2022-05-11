// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gflags/gflags.h>
#include <spdlog/spdlog.h>
#include <unistd.h>
#include <cstddef>
#include <mgclient.hpp>

#include "common.hpp"
#include "utils/logging.hpp"

DEFINE_uint64(bolt_port, 7687, "Bolt port");

void EstablishConnection(const auto bolt_port) {
  spdlog::info("Testing successfull connection from one client");
  mg::Client::Init();
  auto client = mg::Client::Connect({.host = "127.0.0.1", .port = bolt_port, .use_ssl = false});
  MG_ASSERT(client, "Failed to connect!");
}

void EstablishMultipleConnections(const auto bolt_port) {
  spdlog::info("Testing successfull connection from multiple clients");
  mg::Client::Init();
  auto client1 = mg::Client::Connect({.host = "127.0.0.1", .port = bolt_port, .use_ssl = false});
  auto client2 = mg::Client::Connect({.host = "127.0.0.1", .port = bolt_port, .use_ssl = false});
  auto client3 = mg::Client::Connect({.host = "127.0.0.1", .port = bolt_port, .use_ssl = false});

  MG_ASSERT(client1, "Failed to connect!");
  MG_ASSERT(client2, "Failed to connect!");
  MG_ASSERT(client3, "Failed to connect!");
}

void EstablishSSLConnectionToNonSSLServer(const auto bolt_port) {
  spdlog::info("Testing that connection fails when connecting to SSL");
  mg::Client::Init();
  auto client = mg::Client::Connect({.host = "127.0.0.1", .port = bolt_port, .use_ssl = true});

  MG_ASSERT(client == nullptr, "Connection not refused when conneting with SSL turned on to non SSL server!");
}

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E server connection!");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  MG_ASSERT(FLAGS_bolt_port != 0);
  memgraph::logging::RedirectToStderr();

  const auto bolt_port = static_cast<uint16_t>(FLAGS_bolt_port);

  EstablishConnection(bolt_port);
  EstablishMultipleConnections(bolt_port);
  EstablishSSLConnectionToNonSSLServer(bolt_port);

  return 0;
}
