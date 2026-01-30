// Copyright 2025 Memgraph Ltd.
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
#include <mgclient.hpp>

#include "utils/logging.hpp"
#include "utils/timer.hpp"

DEFINE_uint64(bolt_port, 7687, "Bolt port");
DEFINE_uint64(timeout, 120, "Timeout seconds");
DEFINE_bool(multi_db, false, "Run test in multi db environment");

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E Memory Control");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memgraph::logging::RedirectToStderr();

  mg::Client::Init();

  auto client =
      mg::Client::Connect({.host = "127.0.0.1", .port = static_cast<uint16_t>(FLAGS_bolt_port), .use_ssl = false});
  if (!client) {
    LOG_FATAL("Failed to connect!");
  }

  client->Execute("FREE MEMORY;");
  client->DiscardAll();
  client->Execute("STORAGE MODE IN_MEMORY_ANALYTICAL;");
  client->DiscardAll();
  client->Execute("DROP GRAPH;");
  client->DiscardAll();
  client->Execute("STORAGE MODE IN_MEMORY_TRANSACTIONAL;");
  client->DiscardAll();
  client->Execute("MATCH (n) DETACH DELETE n;");
  client->DiscardAll();
  client->Execute("FREE MEMORY;");
  client->DiscardAll();

  if (FLAGS_multi_db) {
    try {
      client->Execute("DROP DATABASE clean;");
      client->DiscardAll();
    } catch (const std::exception & /*unused*/) {
      // Ignore
    }
    client->Execute("CREATE DATABASE clean;");
    client->DiscardAll();
    client->Execute("USE DATABASE clean;");
    client->DiscardAll();
  }

  const auto *create_query = "UNWIND range(1, 100) as u CREATE (n {string: \"Some longer string\"}) RETURN n;";

  memgraph::utils::Timer timer;
  while (true) {
    if (timer.Elapsed<std::chrono::duration<uint64_t>>().count() > FLAGS_timeout) {
      LOG_FATAL("The test timed out");
    }
    if (!client->Execute(create_query)) {
      // NOTE: No way to know what error happened, so we just break
      break;
    }
    try {
      client->DiscardAll();
    } catch (const mg::TransientException & /*unused*/) {
      break;
    }
  }

  spdlog::info("Memgraph is out of memory");
  return 0;
}
