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
#include <cstdlib>

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"
#include "utils/logging.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "admin", "Username for the database");
DEFINE_string(password, "admin", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");

/**
 * Verifies that user 'user' has privileges that are given as positional
 * arguments.
 */
int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  memgraph::communication::SSLInit sslInit;

  memgraph::io::network::Endpoint endpoint(memgraph::io::network::ResolveHostname(FLAGS_address), FLAGS_port);

  memgraph::communication::ClientContext context(FLAGS_use_ssl);
  memgraph::communication::bolt::Client client(context);

  client.Connect(endpoint, FLAGS_username, FLAGS_password);

  try {
    std::string query(argv[1]);
    auto ret = client.Execute(query, {});
    uint64_t count_got = ret.records.size();

    if (count_got != std::atoi(argv[2])) {
      LOG_FATAL("Expected the record to have {} entries but they had {} entries!", argv[2], count_got);
    }

  } catch (const memgraph::communication::bolt::ClientQueryException &e) {
    LOG_FATAL(
        "The query shouldn't have failed but it failed with an "
        "error message '{}', {}",
        e.what(), argv[0]);
  }

  return 0;
}
