// Copyright 2024 Memgraph Ltd.
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

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"
#include "utils/logging.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");

// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memgraph::logging::RedirectToStderr();

  memgraph::communication::SSLInit sslInit;

  memgraph::io::network::Endpoint endpoint(memgraph::io::network::ResolveHostname(FLAGS_address), FLAGS_port);

  memgraph::communication::ClientContext context(FLAGS_use_ssl);
  memgraph::communication::bolt::Client client(context);

  client.Connect(endpoint, FLAGS_username, FLAGS_password);
  auto ret = client.Execute("SHOW USERS", {});
  auto size = ret.records.size();
  MG_ASSERT(size == 1, "Too much users returned for SHOW USERA (got {}, expected 1)!", size);
  auto row0_size = ret.records[0].size();
  MG_ASSERT(row0_size == 1, "Too much entries in query dump row (got {}, expected 1)!", row0_size);
  auto user = ret.records[0][0].ValueString();
  MG_ASSERT(user == "memgraph1", "Unexpected user returned for SHOW USERS (got {}, expected memgraph)!", user);

  return 0;
}
