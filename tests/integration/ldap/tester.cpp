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
#include <json/json.hpp>

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");

DEFINE_bool(auth_should_fail, false, "Set to true to expect authentication failure.");
DEFINE_bool(query_should_fail, false, "Set to true to expect query execution failure.");

/**
 * Logs into the server and executes the queries specified as arguments. On any
 * errors it exits with a non-zero exit code.
 */
int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  memgraph::communication::SSLInit sslInit;

  memgraph::io::network::Endpoint endpoint(memgraph::io::network::ResolveHostname(FLAGS_address), FLAGS_port);

  memgraph::communication::ClientContext context(FLAGS_use_ssl);
  memgraph::communication::bolt::Client client(context);

  {
    std::string what;
    try {
      client.Connect(endpoint, FLAGS_username, FLAGS_password);
    } catch (const memgraph::communication::bolt::ClientFatalException &e) {
      what = e.what();
    }
    if (FLAGS_auth_should_fail) {
      MG_ASSERT(!what.empty(), "The authentication should have failed!");
    } else {
      MG_ASSERT(what.empty(),
                "The authentication should have succeeded, but "
                "failed with message: {}",
                what);
    }
  }

  for (int i = 1; i < argc; ++i) {
    std::string query(argv[i]);
    std::string what;
    try {
      client.Execute(query, {});
    } catch (const memgraph::communication::bolt::ClientQueryException &e) {
      what = e.what();
    }
    if (FLAGS_query_should_fail) {
      MG_ASSERT(!what.empty(), "The query execution should have failed!");
    } else {
      MG_ASSERT(what.empty(),
                "The query execution should have succeeded, but "
                "failed with message: {}",
                what);
    }
  }

  return 0;
}
