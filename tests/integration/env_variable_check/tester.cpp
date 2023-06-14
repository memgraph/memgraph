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

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");

DEFINE_bool(check_failure, false, "Set to true to enable failure checking.");
DEFINE_bool(should_fail, false, "Set to true to expect a failure.");
DEFINE_string(failure_message, "", "Set to the expected failure message.");

int ProcessException(const std::string &exception_message) {
  if (FLAGS_should_fail) {
    if (!FLAGS_failure_message.empty() && exception_message != FLAGS_failure_message) {
      LOG_FATAL(
          "The query should have failed with an error message of '{}'' but "
          "instead it failed with '{}'",
          FLAGS_failure_message, exception_message);
    }
    return 0;
  } else {
    LOG_FATAL(
        "The query shouldn't have failed but it failed with an "
        "error message '{}'",
        exception_message);
    return 1;
  }
}
/**
 * Executes queries passed as positional arguments and verifies whether they
 * succeeded, failed, failed with a specific error message or executed without a
 * specific error occurring.
 */
int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  memgraph::communication::SSLInit sslInit;

  memgraph::io::network::Endpoint endpoint(memgraph::io::network::ResolveHostname(FLAGS_address), FLAGS_port);

  memgraph::communication::ClientContext context(FLAGS_use_ssl);
  memgraph::communication::bolt::Client client(context);

  try {
    client.Connect(endpoint, FLAGS_username, FLAGS_password);
  } catch (const memgraph::utils::BasicException &e) {
    return ProcessException(e.what());
  }

  for (int i = 1; i < argc; ++i) {
    std::string query(argv[i]);
    try {
      client.Execute(query, {});
    } catch (const memgraph::communication::bolt::ClientQueryException &e) {
      if (!FLAGS_check_failure) {
        if (!FLAGS_failure_message.empty() && e.what() == FLAGS_failure_message) {
          LOG_FATAL(
              "The query should have succeeded or failed with an error "
              "message that isn't equal to '{}' but it failed with that error "
              "message",
              FLAGS_failure_message);
        }
        continue;
      }
      if (!ProcessException(e.what())) {
        return 0;
      }
    }
    if (!FLAGS_check_failure) continue;
    if (FLAGS_should_fail) {
      LOG_FATAL(
          "The query should have failed but instead it executed "
          "successfully!");
    }
  }

  return 0;
}
