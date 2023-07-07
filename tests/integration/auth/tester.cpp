// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <regex>

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
DEFINE_bool(connection_should_fail, false, "Set to true to expect a connection failure.");
DEFINE_string(failure_message, "", "Set to the expected failure message.");

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

  std::regex re(FLAGS_failure_message);

  try {
    client.Connect(endpoint, FLAGS_username, FLAGS_password);
  } catch (const memgraph::communication::bolt::ClientFatalException &e) {
    if (FLAGS_connection_should_fail) {
      if (!FLAGS_failure_message.empty() && !std::regex_match(e.what(), re)) {
        LOG_FATAL(
            "The connection should have failed with an error message of '{}'' but "
            "instead it failed with '{}'",
            FLAGS_failure_message, e.what());
      }
      return 0;
    } else {
      LOG_FATAL(
          "The connection shoudn't have failed but it failed with an "
          "error message '{}'",
          e.what());
    }
  }

  for (int i = 1; i < argc; ++i) {
    std::string query(argv[i]);
    try {
      client.Execute(query, {});
    } catch (const memgraph::communication::bolt::ClientQueryException &e) {
      if (!FLAGS_check_failure) {
        if (!FLAGS_failure_message.empty() && std::regex_match(e.what(), re)) {
          LOG_FATAL(
              "The query should have succeeded or failed with an error "
              "message that isn't equal to '{}' but it failed with that error "
              "message",
              FLAGS_failure_message);
        }
        continue;
      }
      if (FLAGS_should_fail) {
        if (!FLAGS_failure_message.empty() && !std::regex_match(e.what(), re)) {
          LOG_FATAL(
              "The query should have failed with an error message of '{}'' but "
              "instead it failed with '{}'",
              FLAGS_failure_message, e.what());
        }
        return 0;
      } else {
        LOG_FATAL(
            "The query shoudn't have failed but it failed with an "
            "error message '{}'",
            e.what());
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
