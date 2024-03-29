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
#include "communication/bolt/v1/value.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"
#include "utils/logging.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(field, "", "Expected settings field to check");
DEFINE_string(value, "", "Expected string result from field");

/**
 * Executes queries passed as positional arguments and verifies whether they
 * succeeded, failed, failed with a specific error message or executed without a
 * specific error occurring.
 */
int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  memgraph::communication::SSLInit sslInit;

  memgraph::io::network::Endpoint endpoint(memgraph::io::network::ResolveHostname(FLAGS_address), FLAGS_port);

  memgraph::communication::ClientContext context(false);
  memgraph::communication::bolt::Client client(context);

  try {
    client.Connect(endpoint, "", "");
  } catch (const memgraph::utils::BasicException &e) {
    LOG_FATAL("");
  }

  const auto &res = client.Execute("SHOW DATABASE SETTINGS", {});
  MG_ASSERT(res.fields[0] == "setting_name", "Expected \"setting_name\" field in the query result.");
  MG_ASSERT(res.fields[1] == "setting_value", "Expected \"setting_value\" field in the query result.");

  for (const auto &record : res.records) {
    const auto &settings_name = record[0].ValueString();
    if (settings_name == FLAGS_field) {
      const auto &settings_value = record[1].ValueString();
      // First try to encode the flags as float; if that fails just compare the raw strings
      try {
        MG_ASSERT(std::stof(settings_value) == std::stof(FLAGS_value),
                  "Failed when checking \"{}\"; expected \"{}\", found \"{}\"!", FLAGS_field, FLAGS_value,
                  settings_value);
      } catch (const std::invalid_argument &) {
        MG_ASSERT(settings_value == FLAGS_value, "Failed when checking \"{}\"; expected \"{}\", found \"{}\"!",
                  FLAGS_field, FLAGS_value, settings_value);
      }
      return 0;
    }
  }

  LOG_FATAL("No setting named \"{}\" found!", FLAGS_field);
}
