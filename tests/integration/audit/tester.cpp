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

#include <fmt/core.h>
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

DEFINE_string(query, "", "Query to execute");
DEFINE_string(params_json, "{}", "Params for the query");
DEFINE_string(use_db, "memgraph", "Database to run the query against");

memgraph::communication::bolt::Value JsonToValue(const nlohmann::json &jv) {
  memgraph::communication::bolt::Value ret;
  switch (jv.type()) {
    case nlohmann::json::value_t::null:
      break;
    case nlohmann::json::value_t::boolean:
      ret = jv.get<bool>();
      break;
    case nlohmann::json::value_t::number_integer:
      ret = jv.get<int64_t>();
      break;
    case nlohmann::json::value_t::number_unsigned:
      ret = jv.get<int64_t>();
      break;
    case nlohmann::json::value_t::number_float:
      ret = jv.get<double>();
      break;
    case nlohmann::json::value_t::string:
      ret = jv.get<std::string>();
      break;
    case nlohmann::json::value_t::array: {
      std::vector<memgraph::communication::bolt::Value> vec;
      for (const auto &item : jv) {
        vec.push_back(JsonToValue(item));
      }
      ret = vec;
      break;
    }
    case nlohmann::json::value_t::object: {
      std::map<std::string, memgraph::communication::bolt::Value> map;
      for (auto it = jv.begin(); it != jv.end(); ++it) {
        auto tmp = JsonToValue(it.key());
        MG_ASSERT(tmp.type() == memgraph::communication::bolt::Value::Type::String,
                  "Expected a string as the map key!");
        map.insert({tmp.ValueString(), JsonToValue(it.value())});
      }
      ret = map;
      break;
    }
    case nlohmann::json::value_t::binary:
      LOG_FATAL("Unexpected 'binary' type in json value!");
    case nlohmann::json::value_t::discarded:
      LOG_FATAL("Unexpected 'discarded' type in json value!");
      break;
  }
  return ret;
}

/**
 * Executes the specified query using the specified parameters. On any errors it
 * exits with a non-zero exit code.
 */
int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  memgraph::communication::SSLInit sslInit;

  memgraph::io::network::Endpoint endpoint(memgraph::io::network::ResolveHostname(FLAGS_address), FLAGS_port);

  memgraph::communication::ClientContext context(FLAGS_use_ssl);
  memgraph::communication::bolt::Client client(context);

  client.Connect(endpoint, FLAGS_username, FLAGS_password);
  client.Execute(fmt::format("USE DATABASE {}", FLAGS_use_db), {});
  client.Execute(FLAGS_query, JsonToValue(nlohmann::json::parse(FLAGS_params_json)).ValueMap());

  return 0;
}
