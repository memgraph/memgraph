// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "common.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <thread>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include "utils/logging.hpp"
#include "utils/timer.hpp"

DEFINE_uint64(bolt_port, 7687, "Bolt port");

std::unique_ptr<mg::Client> ConnectWithUser(const std::string_view username) {
  auto client = mg::Client::Connect({.host = "127.0.0.1",
                                     .port = static_cast<uint16_t>(FLAGS_bolt_port),
                                     .username = std::string{username},
                                     .use_ssl = false});
  MG_ASSERT(client, "Failed to connect!");
  return client;
}

std::unique_ptr<mg::Client> Connect() { return ConnectWithUser(""); }

void CreateVertex(mg::Client &client, int vertex_id) {
  mg::Map parameters{
      {"id", mg::Value{vertex_id}},
  };
  client.Execute(fmt::format("CREATE (n: {} {{ id: $id }})", kVertexLabel), mg::ConstMap{parameters.ptr()});
  client.DiscardAll();
}

void CreateEdge(mg::Client &client, int from_vertex, int to_vertex, int edge_id) {
  mg::Map parameters{
      {"from", mg::Value{from_vertex}},
      {"to", mg::Value{to_vertex}},
      {"id", mg::Value{edge_id}},
  };
  client.Execute(fmt::format("MATCH (from: {} {{ id: $from }}), (to: {} {{id: $to }}) "
                             "CREATE (from)-[r: {} {{id: $id}}]->(to)",
                             kVertexLabel, kVertexLabel, kEdgeLabel),
                 mg::ConstMap{parameters.ptr()});
  client.DiscardAll();
}

int GetNumberOfAllVertices(mg::Client &client) {
  client.Execute("MATCH (n) RETURN COUNT(*)");
  const auto value = client.FetchOne();
  MG_ASSERT(value, "Unexpected error");
  MG_ASSERT(value->size() == 1, "Unexpected number of columns!");
  client.FetchAll();
  MG_ASSERT(value->at(0).type() == mg::Value::Type::Int, "Unexpected type!");
  return value->at(0).ValueInt();
}

void WaitForNumberOfAllVertices(mg::Client &client, int number_of_vertices) {
  using namespace std::chrono_literals;
  utils::Timer timer{};
  while ((timer.Elapsed().count() <= 0.5) && GetNumberOfAllVertices(client) != number_of_vertices) {
  }
  CheckNumberOfAllVertices(client, number_of_vertices);
  std::this_thread::sleep_for(100ms);
}

void CheckNumberOfAllVertices(mg::Client &client, int expected_number_of_vertices) {
  const auto number_of_vertices = GetNumberOfAllVertices(client);
  MG_ASSERT(number_of_vertices == expected_number_of_vertices, "There are {} vertices, expected {}!",
            number_of_vertices, expected_number_of_vertices);
}

std::optional<mg::Value> GetVertex(mg::Client &client, std::string_view label, int vertex_id) {
  mg::Map parameters{
      {"id", mg::Value{vertex_id}},
  };

  client.Execute(fmt::format("MATCH (n: {} {{id: $id}}) RETURN n", label), mg::ConstMap{parameters.ptr()});
  const auto result = client.FetchAll();
  MG_ASSERT(result, "Vertex with label {} and id {} cannot be found!", label, vertex_id);
  const auto &rows = *result;
  MG_ASSERT(rows.size() <= 1, "Unexpected number of vertices with label {} and id {}, found {} vertices", label,
            vertex_id, rows.size());
  if (rows.empty()) {
    return std::nullopt;
  }

  return rows[0][0];
}

bool VertexExists(mg::Client &client, std::string_view label, int vertex_id) {
  return GetVertex(client, label, vertex_id).has_value();
}

void CheckVertexMissing(mg::Client &client, std::string_view label, int vertex_id) {
  MG_ASSERT(!VertexExists(client, label, vertex_id), "Not expected vertex exist with label {} and id {}!", label,
            vertex_id);
}

void CheckVertexExists(mg::Client &client, std::string_view label, int vertex_id) {
  MG_ASSERT(VertexExists(client, label, vertex_id), "Expected vertex doesn't exist with label {} and id {}!", label,
            vertex_id);
}

void ExecuteCreateVertex(mg::Client &client, int id) {
  client.Execute(fmt::format("CALL write.create_vertex({}) YIELD v RETURN v", id));
  const auto v1 = client.FetchAll();
  MG_ASSERT(v1);
  MG_ASSERT(v1->size() == 1);
}
