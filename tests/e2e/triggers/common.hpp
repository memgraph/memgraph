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

#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <mgclient.hpp>

inline constexpr std::string_view kVertexLabel{"VERTEX"};
inline constexpr std::string_view kEdgeLabel{"EDGE"};

std::unique_ptr<mg::Client> Connect();
std::unique_ptr<mg::Client> ConnectWithUser(std::string_view username);
void CreateVertex(mg::Client &client, int vertex_id);
void CreateEdge(mg::Client &client, int from_vertex, int to_vertex, int edge_id);

int GetNumberOfAllVertices(mg::Client &client);
void WaitForNumberOfAllVertices(mg::Client &client, int number_of_vertices);
void CheckNumberOfAllVertices(mg::Client &client, int expected_number_of_vertices);
std::optional<mg::Value> GetVertex(mg::Client &client, std::string_view label, int vertex_id);
bool VertexExists(mg::Client &client, std::string_view label, int vertex_id);
void CheckVertexMissing(mg::Client &client, std::string_view label, int vertex_id);
void CheckVertexExists(mg::Client &client, std::string_view label, int vertex_id);
void ExecuteCreateVertex(mg::Client &client, int id);
