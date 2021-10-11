#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <mgclient.hpp>

constexpr std::string_view kVertexLabel{"VERTEX"};
constexpr std::string_view kEdgeLabel{"EDGE"};

std::unique_ptr<mg::Client> Connect();
std::unique_ptr<mg::Client> ConnectWithUser(const std::string_view username);
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
