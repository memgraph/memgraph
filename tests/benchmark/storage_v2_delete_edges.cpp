// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Times Storage::Accessor::DetachDelete on the shapes of graph that stress its two halves differently: collecting the
// edges to erase, and finding them in each vertex's adjacency. Only the DetachDelete call is timed; building the graph,
// looking up the edges and committing are not.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <random>
#include <utility>
#include <vector>

#include <benchmark/benchmark.h>
#include <spdlog/spdlog.h>

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/logging.hpp"

namespace {

using memgraph::storage::Config;
using memgraph::storage::EdgeAccessor;
using memgraph::storage::Gid;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::Storage;
using memgraph::storage::VertexAccessor;
using memgraph::storage::View;

constexpr int64_t kHubs = 25;

std::unique_ptr<Storage> MakeStorage(benchmark::State const &state) {
  Config config{.gc = {.type = Config::Gc::Type::NONE}};
  config.salient.items.properties_on_edges = state.range(0) != 0;
  return std::make_unique<InMemoryStorage>(config);
}

void Commit(Storage::Accessor &acc) {
  MG_ASSERT(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

std::vector<EdgeAccessor> OutEdges(VertexAccessor const &vertex) {
  std::vector<EdgeAccessor> edges;
  for (auto const &edge : vertex.OutEdges(View::OLD)->edges) edges.push_back(edge);
  return edges;
}

std::vector<EdgeAccessor *> Pointers(std::vector<EdgeAccessor> &edges) {
  std::vector<EdgeAccessor *> pointers;
  pointers.reserve(edges.size());
  for (auto &edge : edges) pointers.push_back(&edge);
  return pointers;
}

// Times one DetachDelete and records it as the iteration's time.
void TimedDetachDelete(benchmark::State &state, Storage::Accessor &acc, std::vector<VertexAccessor *> nodes,
                       std::vector<EdgeAccessor *> edges, bool detach) {
  auto const start = std::chrono::steady_clock::now();
  auto result = acc.DetachDelete(std::move(nodes), std::move(edges), detach);
  auto const elapsed = std::chrono::steady_clock::now() - start;
  MG_ASSERT(result.has_value() && result->has_value());
  state.SetIterationTime(std::chrono::duration<double>(elapsed).count());
}

// kHubs hubs and `users` users, each user linked to every hub, created user by user so the hubs' in-edges are in user
// order: the oldest user's edges at the front, the newest one's at the back.
struct HubGraph {
  std::unique_ptr<Storage> storage;
  std::vector<Gid> users;
};

HubGraph MakeHubGraph(benchmark::State const &state, int64_t users) {
  HubGraph graph{.storage = MakeStorage(state), .users = {}};
  graph.users.reserve(users);
  auto acc = graph.storage->Access(memgraph::storage::WRITE);
  auto const edge_type = acc->NameToEdgeType("LINKED_TO");
  std::vector<VertexAccessor> hubs;
  for (int64_t i = 0; i < kHubs; ++i) hubs.push_back(acc->CreateVertex());
  for (int64_t i = 0; i < users; ++i) {
    auto user = acc->CreateVertex();
    for (auto &hub : hubs) MG_ASSERT(acc->CreateEdge(&user, &hub, edge_type).has_value());
    graph.users.push_back(user.Gid());
  }
  Commit(*acc);
  return graph;
}

// `vertices` vertices and `edges` edges between uniformly random pairs, so every vertex has a low degree.
struct SparseGraph {
  std::unique_ptr<Storage> storage;
  std::vector<std::pair<Gid, Gid>> edges;  // edge, from vertex
};

SparseGraph MakeSparseGraph(benchmark::State const &state, int64_t vertices, int64_t edges) {
  SparseGraph graph{.storage = MakeStorage(state), .edges = {}};
  graph.edges.reserve(edges);
  auto acc = graph.storage->Access(memgraph::storage::WRITE);
  auto const edge_type = acc->NameToEdgeType("E");
  std::vector<VertexAccessor> created;
  created.reserve(vertices);
  for (int64_t i = 0; i < vertices; ++i) created.push_back(acc->CreateVertex());
  std::mt19937_64 rng{42};
  std::uniform_int_distribution<int64_t> pick{0, vertices - 1};
  for (int64_t i = 0; i < edges; ++i) {
    auto &from = created[pick(rng)];
    auto edge = acc->CreateEdge(&from, &created[pick(rng)], edge_type);
    MG_ASSERT(edge.has_value());
    graph.edges.emplace_back(edge->Gid(), from.Gid());
  }
  Commit(*acc);
  return graph;
}

// One vertex with range(1) in-edges, each from a different vertex, detach-deleted: every opposing vertex loses one
// edge, so this is the case with the most vertices to clean up per deleted edge.
// NOLINTNEXTLINE(google-runtime-references)
void DetachDeleteHub(benchmark::State &state) {
  for (auto _ : state) {
    auto storage = MakeStorage(state);
    Gid hub_gid;
    {
      auto acc = storage->Access(memgraph::storage::WRITE);
      auto const edge_type = acc->NameToEdgeType("E");
      auto hub = acc->CreateVertex();
      for (int64_t i = 0; i < state.range(1); ++i) {
        auto source = acc->CreateVertex();
        MG_ASSERT(acc->CreateEdge(&source, &hub, edge_type).has_value());
      }
      hub_gid = hub.Gid();
      Commit(*acc);
    }
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto hub = acc->FindVertex(hub_gid, View::OLD);
    MG_ASSERT(hub);
    TimedDetachDelete(state, *acc, {&*hub}, {}, true);
  }
}

// Every edge of a sparse graph deleted in one call: the largest set of edges to erase, spread over many vertices.
// NOLINTNEXTLINE(google-runtime-references)
void DeleteAllEdges(benchmark::State &state) {
  for (auto _ : state) {
    auto graph = MakeSparseGraph(state, state.range(1), state.range(2));
    auto acc = graph.storage->Access(memgraph::storage::WRITE);
    std::vector<EdgeAccessor> edges;
    edges.reserve(graph.edges.size());
    for (auto vertex : acc->Vertices(View::OLD)) {
      for (auto const &edge : vertex.OutEdges(View::OLD)->edges) edges.push_back(edge);
    }
    TimedDetachDelete(state, *acc, {}, Pointers(edges), false);
  }
}

// range(2) of range(1) users detach-deleted from the hub graph: each hub loses range(2) edges in one scan.
// NOLINTNEXTLINE(google-runtime-references)
void DetachDeleteManyUsers(benchmark::State &state) {
  for (auto _ : state) {
    auto graph = MakeHubGraph(state, state.range(1));
    auto acc = graph.storage->Access(memgraph::storage::WRITE);
    std::vector<VertexAccessor> users;
    users.reserve(state.range(2));
    for (int64_t i = 0; i < state.range(2); ++i) {
      auto user = acc->FindVertex(graph.users[i], View::OLD);
      MG_ASSERT(user);
      users.push_back(*user);
    }
    std::vector<VertexAccessor *> pointers;
    for (auto &user : users) pointers.push_back(&user);
    TimedDetachDelete(state, *acc, std::move(pointers), {}, true);
  }
}

// One user's kHubs edges deleted per iteration, one transaction each. Taking users oldest first keeps their edges at
// the front of the hubs' adjacency, newest first keeps them at the back. The order only matters to a search that
// scans the adjacency from the back and stops once all edges are found; partitioning the whole adjacency costs the
// same either way.
// NOLINTNEXTLINE(google-runtime-references)
void DeleteUserHubEdges(benchmark::State &state, bool oldest_first) {
  auto graph = MakeHubGraph(state, state.range(1));
  if (!oldest_first) std::ranges::reverse(graph.users);
  auto next_user = graph.users.begin();
  for (auto _ : state) {
    MG_ASSERT(next_user != graph.users.end(), "Ran out of users; lower the iteration count");
    auto acc = graph.storage->Access(memgraph::storage::WRITE);
    auto user = acc->FindVertex(*next_user++, View::OLD);
    MG_ASSERT(user);
    auto edges = OutEdges(*user);
    TimedDetachDelete(state, *acc, {}, Pointers(edges), false);
    Commit(*acc);
  }
}

// NOLINTNEXTLINE(google-runtime-references)
void DeleteOldestUserHubEdges(benchmark::State &state) { DeleteUserHubEdges(state, true); }

// NOLINTNEXTLINE(google-runtime-references)
void DeleteNewestUserHubEdges(benchmark::State &state) { DeleteUserHubEdges(state, false); }

// One random edge of a sparse graph deleted per iteration, one transaction each: the common case, dominated by the
// fixed cost of a call.
// NOLINTNEXTLINE(google-runtime-references)
void DeleteRandomEdge(benchmark::State &state) {
  auto graph = MakeSparseGraph(state, state.range(1), state.range(2));
  std::ranges::shuffle(graph.edges, std::mt19937_64{7});
  auto next_edge = graph.edges.begin();
  for (auto _ : state) {
    MG_ASSERT(next_edge != graph.edges.end(), "Ran out of edges; lower the iteration count");
    auto const [edge_gid, from_gid] = *next_edge++;
    auto acc = graph.storage->Access(memgraph::storage::WRITE);
    // FindEdge reads the edge skip list, which is empty without properties on edges, so go through the source.
    auto from = acc->FindVertex(from_gid, View::OLD);
    MG_ASSERT(from);
    auto edges = OutEdges(*from);
    auto edge = std::ranges::find_if(edges, [&](auto const &e) { return e.Gid() == edge_gid; });
    MG_ASSERT(edge != edges.end());
    TimedDetachDelete(state, *acc, {}, {&*edge}, false);
    Commit(*acc);
  }
}

// Every case runs with and without properties on edges, since EdgeRef holds an Edge pointer in one mode and a Gid in
// the other.
BENCHMARK(DetachDeleteHub)
    ->ArgNames({"properties_on_edges", "edges"})
    ->ArgsProduct({{0, 1}, {100'000}})
    ->Iterations(5)
    ->UseManualTime()
    ->Unit(benchmark::kMillisecond);

BENCHMARK(DeleteAllEdges)
    ->ArgNames({"properties_on_edges", "vertices", "edges"})
    ->ArgsProduct({{0, 1}, {100'000}, {1'000'000}})
    ->Iterations(3)
    ->UseManualTime()
    ->Unit(benchmark::kMillisecond);

BENCHMARK(DetachDeleteManyUsers)
    ->ArgNames({"properties_on_edges", "users", "deleted_users"})
    ->ArgsProduct({{0, 1}, {20'000}, {2'000}})
    ->Iterations(3)
    ->UseManualTime()
    ->Unit(benchmark::kMillisecond);

BENCHMARK(DeleteOldestUserHubEdges)
    ->ArgNames({"properties_on_edges", "users"})
    ->ArgsProduct({{0, 1}, {40'000}})
    ->Iterations(200)
    ->UseManualTime()
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(DeleteNewestUserHubEdges)
    ->ArgNames({"properties_on_edges", "users"})
    ->ArgsProduct({{0, 1}, {40'000}})
    ->Iterations(200)
    ->UseManualTime()
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(DeleteRandomEdge)
    ->ArgNames({"properties_on_edges", "vertices", "edges"})
    ->ArgsProduct({{0, 1}, {100'000}, {1'000'000}})
    ->Iterations(1'000)
    ->UseManualTime()
    ->Unit(benchmark::kMicrosecond);

}  // namespace

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::off);
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
