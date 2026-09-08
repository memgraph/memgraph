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

// Deterministic flag-off identity harness for the pipelined-commit work. Runs a fixed accessor sequence on an
// InMemoryStorage with a fixed UUID, epoch and WAL sequence number, GC and snapshots disabled and no background
// activity, and leaves the finalized WAL files in the directory named on the command line. Built against two
// revisions and run on each, the resulting WAL files must compare byte for byte (CRCs and summaries included).
//
//   storage_v2_off_identity <output-directory>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"

using memgraph::storage::Config;
using memgraph::storage::Gid;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::PropertyValue;
using memgraph::storage::View;

namespace {

constexpr int kTransactions = 200;
constexpr std::string_view kUuid = "0f6c1d2e-7b3a-4c5d-8e9f-0a1b2c3d4e5f";
constexpr std::string_view kEpoch = "off-identity-epoch";

int Run(std::filesystem::path const &out_dir) {
  std::filesystem::remove_all(out_dir);
  std::filesystem::create_directories(out_dir);

  Config config;
  config.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  config.durability.snapshot_on_exit = false;
  config.durability.snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)};
  config.durability.wal_file_flush_every_n_tx = 1;
  config.gc.type = Config::Gc::Type::NONE;
  config.salient.uuid.set(kUuid);
  config.register_metrics = false;
  memgraph::storage::UpdatePaths(config, out_dir);
  {
    auto storage = std::make_unique<InMemoryStorage>(config);
    storage->repl_storage_state_.epoch_.SetEpoch(std::string{kEpoch});
    auto const label = storage->NameToLabel("L");
    auto const other = storage->NameToLabel("M");
    auto const prop = storage->NameToProperty("p");
    auto const text = storage->NameToProperty("t");
    auto const edge_type = storage->NameToEdgeType("E");
    std::vector<Gid> vertices;
    std::vector<Gid> edges;
    for (int i = 0; i < kTransactions; ++i) {
      auto acc = storage->Access(memgraph::storage::WRITE);
      auto const kind = i % 5;
      if (kind == 0 || vertices.size() < 2) {
        auto vertex = acc->CreateVertex();
        vertices.push_back(vertex.Gid());
        if (!vertex.SetProperty(prop, PropertyValue(i)).has_value()) return 2;
      } else if (kind == 1) {
        auto vertex = acc->FindVertex(vertices[i % vertices.size()], View::NEW);
        if (!vertex || !vertex->AddLabel(i % 2 == 0 ? label : other).has_value()) return 2;
      } else if (kind == 2) {
        auto vertex = acc->FindVertex(vertices[i % vertices.size()], View::NEW);
        if (!vertex || !vertex->SetProperty(text, PropertyValue("value-" + std::to_string(i))).has_value()) return 2;
        if (!vertex->SetProperty(prop, PropertyValue(static_cast<double>(i) / 3.0)).has_value()) return 2;
      } else if (kind == 3) {
        auto from = acc->FindVertex(vertices[i % vertices.size()], View::NEW);
        auto to = acc->FindVertex(vertices[(i + 1) % vertices.size()], View::NEW);
        if (!from || !to) return 2;
        auto edge = acc->CreateEdge(&*from, &*to, edge_type);
        if (!edge.has_value()) return 2;
        edges.push_back(edge->Gid());
        if (!edge->SetProperty(prop, PropertyValue(i)).has_value()) return 2;
      } else {
        // Delete the oldest vertex still present (detaching its edges) roughly every fifth transaction, but keep at
        // least two vertices so later transactions have something to touch.
        if (vertices.size() > 2) {
          auto victim = acc->FindVertex(vertices.front(), View::NEW);
          if (!victim) return 2;
          if (!acc->DetachDeleteVertex(&*victim).has_value()) return 2;
          vertices.erase(vertices.begin());
        } else {
          auto vertex = acc->FindVertex(vertices.back(), View::NEW);
          if (!vertex || !vertex->RemoveLabel(label).has_value()) return 2;
        }
      }
      if (!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) return 3;
    }
  }
  // The storage destructor finalizes the current WAL file.
  size_t files = 0;
  for (auto const &entry : std::filesystem::directory_iterator(out_dir / "wal")) {
    if (entry.is_regular_file()) ++files;
  }
  std::cout << "wrote " << files << " WAL file(s) to " << (out_dir / "wal").string() << '\n';
  return files == 0 ? 4 : 0;
}

}  // namespace

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cerr << "usage: " << argv[0] << " <output-directory>\n";
    return 1;
  }
  return Run(std::filesystem::path{argv[1]});
}
