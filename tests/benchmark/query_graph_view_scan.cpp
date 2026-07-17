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

// Isolates the per-row cost of routing an unlabeled full scan through the
// GraphView seam. RawIterate reads the real graph through VerticesIterable
// directly (the pre-seam path, and the path ScanAllByLabel still takes).
// ViaVertexRange reads the same vertices through VertexRange, the variant the
// seam interposes: every ScanAll now takes this path because the interpreter
// binds the identity GraphView unconditionally. The delta is the variant tax.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <variant>

#include "dbms/database.hpp"
#include "query/db_accessor.hpp"
#include "query/graph_view.hpp"
#include "replication/state.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/view.hpp"
#include "system/system.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/logging.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace {

std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "graph-view-scan-benchmark"};

class ScanBenchFixture : public benchmark::Fixture {
 protected:
  std::optional<memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock>>
      repl_state;
  std::optional<memgraph::utils::Gatekeeper<memgraph::dbms::Database>> db_gk;

  void SetUp(const benchmark::State &state) override {
    repl_state.emplace(std::nullopt);
    memgraph::storage::Config config{};
    config.durability.storage_directory = data_directory;
    config.disk.main_storage_directory = data_directory / "disk";
    db_gk.emplace(std::move(config));

    auto db_acc_opt = db_gk->access();
    MG_ASSERT(db_acc_opt, "Failed to access db");
    auto &db_acc = *db_acc_opt;

    auto storage_acc = db_acc->Access(memgraph::storage::StorageAccessType::WRITE);
    memgraph::query::DbAccessor dba{storage_acc.get()};
    for (int64_t i = 0; i < state.range(0); i++) dba.InsertVertex();
    MG_ASSERT(dba.Commit(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  void TearDown(const benchmark::State &) override {
    db_gk.reset();
    repl_state.reset();
    std::filesystem::remove_all(data_directory);
  }
};

}  // namespace

BENCHMARK_DEFINE_F(ScanBenchFixture, RawIterate)(benchmark::State &state) {
  auto db_acc_opt = db_gk->access();
  auto &db_acc = *db_acc_opt;
  auto storage_acc = db_acc->Access(memgraph::storage::StorageAccessType::READ);
  memgraph::query::DbAccessor dba{storage_acc.get()};

  for (auto _ : state) {
    uint64_t sum = 0;
    for (const auto &vertex : dba.Vertices(memgraph::storage::View::OLD)) {
      sum += vertex.Gid().AsUint();
    }
    benchmark::DoNotOptimize(sum);
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK_DEFINE_F(ScanBenchFixture, ViaVertexRange)(benchmark::State &state) {
  auto db_acc_opt = db_gk->access();
  auto &db_acc = *db_acc_opt;
  auto storage_acc = db_acc->Access(memgraph::storage::StorageAccessType::READ);
  memgraph::query::DbAccessor dba{storage_acc.get()};

  for (auto _ : state) {
    uint64_t sum = 0;
    for (auto scanned : memgraph::query::VertexRange{dba.Vertices(memgraph::storage::View::OLD)}) {
      // Mirror WriteScannedVertex: the deref already materialized a ScanVertex
      // variant; a second visit extracts the element, as the cursor does to
      // write it to the frame.
      std::visit([&](const auto &element) { sum += element.Gid().AsUint(); }, scanned);
    }
    benchmark::DoNotOptimize(sum);
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK_REGISTER_F(ScanBenchFixture, RawIterate)->RangeMultiplier(8)->Range(1 << 10, 1 << 22);
BENCHMARK_REGISTER_F(ScanBenchFixture, ViaVertexRange)->RangeMultiplier(8)->Range(1 << 10, 1 << 22);

BENCHMARK_MAIN();
