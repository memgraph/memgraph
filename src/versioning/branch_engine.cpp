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

#include "versioning/branch_engine.hpp"

#include <fmt/format.h>

#include <utility>

#include "memory/db_arena.hpp"
#include "utils/atomic_utils.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"

namespace memgraph::versioning {

namespace {

// Mirrors rather than shares main's EnumStore (separate Storage instances, separate locks); in-order registration
// reproduces the positional enum ids (RegisterEnum: id = insertion position).
void MirrorMainEnumsIntoDiffEngine(storage::InMemoryStorage &diff_engine, const storage::EnumStore &main_enums) {
  auto registered = main_enums.AllRegistered();

  // One UniqueAccess for all enums: unlike CreateIndex, CreateEnum does not downgrade the access.
  auto unique_acc = diff_engine.UniqueAccess();
  for (const auto &[type_str, value_strs] : registered) {
    auto created = unique_acc->CreateEnum(type_str, value_strs);
    MG_ASSERT(created.has_value(),
              "BranchCore::Build: mirroring main's enum '{}' onto a freshly-built, otherwise-empty "
              "diff engine failed -- should be impossible.",
              type_str);
  }
  auto protector = diff_engine.make_database_protector();
  MG_ASSERT(protector != nullptr,
            "BranchCore::Build: failed to obtain a database protector for the freshly-built diff "
            "engine while mirroring main's enum definitions -- should be impossible (a null "
            "database_protector_factory_ always resolves to a non-null DefaultDatabaseProtector, storage.cpp).");
  auto commit_res = unique_acc->PrepareForCommitPhase(storage::CommitArgs::make_main(std::move(protector)));
  MG_ASSERT(commit_res.has_value(), "BranchCore::Build: enum-definition mirror commit failed.");
}

}  // namespace

std::expected<std::shared_ptr<BranchCore>, BranchCore::BuildError> BranchCore::Build(
    storage::InMemoryStorage &main, uint64_t fork_ts, std::optional<RecordedFloors> recorded) {
  const memory::DbArenaScope db_arena_scope{main.DbArenaPool()};

  // Pinned first so an unpinned fork fails before the diff engine is built; the accessor's shared hold also keeps
  // CREATE ENUM (UNIQUE) out while the enum store is mirrored below.
  auto hist_exp = main.HistoricalAccess(fork_ts);
  if (!hist_exp.has_value()) {
    return std::unexpected(
        BuildError{.message = fmt::format(
                       "Cannot open the branch: its fork point (timestamp {}) is no longer retained.", fork_ts)});
  }
  std::unique_ptr<storage::Storage::Accessor> historical = std::move(*hist_exp);

  storage::Config config = main.config_;
  // main.config_ keeps the mode main started in; SET STORAGE MODE changes only the live one. A stale analytical
  // mode would build a diff engine without MVCC deltas.
  config.salient.storage_mode = main.GetStorageMode();
  // Fresh UUID so no registry aliases main; the NAME stays main's, read live via ReportNameOf below, because
  // DbAccessor::DatabaseName() reads it and main can be renamed.
  config.salient.uuid = utils::UUID{};
  // The diff engine's indexes mirror main's at the fork. An auto-created one would be built on the diff engine only,
  // under read-only access that blocks branch writers, and its metadata delta is not a delta a branch WAL can replay.
  config.salient.items.enable_label_index_auto_creation = false;
  config.salient.items.enable_edge_type_index_auto_creation = false;
  // SHOW SCHEMA INFO reads main's tracker, never the diff engine's, so per-commit tracking here would be waste.
  config.salient.items.enable_schema_info = false;
  // Fork-edge stubs are heavy Edge objects, so the diff engine keeps properties on edges whatever main has.
  config.salient.items.properties_on_edges = true;
  // Narrowing is unbenched on the diff engine, so branch commits keep the full commit lock.
  config.experimental_commit_lock_narrowing = false;
  // config.gc stays main's: the diff engine needs its own GC or committed branch deltas are never freed while another
  // transaction is active.
  config.durability.snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::DISABLED;
  config.durability.snapshot_interval = utils::SchedulerInterval{};
  config.durability.recover_on_startup = false;
  // Main's retention needs are not the diff engine's.
  config.durability.recovery_history_bound.reset();
  config.durability.retention_floor.reset();
  config.durability.snapshot_on_exit = false;
  // Unique per build so the diff engine never aliases main's directory (disk usage, file-retainer locks).
  config.durability.storage_directory =
      config.durability.storage_directory / "versioning_diff_engine" / utils::GenerateUUID();
  config.durability.root_data_directory = config.durability.storage_directory;
  // Only the unreleased-delta gauge is shared, so branch memory shows up under the tenant.
  metrics::DatabaseMetricHandles metric_handles{};
  metric_handles.unreleased_delta_objects = main.metric_handles_.unreleased_delta_objects;
  // The shared mapper carries main's ids, and a recovery failure at construction would Clear() it.
  MG_ASSERT(!config.durability.recover_on_startup, "a diff engine must not run recovery over main's name mapper");
  auto diff_engine = std::make_unique<storage::InMemoryStorage>(config,
                                                                std::nullopt,
                                                                std::make_unique<storage::PlanInvalidatorDefault>(),
                                                                metric_handles,
                                                                nullptr,
                                                                main.DbArenaPool(),
                                                                nullptr,
                                                                main.GetSharedNameIdMapper());
  diff_engine->ReportNameOf(main);

  // Done before the core is visible to any session, so the diff engine never needs UNIQUE access afterwards.
  MirrorMainEnumsIntoDiffEngine(*diff_engine, historical->GetEnumStoreShared());

  // Read after the fork pin: gids visible at fork_ts are below the floors, every later allocation is at or above.
  return std::shared_ptr<BranchCore>(
      new BranchCore(&main,
                     fork_ts,
                     std::move(diff_engine),
                     recorded ? recorded->vertex_gid_floor : main.vertex_id_.load(std::memory_order_acquire),
                     recorded ? recorded->edge_gid_floor : main.edge_id_.load(std::memory_order_acquire)));
}

BranchCore::BranchCore(storage::InMemoryStorage *main, uint64_t fork_ts,
                       std::unique_ptr<storage::InMemoryStorage> diff_engine, uint64_t vertex_gid_floor,
                       uint64_t edge_gid_floor)
    : main_(main),
      fork_ts_(fork_ts),
      vertex_gid_floor_(vertex_gid_floor),
      edge_gid_floor_(edge_gid_floor),
      vertex_gid_ceiling_(vertex_gid_floor),
      edge_gid_ceiling_(edge_gid_floor),
      diff_engine_(std::move(diff_engine)) {
  // Stubs keep fork gids and the diff engine's index scans skip entries at or above its own counters, so the counters
  // start at the floors; native gids come from main's counters and raise these again by fetch_max.
  atomic_fetch_max_explicit(&diff_engine_->vertex_id_, vertex_gid_floor_, std::memory_order_acq_rel);
  atomic_fetch_max_explicit(&diff_engine_->edge_id_, edge_gid_floor_, std::memory_order_acq_rel);
}

void BranchCore::ReserveNativeGid(GidKind kind, uint64_t gid) {
  auto &known = kind == GidKind::kVertex ? vertex_gid_ceiling_ : edge_gid_ceiling_;
  if (gid < known.load(std::memory_order_acquire)) [[likely]]
    return;
  if (!gid_reserver_) return;
  const auto ceiling = gid_reserver_(kind, gid + 1);
  if (!ceiling) throw utils::BasicException("{}", ceiling.error());
  atomic_fetch_max_explicit(&known, *ceiling, std::memory_order_release);
}

// Reset first: the diff engine's deltas and indexes point at stubs owned by the stub indexes.
BranchCore::~BranchCore() {
  teardown_scope_.emplace(main_->DbArenaPool());
  diff_engine_.reset();
}

}  // namespace memgraph::versioning
