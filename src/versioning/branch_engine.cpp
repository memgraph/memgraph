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

#include <string_view>
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

std::string HistoricalOpenFailure(storage::InMemoryStorage::HistoricalAccessError error, uint64_t fork_ts,
                                  std::string_view what) {
  using Error = storage::InMemoryStorage::HistoricalAccessError;
  switch (error) {
    case Error::ForkTimestampNotPinned:
      return fmt::format("{}: its fork point (timestamp {}) is no longer retained.", what, fork_ts);
    case Error::StorageModeNotTransactional:
      return fmt::format("{}: graph versioning requires IN_MEMORY_TRANSACTIONAL storage mode.", what);
  }
  std::unreachable();
}

}  // namespace

storage::Config DiffEngineConfig(const storage::Config &main_config, storage::StorageMode live_mode) {
  storage::Config config = main_config;
  // main_config keeps the mode main started in; SET STORAGE MODE changes only the live one, and a stale analytical
  // mode would build a diff engine without MVCC deltas.
  config.salient.storage_mode = live_mode;
  // Fresh UUID so no registry aliases main; the NAME stays main's, read live via ReportNameOf, because main can be
  // renamed.
  config.salient.uuid = utils::UUID{};
  // The diff engine's indexes mirror main's at the fork; an auto-created one would be built under read-only access
  // that blocks branch writers, and its metadata delta is not one a branch WAL can replay.
  config.salient.items.enable_label_index_auto_creation = false;
  config.salient.items.enable_edge_type_index_auto_creation = false;
  // SHOW SCHEMA INFO reads main's tracker, never the diff engine's.
  config.salient.items.enable_schema_info = false;
  // Base-edge overlays are Edge objects, so the diff engine keeps properties on edges whatever main has.
  config.salient.items.properties_on_edges = true;
  // Overlays and native edges are all heavy, so teardown frees nothing by hand.
  config.salient.items.storage_light_edge = false;
  // Main's telemetry label counts are not the diff engine's.
  config.track_label_counts = false;
  config.experimental_commit_lock_narrowing = false;
  // config.gc stays main's: the diff engine needs its own GC or committed branch deltas are never freed while another
  // transaction is active.
  // The diff engine is never durable: no WAL, snapshot or recovery (a recovery failure would Clear() the shared
  // mapper).
  config.durability.snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::DISABLED;
  config.durability.snapshot_interval = utils::SchedulerInterval{};
  config.durability.recover_on_startup = false;
  config.durability.snapshot_on_exit = false;
  // Unique per build so the diff engine never aliases main's directory (disk usage, file-retainer locks).
  config.durability.storage_directory =
      config.durability.storage_directory / "versioning_diff_engine" / utils::GenerateUUID();
  config.durability.root_data_directory = config.durability.storage_directory;
  return config;
}

std::expected<std::shared_ptr<BranchCore>, BranchCore::BuildError> BranchCore::Build(storage::InMemoryStorage &main,
                                                                                     const ForkPoint &fork,
                                                                                     uint64_t fork_ts) {
  const memory::DbArenaScope db_arena_scope{main.DbArenaPool()};

  // Pinned first so an unpinned fork fails before the diff engine is built; the accessor's shared hold also keeps
  // CREATE ENUM (UNIQUE) out while the enum store is mirrored below.
  auto hist_exp = main.HistoricalAccess(fork_ts);
  if (!hist_exp.has_value()) {
    return std::unexpected(
        BuildError{.message = HistoricalOpenFailure(hist_exp.error(), fork_ts, "Cannot open the branch")});
  }
  std::unique_ptr<storage::Storage::Accessor> historical = std::move(*hist_exp);

  const storage::Config config = DiffEngineConfig(main.config_, main.GetStorageMode());
  // Only the unreleased-delta gauge is shared, so branch memory shows up under the tenant.
  metrics::DatabaseMetricHandles metric_handles{};
  metric_handles.unreleased_delta_objects = main.metric_handles_.unreleased_delta_objects;
  auto diff_engine = std::make_unique<storage::InMemoryStorage>(
      config,
      std::nullopt,
      std::make_unique<storage::PlanInvalidatorDefault>(),
      metric_handles,
      nullptr,
      main.DbArenaPool(),
      nullptr,
      main.GetSharedNameIdMapper(),
      storage::durability::HistoryBounds{},
      storage::RetainedGidFloors{.vertex = fork.vertex_gid_floor, .edge = fork.edge_gid_floor});
  diff_engine->ReportNameOf(main);

  // Done before the core is visible to any session, so the diff engine never needs UNIQUE access afterwards.
  MirrorMainEnumsIntoDiffEngine(*diff_engine, historical->GetEnumStoreShared());

  return std::shared_ptr<BranchCore>(new BranchCore(&main, fork, fork_ts, std::move(diff_engine)));
}

BranchCore::BranchCore(storage::InMemoryStorage *main, const ForkPoint &fork, uint64_t fork_ts,
                       std::unique_ptr<storage::InMemoryStorage> diff_engine)
    : main_(main),
      fork_(fork),
      fork_ts_(fork_ts),
      diff_engine_(std::move(diff_engine)),
      overlay_index_(fork.vertex_gid_floor) {
  // Overlays keep base gids and the diff engine's index scans skip entries at or above its own counters, so the
  // counters start at the floors; native gids come from main's counters and raise these again by fetch_max.
  atomic_fetch_max_explicit(&diff_engine_->vertex_id_, fork_.vertex_gid_floor, std::memory_order_acq_rel);
  atomic_fetch_max_explicit(&diff_engine_->edge_id_, fork_.edge_gid_floor, std::memory_order_acq_rel);
}

// Reset first: the diff engine's deltas and indexes point at overlays owned by the overlay indexes.
BranchCore::~BranchCore() {
  teardown_scope_.emplace(main_->DbArenaPool());
  diff_engine_.reset();
}

}  // namespace memgraph::versioning
