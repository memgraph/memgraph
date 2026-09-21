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

#include <mutex>
#include <unordered_map>

#include "storage/v2/durability/wal.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/logging.hpp"
#include "utils/uuid.hpp"
#include "utils/variant_helpers.hpp"
#include "versioning/edge_lookup.hpp"

namespace memgraph::versioning {

namespace {

namespace sd = storage::durability;

// Enum anywhere in a value (top-level, List, or Map) blocks the whole COW; see CowError (branch_engine.hpp).
bool ContainsEnum(const storage::PropertyValue &v) {
  if (v.IsEnum()) return true;
  if (v.IsList()) {
    for (const auto &elem : v.ValueList()) {
      if (ContainsEnum(elem)) return true;
    }
    return false;
  }
  if (v.IsMap()) {
    for (const auto &[key, elem] : v.ValueMap()) {
      if (ContainsEnum(elem)) return true;
    }
    return false;
  }
  return false;
}

// Branch-native gid range: gids ≥ this are exclusively branch-native (main allocates from 0 upward).
// A sacrificial create+delete at this gid advances vertex_id_/edge_id_ past it (atomic_fetch_max_explicit in
// CreateVertexEx/CreateEdgeEx, inmemory/storage.cpp) — the only way to advance the counter without a direct setter.
constexpr uint64_t kBranchNativeGidWatermark = 1ULL << 62;

void ReserveBranchNativeGidRange(storage::InMemoryStorage &diff_engine, storage::CommitArgs commit_args) {
  std::unique_ptr<storage::ReplicationAccessor> reserve(
      static_cast<storage::ReplicationAccessor *>(diff_engine.Access(storage::StorageAccessType::WRITE).release()));

  auto sacrificial = reserve->CreateVertexEx(storage::Gid::FromUint(kBranchNativeGidWatermark));
  MG_ASSERT(sacrificial.has_value(),
            "BranchContext::BuildFromFork: gid-watermark reservation collided on a freshly-created, empty diff "
            "engine -- should be impossible.");

  // CreateEdgeEx MG_ASSERTs same-transaction live endpoints (inmemory/storage.cpp:987); sac_from/sac_to
  // are auto-gid and deleted in this same transaction, so their gids never escape this function.
  auto sac_from = reserve->CreateVertex();
  auto sac_to = reserve->CreateVertex();
  auto edge_type = reserve->NameToEdgeType("__branch_native_gid_watermark__");
  auto sacrificial_edge =
      reserve->CreateEdgeEx(&sac_from, &sac_to, edge_type, storage::Gid::FromUint(kBranchNativeGidWatermark));
  MG_ASSERT(sacrificial_edge.has_value(),
            "BranchContext::BuildFromFork: edge gid-watermark reservation collided on a freshly-created, empty diff "
            "engine -- should be impossible.");

  // Edge before endpoints: DeleteVertex returns VERTEX_HAS_EDGES without cascading; endpoints must be edge-free.
  auto deleted_edge = reserve->DeleteEdge(&*sacrificial_edge);
  MG_ASSERT(deleted_edge.has_value() && deleted_edge->has_value(),
            "BranchContext::BuildFromFork: failed to remove the edge gid-watermark placeholder edge.");

  auto deleted_to = reserve->DeleteVertex(&sac_to);
  MG_ASSERT(deleted_to.has_value() && deleted_to->has_value(),
            "BranchContext::BuildFromFork: failed to remove an edge gid-watermark sacrificial vertex.");
  auto deleted_from = reserve->DeleteVertex(&sac_from);
  MG_ASSERT(deleted_from.has_value() && deleted_from->has_value(),
            "BranchContext::BuildFromFork: failed to remove an edge gid-watermark sacrificial vertex.");

  auto deleted = reserve->DeleteVertex(&*sacrificial);
  MG_ASSERT(deleted.has_value() && deleted->has_value(),
            "BranchContext::BuildFromFork: failed to remove the gid-watermark placeholder vertex.");

  auto commit_res = reserve->PrepareForCommitPhase(std::move(commit_args));
  MG_ASSERT(commit_res.has_value(), "BranchContext::BuildFromFork: gid-watermark reservation commit failed.");
}

// Replays all prior-session changelog records into a fresh diff engine: (a) read-your-writes across
// checkout cycles; (b) each explicit-gid create advances vertex_id_/edge_id_ past that gid so a
// later session's auto-gid creates can never reuse a gid a prior session captured.
// tombstoned_vertices/tombstoned_edges are OUT params — BranchContext doesn't exist yet at this stage.
void ReplayChangelogIntoDiffEngine(storage::InMemoryStorage &diff_engine,
                                   const std::vector<storage::durability::WalDeltaData> &changelog,
                                   storage::CommitArgs commit_args,
                                   std::unordered_set<storage::Gid> &tombstoned_vertices,
                                   std::unordered_set<storage::Gid> &tombstoned_edges) {
  if (changelog.empty()) return;

  std::unique_ptr<storage::ReplicationAccessor> replay(
      static_cast<storage::ReplicationAccessor *>(diff_engine.Access(storage::StorageAccessType::WRITE).release()));

  auto apply = utils::Overloaded{
      [&](sd::WalVertexCreate const &data) {
        auto v = replay->CreateVertexEx(data.gid);
        MG_ASSERT(v.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout found vertex gid {} already occupied in a "
                  "freshly-built, otherwise-empty diff engine -- should be impossible.",
                  data.gid.AsUint());
      },
      [&](sd::WalVertexAddLabel const &data) {
        auto v = replay->FindVertex(data.gid, storage::View::NEW);
        MG_ASSERT(v.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout could not find vertex {} to add a label to -- "
                  "the branch's own captured change-log should always create a vertex before mutating it.",
                  data.gid.AsUint());
        auto ret = v->AddLabel(replay->NameToLabel(data.label));
        MG_ASSERT(ret.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout failed to add a label to vertex {}.",
                  data.gid.AsUint());
      },
      [&](sd::WalVertexRemoveLabel const &data) {
        auto v = replay->FindVertex(data.gid, storage::View::NEW);
        MG_ASSERT(v.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout could not find vertex {} to remove a label from "
                  "-- the branch's own captured change-log should always create a vertex before mutating it.",
                  data.gid.AsUint());
        auto ret = v->RemoveLabel(replay->NameToLabel(data.label));
        MG_ASSERT(ret.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout failed to remove a label from vertex {}.",
                  data.gid.AsUint());
      },
      [&](sd::WalVertexSetProperty const &data) {
        auto v = replay->FindVertex(data.gid, storage::View::NEW);
        MG_ASSERT(v.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout could not find vertex {} to set a property on.",
                  data.gid.AsUint());
        auto ret = v->SetProperty(replay->NameToProperty(data.property),
                                  storage::ToPropertyValue(data.value, replay->GetNameIdMapper()));
        MG_ASSERT(ret.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout failed to set a property on vertex {}.",
                  data.gid.AsUint());
      },
      [&](sd::WalEdgeCreate const &data) {
        // View::NEW: endpoint may have been created earlier in this same replay pass (AdvanceCommand not called).
        auto from_v = replay->FindVertex(data.from_vertex, storage::View::NEW);
        auto to_v = replay->FindVertex(data.to_vertex, storage::View::NEW);
        MG_ASSERT(from_v.has_value() && to_v.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout could not find edge {}'s endpoints -- the "
                  "branch's own captured change-log should always create both endpoints before the edge (see "
                  "this function's own ORDERING doc-comment).",
                  data.gid.AsUint());
        auto edge_type = replay->NameToEdgeType(data.edge_type);
        auto e = replay->CreateEdgeEx(&*from_v, &*to_v, edge_type, data.gid);
        MG_ASSERT(e.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout found edge gid {} already occupied -- should "
                  "be impossible.",
                  data.gid.AsUint());
      },
      [&](sd::WalEdgeSetProperty const &data) {
        // Diff engine only — no historical_ fallback during replay, so bare-gid FindEdge is reliable.
        auto e = replay->FindEdge(data.gid, storage::View::NEW);
        MG_ASSERT(e.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout could not find edge {} to set a property on.",
                  data.gid.AsUint());
        auto ret = e->SetProperty(replay->NameToProperty(data.property),
                                  storage::ToPropertyValue(data.value, replay->GetNameIdMapper()));
        MG_ASSERT(ret.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout failed to set a property on edge {}.",
                  data.gid.AsUint());
      },
      // Plain DeleteVertex (not DetachDelete): vertex is guaranteed edge-free in replay order.
      [&](sd::WalVertexDelete const &data) {
        auto v = replay->FindVertex(data.gid, storage::View::NEW);
        MG_ASSERT(v.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout could not find vertex {} to delete -- the "
                  "branch's own captured change-log should always create a vertex before deleting it.",
                  data.gid.AsUint());
        auto ret = replay->DeleteVertex(&*v);
        MG_ASSERT(ret.has_value() && ret->has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout failed to delete vertex {}.",
                  data.gid.AsUint());
        tombstoned_vertices.insert(data.gid);
      },
      // Same as WalEdgeSetProperty: diff engine only, bare-gid FindEdge is reliable here.
      [&](sd::WalEdgeDelete const &data) {
        auto e = replay->FindEdge(data.gid, storage::View::NEW);
        MG_ASSERT(e.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout could not find edge {} to delete.",
                  data.gid.AsUint());
        auto ret = replay->DeleteEdge(&*e);
        MG_ASSERT(ret.has_value() && ret->has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout failed to delete edge {}.",
                  data.gid.AsUint());
        tombstoned_edges.insert(data.gid);
      },
      [&](sd::WalTransactionStart const &) {},
      [&](sd::WalTransactionEnd const &) {},
      [&](auto const &) {}};

  for (const auto &delta : changelog) {
    std::visit(apply, delta.data_);
  }

  auto commit_res = replay->PrepareForCommitPhase(std::move(commit_args));
  MG_ASSERT(commit_res.has_value(), "BranchContext::BuildFromFork: replay-on-checkout commit failed.");
}

// Mirrors main's index DEFINITIONS (not contents) onto the diff engine so the query planner can
// select index scans on a checked-out branch; diff engine auto-populates them via ordinary index
// maintenance. Uses fork_ts-pinned snapshot from `historical`. CreateIndex requires READ_ONLY
// access (MG_ASSERTs it, inmemory/storage.cpp); diff_engine.make_database_protector() supplies
// the commit protector (null database_protector_factory_ resolves to DefaultDatabaseProtector).
void MirrorMainIndexDefinitionsIntoDiffEngine(storage::InMemoryStorage &diff_engine,
                                              const storage::IndicesInfo &main_indices) {
  // No early-return guard: a guard enumerated all index kinds in one check, was twice forgotten when
  // a new kind was added, and silently fell branches back to full scans. Loops are the single truth.
  // ONE ReadOnlyAccess per index: CreateIndex calls DowngradeToReadIfValid (inmemory/storage.cpp),
  // dropping READ_ONLY→READ after the first call; a second CreateIndex on the same access trips its
  // own MG_ASSERT(type() == UNIQUE || READ_ONLY). Each index gets its own fresh accessor.
  auto commit_one = [&diff_engine](storage::Storage::Accessor &acc) {
    auto protector = diff_engine.make_database_protector();
    MG_ASSERT(protector != nullptr,
              "BranchContext::BuildFromFork: failed to obtain a database protector for the freshly-built diff "
              "engine while mirroring main's index definitions -- should be impossible (a null "
              "database_protector_factory_ always resolves to a non-null DefaultDatabaseProtector, storage.cpp).");
    auto commit_res = acc.PrepareForCommitPhase(storage::CommitArgs::make_main(std::move(protector)));
    MG_ASSERT(commit_res.has_value(), "BranchContext::BuildFromFork: index-definition mirror commit failed.");
  };

  for (const auto &label : main_indices.label) {
    auto mirror = diff_engine.ReadOnlyAccess();
    auto created = mirror->CreateIndex(label);
    MG_ASSERT(created.has_value(),
              "BranchContext::BuildFromFork: mirroring main's label index (label {}) onto a freshly-built, "
              "otherwise-empty diff engine failed -- should be impossible.",
              label.AsUint());
    commit_one(*mirror);
  }
  for (const auto &entry : main_indices.label_properties) {
    // All variants (single, composite, nested) mirrored; branch read path handles all of them.
    auto mirror = diff_engine.ReadOnlyAccess();
    auto created = mirror->CreateIndex(entry.label, entry.properties, entry.order);
    MG_ASSERT(created.has_value(),
              "BranchContext::BuildFromFork: mirroring main's label-property index (label {}) onto a "
              "freshly-built, otherwise-empty diff engine failed -- should be impossible.",
              entry.label.AsUint());
    commit_one(*mirror);
  }

  // Edge-type and edge-property indexes require properties_on_edges: PopulateIndex reads EdgeRef.ptr,
  // which is garbage when only .gid is set (reference-only edges); CreateIndex refuses without it.
  if (diff_engine.config_.salient.items.properties_on_edges) {
    for (const auto &edge_type : main_indices.edge_type) {
      auto mirror = diff_engine.ReadOnlyAccess();
      auto created = mirror->CreateIndex(edge_type);
      MG_ASSERT(created.has_value(),
                "BranchContext::BuildFromFork: mirroring main's edge-type index (edge type {}) onto a "
                "freshly-built, otherwise-empty diff engine failed -- should be impossible.",
                edge_type.AsUint());
      commit_one(*mirror);
    }
    // Edge-type+property: ordering-sensitive — planner elides Sort for edge-property index scans,
    // so the ASC-ordered branch merge must be paired with these mirrored definitions.
    for (const auto &[edge_type, property] : main_indices.edge_type_property) {
      auto mirror = diff_engine.ReadOnlyAccess();
      auto created = mirror->CreateIndex(edge_type, property);
      MG_ASSERT(created.has_value(),
                "BranchContext::BuildFromFork: mirroring main's edge-type+property index (edge type {}, "
                "property {}) onto a freshly-built, otherwise-empty diff engine failed -- should be impossible.",
                edge_type.AsUint(),
                property.AsUint());
      commit_one(*mirror);
    }
    // Global edge-property: same ordering constraint as edge-type+property (ASC-ordered merge, Sort elided).
    for (const auto &property : main_indices.edge_property) {
      auto mirror = diff_engine.ReadOnlyAccess();
      auto created = mirror->CreateGlobalEdgeIndex(property);
      MG_ASSERT(created.has_value(),
                "BranchContext::BuildFromFork: mirroring main's global edge-property index (property {}) onto a "
                "freshly-built, otherwise-empty diff engine failed -- should be impossible.",
                property.AsUint());
      commit_one(*mirror);
    }
  }
  // Global vertex-property: no properties_on_edges dependency — vertex scans are always available regardless
  // of the edge storage mode. Mirrors main_indices.vertex_property via CreateGlobalVertexIndex so
  // VertexPropertyIndexReady returns true on the branch and branch-aware vertex-property scans are reachable.
  for (const auto &property : main_indices.vertex_property) {
    auto mirror = diff_engine.ReadOnlyAccess();
    auto created = mirror->CreateGlobalVertexIndex(property);
    MG_ASSERT(created.has_value(),
              "BranchContext::BuildFromFork: mirroring main's global vertex-property index (property {}) onto a "
              "freshly-built, otherwise-empty diff engine failed -- should be impossible.",
              property.AsUint());
    commit_one(*mirror);
  }
}

// Mirrors main's EnumStore onto the diff engine (MIRROR not SHARE: two Storage instances, two locks).
// Order-preserving replay reproduces identical positional enum ids (id = insertion position, enum_store.hpp).
void MirrorMainEnumsIntoDiffEngine(storage::InMemoryStorage &diff_engine, const storage::EnumStore &main_enums) {
  // No early-return guard: same reason as MirrorMainIndexDefinitionsIntoDiffEngine (foot-gun risk outweighs the no-op
  // cost).
  auto registered = main_enums.AllRegistered();

  // ONE UniqueAccess for all enums: CreateEnum does not call DowngradeToReadIfValid (unlike CreateIndex),
  // so all enums can be registered and committed in a single transaction.
  auto unique_acc = diff_engine.UniqueAccess();
  for (const auto &[type_str, value_strs] : registered) {
    auto created = unique_acc->CreateEnum(type_str, value_strs);
    MG_ASSERT(created.has_value(),
              "BranchContext::BuildFromFork: mirroring main's enum '{}' onto a freshly-built, otherwise-empty "
              "diff engine failed -- should be impossible.",
              type_str);
  }
  auto protector = diff_engine.make_database_protector();
  MG_ASSERT(protector != nullptr,
            "BranchContext::BuildFromFork: failed to obtain a database protector for the freshly-built diff "
            "engine while mirroring main's enum definitions -- should be impossible (a null "
            "database_protector_factory_ always resolves to a non-null DefaultDatabaseProtector, storage.cpp).");
  auto commit_res = unique_acc->PrepareForCommitPhase(storage::CommitArgs::make_main(std::move(protector)));
  MG_ASSERT(commit_res.has_value(), "BranchContext::BuildFromFork: enum-definition mirror commit failed.");
}

}  // namespace

std::expected<std::unique_ptr<BranchContext>, BranchContext::BuildError> BranchContext::BuildFromFork(
    storage::InMemoryStorage &main, uint64_t fork_ts, storage::CommitArgs commit_args,
    storage::CommitArgs replay_commit_args, std::filesystem::path branch_wal_root_directory,
    const std::vector<storage::durability::WalDeltaData> &changelog) {
  // 1. Time-travel main to fork_ts first — fail before paying for the diff engine construction.
  auto hist_exp = main.HistoricalAccess(fork_ts);
  if (!hist_exp.has_value()) {
    return std::unexpected(
        BuildError{.message = fmt::format("Cannot check out a version: fork timestamp {} is not (or no longer) pinned.",
                                          fork_ts)});
  }
  std::unique_ptr<storage::Storage::Accessor> historical = std::move(*hist_exp);

  // 2. Construct the empty diff engine (O(1) — nothing copied from main's graph).
  //    Shares main's NameIdMapper: historical vertices carry main's label/property ids; a separate
  //    mapper would silently misdecode them. Config derived from main.config_ wholesale so any
  //    future salient flag (properties_on_edges, storage_light_edge, etc.) is inherited automatically.
  storage::Config config = main.config_;
  // Distinct throwaway instance — avoid aliasing main's UUID in any registry.
  config.salient.uuid = utils::UUID{};
  config.gc.type = storage::Config::Gc::Type::NONE;
  // Transient in-RAM copy: all durability mechanisms off regardless of what main configured.
  config.durability.snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::DISABLED;
  config.durability.recover_on_startup = false;
  config.durability.snapshot_on_exit = false;
  config.durability.restore_replication_state_on_startup = false;
  // Clear copied recovery fields so they don't carry main's unrelated recovery bookkeeping.
  config.durability.recover_oldest_fork_ts.reset();
  config.durability.recover_fork_timestamps.clear();
  // Repoint storage/disk/name paths away from main's own directories; durability is off, but a
  // stale path is one accidental future durability flip away from aliasing main's live data.
  {
    const auto diff_engine_directory =
        config.durability.storage_directory / "versioning_diff_engine" / utils::GenerateUUID();
    config.durability.storage_directory = diff_engine_directory;
    config.durability.root_data_directory = diff_engine_directory;
    config.disk = storage::Config::DiskConfig{};
    config.disk.main_storage_directory = diff_engine_directory / config.disk.main_storage_directory;
    config.disk.label_index_directory = diff_engine_directory / config.disk.label_index_directory;
    config.disk.label_property_index_directory = diff_engine_directory / config.disk.label_property_index_directory;
    config.disk.unique_constraints_directory = diff_engine_directory / config.disk.unique_constraints_directory;
    config.disk.name_id_mapper_directory = diff_engine_directory / config.disk.name_id_mapper_directory;
    config.disk.id_name_mapper_directory = diff_engine_directory / config.disk.id_name_mapper_directory;
    config.disk.durability_directory = diff_engine_directory / config.disk.durability_directory;
    config.disk.wal_directory = diff_engine_directory / config.disk.wal_directory;
    config.salient.name = utils::SafeString{fmt::format("{}__versioning_diff_engine", config.salient.name.str())};
  }
  config.force_on_disk = false;
  // Avoid duplicating/aliasing main's Prometheus metrics as a spurious second database.
  config.register_metrics = false;
  auto diff_engine = std::make_unique<storage::InMemoryStorage>(config,
                                                                std::nullopt,
                                                                std::make_unique<storage::PlanInvalidatorDefault>(),
                                                                metrics::DatabaseMetricHandles{},
                                                                nullptr,
                                                                nullptr,
                                                                nullptr,
                                                                main.GetSharedNameIdMapper());

  // 3. Reserve the branch-native gid range (see kBranchNativeGidWatermark).
  ReserveBranchNativeGidRange(*diff_engine, std::move(commit_args));

  // 3b. Replay prior-session changelog (see ReplayChangelogIntoDiffEngine). Placed after step 3
  //     for readability; order is not load-bearing (atomic_fetch_max lands the same either way).
  //     tombstoned_vertices/tombstoned_edges are OUT params fed to the BranchContext ctor below.
  std::unordered_set<storage::Gid> tombstoned_vertices;
  std::unordered_set<storage::Gid> tombstoned_edges;
  ReplayChangelogIntoDiffEngine(
      *diff_engine, changelog, std::move(replay_commit_args), tombstoned_vertices, tombstoned_edges);

  // 3c. Mirror main's fork_ts-pinned index definitions (see MirrorMainIndexDefinitionsIntoDiffEngine).
  //     Placed after 3b so PopulateIndex captures already-replayed vertices in the initial population.
  MirrorMainIndexDefinitionsIntoDiffEngine(*diff_engine, historical->ListAllIndices());

  // 3c-ii. Mirror main's current EnumStore (historical's SHARED guard on main_lock_ blocks concurrent
  //        CREATE ENUM — see MirrorMainEnumsIntoDiffEngine).
  MirrorMainEnumsIntoDiffEngine(*diff_engine, historical->GetEnumStoreShared());

  // 4. Mint a per-session UUID subdirectory; CreateCommitLog() builds a fresh BranchLog per commit
  //    on demand (see its doc-comment, branch_engine.hpp) — avoids the ReadWalInfo multi-txn bug.
  auto branch_log_session_directory = branch_wal_root_directory / utils::GenerateUUID();

  // `new` not make_unique: private ctor. Seeded with changelog.size() so the retention cap
  // (FLAGS_versioning_max_changelog_length) counts the branch's whole life, not just this session.
  // Named local (not direct return): step 5 below calls MarkMainObjectBranched on this instance.
  auto branch_ctx = std::unique_ptr<BranchContext>(new BranchContext(std::move(diff_engine),
                                                                     std::move(historical),
                                                                     std::move(tombstoned_vertices),
                                                                     std::move(tombstoned_edges),
                                                                     std::move(branch_log_session_directory),
                                                                     config.salient.items,
                                                                     main.GetSharedNameIdMapper().get(),
                                                                     changelog.size()));

  // 5. Re-seed branched() bits from changelog (lost on restart). Vertex gids only: historical_->
  //    FindEdge by bare gid is unreliable (see ResolveEdge); edge endpoints are covered via
  //    WalEdgeCreate's from_vertex/to_vertex vertex gids. No de-dup: MarkMainObjectBranched is
  //    idempotent and changelog is bounded by FLAGS_versioning_max_changelog_length.
  auto mark_if_main_vertex_gid = [&branch_ctx](storage::Gid gid) {
    if (gid.AsUint() < kBranchNativeGidWatermark) branch_ctx->MarkMainObjectBranched(gid);
  };
  // Re-seed branch change filters: live write hooks are bypassed during replay, so filters must be
  // rebuilt from the changelog. COW records (WalVertexAddLabel/SetProperty) over-flag the kind —
  // false positives (extra resolves) only, never false negatives; in-session changes are precise.
  auto record_vertex_kind = [&branch_ctx](storage::Gid gid, BranchChangeKind kind) {
    if (gid.AsUint() < kBranchNativeGidWatermark) branch_ctx->RecordVertexChange(gid, kind);
  };
  auto record_edge_endpoint = [&branch_ctx](storage::Gid gid) {
    if (gid.AsUint() < kBranchNativeGidWatermark) branch_ctx->RecordEdgeChange(gid);
  };
  // Re-seed Edge::branched() bits (see MarkMainEdgeBranched). Required because edge_accessor.cpp
  // reads the bit as a fast path; a missed re-seed means stale reads after a re-checkout.
  auto mark_main_edge_branched = [&branch_ctx](storage::Gid from_vertex_gid, storage::Gid edge_gid) {
    if (from_vertex_gid.AsUint() < kBranchNativeGidWatermark)
      branch_ctx->MarkMainEdgeBranched(from_vertex_gid, edge_gid);
  };
  // Fine property field filter — REQUIRED for correctness (GetProperty gates on it alone; a missed
  // re-seed causes a stale fork-state read after re-checkout). Same COW over-flagging as above.
  auto record_property_field = [&branch_ctx](storage::Gid gid, const std::string &prop_name) {
    if (gid.AsUint() < kBranchNativeGidWatermark)
      branch_ctx->RecordPropertyFieldChange(gid, branch_ctx->diff_engine().NameToProperty(prop_name));
  };
  for (const auto &delta : changelog) {
    std::visit(utils::Overloaded{[&](sd::WalVertexCreate const &data) { mark_if_main_vertex_gid(data.gid); },
                                 [&](sd::WalVertexAddLabel const &data) {
                                   mark_if_main_vertex_gid(data.gid);
                                   record_vertex_kind(data.gid, BranchChangeKind::kLabel);
                                 },
                                 [&](sd::WalVertexRemoveLabel const &data) {
                                   mark_if_main_vertex_gid(data.gid);
                                   record_vertex_kind(data.gid, BranchChangeKind::kLabel);
                                 },
                                 [&](sd::WalVertexSetProperty const &data) {
                                   mark_if_main_vertex_gid(data.gid);
                                   record_vertex_kind(data.gid, BranchChangeKind::kProperty);
                                   record_property_field(data.gid, data.property);
                                 },
                                 [&](sd::WalVertexDelete const &data) { mark_if_main_vertex_gid(data.gid); },
                                 [&](sd::WalEdgeCreate const &data) {
                                   // WalEdgeCreate carries both endpoint gids; covers all edge changes
                                   // (WalEdgeSetProperty/WalEdgeDelete carry only the edge gid).
                                   mark_if_main_vertex_gid(data.from_vertex);
                                   mark_if_main_vertex_gid(data.to_vertex);
                                   record_edge_endpoint(data.from_vertex);
                                   record_edge_endpoint(data.to_vertex);
                                   mark_main_edge_branched(data.from_vertex, data.gid);
                                 },
                                 [&](auto const &) {}},
               delta.data_);
  }

  return branch_ctx;
}

void BranchContext::MarkMainObjectBranched(storage::Gid gid) {
  auto hist_vertex = historical_base_->FindVertex(gid, storage::View::OLD);
  if (!hist_vertex) return;  // best-effort hint -- see doc-comment.
  storage::Vertex *main_vertex = hist_vertex->vertex_;
  auto guard = std::unique_lock{main_vertex->lock};
  main_vertex->SetBranched(true);
}

// Does NOT use historical_->FindEdge (bare-gid scan of CURRENT adjacency vectors — wrong for
// deleted light edges). Delegates the View::OLD endpoint-relative walk to FindHistoricalEdgeByEndpoint
// (edge_lookup.hpp), shared with merge.cpp's pass-1 classifier.
void BranchContext::MarkMainEdgeBranched(storage::Gid from_vertex_gid, storage::Gid edge_gid) {
  if (!diff_engine_->config_.salient.items.properties_on_edges) return;  // reference edges: no Edge object to mark.
  auto found = FindHistoricalEdgeByEndpoint(*historical_base_, from_vertex_gid, edge_gid);
  if (!found) return;  // best-effort hint -- see doc-comment.
  storage::Edge *main_edge = found->edge_.ptr;
  auto edge_guard = std::unique_lock{main_edge->lock};
  main_edge->SetBranched(true);
}

std::expected<storage::VertexAccessor, BranchContext::CowError> BranchContext::CowVertex(storage::Gid gid) {
  // MG_ASSERT not DMG_ASSERT: null current_diff_txn_ is an invariant break (SetupDatabaseTransaction
  // never ran), not a debug-only condition.
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::CowVertex: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call set_current_diff_txn() before any query "
            "runs against a checked-out branch.");
  storage::Storage::Accessor *diff_txn = current_diff_txn_;

  // Defense-in-depth: tombstoned gid must not be re-COW'd. Idempotency check below cannot
  // distinguish "deleted" from "never touched" (both appear as a View::NEW miss in the diff engine).
  // IsVertexTombstoned consults both committed and pending sets (same-txn deletes caught too).
  if (IsVertexTombstoned(gid)) {
    return std::unexpected(
        CowError{fmt::format("Cannot copy-on-write vertex {}: already deleted on this branch.", gid.AsUint())});
  }

  // Idempotent: a prior COW already in the diff engine wins outright.
  if (auto existing = diff_txn->FindVertex(gid, storage::View::NEW)) {
    return *existing;
  }

  auto hist_vertex = historical_base_->FindVertex(gid, storage::View::OLD);
  if (!hist_vertex) {
    // Defensive: callers resolve a vertex before mutating it; gid should exist in one of the two stores.
    return std::unexpected(
        CowError{fmt::format("Cannot copy-on-write vertex {}: not found in the branch's fork-state base either -- "
                             "this should be unreachable (callers resolve a vertex before mutating it).",
                             gid.AsUint())});
  }

  // Marked before the enum check: over-marking is safe (branched()==true is a hint, never a
  // claim of actual divergence); no need to gate on the COW succeeding.
  MarkMainObjectBranched(gid);

  auto labels_res = hist_vertex->Labels(storage::View::OLD);
  MG_ASSERT(labels_res.has_value(),
            "BranchContext::CowVertex: failed to read fork-state labels for vertex {}.",
            gid.AsUint());
  auto props_res = hist_vertex->Properties(storage::View::OLD);
  MG_ASSERT(props_res.has_value(),
            "BranchContext::CowVertex: failed to read fork-state properties for vertex {}.",
            gid.AsUint());

  // Enum check BEFORE touching the diff engine: a rejected COW must leave zero diff-engine side
  // effects — a half-created vertex is indistinguishable from a successful COW by the idempotency check.
  for (const auto &[pid, val] : *props_res) {
    if (ContainsEnum(val)) {
      return std::unexpected(CowError{
          fmt::format("Cannot copy-on-write vertex {}: has an enum property, which versioned branches do not yet "
                      "support.",
                      gid.AsUint())});
    }
  }

  // Shared NameIdMapper: historical_ ids are already valid in the diff engine — no by-name translation.
  // diff_txn is a ReplicationAccessor (adds no data members over InMemoryAccessor); cast is safe.
  auto *replication_accessor = static_cast<storage::ReplicationAccessor *>(diff_txn);
  auto nv = replication_accessor->CreateVertexEx(gid);
  MG_ASSERT(nv.has_value(),
            "BranchContext::CowVertex: gid {} collided while COW'ing into the diff engine -- should be impossible "
            "(the idempotency check above already ruled out a prior occupant, and the gid-watermark reservation "
            "keeps branch-native creates out of historical_'s gid range).",
            gid.AsUint());

  for (auto lbl : *labels_res) {
    auto add_res = nv->AddLabel(lbl);
    MG_ASSERT(
        add_res.has_value(), "BranchContext::CowVertex: failed to add a label while COW'ing vertex {}.", gid.AsUint());
  }
  for (const auto &[pid, val] : *props_res) {
    auto set_res = nv->SetProperty(pid, val);
    MG_ASSERT(set_res.has_value(),
              "BranchContext::CowVertex: failed to set a property while COW'ing vertex {}.",
              gid.AsUint());
  }

  return *nv;
}

std::expected<storage::EdgeAccessor, BranchContext::CowError> BranchContext::CowEdge(
    const storage::EdgeAccessor &fork_edge) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::CowEdge: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call set_current_diff_txn() before any query "
            "runs against a checked-out branch.");

  const auto edge_gid = fork_edge.Gid();

  // Mirrors CowVertex's tombstone guard: IsEdgeTombstoned consults both committed and pending sets.
  if (IsEdgeTombstoned(edge_gid)) {
    return std::unexpected(
        CowError{fmt::format("Cannot copy-on-write edge {}: already deleted on this branch.", edge_gid.AsUint())});
  }

  // Idempotent: prior COW in the diff engine wins outright; fork_edge endpoints/properties not read here.
  if (auto existing = FindDiffEdge(edge_gid, storage::View::NEW)) {
    return *existing;
  }

  // fork_edge is still historical_'s copy; CowVertex resolves endpoints idempotently.
  const auto from_gid = fork_edge.FromVertex().Gid();
  const auto to_gid = fork_edge.ToVertex().Gid();
  const auto edge_type = fork_edge.EdgeType();

  auto props_res = fork_edge.Properties(storage::View::OLD);
  MG_ASSERT(props_res.has_value(),
            "BranchContext::CowEdge: failed to read fork-state properties for edge {}.",
            edge_gid.AsUint());

  // Enum check BEFORE COW'ing endpoints: a rejected COW must leave zero diff-engine side effects
  // — endpoint vertex copies must not be promoted for an edge COW that never completed.
  for (const auto &[pid, val] : *props_res) {
    if (ContainsEnum(val)) {
      return std::unexpected(CowError{
          fmt::format("Cannot copy-on-write edge {}: has an enum property, which versioned branches do not yet "
                      "support.",
                      edge_gid.AsUint())});
    }
  }

  auto from_diff = CowVertex(from_gid);
  if (!from_diff) return std::unexpected(from_diff.error());
  auto to_diff = CowVertex(to_gid);
  if (!to_diff) return std::unexpected(to_diff.error());

  // Supplementary Edge::branched() mark (gated on properties_on_edges: reference-only edges have no
  // Edge object). fork_edge.edge_.ptr is main's live Edge* directly (historical_ is a view over
  // main's physical objects); no by-gid lookup because historical_->FindEdge is unreliable (see ResolveEdge).
  if (diff_engine_->config_.salient.items.properties_on_edges) {
    storage::Edge *main_edge = fork_edge.edge_.ptr;
    auto edge_guard = std::unique_lock{main_edge->lock};
    main_edge->SetBranched(true);
  }

  auto *replication_accessor = static_cast<storage::ReplicationAccessor *>(current_diff_txn_);
  // CreateEdgeEx preserves edge_gid so ResolveEdges' de-dupe (keyed on gid) treats this as the
  // same logical edge as historical_'s copy, not a second unrelated one.
  auto nv = replication_accessor->CreateEdgeEx(&*from_diff, &*to_diff, edge_type, edge_gid);
  MG_ASSERT(nv.has_value(),
            "BranchContext::CowEdge: gid {} collided while COW'ing into the diff engine -- should be impossible "
            "(the idempotency check above already ruled out a prior occupant, and the gid-watermark reservation "
            "keeps branch-native creates out of historical_'s gid range).",
            edge_gid.AsUint());

  for (const auto &[pid, val] : *props_res) {
    auto set_res = nv->SetProperty(pid, val);
    MG_ASSERT(set_res.has_value(),
              "BranchContext::CowEdge: failed to set a property while COW'ing edge {}.",
              edge_gid.AsUint());
  }

  // Flag both endpoints so expansion from either side resolves instead of trusting main's adjacency.
  // Recorded after diff-engine writes (filter's release-store publishes them). First COW only (idempotent early-return
  // skips re-COW).
  RecordEdgeChange(from_gid);
  RecordEdgeChange(to_gid);

  return *nv;
}

std::optional<storage::VertexAccessor> BranchContext::ResolveVertex(storage::Gid gid, storage::View view) {
  // MG_ASSERT not DMG_ASSERT: on the hot read path; a null deref in release would be frequently reachable.
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::ResolveVertex: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call set_current_diff_txn() before any query "
            "runs against a checked-out branch.");
  // Tombstone checked first: a diff-engine miss cannot distinguish "never touched" from "deleted"
  // for a not-yet-COW'd fork vertex (both appear as nullopt). Consults committed and pending sets.
  if (IsVertexTombstoned(gid)) return std::nullopt;
  if (auto diff_vertex = current_diff_txn_->FindVertex(gid, view)) {
    return diff_vertex;
  }
  return historical_base_->FindVertex(gid, storage::View::OLD);
}

std::optional<storage::EdgeAccessor> BranchContext::FindDiffEdge(storage::Gid edge_gid, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::FindDiffEdge: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call set_current_diff_txn() before any query "
            "runs against a checked-out branch.");
  return current_diff_txn_->FindEdge(edge_gid, view);
}

// historical_ uses View::OLD throughout (frozen snapshot — no concept of NEW relative to any txn).
std::vector<storage::EdgeAccessor> BranchContext::ResolveEdges(storage::Gid vertex_gid,
                                                               storage::EdgeDirection direction, storage::View view,
                                                               const std::vector<storage::EdgeTypeId> &edge_types) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::ResolveEdges: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call set_current_diff_txn() before any query "
            "runs against a checked-out branch.");

  std::vector<storage::EdgeAccessor> result;

  // Diff side first: a branch touches only a FEW of a vertex's edges, so the diff set is small.
  // Historical pass skips shadowed gids with a cheap linear scan over that small prefix, avoiding
  // the per-call heap allocation of the old unordered_map<Gid,size_t> seen set.
  if (auto diff_vertex = current_diff_txn_->FindVertex(vertex_gid, view)) {
    auto maybe_result = (direction == storage::EdgeDirection::OUT) ? diff_vertex->OutEdges(view, edge_types, nullptr)
                                                                   : diff_vertex->InEdges(view, edge_types, nullptr);
    if (maybe_result.has_value()) {
      result.reserve(maybe_result->edges.size());
      for (auto &edge : maybe_result->edges) {
        // Tombstoned edges must not enter result from either side; pending set consulted too.
        if (IsEdgeTombstoned(edge.Gid())) continue;
        result.push_back(edge);
      }
    }
  }
  const size_t diff_count = result.size();

  // Historical side: append non-shadowed, non-tombstoned edges. Linear scan over diff_count (small).
  if (auto hist_vertex = historical_base_->FindVertex(vertex_gid, storage::View::OLD)) {
    auto maybe_result = (direction == storage::EdgeDirection::OUT)
                            ? hist_vertex->OutEdges(storage::View::OLD, edge_types, nullptr)
                            : hist_vertex->InEdges(storage::View::OLD, edge_types, nullptr);
    if (maybe_result.has_value()) {
      result.reserve(diff_count + maybe_result->edges.size());
      for (auto &edge : maybe_result->edges) {
        const auto gid = edge.Gid();
        if (IsEdgeTombstoned(gid)) continue;
        bool shadowed = false;
        for (size_t i = 0; i < diff_count; ++i) {
          if (result[i].Gid() == gid) {
            shadowed = true;
            break;
          }
        }
        if (!shadowed) result.push_back(edge);
      }
    }
  }

  return result;
}

BranchContext::UnionVerticesIterable BranchContext::Vertices(storage::View view) {
  // MG_ASSERT not DMG_ASSERT (same rationale as CowVertex).
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::Vertices: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call set_current_diff_txn() before any query "
            "runs against a checked-out branch.");
  // Two raw tombstone pointers passed to Iterator (not IsVertexTombstoned) — see UnionVerticesIterable ctor
  // (branch_engine.hpp).
  return UnionVerticesIterable(historical_base_->Vertices(storage::View::OLD),
                               current_diff_txn_->Vertices(view),
                               &tombstoned_vertices_,
                               &pending_tombstoned_vertices_);
}

BranchContext::UnionVerticesIterable::UnionVerticesIterable(
    storage::VerticesIterable hist_vertices, storage::VerticesIterable diff_vertices,
    const std::unordered_set<storage::Gid> *tombstoned_vertices,
    const std::unordered_set<storage::Gid> *pending_tombstoned_vertices)
    : hist_vertices_(std::move(hist_vertices)),
      diff_vertices_(std::move(diff_vertices)),
      tombstoned_vertices_(tombstoned_vertices),
      pending_tombstoned_vertices_(pending_tombstoned_vertices) {}

BranchContext::UnionVerticesIterable::Iterator BranchContext::UnionVerticesIterable::begin() {
  return Iterator(hist_vertices_.begin(),
                  hist_vertices_.end(),
                  diff_vertices_.begin(),
                  diff_vertices_.end(),
                  tombstoned_vertices_,
                  pending_tombstoned_vertices_);
}

BranchContext::UnionVerticesIterable::Iterator::Iterator(
    storage::VerticesIterable::Iterator hist_it, storage::VerticesIterable::Iterator hist_end,
    storage::VerticesIterable::Iterator diff_it, storage::VerticesIterable::Iterator diff_end,
    const std::unordered_set<storage::Gid> *tombstoned_vertices,
    const std::unordered_set<storage::Gid> *pending_tombstoned_vertices)
    : hist_it_(std::move(hist_it)),
      hist_end_(std::move(hist_end)),
      diff_it_(std::move(diff_it)),
      diff_end_(std::move(diff_end)),
      tombstoned_vertices_(tombstoned_vertices),
      pending_tombstoned_vertices_(pending_tombstoned_vertices) {
  SeekNext();
}

BranchContext::UnionVerticesIterable::Iterator &BranchContext::UnionVerticesIterable::Iterator::operator++() {
  SeekNext();
  return *this;
}

// Streaming gid-ordered merge (O(H+D); see class comment in branch_engine.hpp). Skip-loop needed
// because a not-yet-COW'd fork vertex deleted on the branch won't appear in the diff engine's own
// scan — only the historical_ side needs the tombstone check; diff-side MVCC visibility handles the rest.
void BranchContext::UnionVerticesIterable::Iterator::SeekNext() {
  while (true) {
    const bool hist_has = hist_it_.has_value() && !(*hist_it_ == *hist_end_);
    const bool diff_has = diff_it_.has_value() && !(*diff_it_ == *diff_end_);

    if (!hist_has && !diff_has) {
      current_.reset();
      done_ = true;
      return;
    }

    if (hist_has && diff_has) {
      const auto hist_gid = (**hist_it_).Gid();
      const auto diff_gid = (**diff_it_).Gid();

      if (hist_gid < diff_gid) {
        // historical_-only: skip if tombstoned. Two raw-pointer checks (not IsVertexTombstoned)
        // because the Iterator has no BranchContext* to call through — see ctor doc (branch_engine.hpp).
        const bool tombstoned =
            (tombstoned_vertices_ != nullptr && tombstoned_vertices_->contains(hist_gid)) ||
            (pending_tombstoned_vertices_ != nullptr && pending_tombstoned_vertices_->contains(hist_gid));
        if (tombstoned) {
          ++(*hist_it_);
          continue;
        }
        current_ = **hist_it_;
        ++(*hist_it_);
        done_ = false;
        return;
      }
      if (diff_gid < hist_gid) {
        current_ = **diff_it_;
        ++(*diff_it_);
        done_ = false;
        return;
      }
      // Tie: diff engine's copy wins (COW'd or modified); historical_'s fork-state superseded wholesale.
      // No tombstone check: diff-side MVCC visibility already excludes deleted diff-resident gids.
      current_ = **diff_it_;
      ++(*hist_it_);
      ++(*diff_it_);
      done_ = false;
      return;
    }

    if (hist_has) {
      const auto hist_gid = (**hist_it_).Gid();
      const bool tombstoned =
          (tombstoned_vertices_ != nullptr && tombstoned_vertices_->contains(hist_gid)) ||
          (pending_tombstoned_vertices_ != nullptr && pending_tombstoned_vertices_->contains(hist_gid));
      if (tombstoned) {
        ++(*hist_it_);
        continue;
      }
      current_ = **hist_it_;
      ++(*hist_it_);
      done_ = false;
      return;
    }

    current_ = **diff_it_;
    ++(*diff_it_);
    done_ = false;
    return;
  }
}

}  // namespace memgraph::versioning
