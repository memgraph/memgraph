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

#include <algorithm>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>

#include "storage/v2/durability/wal.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/logging.hpp"
#include "utils/uuid.hpp"

namespace memgraph::versioning {

namespace {

constexpr std::string_view kReservedPrefix = "__mg_";
constexpr std::string_view kRemovalMarkerPrefix = "__mg_rm:";
constexpr std::string_view kRemovedPropsName = "__mg_removed";
constexpr std::string_view kDeletedLabelName = "__mg_deleted";
constexpr std::string_view kDeletedEdgesName = "__mg_deleted_edges";

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

  // Overlay support for label+property scans (query::DbAccessor::Vertices(view, label, properties, ...)). A touched
  // fork vertex is a sparse stub, so a stub that newly matches (:L gained, or a key property set) is absent from the
  // diff composite index. It is enumerated instead through a diff label index on L and a diff property-only index on
  // each key property's top-level pid ("stubs that SET p"). Created here only when not already mirrored above.
  {
    std::unordered_set<storage::LabelId> labels(main_indices.label.begin(), main_indices.label.end());
    std::unordered_set<storage::PropertyId> pids(main_indices.vertex_property.begin(),
                                                 main_indices.vertex_property.end());
    for (const auto &entry : main_indices.label_properties) {
      if (labels.insert(entry.label).second) {
        auto mirror = diff_engine.ReadOnlyAccess();
        auto created = mirror->CreateIndex(entry.label);
        MG_ASSERT(created.has_value(),
                  "BranchContext::BuildFromFork: creating the diff-side overlay label index (label {}) on a "
                  "freshly-built diff engine failed -- should be impossible.",
                  entry.label.AsUint());
        commit_one(*mirror);
      }
      for (const auto &path : entry.properties) {
        const auto pid = path.front();
        if (!pids.insert(pid).second) continue;
        auto mirror = diff_engine.ReadOnlyAccess();
        auto created = mirror->CreateGlobalVertexIndex(pid);
        MG_ASSERT(created.has_value(),
                  "BranchContext::BuildFromFork: creating the diff-side overlay property index (property {}) on a "
                  "freshly-built diff engine failed -- should be impossible.",
                  pid.AsUint());
        commit_one(*mirror);
      }
    }
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

std::expected<std::shared_ptr<BranchCore>, BranchCore::BuildError> BranchCore::Build(
    storage::InMemoryStorage &main, uint64_t fork_ts, storage::CommitArgs commit_args) {
  // Everything below allocates on behalf of main's tenant: scope the build to main's arena pool (borrowed when the
  // caller is already in it) so the diff engine's structures are attributed like main's own. A no-op with no pool.
  const memory::DbArenaScope db_arena_scope{main.DbArenaPool()};

  // 1. Time-travel main to fork_ts first -- fail before paying for the diff engine construction. This accessor only
  //    lives for the duration of Build: it supplies main's fork-time index set, and its READ guard blocks a concurrent
  //    CREATE ENUM while the enum store is mirrored.
  auto hist_exp = main.HistoricalAccess(fork_ts);
  if (!hist_exp.has_value()) {
    return std::unexpected(
        BuildError{.message = fmt::format("Cannot check out a version: fork timestamp {} is not (or no longer) pinned.",
                                          fork_ts)});
  }
  std::unique_ptr<storage::Storage::Accessor> historical = std::move(*hist_exp);

  // 2. Construct the empty diff engine (O(1) -- nothing copied from main's graph).
  //    Shares main's NameIdMapper: historical vertices carry main's label/property ids; a separate
  //    mapper would silently misdecode them. Config derived from main.config_ wholesale so any
  //    future salient flag (properties_on_edges, storage_light_edge, etc.) is inherited automatically.
  storage::Config config = main.config_;
  // Distinct throwaway instance -- avoid aliasing main's UUID in any registry. The NAME is deliberately kept as
  // main's: query::DbAccessor::DatabaseName() (auth, FGA, SHOW, DUMP) reads it, and a branch must look like main.
  config.salient.uuid = utils::UUID{};
  // config.gc stays main's: the diff engine needs its own GC, else committed branch deltas are never freed while any
  // other transaction is active (fast-discard needs a quiet engine) and branch memory grows with write volume.
  // Transient in-RAM copy: all durability mechanisms off regardless of what main configured.
  config.durability.snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::DISABLED;
  config.durability.recover_on_startup = false;
  config.durability.snapshot_on_exit = false;
  config.durability.restore_replication_state_on_startup = false;
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
  }
  config.force_on_disk = false;
  // Avoid duplicating/aliasing main's Prometheus metrics as a spurious second database.
  config.register_metrics = false;
  // Memory attribution: main's arena pool (its GC thread installs a DbArenaScope on that pool, and every allocating
  // thread is already in main's scope), main's embedding tracker, and main's unreleased-delta gauge so branch deltas
  // show up in the tenant's storage info. No other metric is shared.
  metrics::DatabaseMetricHandles metric_handles{};
  metric_handles.unreleased_delta_objects = main.metric_handles_.unreleased_delta_objects;
  auto diff_engine = std::make_unique<storage::InMemoryStorage>(config,
                                                                std::nullopt,
                                                                std::make_unique<storage::PlanInvalidatorDefault>(),
                                                                metric_handles,
                                                                nullptr,
                                                                main.DbArenaPool(),
                                                                main.DbEmbeddingMemoryTracker(),
                                                                main.GetSharedNameIdMapper());

  // 3. Reserve the branch-native gid range (see kBranchNativeGidWatermark).
  ReserveBranchNativeGidRange(*diff_engine, std::move(commit_args));

  // 4. Mirror main's fork_ts-pinned index definitions (see MirrorMainIndexDefinitionsIntoDiffEngine). Done once here,
  //    before the core is visible to any session, so the diff engine never needs UNIQUE / READ_ONLY afterwards.
  MirrorMainIndexDefinitionsIntoDiffEngine(*diff_engine, historical->ListAllIndices());

  // 5. Mirror main's current EnumStore (historical's SHARED guard on main_lock_ blocks concurrent
  //    CREATE ENUM -- see MirrorMainEnumsIntoDiffEngine).
  MirrorMainEnumsIntoDiffEngine(*diff_engine, historical->GetEnumStoreShared());

  // `new` not make_shared: private ctor.
  return std::shared_ptr<BranchCore>(new BranchCore(&main, fork_ts, std::move(diff_engine)));
}

std::expected<std::unique_ptr<BranchContext>, BranchContext::BuildError> BranchContext::BuildFromFork(
    storage::InMemoryStorage &main, uint64_t fork_ts, storage::CommitArgs commit_args) {
  auto core = BranchCore::Build(main, fork_ts, std::move(commit_args));
  if (!core) return std::unexpected(std::move(core.error()));
  return std::make_unique<BranchContext>(std::move(*core));
}

std::expected<void, BranchContext::BuildError> BranchContext::OpenHistorical() {
  EndTransaction();
  auto hist_exp = core_->main().HistoricalAccess(core_->fork_ts());
  if (!hist_exp.has_value()) {
    return std::unexpected(BuildError{
        .message = fmt::format("Version is no longer available: fork timestamp {} is not pinned (branch dropped).",
                               core_->fork_ts())});
  }
  historical_ = std::move(*hist_exp);
  return {};
}

BranchCore::LabelInfo BranchCore::ClassifyLabel(storage::LabelId label) {
  auto guard = std::lock_guard{marker_mutex_};
  if (auto it = label_info_.find(label); it != label_info_.end()) return it->second;
  const auto &name = diff_engine_->LabelToName(label);
  LabelInfo info{.reserved = name.starts_with(kReservedPrefix), .removed_label = std::nullopt};
  if (name.starts_with(kRemovalMarkerPrefix)) {
    info.removed_label = diff_engine_->NameToLabel(std::string_view{name}.substr(kRemovalMarkerPrefix.size()));
  }
  label_info_.emplace(label, info);
  return info;
}

storage::LabelId BranchCore::RemovalMarker(storage::LabelId label) {
  auto guard = std::lock_guard{marker_mutex_};
  if (auto it = removal_marker_.find(label); it != removal_marker_.end()) return it->second;
  const auto marker = diff_engine_->NameToLabel(std::string{kRemovalMarkerPrefix} + diff_engine_->LabelToName(label));
  removal_marker_.emplace(label, marker);
  label_info_.emplace(marker, LabelInfo{.reserved = true, .removed_label = label});
  return marker;
}

bool BranchCore::IsReservedProperty(storage::PropertyId property) {
  auto guard = std::lock_guard{marker_mutex_};
  if (auto it = reserved_property_.find(property); it != reserved_property_.end()) return it->second;
  const bool reserved = diff_engine_->PropertyToName(property).starts_with(kReservedPrefix);
  reserved_property_.emplace(property, reserved);
  return reserved;
}

storage::PropertyId BranchCore::RemovedPropsKey() {
  auto guard = std::lock_guard{marker_mutex_};
  if (!removed_props_key_) removed_props_key_ = diff_engine_->NameToProperty(kRemovedPropsName);
  return *removed_props_key_;
}

storage::LabelId BranchCore::DeletedLabel() {
  auto guard = std::lock_guard{marker_mutex_};
  if (!deleted_label_) deleted_label_ = diff_engine_->NameToLabel(kDeletedLabelName);
  return *deleted_label_;
}

storage::PropertyId BranchCore::DeletedEdgesKey() {
  auto guard = std::lock_guard{marker_mutex_};
  if (!deleted_edges_key_) deleted_edges_key_ = diff_engine_->NameToProperty(kDeletedEdgesName);
  return *deleted_edges_key_;
}

void BranchContext::MarkMainObjectBranched(storage::Gid gid) {
  auto hist_vertex = historical().FindVertex(gid, storage::View::OLD);
  if (!hist_vertex) return;  // best-effort hint -- see doc-comment.
  storage::Vertex *main_vertex = hist_vertex->vertex_;
  auto guard = std::unique_lock{main_vertex->lock};
  main_vertex->SetBranched(true);
}

void BranchContext::MarkForkEdgeBranched(const storage::EdgeAccessor &fork_edge) {
  if (!diff_engine().config_.salient.items.properties_on_edges) return;  // reference edges: no Edge object to mark.
  // fork_edge.edge_.ptr is main's live Edge* directly (historical_ is a view over main's physical objects).
  storage::Edge *main_edge = fork_edge.edge_.ptr;
  auto edge_guard = std::unique_lock{main_edge->lock};
  main_edge->SetBranched(true);
}

std::expected<storage::VertexAccessor, BranchContext::CowError> BranchContext::CowVertex(storage::Gid gid) {
  // MG_ASSERT not DMG_ASSERT: null current_diff_txn_ is an invariant break (SetupDatabaseTransaction
  // never ran), not a debug-only condition.
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::CowVertex: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call BeginTransaction() before any query "
            "runs against a checked-out branch.");
  storage::Storage::Accessor *diff_txn = current_diff_txn_;

  // Idempotent: a prior COW already in the diff engine wins outright -- unless it is a stub marked deleted, which
  // must not be mutated again.
  if (auto existing = diff_txn->FindVertex(gid, storage::View::NEW)) {
    if (IsForkGid(gid) && IsStubDeleted(*existing, storage::View::NEW)) {
      return std::unexpected(
          CowError{fmt::format("Cannot copy-on-write vertex {}: already deleted on this branch.", gid.AsUint())});
    }
    return *existing;
  }

  auto hist_vertex = historical().FindVertex(gid, storage::View::OLD);
  if (!hist_vertex) {
    // Defensive: callers resolve a vertex before mutating it; gid should exist in one of the two stores.
    return std::unexpected(
        CowError{fmt::format("Cannot copy-on-write vertex {}: not found in the branch's fork-state base either -- "
                             "this should be unreachable (callers resolve a vertex before mutating it).",
                             gid.AsUint())});
  }

  // Over-marking is safe (branched()==true is a hint, never a claim of actual divergence).
  MarkMainObjectBranched(gid);

  // Empty delta-less stub: fork labels/properties are read from historical_ and merged on read.
  // diff_txn is a ReplicationAccessor (adds no data members over InMemoryAccessor); cast is safe.
  return static_cast<storage::ReplicationAccessor *>(diff_txn)->MaterializeStubVertex(gid);
}

std::expected<storage::EdgeAccessor, BranchContext::CowError> BranchContext::CowEdge(
    const storage::EdgeAccessor &fork_edge) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::CowEdge: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call BeginTransaction() before any query "
            "runs against a checked-out branch.");

  const auto edge_gid = fork_edge.Gid();
  const auto from_gid = fork_edge.FromVertex().Gid();
  const auto to_gid = fork_edge.ToVertex().Gid();

  if (IsForkEdgeHidden(fork_edge, storage::View::NEW)) {
    return std::unexpected(
        CowError{fmt::format("Cannot copy-on-write edge {}: already deleted on this branch.", edge_gid.AsUint())});
  }

  // Idempotent: a prior stub (or branch-native edge) in the diff engine wins outright.
  if (auto existing = FindDiffEdge(edge_gid, from_gid, storage::View::NEW)) {
    return *existing;
  }

  if (!diff_engine().config_.salient.items.properties_on_edges) {
    return std::unexpected(CowError{fmt::format(
        "Cannot copy-on-write edge {}: edge properties are disabled.", edge_gid.AsUint())});
  }

  // Endpoint stubs first: CowVertex also flags the main vertices as branched, which gates their stub reads.
  auto from_diff = CowVertex(from_gid);
  if (!from_diff) return std::unexpected(from_diff.error());
  auto to_diff = CowVertex(to_gid);
  if (!to_diff) return std::unexpected(to_diff.error());

  if (fork_edge.storage_ != &diff_engine()) MarkForkEdgeBranched(fork_edge);

  return static_cast<storage::ReplicationAccessor *>(current_diff_txn_)
      ->MaterializeStubEdge(edge_gid, from_gid, to_gid, fork_edge.EdgeType());
}

std::optional<storage::EdgeAccessor> BranchContext::FindForkEdge(const storage::EdgeAccessor &stub) {
  auto from = historical().FindVertex(stub.from_vertex_->gid, storage::View::OLD);
  auto to = historical().FindVertex(stub.to_vertex_->gid, storage::View::OLD);
  if (!from || !to) return std::nullopt;
  auto out = from->OutEdges(storage::View::OLD, {stub.EdgeType()}, &*to);
  if (!out) return std::nullopt;
  for (const auto &edge : out->edges) {
    if (edge.Gid() == stub.Gid()) return edge;
  }
  return std::nullopt;
}

bool BranchContext::IsEdgeStub(const storage::EdgeAccessor &diff_edge) const {
  return diff_edge.storage_ == &core_->diff_engine() && IsForkGid(diff_edge.Gid());
}

bool BranchContext::StubEdgeTouches(const storage::EdgeAccessor &stub, storage::PropertyId property,
                                    storage::View view) {
  auto props = stub.Properties(view);
  if (!props) return false;
  if (props->contains(property)) return true;
  auto removed = props->find(RemovedPropsKey());
  if (removed == props->end()) return false;
  bool listed = false;
  ForEachRemovedPid(removed->second, [&](storage::PropertyId pid) { listed = listed || pid == property; });
  return listed;
}

bool BranchContext::IsForkGid(storage::Gid gid) const { return gid.AsUint() < kBranchNativeGidWatermark; }

bool BranchContext::IsStubDeleted(const storage::VertexAccessor &stub, storage::View view) {
  auto deleted = stub.HasLabel(DeletedLabel(), view);
  return deleted.has_value() && *deleted;
}

bool BranchContext::IsVertexDeleted(storage::Gid gid, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::IsVertexDeleted: no current diff transaction set.");
  if (!IsForkGid(gid)) return false;
  auto stub = current_diff_txn_->FindVertex(gid, view);
  return stub.has_value() && IsStubDeleted(*stub, view);
}

bool BranchContext::IsForkVertexDeleted(const storage::VertexAccessor &fork_vertex, storage::View view) {
  if (!fork_vertex.vertex_->branched()) return false;
  return IsVertexDeleted(fork_vertex.Gid(), view);
}

bool BranchContext::IsForkEdgeHidden(const storage::EdgeAccessor &fork_edge, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::IsForkEdgeHidden: no current diff transaction set.");
  if (!IsForkGid(fork_edge.Gid())) return false;
  // A diff-engine copy's endpoints are diff-engine vertices, whose branched() bit means nothing: always look.
  const bool in_diff = fork_edge.storage_ == &diff_engine();
  const auto from = fork_edge.FromVertex();
  const auto to = fork_edge.ToVertex();
  auto stub_of = [&](const storage::VertexAccessor &v) -> std::optional<storage::VertexAccessor> {
    if (!in_diff && !v.vertex_->branched()) return std::nullopt;
    return current_diff_txn_->FindVertex(v.Gid(), view);
  };
  if (auto from_stub = stub_of(from)) {
    if (IsStubDeleted(*from_stub, view)) return true;
    if (std::ranges::binary_search(DeletedEdgeGids(*from_stub, view), fork_edge.Gid().AsUint())) return true;
  }
  if (to.Gid() != from.Gid()) {
    if (auto to_stub = stub_of(to); to_stub && IsStubDeleted(*to_stub, view)) return true;
  }
  return false;
}

std::vector<uint64_t> BranchContext::DeletedEdgeGids(const storage::VertexAccessor &stub, storage::View view) {
  std::vector<uint64_t> gids;
  auto value = stub.GetProperty(DeletedEdgesKey(), view);
  if (!value) return gids;
  ForEachListedInt(*value, [&](int64_t gid) { gids.push_back(static_cast<uint64_t>(gid)); });
  std::ranges::sort(gids);
  return gids;
}

storage::Result<bool> BranchContext::StoreDeletedEdgeGids(storage::VertexAccessor &stub,
                                                          const std::vector<uint64_t> &gids) {
  storage::PropertyValue value;
  if (!gids.empty()) {
    storage::PropertyValue::list_t list;
    list.reserve(gids.size());
    for (auto const gid : gids) list.emplace_back(static_cast<int64_t>(gid));
    value = storage::PropertyValue{std::move(list)};
  }
  auto set = stub.SetProperty(DeletedEdgesKey(), value);
  if (!set) return std::unexpected{set.error()};
  return true;
}

storage::Result<std::optional<storage::VertexAccessor>> BranchContext::DeleteForkVertex(storage::Gid gid) {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::DeleteForkVertex: no current diff transaction set.");
  if (IsVertexDeleted(gid, storage::View::NEW)) return std::optional<storage::VertexAccessor>{};
  auto stub = CowVertex(gid);
  if (!stub) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  if (auto marked = stub->AddLabel(DeletedLabel()); !marked) return std::unexpected{marked.error()};
  // The marker is a label write on a stub: open the label/property read gates and the adjacency fast path.
  RecordVertexChange(gid, BranchChangeKind::kLabel);
  RecordVertexChange(gid, BranchChangeKind::kProperty);
  RecordEdgeChange(gid);
  return std::make_optional(*stub);
}

storage::Result<bool> BranchContext::DeleteForkEdge(const storage::EdgeAccessor &fork_edge) {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::DeleteForkEdge: no current diff transaction set.");
  if (IsForkEdgeHidden(fork_edge, storage::View::NEW)) return false;

  const auto edge_gid = fork_edge.Gid();
  const auto from_gid = fork_edge.FromVertex().Gid();
  const auto to_gid = fork_edge.ToVertex().Gid();

  if (fork_edge.storage_ != &diff_engine()) MarkForkEdgeBranched(fork_edge);

  auto from = CowVertex(from_gid);
  if (!from) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  auto to = from_gid == to_gid ? from : CowVertex(to_gid);
  if (!to) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};

  // An earlier property write left a stub: it is deleted normally (MVCC).
  if (auto copy = FindDiffEdge(edge_gid, from_gid, storage::View::NEW)) {
    if (auto deleted = current_diff_txn_->DeleteEdge(&*copy); !deleted) return std::unexpected{deleted.error()};
  }

  auto list_on = [&](storage::VertexAccessor &stub) -> storage::Result<bool> {
    auto gids = DeletedEdgeGids(stub, storage::View::NEW);
    if (std::ranges::binary_search(gids, edge_gid.AsUint())) return true;
    gids.insert(std::ranges::upper_bound(gids, edge_gid.AsUint()), edge_gid.AsUint());
    return StoreDeletedEdgeGids(stub, gids);
  };
  if (auto listed = list_on(*from); !listed) return std::unexpected{listed.error()};
  if (from_gid != to_gid) {
    if (auto listed = list_on(*to); !listed) return std::unexpected{listed.error()};
  }

  RecordEdgeChange(from_gid);
  RecordEdgeChange(to_gid);
  return true;
}

std::optional<storage::VertexAccessor> BranchContext::FindForkVertex(storage::Gid gid) {
  return historical().FindVertex(gid, storage::View::OLD);
}

std::optional<storage::VertexAccessor> BranchContext::FindStubOrDiffVertex(storage::Gid gid, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::FindStubOrDiffVertex: no current diff transaction set.");
  return current_diff_txn_->FindVertex(gid, view);
}

std::optional<storage::VertexAccessor> BranchContext::ResolveVertex(storage::Gid gid, storage::View view) {
  // MG_ASSERT not DMG_ASSERT: on the hot read path; a null deref in release would be frequently reachable.
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::ResolveVertex: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call BeginTransaction() before any query "
            "runs against a checked-out branch.");
  if (auto diff_vertex = current_diff_txn_->FindVertex(gid, view)) {
    if (IsForkGid(gid) && IsStubDeleted(*diff_vertex, view)) return std::nullopt;
    return diff_vertex;
  }
  return historical().FindVertex(gid, storage::View::OLD);
}

std::optional<storage::EdgeAccessor> BranchContext::FindDiffEdge(storage::Gid edge_gid, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::FindDiffEdge: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call BeginTransaction() before any query "
            "runs against a checked-out branch.");
  return current_diff_txn_->FindEdge(edge_gid, view);
}

std::optional<storage::EdgeAccessor> BranchContext::FindDiffEdge(storage::Gid edge_gid, storage::Gid from_gid,
                                                                 storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::FindDiffEdge: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call BeginTransaction() before any query "
            "runs against a checked-out branch.");
  return current_diff_txn_->FindEdge(edge_gid, from_gid, view);
}

// historical_ uses View::OLD throughout (frozen snapshot — no concept of NEW relative to any txn).
std::vector<storage::EdgeAccessor> BranchContext::ResolveEdges(storage::Gid vertex_gid,
                                                               storage::EdgeDirection direction, storage::View view,
                                                               const std::vector<storage::EdgeTypeId> &edge_types,
                                                               bool *vertex_deleted) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::ResolveEdges: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call BeginTransaction() before any query "
            "runs against a checked-out branch.");

  if (vertex_deleted != nullptr) *vertex_deleted = false;
  const bool out = direction == storage::EdgeDirection::OUT;
  std::vector<storage::EdgeAccessor> result;
  std::vector<uint64_t> listed_deleted;

  // Diff side first: only branch-native edges are emitted from it. A fork-gid edge here is a stub whose fork edge is
  // emitted by the historical pass below, in fork identity (readers overlay the stub on it by gid).
  if (auto diff_vertex = current_diff_txn_->FindVertex(vertex_gid, view)) {
    if (IsForkGid(vertex_gid)) {
      if (IsStubDeleted(*diff_vertex, view)) {
        if (vertex_deleted != nullptr) *vertex_deleted = true;
        return result;
      }
      // One stub read covers every fork edge of this vertex.
      listed_deleted = DeletedEdgeGids(*diff_vertex, view);
    }
    auto maybe_result =
        out ? diff_vertex->OutEdges(view, edge_types, nullptr) : diff_vertex->InEdges(view, edge_types, nullptr);
    if (maybe_result.has_value()) {
      result.reserve(maybe_result->edges.size());
      for (auto &edge : maybe_result->edges) {
        if (!IsForkGid(edge.Gid())) result.push_back(edge);
      }
    }
  }

  // Historical side: fork edges the branch has not deleted.
  if (auto hist_vertex = historical().FindVertex(vertex_gid, storage::View::OLD)) {
    auto maybe_result = out ? hist_vertex->OutEdges(storage::View::OLD, edge_types, nullptr)
                            : hist_vertex->InEdges(storage::View::OLD, edge_types, nullptr);
    if (maybe_result.has_value()) {
      result.reserve(result.size() + maybe_result->edges.size());
      for (auto &edge : maybe_result->edges) {
        if (std::ranges::binary_search(listed_deleted, edge.Gid().AsUint())) continue;
        if (IsForkVertexDeleted(out ? edge.ToVertex() : edge.FromVertex(), view)) continue;
        result.push_back(edge);
      }
    }
  }

  return result;
}

BranchContext::UnionVerticesIterable BranchContext::Vertices(storage::View view) {
  // MG_ASSERT not DMG_ASSERT (same rationale as CowVertex).
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::Vertices: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call BeginTransaction() before any query "
            "runs against a checked-out branch.");
  return UnionVerticesIterable(
      historical().Vertices(storage::View::OLD), current_diff_txn_->Vertices(view), DeletedLabel(), view);
}

BranchContext::UnionVerticesIterable::UnionVerticesIterable(storage::VerticesIterable hist_vertices,
                                                            storage::VerticesIterable diff_vertices,
                                                            storage::LabelId deleted_label, storage::View view)
    : hist_vertices_(std::move(hist_vertices)),
      diff_vertices_(std::move(diff_vertices)),
      deleted_label_(deleted_label),
      view_(view) {}

BranchContext::UnionVerticesIterable::Iterator BranchContext::UnionVerticesIterable::begin() {
  return Iterator(hist_vertices_.begin(),
                  hist_vertices_.end(),
                  diff_vertices_.begin(),
                  diff_vertices_.end(),
                  deleted_label_,
                  view_);
}

BranchContext::UnionVerticesIterable::Iterator::Iterator(storage::VerticesIterable::Iterator hist_it,
                                                         storage::VerticesIterable::Iterator hist_end,
                                                         storage::VerticesIterable::Iterator diff_it,
                                                         storage::VerticesIterable::Iterator diff_end,
                                                         storage::LabelId deleted_label, storage::View view)
    : hist_it_(std::move(hist_it)),
      hist_end_(std::move(hist_end)),
      diff_it_(std::move(diff_it)),
      diff_end_(std::move(diff_end)),
      deleted_label_(deleted_label),
      view_(view) {
  SeekNext();
}

BranchContext::UnionVerticesIterable::Iterator &BranchContext::UnionVerticesIterable::Iterator::operator++() {
  SeekNext();
  return *this;
}

// Streaming gid-ordered merge (O(H+D); see class comment in branch_engine.hpp). A fork vertex deleted on the branch
// has a stub in the diff stream carrying the delete marker, so it is dropped when the two streams meet on its gid;
// a fork vertex without a stub is untouched and passes through.
void BranchContext::UnionVerticesIterable::Iterator::SeekNext() {
  auto deleted_stub = [this](const storage::VertexAccessor &v) {
    if (v.Gid().AsUint() >= kBranchNativeGidWatermark) return false;
    auto deleted = v.HasLabel(deleted_label_, view_);
    return deleted.has_value() && *deleted;
  };
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
        current_ = **hist_it_;
        ++(*hist_it_);
        done_ = false;
        return;
      }
      if (diff_gid < hist_gid) {
        if (deleted_stub(**diff_it_)) {
          ++(*diff_it_);
          continue;
        }
        current_ = **diff_it_;
        ++(*diff_it_);
        done_ = false;
        return;
      }
      // Tie: the diff engine's stub wins (the reader overlays it on the fork vertex), unless it is a delete marker.
      const bool deleted = deleted_stub(**diff_it_);
      if (!deleted) current_ = **diff_it_;
      ++(*hist_it_);
      ++(*diff_it_);
      if (deleted) continue;
      done_ = false;
      return;
    }

    if (hist_has) {
      current_ = **hist_it_;
      ++(*hist_it_);
      done_ = false;
      return;
    }

    if (deleted_stub(**diff_it_)) {
      ++(*diff_it_);
      continue;
    }
    current_ = **diff_it_;
    ++(*diff_it_);
    done_ = false;
    return;
  }
}

}  // namespace memgraph::versioning
