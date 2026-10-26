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

#include "metrics/prometheus_metrics.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/logging.hpp"
#include "utils/uuid.hpp"
#include "versioning/stub_properties.hpp"

namespace memgraph::versioning {

namespace {

constexpr std::string_view kReservedPrefix = "__mg_";
constexpr std::string_view kRemovalMarkerPrefix = "__mg_rm:";
constexpr std::string_view kRemovedPropsName = "__mg_removed";
constexpr std::string_view kDeletedLabelName = "__mg_deleted";
constexpr std::string_view kDeletedEdgesName = "__mg_deleted_edges";

// Mirrors main's index definitions (not contents) so the planner can pick index scans on a checked-out branch;
// the diff engine populates them through ordinary index maintenance.
void MirrorMainIndexDefinitionsIntoDiffEngine(storage::InMemoryStorage &diff_engine,
                                              const storage::IndicesInfo &main_indices) {
  // No early-return emptiness guard: it enumerated every index kind, was twice forgotten for a new kind, and silently
  // fell branches back to full scans. One accessor per index: CreateIndex downgrades READ_ONLY to READ and asserts.
  auto commit_one = [&diff_engine](storage::Storage::Accessor &acc) {
    auto protector = diff_engine.make_database_protector();
    MG_ASSERT(protector != nullptr,
              "BranchCore::Build: failed to obtain a database protector for the freshly-built diff "
              "engine while mirroring main's index definitions -- should be impossible (a null "
              "database_protector_factory_ always resolves to a non-null DefaultDatabaseProtector, storage.cpp).");
    auto commit_res = acc.PrepareForCommitPhase(storage::CommitArgs::make_main(std::move(protector)));
    MG_ASSERT(commit_res.has_value(), "BranchCore::Build: index-definition mirror commit failed.");
  };

  for (const auto &label : main_indices.label) {
    auto mirror = diff_engine.ReadOnlyAccess();
    auto created = mirror->CreateIndex(label);
    MG_ASSERT(created.has_value(),
              "BranchCore::Build: mirroring main's label index (label {}) onto a freshly-built, "
              "otherwise-empty diff engine failed -- should be impossible.",
              label.AsUint());
    commit_one(*mirror);
  }
  for (const auto &entry : main_indices.label_properties) {
    auto mirror = diff_engine.ReadOnlyAccess();
    auto created = mirror->CreateIndex(entry.label, entry.properties, entry.order);
    MG_ASSERT(created.has_value(),
              "BranchCore::Build: mirroring main's label-property index (label {}) onto a "
              "freshly-built, otherwise-empty diff engine failed -- should be impossible.",
              entry.label.AsUint());
    commit_one(*mirror);
  }

  // A sparse stub that newly matches a label+property scan (:L gained or key property set) is absent from the diff
  // composite index, so it is enumerated via a diff label index on L and a property-only index on each key pid.
  {
    std::unordered_set<storage::LabelId> labels(main_indices.label.begin(), main_indices.label.end());
    std::unordered_set<storage::PropertyId> pids(main_indices.vertex_property.begin(),
                                                 main_indices.vertex_property.end());
    for (const auto &entry : main_indices.label_properties) {
      if (labels.insert(entry.label).second) {
        auto mirror = diff_engine.ReadOnlyAccess();
        auto created = mirror->CreateIndex(entry.label);
        MG_ASSERT(created.has_value(),
                  "BranchCore::Build: creating the diff-side overlay label index (label {}) on a "
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
                  "BranchCore::Build: creating the diff-side overlay property index (property {}) on a "
                  "freshly-built diff engine failed -- should be impossible.",
                  pid.AsUint());
        commit_one(*mirror);
      }
    }
  }

  // Edge indexes are only populated with properties_on_edges: without it EdgeRef.ptr aliases a bare gid.
  if (diff_engine.config_.salient.items.properties_on_edges) {
    for (const auto &edge_type : main_indices.edge_type) {
      auto mirror = diff_engine.ReadOnlyAccess();
      auto created = mirror->CreateIndex(edge_type);
      MG_ASSERT(created.has_value(),
                "BranchCore::Build: mirroring main's edge-type index (edge type {}) onto a "
                "freshly-built, otherwise-empty diff engine failed -- should be impossible.",
                edge_type.AsUint());
      commit_one(*mirror);
    }
    // Edge-property mirrors feed MergeBranchEdgePropertyScan, which needs both streams ascending (Sort is elided).
    for (const auto &[edge_type, property] : main_indices.edge_type_property) {
      auto mirror = diff_engine.ReadOnlyAccess();
      auto created = mirror->CreateIndex(edge_type, property);
      MG_ASSERT(created.has_value(),
                "BranchCore::Build: mirroring main's edge-type+property index (edge type {}, "
                "property {}) onto a freshly-built, otherwise-empty diff engine failed -- should be impossible.",
                edge_type.AsUint(),
                property.AsUint());
      commit_one(*mirror);
    }
    for (const auto &property : main_indices.edge_property) {
      auto mirror = diff_engine.ReadOnlyAccess();
      auto created = mirror->CreateGlobalEdgeIndex(property);
      MG_ASSERT(created.has_value(),
                "BranchCore::Build: mirroring main's global edge-property index (property {}) onto a "
                "freshly-built, otherwise-empty diff engine failed -- should be impossible.",
                property.AsUint());
      commit_one(*mirror);
    }
  }
  // Vertex-property indexes do not depend on properties_on_edges, so they sit outside the gate above.
  for (const auto &property : main_indices.vertex_property) {
    auto mirror = diff_engine.ReadOnlyAccess();
    auto created = mirror->CreateGlobalVertexIndex(property);
    MG_ASSERT(created.has_value(),
              "BranchCore::Build: mirroring main's global vertex-property index (property {}) onto a "
              "freshly-built, otherwise-empty diff engine failed -- should be impossible.",
              property.AsUint());
    commit_one(*mirror);
  }
}

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

std::expected<std::shared_ptr<BranchCore>, BranchCore::BuildError> BranchCore::Build(storage::InMemoryStorage &main,
                                                                                     uint64_t fork_ts) {
  // Attribute the diff engine's allocations to main's tenant arena pool.
  const memory::DbArenaScope db_arena_scope{main.DbArenaPool()};

  // Pin main at fork_ts first so an unpinned fork fails before the diff engine is built. This accessor's READ guard
  // also blocks a concurrent CREATE ENUM while the enum store is mirrored below.
  auto hist_exp = main.HistoricalAccess(fork_ts);
  if (!hist_exp.has_value()) {
    return std::unexpected(
        BuildError{.message = fmt::format("Cannot check out a version: fork timestamp {} is not (or no longer) pinned.",
                                          fork_ts)});
  }
  std::unique_ptr<storage::Storage::Accessor> historical = std::move(*hist_exp);

  // Shares main's NameIdMapper (historical vertices carry main's ids; a separate mapper would misdecode them).
  // Config is copied wholesale so future salient flags are inherited.
  storage::Config config = main.config_;
  // Fresh UUID avoids aliasing main in any registry; the NAME stays main's because DbAccessor::DatabaseName() (auth,
  // FGA, SHOW, DUMP) reads it.
  config.salient.uuid = utils::UUID{};
  // config.gc stays main's: the diff engine needs its own GC or committed branch deltas are never freed while another
  // transaction is active.
  config.durability.snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::DISABLED;
  config.durability.recover_on_startup = false;
  config.durability.snapshot_on_exit = false;
  config.durability.restore_replication_state_on_startup = false;
  // Durability is off, but a stale path is one flip away from aliasing main's live data.
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
  // Else main's Prometheus metrics would be duplicated as a spurious second database.
  config.register_metrics = false;
  // Share main's arena pool, embedding tracker and unreleased-delta gauge so branch memory shows up under the tenant;
  // no other metric is shared.
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

  // Done before the core is visible to any session, so the diff engine never needs UNIQUE/READ_ONLY afterwards.
  MirrorMainIndexDefinitionsIntoDiffEngine(*diff_engine, historical->ListAllIndices());

  MirrorMainEnumsIntoDiffEngine(*diff_engine, historical->GetEnumStoreShared());

  // After the fork pin: every gid visible at fork_ts is below these floors, every later allocation (main or any
  // branch) is at or above them.
  return std::shared_ptr<BranchCore>(new BranchCore(&main,
                                                    fork_ts,
                                                    std::move(diff_engine),
                                                    main.vertex_id_.load(std::memory_order_acquire),
                                                    main.edge_id_.load(std::memory_order_acquire)));
}

BranchCore::BranchCore(storage::InMemoryStorage *main, uint64_t fork_ts,
                       std::unique_ptr<storage::InMemoryStorage> diff_engine, uint64_t vertex_gid_floor,
                       uint64_t edge_gid_floor)
    : main_(main),
      fork_ts_(fork_ts),
      vertex_gid_floor_(vertex_gid_floor),
      edge_gid_floor_(edge_gid_floor),
      diff_engine_(std::move(diff_engine)),
      removed_props_key_(diff_engine_->NameToProperty(kRemovedPropsName)),
      deleted_label_(diff_engine_->NameToLabel(kDeletedLabelName)),
      deleted_edges_key_(diff_engine_->NameToProperty(kDeletedEdgesName)) {}

BranchCore::~BranchCore() {
  const bool light_edges = diff_engine_->config_.salient.items.storage_light_edge;
  diff_engine_.reset();
  // The engine's teardown frees light edges reachable from vertices_ only; those linked from a stub are freed here.
  if (light_edges) {
    stub_index_.ForEachStub([](storage::Vertex *stub) {
      for (const auto &[edge_type, to_vertex, edge_ref] : stub->out_edges) {
        storage::InMemoryStorage::LightEdgePool::Destroy(edge_ref.ptr);
      }
    });
  }
}

std::expected<std::unique_ptr<BranchContext>, BranchContext::BuildError> BranchContext::BuildFromFork(
    storage::InMemoryStorage &main, uint64_t fork_ts) {
  auto core = BranchCore::Build(main, fork_ts);
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

void BranchCore::RecordVertexDeleted(storage::Gid gid) {
  change_filters_.RecordVertexDeleted(gid);
  std::lock_guard lock(touched_mutex_);
  if (deleted_exact_.insert(gid.AsUint()).second) ++touched_version_;
}

void BranchCore::RecordLabelTouched(storage::LabelId label, storage::Gid gid) {
  std::lock_guard lock(touched_mutex_);
  if (label_touched_[label].insert(gid.AsUint()).second) ++touched_version_;
}

std::shared_ptr<const BranchCore::LabelTouchedSet> BranchCore::LabelTouchedSnapshot(storage::LabelId label) {
  std::lock_guard lock(touched_mutex_);
  auto &cached = label_snapshots_[label];
  if (cached.set && cached.version == touched_version_) return cached.set;
  auto merged = std::make_shared<LabelTouchedSet>(deleted_exact_);
  if (auto const it = label_touched_.find(label); it != label_touched_.end()) {
    merged->insert(it->second.begin(), it->second.end());
  }
  cached = CachedLabelSnapshot{touched_version_, std::move(merged)};
  return cached.set;
}

bool BranchCore::IsReservedProperty(storage::PropertyId property) {
  auto guard = std::lock_guard{marker_mutex_};
  if (auto it = reserved_property_.find(property); it != reserved_property_.end()) return it->second;
  const bool reserved = diff_engine_->PropertyToName(property).starts_with(kReservedPrefix);
  reserved_property_.emplace(property, reserved);
  return reserved;
}

void BranchContext::MarkMainObjectBranched(storage::Vertex *main_vertex) {
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
  // MG_ASSERT not DMG_ASSERT: null current_diff_txn_ is an invariant break, not a debug-only condition (applies to
  // every such assert below).
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::CowVertex: no current diff transaction set -- "
            "CurrentDB must call OpenHistorical() and SetCurrentDiffTxn() before any query "
            "runs against a checked-out branch.");

  // A prior COW in the diff engine wins, unless it is a stub marked deleted.
  if (auto existing = FindStub(gid, storage::View::NEW)) {
    if (IsForkVertexGid(gid) && IsStubDeleted(*existing, storage::View::NEW)) {
      return std::unexpected(
          CowError{fmt::format("Cannot copy-on-write vertex {}: already deleted on this branch.", gid.AsUint())});
    }
    return *existing;
  }

  auto hist_vertex = FindForkVertex(gid);
  if (!hist_vertex) {
    return std::unexpected(
        CowError{fmt::format("Cannot copy-on-write vertex {}: not found in the branch's fork-state base either -- "
                             "this should be unreachable (callers resolve a vertex before mutating it).",
                             gid.AsUint())});
  }

  // Over-marking is safe: branched() is a hint, not a claim of divergence.
  MarkMainObjectBranched(hist_vertex->vertex_);

  auto *stub = core_->stub_index().GetOrCreateStub(gid.AsUint(), hist_vertex->vertex_);
  return storage::VertexAccessor{stub, &core_->diff_engine(), current_diff_txn_->GetTransaction()};
}

std::expected<storage::EdgeAccessor, BranchContext::CowError> BranchContext::CowEdge(
    const storage::EdgeAccessor &fork_edge) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::CowEdge: no current diff transaction set -- "
            "CurrentDB must call OpenHistorical() and SetCurrentDiffTxn() before any query "
            "runs against a checked-out branch.");

  const auto edge_gid = fork_edge.Gid();
  const auto from_gid = fork_edge.FromVertex().Gid();
  const auto to_gid = fork_edge.ToVertex().Gid();

  if (IsForkEdgeHidden(fork_edge, storage::View::NEW)) {
    return std::unexpected(
        CowError{fmt::format("Cannot copy-on-write edge {}: already deleted on this branch.", edge_gid.AsUint())});
  }

  // A prior stub (or branch-native edge) in the diff engine wins.
  if (auto existing = FindDiffEdge(edge_gid, from_gid, storage::View::NEW)) {
    return *existing;
  }

  if (!diff_engine().config_.salient.items.properties_on_edges) {
    return std::unexpected(
        CowError{fmt::format("Cannot copy-on-write edge {}: edge properties are disabled.", edge_gid.AsUint())});
  }

  // Endpoint stubs first: CowVertex also sets the main vertices' branched bit, which gates their stub reads.
  auto from_diff = CowVertex(from_gid);
  if (!from_diff) return std::unexpected(from_diff.error());
  auto to_diff = CowVertex(to_gid);
  if (!to_diff) return std::unexpected(to_diff.error());

  if (fork_edge.storage_ != &diff_engine()) MarkForkEdgeBranched(fork_edge);

  return static_cast<storage::ReplicationAccessor *>(current_diff_txn_)
      ->MaterializeStubEdge(edge_gid, from_diff->vertex_, to_diff->vertex_, fork_edge.EdgeType());
}

std::optional<storage::EdgeAccessor> BranchContext::FindForkEdge(const storage::EdgeAccessor &stub) {
  auto from = FindForkVertex(stub.from_vertex_->gid);
  auto to = FindForkVertex(stub.to_vertex_->gid);
  if (!from || !to) return std::nullopt;
  auto out = from->OutEdges(storage::View::OLD, {stub.EdgeType()}, &*to);
  if (!out) return std::nullopt;
  for (const auto &edge : out->edges) {
    if (edge.Gid() == stub.Gid()) return edge;
  }
  return std::nullopt;
}

bool BranchContext::StubEdgeTouches(const storage::EdgeAccessor &stub, storage::PropertyId property,
                                    storage::View view) {
  auto props = stub.Properties(view);
  if (!props) return false;
  if (props->contains(property)) return true;
  return std::ranges::contains(TakeRemoved(*this, *props), property);
}

bool BranchContext::IsForkVertexGid(storage::Gid gid) const { return gid.AsUint() < core_->vertex_gid_floor(); }

bool BranchContext::IsForkEdgeGid(storage::Gid gid) const { return gid.AsUint() < core_->edge_gid_floor(); }

storage::VertexAccessor BranchContext::CreateNativeVertex() {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::CreateNativeVertex: no current diff transaction set.");
  const auto gid = storage::Gid::FromUint(core_->main().vertex_id_.fetch_add(1, std::memory_order_acq_rel));
  auto created = static_cast<storage::ReplicationAccessor *>(current_diff_txn_)->CreateVertexEx(gid);
  MG_ASSERT(created.has_value(),
            "BranchContext::CreateNativeVertex: gid {} already exists in the diff engine.",
            gid.AsUint());
  return *created;
}

storage::Result<storage::EdgeAccessor> BranchContext::CreateNativeEdge(storage::VertexAccessor *from,
                                                                       storage::VertexAccessor *to,
                                                                       storage::EdgeTypeId edge_type) {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::CreateNativeEdge: no current diff transaction set.");
  const auto gid = storage::Gid::FromUint(core_->main().edge_id_.fetch_add(1, std::memory_order_acq_rel));
  return static_cast<storage::ReplicationAccessor *>(current_diff_txn_)->CreateEdgeEx(from, to, edge_type, gid);
}

bool BranchContext::IsStubDeleted(const storage::VertexAccessor &stub, storage::View view) {
  if (!MayHaveVertexDeleted(stub.Gid())) return false;  // the marker is only written after RecordVertexDeleted
  auto deleted = stub.HasLabel(DeletedLabel(), view);
  return deleted.has_value() && *deleted;
}

bool BranchContext::IsVertexDeleted(storage::Gid gid, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::IsVertexDeleted: no current diff transaction set.");
  if (!IsForkVertexGid(gid)) return false;
  if (!MayHaveVertexDeleted(gid)) return false;
  auto stub = FindStub(gid, view);
  return stub.has_value() && IsStubDeleted(*stub, view);
}

bool BranchContext::IsForkVertexDeleted(const storage::VertexAccessor &fork_vertex, storage::View view) {
  if (!fork_vertex.vertex_->branched()) return false;
  return IsVertexDeleted(fork_vertex.Gid(), view);
}

bool BranchContext::IsForkEdgeHidden(const storage::EdgeAccessor &fork_edge, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::IsForkEdgeHidden: no current diff transaction set.");
  if (!IsForkEdgeGid(fork_edge.Gid())) return false;
  // A diff-engine copy's endpoints are diff-engine vertices, whose branched() bit means nothing: always look.
  const bool in_diff = fork_edge.storage_ == &diff_engine();
  const auto from = fork_edge.FromVertex();
  const auto to = fork_edge.ToVertex();
  // Not in the diff: the FROM stub hides the edge via its delete marker or deleted-edge list, the TO stub only via its
  // delete marker; the per-branch filters rule out a stub that cannot carry either.
  auto stub_of = [&](const storage::VertexAccessor &v, bool may_hide) -> std::optional<storage::VertexAccessor> {
    if (!in_diff && !(v.vertex_->branched() && may_hide)) return std::nullopt;
    return FindStub(v.Gid(), view);
  };
  if (auto from_stub = stub_of(from, MayHaveVertexDeleted(from.Gid()) || MayHaveEdgeChange(from.Gid()))) {
    if (IsStubDeleted(*from_stub, view)) return true;
    if (std::ranges::binary_search(DeletedEdgeGids(*from_stub, view), fork_edge.Gid().AsUint())) return true;
  }
  if (to.Gid() != from.Gid()) {
    auto to_stub = stub_of(to, MayHaveVertexDeleted(to.Gid()));
    if (to_stub && IsStubDeleted(*to_stub, view)) return true;
  }
  return false;
}

std::vector<uint64_t> BranchContext::DeletedEdgeGids(const storage::VertexAccessor &stub, storage::View view) {
  auto gids = ReadIntList<uint64_t>(stub, DeletedEdgesKey(), view);
  if (!gids) return {};
  std::ranges::sort(*gids);
  return std::move(*gids);
}

storage::Result<bool> BranchContext::StoreDeletedEdgeGids(storage::VertexAccessor &stub,
                                                          const std::vector<uint64_t> &gids) {
  return WriteIntList(stub, DeletedEdgesKey(), gids);
}

storage::Result<std::optional<storage::VertexAccessor>> BranchContext::DeleteForkVertex(storage::Gid gid) {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::DeleteForkVertex: no current diff transaction set.");
  if (IsVertexDeleted(gid, storage::View::NEW)) return std::optional<storage::VertexAccessor>{};
  auto stub = CowVertex(gid);
  if (!stub) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  RecordVertexDeleted(gid);
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
  auto &hist = historical();
  if (auto *fork = core_->stub_index().FindFork(gid.AsUint(), stub_cursor_)) {
    // Same construction as InMemoryAccessor::FindVertex on hist (storage_ is main, see HistoricalAccess).
    return storage::VertexAccessor::Create(fork, &core_->main(), hist.GetTransaction(), storage::View::OLD);
  }
  return hist.FindVertex(gid, storage::View::OLD);
}

std::optional<storage::VertexAccessor> BranchContext::FindStub(storage::Gid gid, storage::View view) {
  if (auto *stub = core_->stub_index().FindStub(gid.AsUint(), stub_cursor_)) {
    // Same construction as InMemoryAccessor::FindVertex on the diff txn (storage_ is the diff engine).
    return storage::VertexAccessor::Create(stub, &core_->diff_engine(), current_diff_txn_->GetTransaction(), view);
  }
  if (IsForkVertexGid(gid)) return std::nullopt;  // fork stubs live only in the stub index
  return current_diff_txn_->FindVertex(gid, view);
}

std::optional<storage::VertexAccessor> BranchContext::FindStubOrDiffVertex(storage::Gid gid, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::FindStubOrDiffVertex: no current diff transaction set.");
  return FindStub(gid, view);
}

std::optional<storage::VertexAccessor> BranchContext::ResolveVertex(storage::Gid gid, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::ResolveVertex: no current diff transaction set -- "
            "CurrentDB must call OpenHistorical() and SetCurrentDiffTxn() before any query "
            "runs against a checked-out branch.");
  if (auto diff_vertex = FindStub(gid, view)) {
    if (IsForkVertexGid(gid) && IsStubDeleted(*diff_vertex, view)) return std::nullopt;
    return diff_vertex;
  }
  return FindForkVertex(gid);
}

std::optional<storage::EdgeAccessor> BranchContext::FindDiffEdge(storage::Gid edge_gid, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::FindDiffEdge: no current diff transaction set -- "
            "CurrentDB must call OpenHistorical() and SetCurrentDiffTxn() before any query "
            "runs against a checked-out branch.");
  if (auto found = current_diff_txn_->FindEdge(edge_gid, view)) return found;
  // The engine's scan covers vertices_ only; an edge whose FROM vertex is a stub is found through the stubs.
  std::optional<storage::EdgeAccessor> found;
  auto *diff_txn = static_cast<storage::ReplicationAccessor *>(current_diff_txn_);
  core_->stub_index().ForEachStub([&](storage::Vertex *stub) {
    if (!found) found = diff_txn->FindEdgeFrom(edge_gid, stub, view);
  });
  return found;
}

std::optional<storage::EdgeAccessor> BranchContext::FindDiffEdge(storage::Gid edge_gid, storage::Gid from_gid,
                                                                 storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::FindDiffEdge: no current diff transaction set -- "
            "CurrentDB must call OpenHistorical() and SetCurrentDiffTxn() before any query "
            "runs against a checked-out branch.");
  if (auto *from_stub = core_->stub_index().FindStub(from_gid.AsUint(), stub_cursor_)) {
    return static_cast<storage::ReplicationAccessor *>(current_diff_txn_)->FindEdgeFrom(edge_gid, from_stub, view);
  }
  return current_diff_txn_->FindEdge(edge_gid, from_gid, view);
}

// historical_ is a frozen snapshot, so it is read with View::OLD throughout.
std::vector<storage::EdgeAccessor> BranchContext::ResolveEdges(storage::Gid vertex_gid,
                                                               storage::EdgeDirection direction, storage::View view,
                                                               bool *vertex_deleted) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::ResolveEdges: no current diff transaction set -- "
            "CurrentDB must call OpenHistorical() and SetCurrentDiffTxn() before any query "
            "runs against a checked-out branch.");

  if (vertex_deleted != nullptr) *vertex_deleted = false;
  const std::vector<storage::EdgeTypeId> edge_types;  // unfiltered: callers filter after charging hops
  const bool out = direction == storage::EdgeDirection::OUT;
  std::vector<storage::EdgeAccessor> result;
  std::vector<uint64_t> listed_deleted;

  // Diff side first: only branch-native edges are emitted from it. A fork-gid edge here is a stub whose fork edge is
  // emitted by the historical pass below, in fork identity (readers overlay the stub on it by gid).
  if (auto diff_vertex = FindStub(vertex_gid, view)) {
    if (IsForkVertexGid(vertex_gid)) {
      if (IsStubDeleted(*diff_vertex, view)) {
        if (vertex_deleted != nullptr) *vertex_deleted = true;
        return result;
      }
      listed_deleted = DeletedEdgeGids(*diff_vertex, view);
    }
    auto maybe_result =
        out ? diff_vertex->OutEdges(view, edge_types, nullptr) : diff_vertex->InEdges(view, edge_types, nullptr);
    if (maybe_result.has_value()) {
      result.reserve(maybe_result->edges.size());
      for (auto &edge : maybe_result->edges) {
        if (!IsForkEdgeGid(edge.Gid())) result.push_back(edge);
      }
    }
  }

  if (auto hist_vertex = FindForkVertex(vertex_gid)) {
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
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::Vertices: no current diff transaction set -- "
            "CurrentDB must call OpenHistorical() and SetCurrentDiffTxn() before any query "
            "runs against a checked-out branch.");
  return UnionVerticesIterable(
      historical().Vertices(storage::View::OLD), current_diff_txn_->Vertices(view), this, view);
}

BranchContext::UnionVerticesIterable::UnionVerticesIterable(storage::VerticesIterable hist_vertices,
                                                            storage::VerticesIterable diff_vertices, BranchContext *ctx,
                                                            storage::View view)
    : hist_vertices_(std::move(hist_vertices)), diff_vertices_(std::move(diff_vertices)), ctx_(ctx), view_(view) {}

BranchContext::UnionVerticesIterable::Iterator BranchContext::UnionVerticesIterable::begin() {
  return Iterator(
      hist_vertices_.begin(), hist_vertices_.end(), diff_vertices_.begin(), diff_vertices_.end(), ctx_, view_);
}

BranchContext::UnionVerticesIterable::Iterator::Iterator(storage::VerticesIterable::Iterator hist_it,
                                                         storage::VerticesIterable::Iterator hist_end,
                                                         storage::VerticesIterable::Iterator diff_it,
                                                         storage::VerticesIterable::Iterator diff_end,
                                                         BranchContext *ctx, storage::View view)
    : hist_it_(std::move(hist_it)),
      hist_end_(std::move(hist_end)),
      diff_it_(std::move(diff_it)),
      diff_end_(std::move(diff_end)),
      ctx_(ctx),
      view_(view) {
  SeekNext();
}

BranchContext::UnionVerticesIterable::Iterator &BranchContext::UnionVerticesIterable::Iterator::operator++() {
  SeekNext();
  return *this;
}

// A fork vertex whose branched() bit is clear has no stub and passes through; otherwise its stub, if any, is looked up
// live and wins (the reader overlays it on the fork vertex), or drops the vertex when delete-marked. Fork gids are all
// below the diff engine's vertices, so the diff side follows once the fork side is exhausted.
void BranchContext::UnionVerticesIterable::Iterator::SeekNext() {
  while (hist_it_.has_value() && !(*hist_it_ == *hist_end_)) {
    const storage::VertexAccessor fork_vertex = **hist_it_;
    ++(*hist_it_);
    if (fork_vertex.vertex_->branched()) {
      if (auto stub = ctx_->FindStub(fork_vertex.Gid(), view_)) {
        if (ctx_->IsStubDeleted(*stub, view_)) continue;
        current_ = std::move(stub);
        done_ = false;
        return;
      }
    }
    current_ = fork_vertex;
    done_ = false;
    return;
  }

  if (diff_it_.has_value() && !(*diff_it_ == *diff_end_)) {
    current_ = **diff_it_;
    ++(*diff_it_);
    done_ = false;
    return;
  }

  current_.reset();
  done_ = true;
}

}  // namespace memgraph::versioning
