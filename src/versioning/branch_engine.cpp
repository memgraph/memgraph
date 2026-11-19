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
#include <iterator>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "memory/db_arena.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/atomic_utils.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"
#include "versioning/overlay_mark_deleted.hpp"
#include "versioning/overlay_merge.hpp"
#include "versioning/overlay_state.hpp"

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
  // Narrowing is unbenched on the diff engine, so branch commits keep the full commit lock.
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
    : main_(main), fork_(fork), fork_ts_(fork_ts), diff_engine_(std::move(diff_engine)) {
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

std::expected<std::unique_ptr<BranchContext>, BranchContext::BuildError> BranchContext::BuildFromFork(
    storage::InMemoryStorage &main, uint64_t fork_ts) {
  // Read after the fork pin, as the registry does: every gid visible at fork_ts is below the floors.
  ForkPoint const fork{.vertex_gid_floor = main.vertex_id_.load(std::memory_order_acquire),
                       .edge_gid_floor = main.edge_id_.load(std::memory_order_acquire)};
  auto core = BranchCore::Build(main, fork, fork_ts);
  if (!core) return std::unexpected(std::move(core.error()));
  return std::make_unique<BranchContext>(std::move(*core));
}

std::expected<BranchContext::HistoricalAccessor, BranchContext::BuildError> BranchContext::OpenHistorical() {
  EndTransaction();
  auto historical = main().HistoricalAccess(fork_ts());
  if (!historical) {
    return std::unexpected(BuildError{
        .message = HistoricalOpenFailure(historical.error(), fork_ts(), "The branch is no longer available")});
  }
  return std::move(*historical);
}

void BranchContext::BeginTransaction(HistoricalAccessor historical, storage::Storage::Accessor &diff_txn) {
  MG_ASSERT(historical != nullptr, "BranchContext::BeginTransaction: the historical accessor is null.");
  MG_ASSERT(!txn_, "BranchContext::BeginTransaction: a branch transaction is already open.");
  txn_.emplace(std::move(historical), diff_txn);
}

std::expected<void, BranchContext::BuildError> BranchContext::BeginTransaction(storage::Storage::Accessor *diff_txn) {
  MG_ASSERT(diff_txn != nullptr, "BranchContext::BeginTransaction: the diff accessor is null.");
  auto historical = OpenHistorical();
  if (!historical) return std::unexpected(std::move(historical.error()));
  BeginTransaction(std::move(*historical), *diff_txn);
  return {};
}

// ---- Commit

// Published from a commit callback, so only a committed batch's counts are counted.
void BranchContext::PublishBaseDeletes() {
  auto &t = txn();
  if (t.base_deletes == ObjectCounts{}) return;
  t.diff->GetTransaction()->commit_callbacks_.Add(
      [core = core_.get(), deleted = t.base_deletes](uint64_t /*commit_ts*/) { core->AddBaseDeletes(deleted); });
}

std::expected<void, storage::StorageManipulationError> BranchContext::CommitTransaction(
    storage::CommitArgs commit_args) {
  PublishBaseDeletes();
  return txn().diff->PrepareForCommitPhase(std::move(commit_args));
}

std::expected<void, storage::StorageManipulationError> BranchContext::PeriodicCommitTransaction(
    storage::CommitArgs commit_args) {
  PublishBaseDeletes();
  auto result = txn().diff->PeriodicCommit(std::move(commit_args));
  // A batch that did not commit aborts the transaction, so its counts are never needed again.
  txn().base_deletes = {};
  return result;
}

// ---- Overlays

storage::VertexAccessor BranchContext::OverlayAccessor(storage::Vertex *overlay) {
  return storage::VertexAccessor{overlay, &diff_engine(), txn().diff->GetTransaction()};
}

std::optional<storage::VertexAccessor> BranchContext::ReadOverlay(storage::Gid gid) {
  if (!IsBaseCandidateVertex(gid)) return std::nullopt;
  auto *overlay = overlays().FindOverlay(gid);
  if (overlay == nullptr) return std::nullopt;
  return OverlayAccessor(overlay);
}

bool BranchContext::OverlayDeleted(storage::Vertex *overlay, storage::View view) {
  return overlay::IsDeleted(txn().diff->GetTransaction(), overlay, view);
}

template <typename MayDiffer>
std::optional<storage::VertexAccessor> BranchContext::OverlayToRead(const storage::VertexAccessor &vertex,
                                                                    MayDiffer &&may_differ) {
  DMG_ASSERT(!IsOverlay(vertex), "an overlay reached a branch read");
  if (vertex.storage_ == &diff_engine() || !vertex.vertex_->branched()) return std::nullopt;
  auto const gid = vertex.Gid();
  if (!may_differ(gid) && !(core_->MayHaveAnyVertexDeleted() && core_->MayHaveVertexDeleted(gid))) return std::nullopt;
  return ReadOverlay(gid);
}

std::optional<storage::EdgeAccessor> BranchContext::FindEdgeOverlay(const storage::EdgeAccessor &base_edge) {
  auto *overlay = edge_overlays().Find(base_edge.Gid());
  if (overlay == nullptr) return std::nullopt;
  return storage::EdgeAccessor{storage::EdgeRef{overlay},
                               base_edge.EdgeType(),
                               base_edge.from_vertex_,
                               base_edge.to_vertex_,
                               &diff_engine(),
                               txn().diff->GetTransaction()};
}

// ---- Lookups

std::optional<storage::VertexAccessor> BranchContext::BaseAccessorByGid(storage::Gid gid) {
  return historical().FindVertex(gid, storage::View::OLD);
}

std::optional<storage::VertexAccessor> BranchContext::BaseAccessorByPtr(storage::Vertex *base_vertex) {
  return storage::VertexAccessor::Create(base_vertex, &main(), historical().GetTransaction(), storage::View::OLD);
}

std::optional<storage::VertexAccessor> BranchContext::FindBaseVertex(storage::Gid gid) {
  if (!IsBaseCandidateVertex(gid)) return std::nullopt;
  auto *const base_vertex = overlays().FindBase(gid);
  if (base_vertex != nullptr) return BaseAccessorByPtr(base_vertex);
  return BaseAccessorByGid(gid);
}

std::optional<storage::EdgeAccessor> BranchContext::FindBaseEdge(storage::Gid edge_gid, storage::Gid from_gid) {
  if (!IsBaseCandidateEdge(edge_gid) || !IsBaseCandidateVertex(from_gid)) return std::nullopt;
  // Not main's lookup by gid: that scans live adjacency, which lost edges main deleted since the fork.
  auto const from = FindBaseVertex(from_gid);
  if (!from) return std::nullopt;
  auto out = from->OutEdges(storage::View::OLD);
  if (!out) return std::nullopt;
  auto const found = std::ranges::find_if(out->edges, [&](const auto &edge) { return edge.Gid() == edge_gid; });
  if (found == out->edges.end()) return std::nullopt;
  return *found;
}

std::optional<storage::VertexAccessor> BranchContext::ResolveVertex(storage::Gid gid, storage::View view) {
  auto &t = txn();
  if (!IsBaseCandidateVertex(gid)) {
    auto native = t.diff->FindVertex(gid, view);
    DMG_ASSERT(!native || !IsOverlay(*native), "a native gid resolved to an overlay.");
    return native;
  }
  auto const found = overlays().Find(gid);
  if (found.overlay != nullptr && core_->MayHaveVertexDeleted(gid) && OverlayDeleted(found.overlay, view))
    return std::nullopt;
  if (found.base_vertex != nullptr) return BaseAccessorByPtr(found.base_vertex);
  return BaseAccessorByGid(gid);
}

std::optional<storage::EdgeAccessor> BranchContext::ResolveEdge(storage::Gid edge_gid, storage::Gid from_gid,
                                                                storage::View view) {
  if (!IsBaseCandidateEdge(edge_gid)) return FindDiffEdge(edge_gid, from_gid, view);
  auto edge = FindBaseEdge(edge_gid, from_gid);
  if (!edge || IsBaseEdgeHidden(*edge, view)) return std::nullopt;
  return edge;
}

std::optional<storage::EdgeAccessor> BranchContext::FindEdgeInAnchors(storage::Gid edge_gid, storage::View view) {
  std::optional<storage::EdgeAccessor> found;
  overlays().ForEachOverlay([&](storage::Vertex *overlay) {
    found = diff().FindEdgeFrom(edge_gid, OverlayAccessor(overlay), view);
    return !found;
  });
  return found;
}

std::optional<storage::EdgeAccessor> BranchContext::FindDiffEdge(storage::Gid edge_gid, storage::View view) {
  if (IsBaseCandidateEdge(edge_gid)) return std::nullopt;
  auto found = diff().FindEdge(edge_gid, view);
  if (!found) found = FindEdgeInAnchors(edge_gid, view);
  DMG_ASSERT(!found || !IsOverlay(*found), "FindDiffEdge resolved to an edge overlay.");
  return found;
}

std::optional<storage::EdgeAccessor> BranchContext::FindDiffEdge(storage::Gid edge_gid, storage::Gid from_gid,
                                                                 storage::View view) {
  if (IsBaseCandidateEdge(edge_gid)) return std::nullopt;
  auto &acc = diff();
  if (!IsBaseCandidateVertex(from_gid)) return acc.FindEdge(edge_gid, from_gid, view);
  // A native edge off a base vertex hangs on its overlay.
  auto const overlay = ReadOverlay(from_gid);
  return overlay ? acc.FindEdgeFrom(edge_gid, *overlay, view) : std::nullopt;
}

// ---- Deletion

bool BranchContext::IsVertexDeleted(storage::Gid gid, storage::View view) {
  if (!IsBaseCandidateVertex(gid) || !core_->MayHaveVertexDeleted(gid)) return false;
  auto *overlay = overlays().FindOverlay(gid);
  return overlay != nullptr && OverlayDeleted(overlay, view);
}

// A delete sets the branched bit before RecordVertexDeleted publishes the flag (release), so the acquire sees the bit.
bool BranchContext::IsBaseVertexDeleted(const storage::VertexAccessor &base_vertex, storage::View view) {
  if (!core_->MayHaveAnyVertexDeleted() || !base_vertex.vertex_->branched()) return false;
  return IsVertexDeleted(base_vertex.Gid(), view);
}

bool BranchContext::IsBaseEdgeDeleted(const storage::EdgeAccessor &base_edge, storage::View view,
                                      std::optional<storage::EdgeAccessor> *overlay_out) {
  if (!core_->MayHaveBaseEdgeDeleted(base_edge.Gid())) return false;
  auto overlay = FindEdgeOverlay(base_edge);
  if (!overlay) return false;
  auto const deleted = overlay::IsDeleted(txn().diff->GetTransaction(), overlay->edge_.ptr, view);
  if (overlay_out != nullptr) *overlay_out = overlay;
  return deleted;
}

bool BranchContext::IsBaseEdgeHidden(const storage::EdgeAccessor &base_edge, storage::View view,
                                     std::optional<storage::EdgeAccessor> *overlay_out) {
  if (!IsBaseCandidateEdge(base_edge.Gid())) return false;
  if (IsBaseEdgeDeleted(base_edge, view, overlay_out)) return true;
  if (!core_->MayHaveAnyVertexDeleted()) return false;
  auto const from = base_edge.FromVertex();
  auto const to = base_edge.ToVertex();
  return IsBaseVertexDeleted(from, view) || (to.Gid() != from.Gid() && IsBaseVertexDeleted(to, view));
}

// ---- Vertex reads

namespace {

bool DeletedOnBranch(const BranchCore &core, const storage::VertexAccessor &overlay, storage::View view) {
  return core.MayHaveVertexDeleted(overlay.Gid()) && overlay::IsDeleted(overlay.transaction_, overlay.vertex_, view);
}

}  // namespace

// An overlay holds the base vertex's full label set from its creation, so its labels are read as stock reads them; a
// marked-deleted overlay reads DELETED_OBJECT. Properties are merged key by key (overlay_merge.hpp).

storage::Result<storage::VertexKey> BranchContext::Labels(const storage::VertexAccessor &vertex, storage::View view) {
  auto const overlay = OverlayToRead(vertex, [&](storage::Gid gid) { return core_->MayHaveLabelChange(gid); });
  return overlay ? overlay->Labels(view) : vertex.Labels(view);
}

storage::Result<bool> BranchContext::HasLabel(const storage::VertexAccessor &vertex, storage::LabelId label,
                                              storage::View view) {
  auto const overlay = OverlayToRead(
      vertex, [&](storage::Gid gid) { return core_->MayHaveLabelTouched(label) && core_->MayHaveLabelChange(gid); });
  return overlay ? overlay->HasLabel(label, view) : vertex.HasLabel(label, view);
}

storage::Result<bool> BranchContext::HasAnyLabel(const storage::VertexAccessor &vertex, storage::View view) {
  auto const overlay = OverlayToRead(vertex, [&](storage::Gid gid) { return core_->MayHaveLabelChange(gid); });
  return overlay ? overlay->HasAnyLabel(view) : vertex.HasAnyLabel(view);
}

storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> BranchContext::Properties(
    const storage::VertexAccessor &vertex, storage::View view) {
  auto const overlay = OverlayToRead(vertex, [&](storage::Gid gid) { return core_->MayHavePropertyChange(gid); });
  if (!overlay) return vertex.Properties(view);
  if (DeletedOnBranch(*core_, *overlay, view)) return std::unexpected(storage::Error::DELETED_OBJECT);
  return overlay::MergeProperties(overlay::ReadProperties(overlay->transaction_, *overlay->vertex_, view), vertex);
}

storage::Result<storage::PropertyValue> BranchContext::GetProperty(const storage::VertexAccessor &vertex,
                                                                   storage::PropertyId key, storage::View view) {
  auto const overlay =
      OverlayToRead(vertex, [&](storage::Gid gid) { return core_->MayHavePropertyFieldChange(gid, key); });
  if (!overlay) return vertex.GetProperty(key, view);
  if (DeletedOnBranch(*core_, *overlay, view)) return std::unexpected(storage::Error::DELETED_OBJECT);
  return overlay::MergeGetProperty(
      overlay::ReadProperty(overlay->transaction_, *overlay->vertex_, key, view), vertex, key);
}

storage::Result<uint64_t> BranchContext::GetPropertySize(const storage::VertexAccessor &vertex, storage::PropertyId key,
                                                         storage::View view) {
  auto const overlay =
      OverlayToRead(vertex, [&](storage::Gid gid) { return core_->MayHavePropertyFieldChange(gid, key); });
  if (!overlay) return vertex.GetPropertySize(key, view);
  if (DeletedOnBranch(*core_, *overlay, view)) return std::unexpected(storage::Error::DELETED_OBJECT);
  return overlay::MergeGetPropertySize(
      overlay::ReadProperty(overlay->transaction_, *overlay->vertex_, key, view), vertex, key);
}

// ---- Edge reads

// A clear branched bit proves the branch wrote nothing to the base edge, but a deleted endpoint hides it regardless.
// With main's properties_on_edges off the hint is always false and the filters decide.
BranchContext::EdgeRead BranchContext::ResolveEdgeRead(const storage::EdgeAccessor &edge,
                                                       std::optional<storage::PropertyId> field, storage::View view) {
  if (edge.storage_ == &diff_engine()) return {};
  bool const hint = edge.MaybeBranchedHint();
  if (!hint && !core_->MayHaveAnyVertexDeleted()) return {};
  std::optional<storage::EdgeAccessor> overlay;
  if (IsBaseEdgeHidden(edge, view, &overlay)) return {.kind = EdgeRead::Kind::kHidden};
  if (!hint) return {};
  auto const gid = edge.Gid();
  bool const may_have =
      field ? core_->MayHaveBaseEdgePropertyFieldChange(gid, *field) : core_->MayHaveBaseEdgePropertyChange(gid);
  if (!may_have) return {};
  if (!overlay) overlay = FindEdgeOverlay(edge);
  if (overlay) return {.kind = EdgeRead::Kind::kOverlay, .overlay = overlay};
  return {};
}

storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> BranchContext::Properties(
    const storage::EdgeAccessor &edge, storage::View view) {
  auto const read = ResolveEdgeRead(edge, std::nullopt, view);
  if (read.kind == EdgeRead::Kind::kHidden) return std::unexpected(storage::Error::DELETED_OBJECT);
  if (read.kind == EdgeRead::Kind::kDirect) return edge.Properties(view);
  return overlay::MergeProperties(overlay::ReadProperties(read.overlay->transaction_, *read.overlay->edge_.ptr, view),
                                  edge);
}

storage::Result<storage::PropertyValue> BranchContext::GetProperty(const storage::EdgeAccessor &edge,
                                                                   storage::PropertyId key, storage::View view) {
  auto const read = ResolveEdgeRead(edge, key, view);
  if (read.kind == EdgeRead::Kind::kHidden) return std::unexpected(storage::Error::DELETED_OBJECT);
  if (read.kind == EdgeRead::Kind::kDirect) return edge.GetProperty(key, view);
  return overlay::MergeGetProperty(
      overlay::ReadProperty(read.overlay->transaction_, *read.overlay->edge_.ptr, key, view), edge, key);
}

storage::Result<uint64_t> BranchContext::GetPropertySize(const storage::EdgeAccessor &edge, storage::PropertyId key,
                                                         storage::View view) {
  auto const read = ResolveEdgeRead(edge, key, view);
  if (read.kind == EdgeRead::Kind::kHidden) return std::unexpected(storage::Error::DELETED_OBJECT);
  if (read.kind == EdgeRead::Kind::kDirect) return edge.GetPropertySize(key, view);
  return overlay::MergeGetPropertySize(
      overlay::ReadProperty(read.overlay->transaction_, *read.overlay->edge_.ptr, key, view), edge, key);
}

// ---- Vertex writes

namespace {

using PropertyMap = std::map<storage::PropertyId, storage::PropertyValue>;
using PropertyChange = std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>;

// Main's `branched()` bit is a monotonic hint, so marking an object the branch then does not change is safe.
template <typename MainObject>
void MarkBranched(MainObject *main_object) {
  if (main_object->branched()) return;
  auto guard = std::unique_lock{main_object->lock};
  main_object->SetBranched();
}

// A write of several overlay keys is not atomic. Unless Done() is reached, whether by an error return or an exception,
// the overlay may be half-written inside the diff transaction, so the transaction is poisoned and must be aborted.
class PartialWriteGuard {
 public:
  explicit PartialWriteGuard(storage::Transaction *txn) : txn_(txn) {}

  PartialWriteGuard(const PartialWriteGuard &) = delete;
  PartialWriteGuard &operator=(const PartialWriteGuard &) = delete;

  ~PartialWriteGuard() {
    if (!done_) txn_->has_serialization_error = true;
  }

  void Done() { done_ = true; }

 private:
  storage::Transaction *txn_;
  bool done_{false};
};

std::string DeletedMessage(storage::Gid gid) {
  return fmt::format("Cannot modify vertex {}: it was deleted on this branch.", gid.AsUint());
}

}  // namespace

storage::Result<storage::VertexAccessor> BranchContext::MaterializeOverlay(const storage::VertexAccessor &base_vertex) {
  // Before the overlay exists, and so before any write of it (reads gate on this bit).
  MarkBranched(base_vertex.vertex_);
  auto const gid = base_vertex.Gid();
  if (auto *existing = overlays().FindOverlay(gid)) return OverlayAccessor(existing);
  // The base's labels, not main's live ones: main may have changed them since fork_ts.
  auto labels = base_vertex.Labels(storage::View::OLD);
  if (!labels) return std::unexpected(labels.error());
  // The overlay index keeps main's Vertex* for the base vertex. It stays valid because main's GC horizon is clamped to
  // the oldest fork pin (oldest_fork_pin_), so a vertex visible at fork_ts is never freed while the branch lives.
  return OverlayAccessor(overlays().GetOrCreateOverlay(gid, base_vertex.vertex_, *labels));
}

storage::Result<storage::VertexAccessor> BranchContext::OverlayForWrite(const storage::VertexAccessor &base_vertex) {
  DMG_ASSERT(!IsNative(base_vertex), "a native vertex has no overlay.");
  if (IsVertexDeleted(base_vertex.Gid(), storage::View::NEW)) return std::unexpected(storage::Error::DELETED_OBJECT);
  return MaterializeOverlay(base_vertex);
}

std::expected<storage::VertexAccessor, BranchContext::BranchWriteError> BranchContext::EnsureDiffVertex(
    storage::Gid gid) {
  if (!IsBaseCandidateVertex(gid)) {
    // A native vertex the caller resolved and that is not visible now was deleted on the branch.
    if (auto native = txn().diff->FindVertex(gid, storage::View::NEW)) return *native;
    return std::unexpected(BranchWriteError{DeletedMessage(gid)});
  }
  if (IsVertexDeleted(gid, storage::View::NEW)) return std::unexpected(BranchWriteError{DeletedMessage(gid)});
  // An existing overlay means the base vertex was already resolved; reading the base again walks all of main's history
  // of the vertex after the fork.
  if (auto overlay = ReadOverlay(gid)) return *std::move(overlay);
  auto base_vertex = FindBaseVertex(gid);
  if (!base_vertex) {
    return std::unexpected(
        BranchWriteError{fmt::format("Cannot modify vertex {}: it is not in the branch's base state -- this should be "
                                     "unreachable (callers resolve a vertex before mutating it).",
                                     gid.AsUint())});
  }
  auto overlay = MaterializeOverlay(*base_vertex);
  if (!overlay) return std::unexpected(BranchWriteError{DeletedMessage(gid)});
  return *std::move(overlay);
}

storage::VertexAccessor BranchContext::CreateNativeVertex() {
  // Plain fetch_add on main's counter, no reservation; CreateVertexEx raises the diff engine's own counter past the gid
  // before inserting, so the diff engine's index iterators see the new vertex.
  auto const gid = storage::Gid::FromUint(main().vertex_id_.fetch_add(1, std::memory_order_acq_rel));
  auto created = static_cast<storage::ReplicationAccessor &>(diff()).CreateVertexEx(gid);
  MG_ASSERT(created.has_value(),
            "BranchContext::CreateNativeVertex: gid {} already exists in the diff engine.",
            gid.AsUint());
  return *created;
}

storage::Result<BranchContext::EdgeOverlay> BranchContext::MaterializeEdgeOverlay(
    const storage::EdgeAccessor &base_edge) {
  // Before the overlay exists, and so before any write of it (reads gate on this bit). Without properties_on_edges
  // main's base edge is a bare gid with no object to mark, and reads fall back to the filters.
  if (main().config_.salient.items.properties_on_edges) MarkBranched(base_edge.edge_.ptr);
  auto from_overlay = MaterializeOverlay(base_edge.FromVertex());
  if (!from_overlay) return std::unexpected(from_overlay.error());
  auto to_overlay = MaterializeOverlay(base_edge.ToVertex());
  if (!to_overlay) return std::unexpected(to_overlay.error());
  return EdgeOverlay{.edge = edge_overlays().GetOrCreate(base_edge.Gid(), base_edge.edge_),
                     .from = from_overlay->vertex_,
                     .to = to_overlay->vertex_,
                     .type = base_edge.EdgeType()};
}

storage::Result<BranchContext::EdgeOverlay> BranchContext::OverlayForWrite(const storage::EdgeAccessor &base_edge) {
  DMG_ASSERT(!IsNative(base_edge), "a native edge has no overlay.");
  // The diff engine's own config is always properties_on_edges: it is main's that the user's edge properties follow.
  if (!main().config_.salient.items.properties_on_edges) return std::unexpected(storage::Error::PROPERTIES_DISABLED);
  if (IsBaseEdgeHidden(base_edge, storage::View::NEW)) return std::unexpected(storage::Error::DELETED_OBJECT);
  return MaterializeEdgeOverlay(base_edge);
}

storage::Result<storage::EdgeAccessor> BranchContext::CreateNativeEdge(storage::VertexAccessor &from,
                                                                       storage::VertexAccessor &to,
                                                                       storage::EdgeTypeId edge_type) {
  core_->RecordEdgeChange(from.Gid());
  core_->RecordEdgeChange(to.Gid());
  // Plain fetch_add on main's counter; CreateEdgeEx raises the diff engine's own counter past the gid.
  auto const gid = storage::Gid::FromUint(main().edge_id_.fetch_add(1, std::memory_order_acq_rel));
  return static_cast<storage::ReplicationAccessor &>(diff()).CreateEdgeEx(&from, &to, edge_type, gid);
}

bool BranchContext::PrepareOverlayForWrite(const storage::VertexAccessor &overlay) {
  auto guard = std::shared_lock{overlay.vertex_->lock};
  return storage::PrepareForWrite(txn().diff->GetTransaction(), overlay.vertex_);
}

bool BranchContext::PrepareOverlayForWrite(const EdgeOverlay &overlay) {
  auto guard = std::shared_lock{overlay.edge->lock};
  return storage::PrepareForWrite(txn().diff->GetTransaction(), overlay.edge);
}

storage::Result<bool> BranchContext::AddLabel(storage::VertexAccessor &vertex, storage::LabelId label) {
  if (IsNative(vertex)) return vertex.AddLabel(label);
  auto overlay = OverlayForWrite(vertex);
  if (!overlay) return std::unexpected(overlay.error());
  core_->RecordLabelChange(vertex.Gid());
  core_->RecordLabelTouched(label);
  return overlay->AddLabel(label);
}

storage::Result<bool> BranchContext::RemoveLabel(storage::VertexAccessor &vertex, storage::LabelId label) {
  if (IsNative(vertex)) return vertex.RemoveLabel(label);
  auto overlay = OverlayForWrite(vertex);
  if (!overlay) return std::unexpected(overlay.error());
  core_->RecordLabelChange(vertex.Gid());
  core_->RecordLabelTouched(label);
  return overlay->RemoveLabel(label);
}

storage::Result<storage::PropertyValue> BranchContext::WriteOverlayProperty(const storage::VertexAccessor &base_vertex,
                                                                            storage::VertexAccessor &overlay,
                                                                            storage::PropertyId key,
                                                                            const storage::PropertyValue &value) {
  auto *const transaction = txn().diff->GetTransaction();
  auto old = overlay::ReadProperty(transaction, *overlay.vertex_, key, storage::View::NEW);
  if (!old) {
    // Reading the base walks all of main's history of the object after the fork; only a key the overlay holds no entry
    // for needs it.
    auto read = base_vertex.GetProperty(key, storage::View::OLD);
    if (!read) return std::unexpected(read.error());
    old = std::move(*read);
  }
  if (auto written = overlay::WriteProperty(transaction, diff_engine(), overlay.vertex_, key, value); !written) {
    return std::unexpected(written.error());
  }
  return *std::move(old);
}

storage::Result<storage::PropertyValue> BranchContext::WriteOverlayProperty(const storage::EdgeAccessor &base_edge,
                                                                            const EdgeOverlay &overlay,
                                                                            storage::PropertyId key,
                                                                            const storage::PropertyValue &value) {
  auto *const transaction = txn().diff->GetTransaction();
  auto old = overlay::ReadProperty(transaction, *overlay.edge, key, storage::View::NEW);
  if (!old) {
    auto read = base_edge.GetProperty(key, storage::View::OLD);
    if (!read) return std::unexpected(read.error());
    old = std::move(*read);
  }
  if (auto written = overlay::WriteProperty(
          transaction, diff_engine(), overlay.edge, overlay.from, overlay.to, overlay.type, key, value);
      !written) {
    return std::unexpected(written.error());
  }
  return *std::move(old);
}

template <typename Object>
storage::Result<storage::PropertyValue> BranchContext::SetPropertyAt(Object &object, storage::PropertyId key,
                                                                     const storage::PropertyValue &value) {
  if (IsNative(object)) {
    if (PropertyWritesDisabled(object)) return std::unexpected(storage::Error::PROPERTIES_DISABLED);
    return object.SetProperty(key, value);
  }
  auto overlay = OverlayForWrite(object);
  if (!overlay) return std::unexpected(overlay.error());
  RecordPropertiesWrite(object);
  RecordPropertyWrite(object, key);
  return WriteOverlayProperty(object, *overlay, key, value);
}

template <typename Object>
storage::Result<bool> BranchContext::InitPropertiesAt(Object &object, PropertyMap &properties) {
  if (IsNative(object)) {
    if (PropertyWritesDisabled(object)) return std::unexpected(storage::Error::PROPERTIES_DISABLED);
    return object.InitProperties(properties);
  }
  auto overlay = OverlayForWrite(object);
  if (!overlay) return std::unexpected(overlay.error());
  auto existing = Properties(object, storage::View::NEW);
  if (!existing) return std::unexpected(existing.error());
  if (!existing->empty()) {
    if (!PrepareOverlayForWrite(*overlay)) return std::unexpected(storage::Error::SERIALIZATION_ERROR);
    return false;
  }
  if (properties.empty()) {
    if (!PrepareOverlayForWrite(*overlay)) return std::unexpected(storage::Error::SERIALIZATION_ERROR);
    return true;
  }
  RecordPropertiesWrite(object);
  for (auto const &entry : properties) RecordPropertyWrite(object, entry.first);
  PartialWriteGuard guard{txn().diff->GetTransaction()};
  for (auto const &[key, value] : properties) {
    if (auto written = WriteOverlayProperty(object, *overlay, key, value); !written)
      return std::unexpected(written.error());
  }
  guard.Done();
  return true;
}

template <typename Object>
storage::Result<std::vector<PropertyChange>> BranchContext::UpdatePropertiesAt(Object &object,
                                                                               PropertyMap &properties) {
  if (IsNative(object)) {
    if (PropertyWritesDisabled(object)) return std::unexpected(storage::Error::PROPERTIES_DISABLED);
    return object.UpdateProperties(properties);
  }
  auto overlay = OverlayForWrite(object);
  if (!overlay) return std::unexpected(overlay.error());
  auto existing = Properties(object, storage::View::NEW);
  if (!existing) return std::unexpected(existing.error());
  if (properties.empty()) {
    if (!PrepareOverlayForWrite(*overlay)) return std::unexpected(storage::Error::SERIALIZATION_ERROR);
    properties = std::move(*existing);
    return std::vector<PropertyChange>{};
  }
  std::vector<PropertyChange> changes;
  changes.reserve(properties.size());
  for (auto const &[key, value] : properties) {
    auto const old = existing->find(key);
    changes.emplace_back(key, old != existing->end() ? old->second : storage::PropertyValue{}, value);
  }
  RecordPropertiesWrite(object);
  for (auto const &entry : properties) RecordPropertyWrite(object, entry.first);
  PartialWriteGuard guard{txn().diff->GetTransaction()};
  for (auto const &[key, value] : properties) {
    if (auto written = WriteOverlayProperty(object, *overlay, key, value); !written)
      return std::unexpected(written.error());
  }
  guard.Done();
  // As storage's UpdateProperties: the properties the update left alone come back in the caller's map.
  for (auto &[key, old_value] : *existing) properties.emplace(key, std::move(old_value));
  return changes;
}

template <typename Object>
storage::Result<PropertyMap> BranchContext::ClearPropertiesAt(Object &object) {
  if (IsNative(object)) {
    if (PropertyWritesDisabled(object)) return std::unexpected(storage::Error::PROPERTIES_DISABLED);
    return object.ClearProperties();
  }
  auto overlay = OverlayForWrite(object);
  if (!overlay) return std::unexpected(overlay.error());
  auto existing = Properties(object, storage::View::NEW);
  if (!existing) return std::unexpected(existing.error());
  if (existing->empty()) {
    if (!PrepareOverlayForWrite(*overlay)) return std::unexpected(storage::Error::SERIALIZATION_ERROR);
    return existing;
  }
  RecordPropertiesWrite(object);
  for (auto const &entry : *existing) RecordPropertyWrite(object, entry.first);
  PartialWriteGuard guard{txn().diff->GetTransaction()};
  for (auto const &entry : *existing) {
    if (auto removed = WriteOverlayProperty(object, *overlay, entry.first, storage::PropertyValue{}); !removed) {
      return std::unexpected(removed.error());
    }
  }
  guard.Done();
  return existing;
}

storage::Result<storage::PropertyValue> BranchContext::SetProperty(storage::VertexAccessor &vertex,
                                                                   storage::PropertyId key,
                                                                   const storage::PropertyValue &value) {
  return SetPropertyAt(vertex, key, value);
}

storage::Result<bool> BranchContext::InitProperties(storage::VertexAccessor &vertex, PropertyMap &properties) {
  return InitPropertiesAt(vertex, properties);
}

storage::Result<std::vector<PropertyChange>> BranchContext::UpdateProperties(storage::VertexAccessor &vertex,
                                                                             PropertyMap &properties) {
  return UpdatePropertiesAt(vertex, properties);
}

storage::Result<PropertyMap> BranchContext::ClearProperties(storage::VertexAccessor &vertex) {
  return ClearPropertiesAt(vertex);
}

storage::Result<storage::PropertyValue> BranchContext::SetProperty(storage::EdgeAccessor &edge, storage::PropertyId key,
                                                                   const storage::PropertyValue &value) {
  return SetPropertyAt(edge, key, value);
}

storage::Result<bool> BranchContext::InitProperties(storage::EdgeAccessor &edge, PropertyMap &properties) {
  return InitPropertiesAt(edge, properties);
}

storage::Result<std::vector<PropertyChange>> BranchContext::UpdateProperties(const storage::EdgeAccessor &edge,
                                                                             PropertyMap &properties) {
  return UpdatePropertiesAt(edge, properties);
}

storage::Result<PropertyMap> BranchContext::ClearProperties(storage::EdgeAccessor &edge) {
  return ClearPropertiesAt(edge);
}

storage::Result<bool> BranchContext::DeleteBaseEdgeOnBranch(const storage::EdgeAccessor &base_edge) {
  core_->RecordEdgeChange(base_edge.FromVertex().Gid());
  core_->RecordEdgeChange(base_edge.ToVertex().Gid());
  core_->RecordBaseEdgeDeleted(base_edge.Gid());
  auto const overlay = MaterializeEdgeOverlay(base_edge);
  if (!overlay) return std::unexpected(overlay.error());
  // Listed before the delete mark, so a visible delete mark is always found from the endpoints. The edge overlay is the
  // only write: no endpoint overlay is touched, so deletes of different edges at one vertex never conflict.
  ListBaseEdgeDelete(overlay->from, overlay->to, overlay->edge);
  // The edge may already be deleted by a transaction committed after this one began: not a broken precondition but a
  // conflict, which the edge overlay's PrepareForWrite reports. Already deleted by this transaction is a no-op.
  auto const deleted = overlay::MarkEdgeDeleted(txn().diff->GetTransaction(), overlay->edge);
  if (deleted && *deleted) ++txn().base_deletes.edges;
  return deleted;
}

storage::Result<bool> BranchContext::DeleteBaseEdge(const storage::EdgeAccessor &base_edge) {
  DMG_ASSERT(!IsNative(base_edge), "a native edge is deleted by the diff engine.");
  if (IsBaseEdgeHidden(base_edge, storage::View::NEW)) return false;
  return DeleteBaseEdgeOnBranch(base_edge);
}

std::vector<storage::EdgeAccessor> BranchContext::VisibleBaseEdges(const storage::VertexAccessor &base_vertex) {
  auto const found = overlays().Find(base_vertex.Gid());
  std::vector<storage::EdgeAccessor> visible;
  for (auto const direction : {storage::EdgeDirection::OUT, storage::EdgeDirection::IN}) {
    HiddenBaseEdges hidden;
    if (found.overlay != nullptr) hidden = ReadOverlayAdjacency(found.overlay, direction, storage::View::NEW).hidden;
    auto expanded = direction == storage::EdgeDirection::OUT ? base_vertex.OutEdges(storage::View::OLD)
                                                             : base_vertex.InEdges(storage::View::OLD);
    // The accessor is a resolved base vertex, so it is visible and not deleted at the fork.
    DMG_ASSERT(expanded.has_value(), "expanding a base vertex failed.");
    if (!expanded) continue;
    for (auto const &edge : expanded->edges) {
      // A self-loop is in both lists; it was taken as an out-edge.
      if (direction == storage::EdgeDirection::IN && edge.IsCycle()) continue;
      if (!hidden.Contains(edge.edge_)) visible.push_back(edge);
    }
  }
  return visible;
}

storage::Result<bool> BranchContext::DeleteBaseVertex(storage::Gid gid, bool detach) {
  if (IsVertexDeleted(gid, storage::View::NEW)) return false;
  auto base_vertex = FindBaseVertex(gid);
  if (!base_vertex) return std::unexpected(storage::Error::NONEXISTENT_OBJECT);
  // The edges the delete marks as deleted, taken before any is.
  auto const edges = VisibleBaseEdges(*base_vertex);
  if (!detach) {
    // As stock DeleteVertex: refuse before anything is materialized or written.
    if (!edges.empty()) return std::unexpected(storage::Error::VERTEX_HAS_EDGES);
    if (auto existing = ReadOverlay(gid)) {
      for (auto const direction : {storage::EdgeDirection::OUT, storage::EdgeDirection::IN}) {
        if (!ReadOverlayAdjacency(existing->vertex_, direction, storage::View::NEW).natives.empty()) {
          return std::unexpected(storage::Error::VERTEX_HAS_EDGES);
        }
      }
    }
  }
  // MaterializeOverlay sets the branched bit before RecordVertexDeleted publishes the any-deleted flag, and the delete
  // mark comes last: every read that can see it is gated on the filter.
  auto overlay = MaterializeOverlay(*base_vertex);
  if (!overlay) return std::unexpected(overlay.error());
  for (auto const &edge : edges) {
    if (auto deleted = DeleteBaseEdgeOnBranch(edge); !deleted) return std::unexpected(deleted.error());
  }
  // The only path that deletes a base vertex overlay. Expansion relies on base edges being marked deleted, not on the
  // endpoint being deleted, so none may remain visible.
  DMG_ASSERT(VisibleBaseEdges(*base_vertex).empty(), "a base vertex is deleted with a visible base edge.");
  core_->RecordVertexDeleted(gid);
  // The overlay's adjacency holds only native edges, which a detach deletes with it.
  bool marked_deleted = false;
  if (detach) {
    auto deleted = txn().diff->DetachDeleteVertex(&*overlay);
    if (!deleted) return std::unexpected(deleted.error());
    marked_deleted = deleted->has_value();
  } else {
    auto deleted = txn().diff->DeleteVertex(&*overlay);
    if (!deleted) return std::unexpected(deleted.error());
    marked_deleted = deleted->has_value();
  }
  if (marked_deleted) ++txn().base_deletes.vertices;
  return marked_deleted;
}

// ---- Adjacency

storage::Result<std::vector<storage::EdgeAccessor>> BranchContext::Edges(const storage::VertexAccessor &vertex,
                                                                         storage::EdgeDirection direction,
                                                                         storage::View view) {
  DMG_ASSERT(!IsOverlay(vertex), "an overlay reached a branch read");
  bool const out = direction == storage::EdgeDirection::OUT;
  if (vertex.storage_ == &diff_engine()) {
    auto native = out ? vertex.OutEdges(view) : vertex.InEdges(view);
    if (!native) return std::unexpected(native.error());
    return std::vector<storage::EdgeAccessor>(native->edges.begin(), native->edges.end());
  }
  bool deleted = false;
  auto edges = ResolveEdges(vertex.Gid(), direction, view, &deleted, &vertex);
  if (deleted) return std::unexpected(storage::Error::DELETED_OBJECT);
  return edges;
}

storage::Result<std::vector<storage::EdgeAccessor>> BranchContext::OutEdges(const storage::VertexAccessor &vertex,
                                                                            storage::View view) {
  return Edges(vertex, storage::EdgeDirection::OUT, view);
}

storage::Result<std::vector<storage::EdgeAccessor>> BranchContext::InEdges(const storage::VertexAccessor &vertex,
                                                                           storage::View view) {
  return Edges(vertex, storage::EdgeDirection::IN, view);
}

BranchContext::OverlayAdjacency BranchContext::ReadOverlayAdjacency(storage::Vertex *overlay,
                                                                    storage::EdgeDirection direction,
                                                                    storage::View view) {
  OverlayAdjacency adjacency;
  auto const overlay_accessor = OverlayAccessor(overlay);
  auto const expanded =
      direction == storage::EdgeDirection::OUT ? overlay_accessor.OutEdges(view) : overlay_accessor.InEdges(view);
  DMG_ASSERT(expanded.has_value(), "expanding an overlay failed.");
  if (!expanded) return adjacency;
  adjacency.natives.assign(expanded->edges.begin(), expanded->edges.end());
  // The listed edge overlays are only candidates: a stale one (an aborted or not-yet-visible delete) is not hidden.
  auto const *const transaction = txn().diff->GetTransaction();
  for (auto *const candidate : BaseEdgeDeleteCandidates(overlay, direction)) {
    if (!overlay::IsDeleted(transaction, candidate, view)) continue;
    auto const base_edge = EdgeOverlayIndex::BaseOf(candidate);
    DMG_ASSERT(base_edge.has_value(), "a listed edge overlay is created with its base edge.");
    if (base_edge) adjacency.hidden.Add(*base_edge);
  }
  adjacency.hidden.Seal();
  return adjacency;
}

std::vector<storage::EdgeAccessor> BranchContext::ResolveEdges(storage::Gid vertex_gid,
                                                               storage::EdgeDirection direction, storage::View view,
                                                               bool *vertex_deleted,
                                                               const storage::VertexAccessor *base_hint) {
  auto &t = txn();
  if (vertex_deleted != nullptr) *vertex_deleted = false;
  bool const out = direction == storage::EdgeDirection::OUT;
  std::vector<storage::EdgeAccessor> result;
  auto const expand = [&](const storage::VertexAccessor &vertex, storage::View expand_view) {
    auto expanded = out ? vertex.OutEdges(expand_view) : vertex.InEdges(expand_view);
    DMG_ASSERT(expanded.has_value(), "expanding a resolved vertex failed.");
    return expanded;
  };

  if (!IsBaseCandidateVertex(vertex_gid)) {
    if (auto native = t.diff->FindVertex(vertex_gid, view)) {
      if (auto expanded = expand(*native, view)) result.assign(expanded->edges.begin(), expanded->edges.end());
    }
    return result;
  }

  // An overlay can only exist after its vertex's branched bit is set.
  if (base_hint != nullptr && base_hint->storage_ != &diff_engine() && !base_hint->vertex_->branched()) {
    DMG_ASSERT(overlays().FindOverlay(vertex_gid) == nullptr,
               "an overlay exists for a vertex with a clear branched bit");
    DMG_ASSERT(base_hint->transaction_ == t.historical->GetTransaction(), "the base hint is not of the historical txn");
    if (auto expanded = expand(*base_hint, storage::View::OLD))
      result.assign(expanded->edges.begin(), expanded->edges.end());
    return result;
  }

  // No native edge hangs on a branch-deleted base vertex: its creation conflicts with the delete on the overlay.
  auto const found = overlays().Find(vertex_gid);
  HiddenBaseEdges hidden;
  if (found.overlay != nullptr) {
    if (core_->MayHaveVertexDeleted(vertex_gid) && OverlayDeleted(found.overlay, view)) {
      if (vertex_deleted != nullptr) *vertex_deleted = true;
      return result;
    }
    auto adjacency = ReadOverlayAdjacency(found.overlay, direction, view);
    result = std::move(adjacency.natives);
    hidden = std::move(adjacency.hidden);
  }

  auto const hist_vertex =
      found.base_vertex != nullptr ? BaseAccessorByPtr(found.base_vertex) : BaseAccessorByGid(vertex_gid);
  if (hist_vertex) {
    if (auto expanded = expand(*hist_vertex, storage::View::OLD)) {
      result.reserve(result.size() + expanded->edges.size());
      for (auto const &edge : expanded->edges) {
        if (hidden.empty() || !hidden.Contains(edge.edge_)) result.push_back(edge);
      }
    }
  }
  return result;
}

BranchContext::UnionVerticesIterable BranchContext::Vertices(storage::View view) {
  auto &t = txn();
  return {t.historical->Vertices(storage::View::OLD), t.diff->Vertices(view), this, view};
}

BranchContext::UnionVerticesIterable::UnionVerticesIterable(storage::VerticesIterable hist_vertices,
                                                            storage::VerticesIterable diff_vertices, BranchContext *ctx,
                                                            storage::View view)
    : hist_vertices_(std::move(hist_vertices)), diff_vertices_(std::move(diff_vertices)), ctx_(ctx), view_(view) {}

BranchContext::UnionVerticesIterable::Iterator BranchContext::UnionVerticesIterable::begin() {
  return {hist_vertices_.begin(), hist_vertices_.end(), diff_vertices_.begin(), diff_vertices_.end(), ctx_, view_};
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

void BranchContext::UnionVerticesIterable::Iterator::SeekNext() {
  while (hist_it_.has_value() && !(*hist_it_ == *hist_end_)) {
    const storage::VertexAccessor base_vertex = **hist_it_;
    ++(*hist_it_);
    if (ctx_->IsBaseVertexDeleted(base_vertex, view_)) continue;
    current_ = base_vertex;
    done_ = false;
    return;
  }

  if (diff_it_.has_value() && !(*diff_it_ == *diff_end_)) {
    current_ = **diff_it_;
    DMG_ASSERT(!ctx_->IsOverlay(*current_), "the diff engine's vertices_ holds only native vertices.");
    ++(*diff_it_);
    done_ = false;
    return;
  }

  current_.reset();
  done_ = true;
}

}  // namespace memgraph::versioning
