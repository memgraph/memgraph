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
#include <tuple>
#include <utility>
#include <vector>

#include "memory/db_arena.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/atomic_memory_block.hpp"
#include "utils/logging.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"
#include "versioning/reserved_guard_mapper.hpp"
#include "versioning/stub_merge.hpp"
#include "versioning/stub_state.hpp"

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

// Why main would not open the fork point, in the words of the caller's failure (`what`).
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

std::expected<std::shared_ptr<BranchCore>, BranchCore::BuildError> BranchCore::Build(
    storage::InMemoryStorage &main, uint64_t fork_ts, std::optional<RecordedFloors> recorded) {
  // Attribute the diff engine's allocations to main's tenant arena pool.
  const memory::DbArenaScope db_arena_scope{main.DbArenaPool()};

  // Pin main at fork_ts first so an unpinned fork fails before the diff engine is built. This accessor's READ guard
  // also blocks a concurrent CREATE ENUM while the enum store is mirrored below.
  auto hist_exp = main.HistoricalAccess(fork_ts);
  if (!hist_exp.has_value()) {
    return std::unexpected(
        BuildError{.message = HistoricalOpenFailure(hist_exp.error(), fork_ts, "Cannot open the branch")});
  }
  std::unique_ptr<storage::Storage::Accessor> historical = std::move(*hist_exp);

  // The diff engine's mapper wraps main's (historical vertices carry main's ids; a separate mapper would misdecode
  // them).
  // Config is copied wholesale so future salient flags are inherited.
  storage::Config config = main.config_;
  // main.config_ keeps the mode main started in; SET STORAGE MODE changes only the live one. A stale analytical
  // mode would build a diff engine without MVCC deltas.
  config.salient.storage_mode = main.GetStorageMode();
  // Fresh UUID avoids aliasing main in any registry. The NAME is main's too, read live from main below, because
  // DbAccessor::DatabaseName() (auth, FGA, SHOW, DUMP) reads it and main can be renamed.
  config.salient.uuid = utils::UUID{};
  // The diff engine's indexes mirror main's at the fork. An auto-created one would be built on the diff engine only,
  // under read-only access that blocks branch writers, and its metadata delta is not a delta a branch WAL can replay.
  config.salient.items.enable_label_index_auto_creation = false;
  config.salient.items.enable_edge_type_index_auto_creation = false;
  // Schema tracking would run on every branch commit, but SHOW SCHEMA INFO reads main's tracker, never the diff
  // engine's.
  config.salient.items.enable_schema_info = false;
  // Fork-edge stubs are heavy Edge objects, so the diff engine keeps properties on edges whatever main has.
  config.salient.items.properties_on_edges = true;
  // Branch commits keep the stock full commit lock; narrowing is left out of the diff engine until it is benched.
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
  config.durability.restore_replication_state_on_startup = false;
  // Without durability, a stale path is one flip away from aliasing main's live data, so it is unique per build.
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
  auto diff_engine =
      std::make_unique<storage::InMemoryStorage>(config,
                                                 std::nullopt,
                                                 std::make_unique<storage::PlanInvalidatorDefault>(),
                                                 metric_handles,
                                                 nullptr,
                                                 main.DbArenaPool(),
                                                 main.DbEmbeddingMemoryTracker(),
                                                 std::make_shared<ReservedGuardMapper>(main.GetSharedNameIdMapper()));
  diff_engine->ReportNameOf(main);

  // Done before the core is visible to any session, so the diff engine never needs UNIQUE access afterwards.
  MirrorMainEnumsIntoDiffEngine(*diff_engine, historical->GetEnumStoreShared());

  // After the fork pin: every gid visible at fork_ts is below these floors. Gids below them may also be objects main
  // created after fork_ts but before this read, which fork_ts does not see. Every allocation after the read, main's or
  // any branch's, is at or above them.
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
      diff_engine_(std::move(diff_engine)) {}

// The diff engine's deltas and indexes point at stubs, so it goes before the stub indexes (declared after it, hence
// destroyed before the teardown scope but after this reset).
BranchCore::~BranchCore() {
  teardown_scope_.emplace(main_->DbArenaPool());
  diff_engine_.reset();
}

void BranchCore::RecordVertexDeleted(storage::Gid gid) {
  change_filters_.RecordVertexDeleted(gid);
  // OOM-blocked: absl sets are not exception-safe, so a refused growth must not interrupt the insert.
  utils::AtomicMemoryBlock([&] {
    std::lock_guard lock(touched_mutex_);
    if (deleted_exact_.insert(gid.AsUint()).second) ++touched_version_;
  });
}

void BranchCore::RecordLabelTouched(storage::LabelId label, storage::Gid gid) {
  // Before the set insert and the commit: readers that see the commit see the bit.
  label_touched_gate_[GateWord(label.AsUint())].fetch_or(GateBit(label.AsUint()), std::memory_order_release);
  utils::AtomicMemoryBlock([&] {
    std::lock_guard lock(touched_mutex_);
    if (label_touched_[label].insert(gid.AsUint()).second) ++touched_version_;
  });
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

void BranchCore::RecordPropertyTouched(storage::PropertyId pid, storage::Gid gid) {
  property_touched_gate_[GateWord(pid.AsUint())].fetch_or(GateBit(pid.AsUint()), std::memory_order_release);
  utils::AtomicMemoryBlock([&] {
    std::lock_guard lock(touched_mutex_);
    if (property_touched_[pid].insert(gid.AsUint()).second) ++touched_version_;
  });
}

std::shared_ptr<const BranchCore::LabelTouchedSet> BranchCore::PropertyTouchedSnapshot(storage::PropertyId pid) {
  std::lock_guard lock(touched_mutex_);
  auto &cached = property_snapshots_[pid];
  if (cached.set && cached.version == touched_version_) return cached.set;
  auto merged = std::make_shared<LabelTouchedSet>();
  if (auto const it = property_touched_.find(pid); it != property_touched_.end()) *merged = it->second;
  cached = CachedLabelSnapshot{touched_version_, std::move(merged)};
  return cached.set;
}

void BranchCore::RecordEdgePropertyTouched(storage::PropertyId pid, storage::Gid edge_gid, storage::Gid from_gid) {
  utils::AtomicMemoryBlock([&] {
    std::lock_guard lock(touched_mutex_);
    if (edge_property_touched_[pid].try_emplace(edge_gid.AsUint(), from_gid.AsUint()).second) ++touched_version_;
  });
}

std::shared_ptr<const BranchCore::EdgeTouchedMap> BranchCore::EdgePropertyTouchedSnapshot(storage::PropertyId pid) {
  std::lock_guard lock(touched_mutex_);
  auto &cached = edge_property_snapshots_[pid];
  if (cached.map && cached.version == touched_version_) return cached.map;
  auto copy = std::make_shared<EdgeTouchedMap>();
  if (auto const it = edge_property_touched_.find(pid); it != edge_property_touched_.end()) *copy = it->second;
  cached = CachedEdgeSnapshot{touched_version_, std::move(copy)};
  return cached.map;
}

std::expected<std::unique_ptr<BranchContext>, BranchContext::BuildError> BranchContext::BuildFromFork(
    storage::InMemoryStorage &main, uint64_t fork_ts) {
  return BranchCore::Build(main, fork_ts).transform([](std::shared_ptr<BranchCore> core) {
    return std::make_unique<BranchContext>(std::move(core));
  });
}

std::expected<void, BranchContext::BuildError> BranchContext::OpenHistorical() {
  EndTransaction();
  return main()
      .HistoricalAccess(fork_ts())
      .transform([this](std::unique_ptr<storage::Storage::Accessor> historical) {
        txn_.emplace(Txn{.historical = std::move(historical)});
      })
      .transform_error([this](storage::InMemoryStorage::HistoricalAccessError error) {
        return BuildError{.message = HistoricalOpenFailure(error, fork_ts(), "The branch is no longer available")};
      });
}

void BranchContext::SetCurrentDiffTxn(storage::Storage::Accessor *diff_txn) {
  MG_ASSERT(diff_txn != nullptr, "BranchContext::SetCurrentDiffTxn: the diff accessor is null.");
  MG_ASSERT(txn_ && txn_->diff == nullptr,
            "BranchContext::SetCurrentDiffTxn: OpenHistorical must run first, once per transaction.");
  txn_->diff = diff_txn;
}

// ---- Stubs

storage::VertexAccessor BranchContext::StubAccessor(storage::Vertex *stub) {
  return storage::VertexAccessor{stub, &diff_engine(), txn().diff->GetTransaction()};
}

std::optional<storage::VertexAccessor> BranchContext::ReadStub(storage::Gid gid) {
  if (!IsForkCandidateVertex(gid)) return std::nullopt;
  auto &t = txn();
  auto *stub = stubs().FindStub(gid.AsUint(), t.stub_cursor);
  if (stub == nullptr) return std::nullopt;
  return StubAccessor(stub);
}

bool BranchContext::StubDeleted(storage::Vertex *stub, storage::View view) {
  auto deleted = stub::IsDeleted(StubAccessor(stub), view);
  DMG_ASSERT(deleted.has_value(), "a stub is neither created nor deleted by a delta, so a read of it cannot fail.");
  return deleted.value_or(false);
}

template <typename MayDiffer>
std::optional<storage::VertexAccessor> BranchContext::StubToRead(const storage::VertexAccessor &vertex,
                                                                 MayDiffer &&may_differ) {
  if (vertex.storage_ == &diff_engine() || !vertex.vertex_->branched()) return std::nullopt;
  auto const gid = vertex.Gid();
  if (!may_differ(gid) && !(core_->MayHaveAnyVertexDeleted() && core_->MayHaveVertexDeleted(gid))) return std::nullopt;
  return ReadStub(gid);
}

std::optional<storage::EdgeAccessor> BranchContext::FindEdgeStub(const storage::EdgeAccessor &fork_edge) {
  auto *stub = edge_stubs().Find(fork_edge.Gid().AsUint());
  if (stub == nullptr) return std::nullopt;
  // The endpoints are main's fork vertices: the fork pin keeps them alive for the branch's life, and an edge stub is
  // never linked into an adjacency list, so no endpoint stub is needed.
  return storage::EdgeAccessor{storage::EdgeRef{stub},
                               fork_edge.EdgeType(),
                               fork_edge.from_vertex_,
                               fork_edge.to_vertex_,
                               &diff_engine(),
                               txn().diff->GetTransaction()};
}

bool BranchContext::HasVertexStub(storage::Gid gid) {
  return IsForkCandidateVertex(gid) && stubs().FindStub(gid.AsUint(), txn().stub_cursor) != nullptr;
}

// ---- Lookups

std::optional<storage::VertexAccessor> BranchContext::ForkAccessor(storage::Vertex *fork, storage::Gid gid) {
  auto &hist = historical();
  // Same construction as InMemoryAccessor::FindVertex on hist (storage_ is main, see HistoricalAccess).
  if (fork != nullptr) return storage::VertexAccessor::Create(fork, &main(), hist.GetTransaction(), storage::View::OLD);
  return hist.FindVertex(gid, storage::View::OLD);
}

std::optional<storage::VertexAccessor> BranchContext::FindForkVertex(storage::Gid gid) {
  if (!IsForkCandidateVertex(gid)) return std::nullopt;
  return ForkAccessor(stubs().FindFork(gid.AsUint(), txn().stub_cursor), gid);
}

std::optional<storage::EdgeAccessor> BranchContext::FindForkEdge(storage::Gid edge_gid, storage::Gid from_gid) {
  if (!IsForkCandidateEdge(edge_gid) || !IsForkCandidateVertex(from_gid)) return std::nullopt;
  // Walked through the fork vertex's fork-state adjacency: a lookup by gid on main scans main's LIVE adjacency, which
  // has lost an edge main deleted since the fork, and holds no edge objects at all without properties_on_edges.
  auto const from = FindForkVertex(from_gid);
  if (!from) return std::nullopt;
  auto out = from->OutEdges(storage::View::OLD);
  if (!out) return std::nullopt;
  auto const found = std::ranges::find_if(out->edges, [&](const auto &edge) { return edge.Gid() == edge_gid; });
  if (found == out->edges.end()) return std::nullopt;
  return *found;
}

std::optional<storage::VertexAccessor> BranchContext::FindStub(storage::Gid gid, storage::View view) {
  auto &t = txn();
  if (!IsForkCandidateVertex(gid)) return t.diff->FindVertex(gid, view);
  auto *stub = stubs().FindStub(gid.AsUint(), t.stub_cursor);
  if (stub == nullptr) return std::nullopt;
  // Same construction as InMemoryAccessor::FindVertex on the diff txn (storage_ is the diff engine).
  return storage::VertexAccessor::Create(stub, &diff_engine(), t.diff->GetTransaction(), view);
}

std::optional<storage::VertexAccessor> BranchContext::ResolveVertex(storage::Gid gid, storage::View view) {
  auto &t = txn();
  if (!IsForkCandidateVertex(gid)) {
    auto native = t.diff->FindVertex(gid, view);
    DMG_ASSERT(!native || !IsStub(*native), "a native gid resolved to a stub.");
    return native;
  }
  auto const found = stubs().Find(gid.AsUint(), t.stub_cursor);
  if (found.stub != nullptr && core_->MayHaveVertexDeleted(gid) && StubDeleted(found.stub, view)) return std::nullopt;
  return ForkAccessor(found.fork, gid);
}

std::optional<storage::EdgeAccessor> BranchContext::ResolveEdge(storage::Gid edge_gid, storage::Gid from_gid,
                                                                storage::View view) {
  if (!IsForkCandidateEdge(edge_gid)) return FindDiffEdge(edge_gid, from_gid, view);
  auto edge = FindForkEdge(edge_gid, from_gid);
  if (!edge || IsForkEdgeHidden(*edge, view)) return std::nullopt;
  return edge;
}

std::optional<storage::EdgeAccessor> BranchContext::FindEdgeInAnchors(storage::Gid edge_gid, storage::View view) {
  // The engine's scan covers vertices_ only; an edge whose FROM vertex is a stub is found through the stubs.
  std::optional<storage::EdgeAccessor> found;
  stubs().ForEachStub([&](storage::Vertex *stub) {
    found = diff().FindEdgeFrom(edge_gid, StubAccessor(stub), view);
    return !found;
  });
  return found;
}

std::optional<storage::EdgeAccessor> BranchContext::FindDiffEdge(storage::Gid edge_gid, storage::View view) {
  // A fork edge is main's, and an edge stub is neither in edges_ nor in any adjacency: nothing to find here.
  if (IsForkCandidateEdge(edge_gid)) return std::nullopt;
  auto found = diff().FindEdge(edge_gid, view).or_else([&] { return FindEdgeInAnchors(edge_gid, view); });
  DMG_ASSERT(!found || !IsStub(*found), "FindDiffEdge resolved to an edge stub.");
  return found;
}

std::optional<storage::EdgeAccessor> BranchContext::FindDiffEdge(storage::Gid edge_gid, storage::Gid from_gid,
                                                                 storage::View view) {
  if (IsForkCandidateEdge(edge_gid)) return std::nullopt;
  auto &acc = diff();
  if (!IsForkCandidateVertex(from_gid)) return acc.FindEdge(edge_gid, from_gid, view);
  // A native edge off a fork vertex hangs on its stub; the stock lookup by FROM gid would throw without one.
  auto const stub = ReadStub(from_gid);
  return stub ? acc.FindEdgeFrom(edge_gid, *stub, view) : std::nullopt;
}

// ---- Deletion

bool BranchContext::IsVertexDeleted(storage::Gid gid, storage::View view) {
  if (!IsForkCandidateVertex(gid) || !core_->MayHaveVertexDeleted(gid)) return false;  // recorded before the write
  auto *stub = stubs().FindStub(gid.AsUint(), txn().stub_cursor);
  return stub != nullptr && StubDeleted(stub, view);
}

// A delete sets the main object's branched bit before RecordVertexDeleted publishes the any-deleted flag (release), so
// a reader that acquires the flag also sees the bit; the bit itself is a relaxed read.
bool BranchContext::IsForkVertexDeleted(const storage::VertexAccessor &fork_vertex, storage::View view) {
  if (!core_->MayHaveAnyVertexDeleted() || !fork_vertex.vertex_->branched()) return false;
  return IsVertexDeleted(fork_vertex.Gid(), view);
}

bool BranchContext::IsForkEdgeDeleted(const storage::EdgeAccessor &fork_edge, storage::View view) {
  if (!core_->MayHaveForkEdgeDeleted(fork_edge.Gid())) return false;
  auto const stub = FindEdgeStub(fork_edge);
  if (!stub) return false;
  auto deleted = stub::IsEdgeDeleted(*stub, view);
  DMG_ASSERT(deleted.has_value(),
             "an edge stub is neither created nor deleted by a delta, so a read of it cannot fail.");
  return deleted.value_or(false);
}

bool BranchContext::IsForkEdgeHidden(const storage::EdgeAccessor &fork_edge, storage::View view) {
  if (!IsForkCandidateEdge(fork_edge.Gid())) return false;
  return IsForkEdgeHiddenWith(fork_edge, view, true, std::nullopt);
}

bool BranchContext::IsForkEdgeHiddenWith(const storage::EdgeAccessor &fork_edge, storage::View view,
                                         bool edge_may_be_deleted, std::optional<storage::Gid> live_endpoint) {
  if (edge_may_be_deleted && IsForkEdgeDeleted(fork_edge, view)) return true;
  if (!core_->MayHaveAnyVertexDeleted()) return false;
  auto const from = fork_edge.FromVertex();
  auto const to = fork_edge.ToVertex();
  if (from.Gid() != live_endpoint && IsForkVertexDeleted(from, view)) return true;
  return to.Gid() != from.Gid() && to.Gid() != live_endpoint && IsForkVertexDeleted(to, view);
}

// ---- Vertex reads

namespace {

// The label set of a fork vertex as the branch sees it, given its stub.
storage::Result<storage::VertexKey> MergedLabels(const storage::VertexAccessor &fork,
                                                 const storage::VertexAccessor &stub, storage::View view) {
  auto stub_labels = stub.Labels(view);
  if (!stub_labels) return std::unexpected(stub_labels.error());
  if (std::ranges::contains(*stub_labels, stub::kDeleteLabel)) return std::unexpected(storage::Error::DELETED_OBJECT);
  return fork.Labels(storage::View::OLD).transform([&](storage::VertexKey fork_labels) {
    return stub::MergeLabels(*stub_labels, std::move(fork_labels));
  });
}

// DELETED_OBJECT when the branch deleted the stub's vertex; the probe is only worth making when the delete filter
// admits `gid`.
std::optional<storage::Error> DeletedOnBranch(const BranchContext &ctx, const storage::VertexAccessor &stub,
                                              storage::View view) {
  if (!ctx.MayHaveVertexDeleted(stub.Gid())) return std::nullopt;
  auto deleted = stub::IsDeleted(stub, view);
  if (!deleted) return deleted.error();
  if (*deleted) return storage::Error::DELETED_OBJECT;
  return std::nullopt;
}

}  // namespace

storage::Result<storage::VertexKey> BranchContext::Labels(const storage::VertexAccessor &vertex, storage::View view) {
  auto const stub = StubToRead(vertex, [&](storage::Gid gid) { return core_->MayHaveLabelChange(gid); });
  if (!stub) return vertex.Labels(view);
  return MergedLabels(vertex, *stub, view);
}

storage::Result<bool> BranchContext::HasLabel(const storage::VertexAccessor &vertex, storage::LabelId label,
                                              storage::View view) {
  auto const stub = StubToRead(
      vertex, [&](storage::Gid gid) { return core_->MayHaveLabelTouched(label) && core_->MayHaveLabelChange(gid); });
  if (!stub) return vertex.HasLabel(label, view);
  if (auto const error = DeletedOnBranch(*this, *stub, view)) return std::unexpected(*error);
  auto const decided = stub::DecidesLabel(*stub, label, view);
  if (!decided) return std::unexpected(decided.error());
  if (*decided) return **decided;
  return vertex.HasLabel(label, storage::View::OLD);
}

storage::Result<bool> BranchContext::HasAnyLabel(const storage::VertexAccessor &vertex, storage::View view) {
  auto const stub = StubToRead(vertex, [&](storage::Gid gid) { return core_->MayHaveLabelChange(gid); });
  if (!stub) return vertex.HasAnyLabel(view);
  return MergedLabels(vertex, *stub, view).transform([](const storage::VertexKey &labels) { return !labels.empty(); });
}

storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> BranchContext::Properties(
    const storage::VertexAccessor &vertex, storage::View view) {
  auto const stub = StubToRead(vertex, [&](storage::Gid gid) { return core_->MayHavePropertyChange(gid); });
  if (!stub) return vertex.Properties(view);
  if (auto const error = DeletedOnBranch(*this, *stub, view)) return std::unexpected(*error);
  auto stub_properties = stub->Properties(view);
  if (!stub_properties) return std::unexpected(stub_properties.error());
  return stub::MergeProperties(*stub_properties, vertex);
}

storage::Result<storage::PropertyValue> BranchContext::GetProperty(const storage::VertexAccessor &vertex,
                                                                   storage::PropertyId key, storage::View view) {
  auto const stub = StubToRead(vertex, [&](storage::Gid gid) { return core_->MayHavePropertyFieldChange(gid, key); });
  if (!stub) return vertex.GetProperty(key, view);
  if (auto const error = DeletedOnBranch(*this, *stub, view)) return std::unexpected(*error);
  return stub::MergeGetProperty(*stub, vertex, key, view);
}

storage::Result<uint64_t> BranchContext::GetPropertySize(const storage::VertexAccessor &vertex, storage::PropertyId key,
                                                         storage::View view) {
  auto const stub = StubToRead(vertex, [&](storage::Gid gid) { return core_->MayHavePropertyFieldChange(gid, key); });
  if (!stub) return vertex.GetPropertySize(key, view);
  if (auto const error = DeletedOnBranch(*this, *stub, view)) return std::unexpected(*error);
  return stub::MergeGetPropertySize(*stub, vertex, key, view);
}

// ---- Edge reads

// A clear branched bit proves the branch never wrote the fork edge, but a deleted endpoint hides it regardless, so the
// bit short-circuits only while the branch has deleted no vertex.
BranchContext::EdgeRead BranchContext::ResolveEdgeRead(const storage::EdgeAccessor &edge,
                                                       std::optional<storage::PropertyId> field, storage::View view) {
  if (edge.storage_ == &diff_engine()) return {};
  bool const hint = edge.MaybeBranchedHint();
  if (!hint && !core_->MayHaveAnyVertexDeleted()) return {};
  if (IsForkEdgeHidden(edge, view)) return {.kind = EdgeRead::Kind::kHidden};
  if (!hint) return {};
  auto const gid = edge.Gid();
  bool const may_have =
      field ? core_->MayHaveForkEdgePropertyFieldChange(gid, *field) : core_->MayHaveForkEdgePropertyChange(gid);
  if (!may_have) return {};
  if (auto stub = FindEdgeStub(edge)) return {.kind = EdgeRead::Kind::kStub, .stub = std::move(stub)};
  return {};
}

storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> BranchContext::Properties(
    const storage::EdgeAccessor &edge, storage::View view) {
  auto const read = ResolveEdgeRead(edge, std::nullopt, view);
  if (read.kind == EdgeRead::Kind::kHidden) return std::unexpected(storage::Error::DELETED_OBJECT);
  if (read.kind == EdgeRead::Kind::kDirect) return edge.Properties(view);
  auto stub_properties = read.stub->Properties(view);
  if (!stub_properties) return std::unexpected(stub_properties.error());
  return stub::MergeProperties(*stub_properties, edge);
}

storage::Result<storage::PropertyValue> BranchContext::GetProperty(const storage::EdgeAccessor &edge,
                                                                   storage::PropertyId key, storage::View view) {
  auto const read = ResolveEdgeRead(edge, key, view);
  if (read.kind == EdgeRead::Kind::kHidden) return std::unexpected(storage::Error::DELETED_OBJECT);
  if (read.kind == EdgeRead::Kind::kDirect) return edge.GetProperty(key, view);
  return stub::MergeGetProperty(*read.stub, edge, key, view);
}

storage::Result<uint64_t> BranchContext::GetPropertySize(const storage::EdgeAccessor &edge, storage::PropertyId key,
                                                         storage::View view) {
  auto const read = ResolveEdgeRead(edge, key, view);
  if (read.kind == EdgeRead::Kind::kHidden) return std::unexpected(storage::Error::DELETED_OBJECT);
  if (read.kind == EdgeRead::Kind::kDirect) return edge.GetPropertySize(key, view);
  return stub::MergeGetPropertySize(*read.stub, edge, key, view);
}

// ---- Vertex writes

namespace {

using PropertyMap = std::map<storage::PropertyId, storage::PropertyValue>;
using PropertyChange = std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>;

// Main's `branched()` bit is a monotonic hint, so marking a vertex the branch then does not change is safe.
void MarkBranched(storage::Vertex *main_vertex) {
  if (main_vertex->branched()) return;
  auto guard = std::unique_lock{main_vertex->lock};
  main_vertex->SetBranched(true);
}

// A write of several stub keys is not atomic. Unless Done() is reached, whether by an error return or an exception,
// the stub may be half-written inside the diff transaction, so the transaction is poisoned and must be aborted.
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

storage::VertexAccessor BranchContext::MaterializeStub(const storage::VertexAccessor &fork) {
  // Before the stub exists, and so before any write of it (reads gate on this bit).
  MarkBranched(fork.vertex_);
  // The stub index keeps main's Vertex* for the fork vertex. It stays valid because main's GC horizon is clamped to the
  // oldest fork pin (oldest_fork_pin_), so a vertex visible at fork_ts is never freed while the branch lives.
  return StubAccessor(stubs().GetOrCreateStub(fork.Gid().AsUint(), fork.vertex_));
}

storage::Result<storage::VertexAccessor> BranchContext::StubForWrite(const storage::VertexAccessor &fork) {
  DMG_ASSERT(!IsNative(fork), "a native vertex has no stub.");
  if (IsVertexDeleted(fork.Gid(), storage::View::NEW)) return std::unexpected(storage::Error::DELETED_OBJECT);
  return MaterializeStub(fork);
}

std::expected<storage::VertexAccessor, BranchContext::BranchWriteError> BranchContext::EnsureDiffVertex(
    storage::Gid gid) {
  if (!IsForkCandidateVertex(gid)) {
    // A native vertex the caller resolved and that is not visible now was deleted on the branch.
    if (auto native = txn().diff->FindVertex(gid, storage::View::NEW)) return *native;
    return std::unexpected(BranchWriteError{DeletedMessage(gid)});
  }
  if (IsVertexDeleted(gid, storage::View::NEW)) return std::unexpected(BranchWriteError{DeletedMessage(gid)});
  auto fork = FindForkVertex(gid);
  if (!fork) {
    return std::unexpected(
        BranchWriteError{fmt::format("Cannot modify vertex {}: it is not in the branch's fork state -- this should be "
                                     "unreachable (callers resolve a vertex before mutating it).",
                                     gid.AsUint())});
  }
  return MaterializeStub(*fork);
}

storage::VertexAccessor BranchContext::CreateNativeVertex() {
  auto const gid = storage::Gid::FromUint(main().vertex_id_.fetch_add(1, std::memory_order_acq_rel));
  auto created = static_cast<storage::ReplicationAccessor &>(diff()).CreateVertexEx(gid);
  MG_ASSERT(created.has_value(),
            "BranchContext::CreateNativeVertex: gid {} already exists in the diff engine.",
            gid.AsUint());
  return *created;
}

bool BranchContext::TouchStub(const storage::VertexAccessor &stub) {
  auto guard = std::shared_lock{stub.vertex_->lock};
  return storage::PrepareForWrite(txn().diff->GetTransaction(), stub.vertex_);
}

bool BranchContext::CanDeleteAnchor(const storage::VertexAccessor &stub) {
  auto *const transaction = txn().diff->GetTransaction();
  auto guard = std::shared_lock{stub.vertex_->lock};
  // The marker just written is the head, so the head cannot conflict; non-sequential edge deltas can sit under it, a
  // committed older head over a delta this snapshot cannot see. The chain is intact while this transaction is active.
  for (const storage::Delta *delta = stub.vertex_->delta(); delta != nullptr;
       delta = delta->next.load(std::memory_order_acquire)) {
    auto const ts = delta->commit_info->timestamp.load(std::memory_order_acquire);
    if (ts != transaction->transaction_id && ts >= transaction->start_timestamp) {
      transaction->has_serialization_error = true;
      return false;
    }
  }
  return true;
}

void BranchContext::RecordPropertyWrite(storage::Gid gid, storage::PropertyId key) {
  core_->RecordPropertyFieldChange(gid, key);
  core_->RecordPropertyTouched(key, gid);
}

storage::Result<bool> BranchContext::ToggleLabel(storage::VertexAccessor &vertex, storage::LabelId label, bool add) {
  if (IsNative(vertex)) {
    DMG_ASSERT(!IsStub(vertex), "a stub reached a branch write.");
    return add ? vertex.AddLabel(label) : vertex.RemoveLabel(label);
  }
  auto stub = StubForWrite(vertex);
  if (!stub) return std::unexpected(stub.error());
  auto const in_fork = vertex.HasLabel(label, storage::View::OLD);
  if (!in_fork) return std::unexpected(in_fork.error());
  auto const gid = vertex.Gid();
  core_->RecordLabelChange(gid);
  core_->RecordLabelTouched(label, gid);
  // The stub never holds what the fork implies (stub_merge.hpp): a fork label is removed by marking it and added back
  // by dropping its mark, any other label is recorded or dropped as itself.
  auto const written = *in_fork ? stub::Mark(label) : label;
  return add != *in_fork ? stub->AddLabel(written) : stub->RemoveLabel(written);
}

storage::Result<bool> BranchContext::AddLabel(storage::VertexAccessor &vertex, storage::LabelId label) {
  return ToggleLabel(vertex, label, true);
}

storage::Result<bool> BranchContext::RemoveLabel(storage::VertexAccessor &vertex, storage::LabelId label) {
  return ToggleLabel(vertex, label, false);
}

storage::Result<storage::PropertyValue> BranchContext::WriteStubProperty(const storage::VertexAccessor &fork,
                                                                         storage::VertexAccessor &stub,
                                                                         storage::PropertyId key,
                                                                         const storage::PropertyValue &value) {
  auto const own = stub::ReadKey(stub, key, storage::View::NEW);
  if (!own) return std::unexpected(own.error());
  // A removal marker means the fork's value is hidden, so the fork is consulted only when the stub does not decide.
  storage::PropertyValue fork_value;
  if (!own->removed) {
    auto read = fork.GetProperty(key, storage::View::OLD);
    if (!read) return std::unexpected(read.error());
    fork_value = std::move(*read);
  }
  storage::PropertyValue old_value = own->value.IsNull() ? fork_value : own->value;

  PartialWriteGuard guard{txn().diff->GetTransaction()};
  // Per key, the marker goes on before the value is dropped and off after the value is set, so that a READ_COMMITTED
  // reader holding a prefix of this commit sees the old or the new value (stub_merge.hpp).
  if (value.IsNull()) {
    if (!fork_value.IsNull()) {
      if (auto marked = stub.SetProperty(stub::Mark(key), storage::PropertyValue{true}); !marked) {
        return std::unexpected(marked.error());
      }
    }
    if (auto dropped = stub.SetProperty(key, storage::PropertyValue{}); !dropped) {
      return std::unexpected(dropped.error());
    }
  } else {
    if (auto set = stub.SetProperty(key, value); !set) return std::unexpected(set.error());
    if (own->removed) {
      if (auto unmarked = stub.SetProperty(stub::Mark(key), storage::PropertyValue{}); !unmarked) {
        return std::unexpected(unmarked.error());
      }
    }
  }
  guard.Done();
  return old_value;
}

storage::Result<storage::PropertyValue> BranchContext::SetProperty(storage::VertexAccessor &vertex,
                                                                   storage::PropertyId key,
                                                                   const storage::PropertyValue &value) {
  if (IsNative(vertex)) {
    DMG_ASSERT(!IsStub(vertex), "a stub reached a branch write.");
    return vertex.SetProperty(key, value);
  }
  auto stub = StubForWrite(vertex);
  if (!stub) return std::unexpected(stub.error());
  core_->RecordPropertyChange(vertex.Gid());
  RecordPropertyWrite(vertex.Gid(), key);
  return WriteStubProperty(vertex, *stub, key, value);
}

storage::Result<bool> BranchContext::InitProperties(storage::VertexAccessor &vertex, PropertyMap &properties) {
  if (IsNative(vertex)) {
    DMG_ASSERT(!IsStub(vertex), "a stub reached a branch write.");
    return vertex.InitProperties(properties);
  }
  auto stub = StubForWrite(vertex);
  if (!stub) return std::unexpected(stub.error());
  auto existing = Properties(vertex, storage::View::NEW);
  if (!existing) return std::unexpected(existing.error());
  if (!existing->empty()) {
    if (!TouchStub(*stub)) return std::unexpected(storage::Error::SERIALIZATION_ERROR);
    return false;
  }
  auto const gid = vertex.Gid();
  core_->RecordPropertyChange(gid);
  for (auto const &entry : properties) RecordPropertyWrite(gid, entry.first);
  PartialWriteGuard guard{txn().diff->GetTransaction()};
  for (auto const &[key, value] : properties) {
    if (auto written = WriteStubProperty(vertex, *stub, key, value); !written) return std::unexpected(written.error());
  }
  guard.Done();
  return true;
}

storage::Result<std::vector<PropertyChange>> BranchContext::UpdateProperties(storage::VertexAccessor &vertex,
                                                                             PropertyMap &properties) {
  if (IsNative(vertex)) {
    DMG_ASSERT(!IsStub(vertex), "a stub reached a branch write.");
    return vertex.UpdateProperties(properties);
  }
  auto stub = StubForWrite(vertex);
  if (!stub) return std::unexpected(stub.error());
  auto existing = Properties(vertex, storage::View::NEW);
  if (!existing) return std::unexpected(existing.error());
  std::vector<PropertyChange> changes;
  changes.reserve(properties.size());
  for (auto const &[key, value] : properties) {
    auto const old = existing->find(key);
    changes.emplace_back(key, old != existing->end() ? old->second : storage::PropertyValue{}, value);
  }
  auto const gid = vertex.Gid();
  core_->RecordPropertyChange(gid);
  for (auto const &entry : properties) RecordPropertyWrite(gid, entry.first);
  PartialWriteGuard guard{txn().diff->GetTransaction()};
  for (auto const &[key, value] : properties) {
    if (auto written = WriteStubProperty(vertex, *stub, key, value); !written) return std::unexpected(written.error());
  }
  guard.Done();
  // As storage's UpdateProperties: the properties the update left alone come back in the caller's map.
  for (auto &[key, old_value] : *existing) properties.emplace(key, std::move(old_value));
  return changes;
}

storage::Result<PropertyMap> BranchContext::ClearProperties(storage::VertexAccessor &vertex) {
  if (IsNative(vertex)) {
    DMG_ASSERT(!IsStub(vertex), "a stub reached a branch write.");
    return vertex.ClearProperties();
  }
  auto stub = StubForWrite(vertex);
  if (!stub) return std::unexpected(stub.error());
  auto existing = Properties(vertex, storage::View::NEW);
  if (!existing) return std::unexpected(existing.error());
  if (existing->empty()) {
    if (!TouchStub(*stub)) return std::unexpected(storage::Error::SERIALIZATION_ERROR);
    return existing;
  }
  auto const gid = vertex.Gid();
  core_->RecordPropertyChange(gid);
  for (auto const &entry : *existing) RecordPropertyWrite(gid, entry.first);
  PartialWriteGuard guard{txn().diff->GetTransaction()};
  for (auto const &entry : *existing) {
    if (auto removed = WriteStubProperty(vertex, *stub, entry.first, storage::PropertyValue{}); !removed) {
      return std::unexpected(removed.error());
    }
  }
  guard.Done();
  return existing;
}

storage::Result<bool> BranchContext::DeleteForkVertex(storage::Gid gid) {
  if (IsVertexDeleted(gid, storage::View::NEW)) return false;
  auto fork = FindForkVertex(gid);
  if (!fork) return std::unexpected(storage::Error::NONEXISTENT_OBJECT);
  // The branched bit is set before RecordVertexDeleted publishes the any-deleted flag, which in turn comes before the
  // marker is written: every read that can see the marker is gated on the filter.
  auto stub = MaterializeStub(*fork);
  core_->RecordVertexDeleted(gid);
  auto written = stub.AddLabel(stub::kDeleteLabel);
  if (!written) return std::unexpected(written.error());
  if (!CanDeleteAnchor(stub)) return std::unexpected(storage::Error::SERIALIZATION_ERROR);
  return *written;
}

// ---- Adjacency

storage::Result<std::vector<storage::EdgeAccessor>> BranchContext::Edges(const storage::VertexAccessor &vertex,
                                                                         storage::EdgeDirection direction,
                                                                         storage::View view) {
  bool const out = direction == storage::EdgeDirection::OUT;
  if (vertex.storage_ == &diff_engine()) {
    // A native vertex's edges are all native: the vertex says itself whether it is deleted.
    auto native = out ? vertex.OutEdges(view) : vertex.InEdges(view);
    if (!native) return std::unexpected(native.error());
    return std::vector<storage::EdgeAccessor>(native->edges.begin(), native->edges.end());
  }
  bool deleted = false;
  auto edges = ResolveEdges(vertex.Gid(), direction, view, &deleted);
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

// historical_ is a frozen snapshot, so it is read with View::OLD throughout.
std::vector<storage::EdgeAccessor> BranchContext::ResolveEdges(storage::Gid vertex_gid,
                                                               storage::EdgeDirection direction, storage::View view,
                                                               bool *vertex_deleted) {
  auto &t = txn();
  if (vertex_deleted != nullptr) *vertex_deleted = false;
  bool const out = direction == storage::EdgeDirection::OUT;
  std::vector<storage::EdgeAccessor> result;
  // The edge types are left unfiltered: callers filter after charging hops.
  auto const expand = [&](const storage::VertexAccessor &vertex, storage::View expand_view) {
    auto expanded = out ? vertex.OutEdges(expand_view) : vertex.InEdges(expand_view);
    // The accessor was just resolved through the same transaction and view, so it is visible and not deleted.
    DMG_ASSERT(expanded.has_value(), "expanding a resolved vertex failed.");
    return expanded;
  };

  if (!IsForkCandidateVertex(vertex_gid)) {
    if (auto native = t.diff->FindVertex(vertex_gid, view)) {
      if (auto expanded = expand(*native, view)) result.assign(expanded->edges.begin(), expanded->edges.end());
    }
    return result;
  }

  // Diff side first: for a fork vertex it holds only the native edges attached to the vertex's stub. Fork edges come
  // from the historical pass below, in fork identity (reads then merge the branch's changes by gid). A native edge
  // whose other end is a fork vertex the branch deleted cannot exist: the delete conflicts with the edge's creation
  // on that vertex's stub (stock first-writer-wins), and DETACH DELETE removes the vertex's native edges itself.
  auto const found = stubs().Find(vertex_gid.AsUint(), t.stub_cursor);
  if (found.stub != nullptr) {
    if (core_->MayHaveVertexDeleted(vertex_gid) && StubDeleted(found.stub, view)) {
      if (vertex_deleted != nullptr) *vertex_deleted = true;
      return result;
    }
    if (auto expanded = expand(StubAccessor(found.stub), view)) {
      result.assign(expanded->edges.begin(), expanded->edges.end());
    }
  }

  if (auto const hist_vertex = ForkAccessor(found.fork, vertex_gid)) {
    if (auto expanded = expand(*hist_vertex, storage::View::OLD)) {
      // A deleted fork edge flags both endpoints' adjacency, so a clear flag skips the per-edge probes.
      bool const edges_may_be_deleted = core_->MayHaveEdgeChange(vertex_gid);
      if (!edges_may_be_deleted && !core_->MayHaveAnyVertexDeleted()) {
        result.insert(result.end(), expanded->edges.begin(), expanded->edges.end());
        return result;
      }
      result.reserve(result.size() + expanded->edges.size());
      for (auto edge : expanded->edges) {
        if (!IsForkEdgeHiddenWith(edge, view, edges_may_be_deleted, vertex_gid)) result.push_back(edge);
      }
    }
  }
  return result;
}

BranchContext::UnionVerticesIterable BranchContext::Vertices(storage::View view) {
  auto &t = txn();
  return UnionVerticesIterable(t.historical->Vertices(storage::View::OLD), t.diff->Vertices(view), this, view);
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

// Fork vertices are emitted as the fork accessor (reads merge the branch's changes by gid) unless deleted on the
// branch; fork gids are all below the diff engine's, so the diff side follows once the fork side is exhausted.
void BranchContext::UnionVerticesIterable::Iterator::SeekNext() {
  while (hist_it_.has_value() && !(*hist_it_ == *hist_end_)) {
    const storage::VertexAccessor fork_vertex = **hist_it_;
    ++(*hist_it_);
    if (ctx_->IsForkVertexDeleted(fork_vertex, view_)) continue;
    current_ = fork_vertex;
    done_ = false;
    return;
  }

  if (diff_it_.has_value() && !(*diff_it_ == *diff_end_)) {
    current_ = **diff_it_;
    DMG_ASSERT(!ctx_->IsStub(*current_), "the diff engine's vertices_ holds only native vertices.");
    ++(*diff_it_);
    done_ = false;
    return;
  }

  current_.reset();
  done_ = true;
}

}  // namespace memgraph::versioning
