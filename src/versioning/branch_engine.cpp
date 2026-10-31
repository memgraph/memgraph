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
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "memory/db_arena.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/atomic_memory_block.hpp"
#include "utils/atomic_utils.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/scheduler.hpp"
#include "utils/uuid.hpp"
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

// Mirrors main's index definitions (not contents) so the planner picks index scans on the branch. The diff engine's
// indexes hold the branch-native objects and the stubs of changed fork objects.
void MirrorMainIndexDefinitionsIntoDiffEngine(storage::InMemoryStorage &diff_engine,
                                              const storage::IndicesInfo &main_indices) {
  // One accessor per index: CreateIndex downgrades READ_ONLY access to READ.
  auto const mirror = [&diff_engine](auto &&create) {
    auto acc = diff_engine.ReadOnlyAccess();
    auto const created = create(*acc);
    MG_ASSERT(created.has_value(),
              "BranchCore::Build: mirroring one of main's indexes onto the empty diff engine failed.");
    auto protector = diff_engine.make_database_protector();
    MG_ASSERT(protector != nullptr, "BranchCore::Build: no database protector for the diff engine.");
    auto const committed = acc->PrepareForCommitPhase(storage::CommitArgs::make_main(std::move(protector)));
    MG_ASSERT(committed.has_value(), "BranchCore::Build: index-definition mirror commit failed.");
  };
  for (const auto &label : main_indices.label) {
    mirror([&](storage::Storage::Accessor &acc) { return acc.CreateIndex(label); });
  }
  for (const auto &entry : main_indices.label_properties) {
    mirror(
        [&](storage::Storage::Accessor &acc) { return acc.CreateIndex(entry.label, entry.properties, entry.order); });
  }
  for (const auto &property : main_indices.vertex_property) {
    mirror([&](storage::Storage::Accessor &acc) { return acc.CreateGlobalVertexIndex(property); });
  }
  for (const auto &edge_type : main_indices.edge_type) {
    mirror([&](storage::Storage::Accessor &acc) { return acc.CreateIndex(edge_type); });
  }
  for (const auto &[edge_type, property] : main_indices.edge_type_property) {
    mirror([&](storage::Storage::Accessor &acc) { return acc.CreateIndex(edge_type, property); });
  }
  for (const auto &property : main_indices.edge_property) {
    mirror([&](storage::Storage::Accessor &acc) { return acc.CreateGlobalEdgeIndex(property); });
  }
}

}  // namespace

std::expected<std::shared_ptr<BranchCore>, BranchCore::BuildError> BranchCore::Build(
    storage::InMemoryStorage &main, uint64_t fork_ts, std::optional<uint64_t> branch_number,
    std::optional<RecordedFloors> recorded) {
  const memory::DbArenaScope db_arena_scope{main.DbArenaPool()};

  // Pinned first so an unpinned fork fails before the diff engine is built; the accessor's shared hold also keeps
  // CREATE ENUM (UNIQUE) out while the enum store is mirrored below.
  auto hist_exp = main.HistoricalAccess(fork_ts);
  if (!hist_exp.has_value()) {
    return std::unexpected(
        BuildError{.message = HistoricalOpenFailure(hist_exp.error(), fork_ts, "Cannot open the branch")});
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
  bool const durable =
      branch_number.has_value() && main.config_.durability.snapshot_wal_mode ==
                                       storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  config.durability.snapshot_wal_mode = durable
                                            ? storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL
                                            : storage::Config::Durability::SnapshotWalMode::DISABLED;
  // Only CreateSnapshotHandler arms a snapshot runner from this interval, and a diff engine never gets one: no thread
  // snapshots a branch, whose WAL is all its durable state.
  config.durability.snapshot_interval = utils::SchedulerInterval{};
  config.durability.recover_on_startup = false;
  // Main's retention needs are not the diff engine's.
  config.durability.recovery_history_bound.reset();
  config.durability.retention_floor.reset();
  // A durable branch's directory holds its own WAL, which the replay reads after a restart: never moved aside.
  config.durability.keep_existing_files = durable;
  config.durability.snapshot_on_exit = false;
  // A durable branch keeps its WAL in its own directory; otherwise the path is unique per build so the diff engine
  // never aliases main's directory (disk usage, file-retainer locks).
  config.durability.storage_directory =
      durable ? main.config_.durability.storage_directory / "branches" / std::to_string(*branch_number)
              : config.durability.storage_directory / "versioning_diff_engine" / utils::GenerateUUID();
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

  // Done before the core is visible to any session, so the diff engine never needs UNIQUE access afterwards. Mirror
  // commits are rebuilt on every build, so they stay out of the branch WAL.
  const auto main_indices = historical->ListAllIndices();
  {
    const storage::InMemoryStorage::SuppressWalScope no_wal{*diff_engine};
    MirrorMainIndexDefinitionsIntoDiffEngine(*diff_engine, main_indices);
    MirrorMainEnumsIntoDiffEngine(*diff_engine, historical->GetEnumStoreShared());
  }

  // Read after the fork pin: every gid visible at fork_ts is below these floors, every later allocation (main's or a
  // branch's) at or above. Below them may also be main's post-fork objects, which fork_ts does not see.
  auto core = std::shared_ptr<BranchCore>(
      new BranchCore(&main,
                     fork_ts,
                     std::move(diff_engine),
                     recorded ? recorded->vertex_gid_floor : main.vertex_id_.load(std::memory_order_acquire),
                     recorded ? recorded->edge_gid_floor : main.edge_id_.load(std::memory_order_acquire)));
  core->key_parts_ = IndexKeyParts{main_indices.label_properties};
  return core;
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
  const bool light_edges = diff_engine_->config_.salient.items.storage_light_edge;
  diff_engine_.reset();
  // The engine frees the light edges linked from its own vertices; those a native edge hangs on a stub (an edge out of
  // a fork vertex) are linked from the stub, which the engine does not know.
  if (light_edges) {
    stub_index_.ForEachStub([](storage::Vertex *stub) {
      for (const auto &[edge_type, to_vertex, edge_ref] : stub->out_edges) {
        // A marker holds an edge stub, which the EdgeStubIndex owns and frees.
        if (storage::IsReservedId(edge_type)) continue;
        storage::InMemoryStorage::LightEdgePool::Destroy(edge_ref.ptr);
      }
      return true;
    });
  }
}

std::expected<std::unique_ptr<BranchContext>, BranchContext::BuildError> BranchContext::BuildFromFork(
    storage::InMemoryStorage &main, uint64_t fork_ts, std::optional<uint64_t> branch_number) {
  return BranchCore::Build(main, fork_ts, branch_number).transform([](std::shared_ptr<BranchCore> core) {
    return std::make_unique<BranchContext>(std::move(core));
  });
}

std::expected<void, BranchContext::BuildError> BranchContext::OpenHistorical(
    std::optional<std::chrono::milliseconds> timeout) {
  EndTransaction();
  return main()
      .HistoricalAccess(fork_ts(), timeout)
      .transform([this](std::unique_ptr<storage::Storage::Accessor> historical) {
        historical->GetTransaction()->branch_ctx = this;
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
  diff_txn->GetTransaction()->branch_ctx = this;
  diff_txn->GetTransaction()->branch_diff_ctx = this;
}

// ---- Commit

// Published from a commit callback, so only a committed batch's counts are counted.
void BranchContext::PublishForkDeletes() {
  auto &t = txn();
  if (t.fork_deletes == ObjectCounts{}) return;
  t.diff->GetTransaction()->commit_callbacks_.Add(
      [core = core_.get(), deleted = t.fork_deletes](uint64_t /*commit_ts*/) { core->AddForkDeletes(deleted); });
}

std::expected<void, storage::StorageManipulationError> BranchContext::CommitTransaction(
    storage::CommitArgs commit_args) {
  PublishForkDeletes();
  return txn().diff->PrepareForCommitPhase(std::move(commit_args));
}

std::expected<void, storage::StorageManipulationError> BranchContext::PeriodicCommitTransaction(
    storage::CommitArgs commit_args) {
  PublishForkDeletes();
  auto result = txn().diff->PeriodicCommit(std::move(commit_args));
  // A batch that did not commit aborts the transaction, so its counts are never needed again.
  txn().fork_deletes = {};
  return result;
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
  // The endpoints are main's fork vertices, kept alive for the branch's life by the fork pin. The marker entries that
  // hold this stub sit in the endpoint stubs' adjacency, which this accessor never reads.
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

storage::Vertex *BranchContext::FindForkVertexPtr(storage::Gid gid) {
  core_->CountForkVertexRead();
  auto const fork = FindForkVertex(gid);
  return fork ? fork->vertex_ : nullptr;
}

storage::VertexAccessor BranchContext::ForkVertexAccessor(storage::Vertex *fork) {
  DMG_ASSERT(fork != nullptr, "a fork vertex pointer comes from FindForkVertexPtr.");
  return storage::VertexAccessor{fork, &main(), historical().GetTransaction()};
}

storage::VertexAccessor BranchContext::ForkIdentity(const storage::VertexAccessor &vertex) {
  if (!IsStub(vertex)) return vertex;
  auto fork = FindForkVertex(vertex.Gid());
  DMG_ASSERT(fork.has_value(), "a stub's fork vertex is pinned for the branch's life.");
  return fork.value_or(vertex);
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

std::optional<storage::EdgeAccessor> BranchContext::FindForkEdgeOfStub(const storage::EdgeAccessor &stub) {
  auto const fork = edge_stubs().FindFork(stub.Gid().AsUint());
  if (!fork) return std::nullopt;
  return storage::EdgeAccessor{
      *fork, stub.EdgeType(), stub.from_vertex_, stub.to_vertex_, &main(), historical().GetTransaction()};
}

std::optional<storage::VertexAccessor> BranchContext::FindStub(storage::Gid gid, storage::View view) {
  auto &t = txn();
  if (!IsForkCandidateVertex(gid)) return t.diff->FindVertex(gid, view);
  auto *stub = stubs().FindStub(gid.AsUint(), t.stub_cursor);
  if (stub == nullptr) return std::nullopt;
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

std::optional<storage::EdgeAccessor> BranchContext::ResolveEdge(storage::Gid edge_gid, storage::View view) {
  if (!IsForkCandidateEdge(edge_gid)) return FindDiffEdge(edge_gid, view);
  auto edge = historical().FindEdge(edge_gid, storage::View::OLD);
  if (!edge || IsForkEdgeHidden(*edge, view)) return std::nullopt;
  return edge;
}

int64_t BranchContext::ApproximateVertexCount() {
  auto const deleted = core_->ForkDeletes().vertices;
  auto const total = historical().ApproximateVertexCount() + current_diff_txn()->ApproximateVertexCount();
  return static_cast<int64_t>(total > deleted ? total - deleted : 0);
}

int64_t BranchContext::ApproximateEdgeCount() {
  auto const deleted = core_->ForkDeletes().edges;
  auto const total = historical().ApproximateEdgeCount() + current_diff_txn()->ApproximateEdgeCount();
  return static_cast<int64_t>(total > deleted ? total - deleted : 0);
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
  // A fork edge is main's; its edge stub is in neither edges_ nor a native adjacency.
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
  if (!IsForkCandidateVertex(gid) || !core_->MayHaveVertexDeleted(gid)) return false;
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
  if (IsForkEdgeDeleted(fork_edge, view)) return true;
  if (!core_->MayHaveAnyVertexDeleted()) return false;
  auto const from = fork_edge.FromVertex();
  auto const to = fork_edge.ToVertex();
  return IsForkVertexDeleted(from, view) || (to.Gid() != from.Gid() && IsForkVertexDeleted(to, view));
}

// ---- Vertex reads

namespace {

storage::Result<storage::VertexKey> MergedLabels(const storage::VertexAccessor &fork,
                                                 const storage::VertexAccessor &stub, storage::View view) {
  auto stub_labels = stub.Labels(view);
  if (!stub_labels) return std::unexpected(stub_labels.error());
  if (std::ranges::contains(*stub_labels, stub::kDeleteLabel)) return std::unexpected(storage::Error::DELETED_OBJECT);
  return fork.Labels(storage::View::OLD).transform([&](storage::VertexKey fork_labels) {
    return stub::MergeLabels(*stub_labels, std::move(fork_labels));
  });
}

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

namespace {

template <typename Record>
storage::Result<void> ReadEachProperty(BranchContext &ctx, const Record &record,
                                       const storage::OrderedPropertyPaths &ordered_properties,
                                       std::span<storage::PropertyValue> out, storage::View view) {
  MG_ASSERT(out.size() == ordered_properties.size(), "One value slot per property is required");
  auto const paths = ordered_properties.paths();
  for (std::size_t slot = 0; slot != paths.size(); ++slot) {
    MG_ASSERT(paths[slot].size() == 1, "A branch reads a set of paths only for single-property paths");
    auto value = ctx.GetProperty(record, paths[slot][0], view);
    if (!value) return std::unexpected(value.error());
    out[slot] = std::move(*value);
  }
  return {};
}

}  // namespace

storage::Result<void> BranchContext::ReadPropertyValues(const storage::VertexAccessor &vertex,
                                                        const storage::OrderedPropertyPaths &ordered_properties,
                                                        std::span<storage::PropertyValue> out, storage::View view) {
  return ReadEachProperty(*this, vertex, ordered_properties, out, view);
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
  // Ahead of the hidden check: main answers so for a deleted edge too.
  if (PropertyWritesDisabled(edge)) return std::map<storage::PropertyId, storage::PropertyValue>{};
  auto const read = ResolveEdgeRead(edge, std::nullopt, view);
  if (read.kind == EdgeRead::Kind::kHidden) return std::unexpected(storage::Error::DELETED_OBJECT);
  if (read.kind == EdgeRead::Kind::kDirect) return edge.Properties(view);
  auto stub_properties = read.stub->Properties(view);
  if (!stub_properties) return std::unexpected(stub_properties.error());
  return stub::MergeProperties(*stub_properties, edge);
}

storage::Result<storage::PropertyValue> BranchContext::GetProperty(const storage::EdgeAccessor &edge,
                                                                   storage::PropertyId key, storage::View view) {
  if (PropertyWritesDisabled(edge)) return storage::PropertyValue{};
  auto const read = ResolveEdgeRead(edge, key, view);
  if (read.kind == EdgeRead::Kind::kHidden) return std::unexpected(storage::Error::DELETED_OBJECT);
  if (read.kind == EdgeRead::Kind::kDirect) return edge.GetProperty(key, view);
  return stub::MergeGetProperty(*read.stub, edge, key, view);
}

storage::Result<uint64_t> BranchContext::GetPropertySize(const storage::EdgeAccessor &edge, storage::PropertyId key,
                                                         storage::View view) {
  if (PropertyWritesDisabled(edge)) return uint64_t{0};
  auto const read = ResolveEdgeRead(edge, key, view);
  if (read.kind == EdgeRead::Kind::kHidden) return std::unexpected(storage::Error::DELETED_OBJECT);
  if (read.kind == EdgeRead::Kind::kDirect) return edge.GetPropertySize(key, view);
  return stub::MergeGetPropertySize(*read.stub, edge, key, view);
}

storage::Result<void> BranchContext::ReadPropertyValues(const storage::EdgeAccessor &edge,
                                                        const storage::OrderedPropertyPaths &ordered_properties,
                                                        std::span<storage::PropertyValue> out, storage::View view) {
  return ReadEachProperty(*this, edge, ordered_properties, out, view);
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

// The stock index insert reads the stub's own labels and properties, which hold only what the branch changed. So a
// write moving one part of a mirrored key first copies the fork's other parts as delta-free base state: no read
// changes and abort, GC and the WAL never see it. Parts the stub holds or has marked removed are never overwritten.

// Default (null parts): no key copies, for edges and writes that never move a key.
struct IndexedStub {
  const IndexKeyParts *parts{nullptr};
  storage::Vertex *vertex{nullptr};
};

using WantedLabels = absl::InlinedVector<storage::LabelId, 4>;
using WantedProperties = absl::InlinedVector<storage::PropertyId, 4>;

struct KeyParts {
  WantedLabels labels;
  WantedProperties properties;

  bool empty() const { return labels.empty() && properties.empty(); }
};

template <typename Ids, typename Id>
void AddDistinct(Ids &ids, Id id) {
  if (!std::ranges::contains(ids, id)) ids.push_back(id);
}

// The other parts of the keys `property` belongs to. Removing one part of a key moves it only while the key keeps
// another part, so a removal skips the keys of a single property.
KeyParts PartsAroundProperty(const IndexKeyParts &parts, storage::PropertyId property, bool removal) {
  KeyParts around;
  parts.ForEachKeyOfProperty(property, [&](const IndexKeyParts::Key &key) {
    if (removal && key.properties.size() == 1) return;
    AddDistinct(around.labels, key.label);
    for (auto const other : key.properties) {
      if (other != property) AddDistinct(around.properties, other);
    }
  });
  return around;
}

KeyParts PartsAroundLabel(const IndexKeyParts &parts, storage::LabelId label) {
  KeyParts around;
  parts.ForEachKeyOfLabel(label, [&](const IndexKeyParts::Key &key) {
    for (auto const property : key.properties) AddDistinct(around.properties, property);
  });
  return around;
}

// The stub decides a part when it holds it or has it marked removed. The caller holds the stub's lock.
bool LabelDecided(const storage::Vertex &stub, storage::LabelId label) {
  return std::ranges::contains(stub.labels, label) || std::ranges::contains(stub.labels, stub::Mark(label));
}

bool PropertyDecided(const storage::Vertex &stub, storage::PropertyId property) {
  return stub.properties.HasProperty(property) || stub.properties.HasProperty(stub::Mark(property));
}

// Copies onto `stub` what `wanted` names that the fork has and the stub does not decide. Claims the stub first, as the
// write it precedes would, so a stub another transaction holds fails here and nothing is copied.
std::optional<storage::Error> CopyKeyParts(storage::Transaction *transaction, const storage::VertexAccessor &fork,
                                           storage::Vertex &stub, KeyParts wanted) {
  {
    auto guard = std::shared_lock{stub.lock};
    wanted.labels.erase(std::remove_if(wanted.labels.begin(),
                                       wanted.labels.end(),
                                       [&](storage::LabelId label) { return LabelDecided(stub, label); }),
                        wanted.labels.end());
    wanted.properties.erase(
        std::remove_if(wanted.properties.begin(),
                       wanted.properties.end(),
                       [&](storage::PropertyId property) { return PropertyDecided(stub, property); }),
        wanted.properties.end());
  }
  if (wanted.empty()) return std::nullopt;

  WantedLabels labels;
  for (auto const label : wanted.labels) {
    auto const in_fork = fork.HasLabel(label, storage::View::OLD);
    if (!in_fork) return in_fork.error();
    if (*in_fork) labels.push_back(label);
  }
  absl::InlinedVector<std::pair<storage::PropertyId, storage::PropertyValue>, 4> properties;
  for (auto const property : wanted.properties) {
    auto value = fork.GetProperty(property, storage::View::OLD);
    if (!value) return value.error();
    if (!value->IsNull()) properties.emplace_back(property, std::move(*value));
  }
  if (labels.empty() && properties.empty()) return std::nullopt;

  auto guard = std::unique_lock{stub.lock};
  if (!storage::PrepareForWrite(transaction, &stub)) return storage::Error::SERIALIZATION_ERROR;
  utils::AtomicMemoryBlock([&] {
    // Asked again under the lock: a writer may have decided the part since the check above.
    for (auto const label : labels) {
      if (!LabelDecided(stub, label)) stub.labels.push_back(label);
    }
    for (auto const &[property, value] : properties) {
      if (!PropertyDecided(stub, property)) stub.properties.SetProperty(property, value);
    }
  });
  return std::nullopt;
}

// Writes one property through its stub in the per-key order the reads rest on (stub_merge.hpp): removal marker on
// before the value is dropped, off after it is set. Returns the old merged value.
template <typename Fork, typename Stub>
storage::Result<storage::PropertyValue> WriteStubProperty(storage::Transaction *transaction, const Fork &fork,
                                                          Stub &stub, storage::PropertyId key,
                                                          const storage::PropertyValue &value,
                                                          const IndexedStub &indexed = {}) {
  auto const own = stub::ReadKey(stub, key, storage::View::NEW);
  if (!own) return std::unexpected(own.error());
  // The fork is read only when the stub does not decide and the write needs its value (to mark a removal, or as the old
  // value); reading it walks all of main's history of the object after the fork.
  storage::PropertyValue fork_value;
  if (!own->removed && (value.IsNull() || own->value.IsNull())) {
    auto read = fork.GetProperty(key, storage::View::OLD);
    if (!read) return std::unexpected(read.error());
    fork_value = std::move(*read);
  }
  storage::PropertyValue old_value = own->value.IsNull() ? fork_value : own->value;

  PartialWriteGuard guard{transaction};
  // Removing a value nobody holds moves no key.
  if constexpr (std::is_same_v<Fork, storage::VertexAccessor>) {
    if (indexed.parts != nullptr && !indexed.parts->empty() && !(value.IsNull() && old_value.IsNull())) {
      if (auto around = PartsAroundProperty(*indexed.parts, key, value.IsNull()); !around.empty()) {
        if (auto error = CopyKeyParts(transaction, fork, *indexed.vertex, std::move(around))) {
          return std::unexpected(*error);
        }
      }
    }
  }
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

storage::EdgeAccessor BranchContext::MaterializeEdgeStub(const storage::EdgeAccessor &fork_edge) {
  // Before the stub exists, and so before any write of it (reads gate on this bit). With properties_on_edges off on
  // main the fork edge is a bare gid, there is no Edge to mark, and reads fall back to the filters.
  if (main().config_.salient.items.properties_on_edges) MarkBranched(fork_edge.edge_.ptr);
  auto *stub = edge_stubs().GetOrCreate(fork_edge.Gid().AsUint(), fork_edge.edge_);
  return storage::EdgeAccessor{storage::EdgeRef{stub},
                               fork_edge.EdgeType(),
                               fork_edge.from_vertex_,
                               fork_edge.to_vertex_,
                               &diff_engine(),
                               txn().diff->GetTransaction()};
}

storage::Result<storage::EdgeAccessor> BranchContext::StubForWrite(const storage::EdgeAccessor &fork_edge) {
  DMG_ASSERT(!IsNative(fork_edge), "a native edge has no stub.");
  // The diff engine's own config is always properties_on_edges: it is main's that the user's edge properties follow.
  if (!main().config_.salient.items.properties_on_edges) return std::unexpected(storage::Error::PROPERTIES_DISABLED);
  if (IsForkEdgeHidden(fork_edge, storage::View::NEW)) return std::unexpected(storage::Error::DELETED_OBJECT);
  return MaterializeEdgeStub(fork_edge);
}

std::expected<storage::VertexAccessor, BranchContext::BranchWriteError> BranchContext::EnsureDiffVertex(
    storage::Gid gid) {
  if (!IsForkCandidateVertex(gid)) {
    // A native vertex the caller resolved and that is not visible now was deleted on the branch.
    if (auto native = txn().diff->FindVertex(gid, storage::View::NEW)) return *native;
    return std::unexpected(BranchWriteError{DeletedMessage(gid)});
  }
  if (IsVertexDeleted(gid, storage::View::NEW)) return std::unexpected(BranchWriteError{DeletedMessage(gid)});
  // An existing stub means the fork vertex was already resolved; reading the fork again walks all of main's history of
  // the vertex after the fork.
  if (auto stub = ReadStub(gid)) return *std::move(stub);
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
  core_->ReserveNativeGid(GidKind::kVertex, gid.AsUint());
  auto created = static_cast<storage::ReplicationAccessor &>(diff()).CreateVertexEx(gid);
  MG_ASSERT(created.has_value(),
            "BranchContext::CreateNativeVertex: gid {} already exists in the diff engine.",
            gid.AsUint());
  return *created;
}

storage::Result<storage::EdgeAccessor> BranchContext::CreateNativeEdge(storage::VertexAccessor &from,
                                                                       storage::VertexAccessor &to,
                                                                       storage::EdgeTypeId edge_type) {
  auto const gid = storage::Gid::FromUint(main().edge_id_.fetch_add(1, std::memory_order_acq_rel));
  core_->ReserveNativeGid(GidKind::kEdge, gid.AsUint());
  return CreateNativeEdgeAt(gid, from, to, edge_type);
}

storage::Result<storage::EdgeAccessor> BranchContext::CreateNativeEdgeAt(storage::Gid gid,
                                                                         storage::VertexAccessor &from,
                                                                         storage::VertexAccessor &to,
                                                                         storage::EdgeTypeId edge_type) {
  // The stock call refuses a deleted vertex by its deleted bit, which a stub never carries: a delete earlier in this
  // transaction is the marker. Another transaction's delete is ordered by the stub's MVCC.
  for (auto const *endpoint : {&from, &to}) {
    if (IsStub(*endpoint) && IsVertexDeleted(endpoint->Gid(), storage::View::NEW)) {
      return std::unexpected(storage::Error::DELETED_OBJECT);
    }
  }
  core_->RecordEdgeChange(from.Gid());
  core_->RecordEdgeChange(to.Gid());
  return static_cast<storage::ReplicationAccessor &>(diff()).CreateEdgeEx(&from, &to, edge_type, gid);
}

bool BranchContext::TouchStub(const storage::VertexAccessor &stub) {
  auto guard = std::shared_lock{stub.vertex_->lock};
  return storage::PrepareForWrite(txn().diff->GetTransaction(), stub.vertex_);
}

bool BranchContext::TouchStub(const storage::EdgeAccessor &stub) {
  auto guard = std::shared_lock{stub.edge_.ptr->lock};
  return storage::PrepareForWrite(txn().diff->GetTransaction(), stub.edge_.ptr);
}

bool BranchContext::CanDeleteAnchor(const storage::VertexAccessor &stub) {
  auto *const transaction = txn().diff->GetTransaction();
  auto guard = std::shared_lock{stub.vertex_->lock};
  // The head is our own marker, so it cannot conflict; the walk catches a committed delta under it that this snapshot
  // cannot see. The chain is intact while this transaction is active.
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

void BranchContext::RecordPropertyWrite(const storage::VertexAccessor &vertex, storage::PropertyId key) {
  core_->RecordPropertyFieldChange(vertex.Gid(), key);
  core_->RecordPropertyTouched(key);
}

void BranchContext::RecordPropertyWrite(const storage::EdgeAccessor &edge, storage::PropertyId key) {
  core_->RecordForkEdgeProperty(edge.Gid(), key);
}

void BranchContext::RecordPropertiesWrite(const storage::VertexAccessor &vertex) {
  core_->RecordPropertyChange(vertex.Gid());
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
  core_->RecordLabelTouched(label);
  auto const written = *in_fork ? stub::Mark(label) : label;
  if (!add || core_->key_parts().empty()) return add != *in_fork ? stub->AddLabel(written) : stub->RemoveLabel(written);

  auto *const transaction = txn().diff->GetTransaction();
  auto *const stub_vertex = stub->vertex_;
  auto const copy_properties = [&] {
    return CopyKeyParts(transaction, vertex, *stub_vertex, PartsAroundLabel(core_->key_parts(), label));
  };
  if (!*in_fork) {
    // The stock insert of the new label reads the stub's own properties: give it the key's other parts first.
    if (auto error = copy_properties()) return std::unexpected(*error);
    return stub->AddLabel(label);
  }
  // Dropping the mark revives the fork label with no stock insert: a stub holding no copy of it gets the copy and its
  // index entries now, so keys the branch changed while the label was away are found under it.
  auto const unmarked = stub->RemoveLabel(written);
  if (!unmarked) return unmarked;
  PartialWriteGuard guard{transaction};
  if (*unmarked) {
    bool held;
    {
      auto lock = std::shared_lock{stub_vertex->lock};
      held = std::ranges::contains(stub_vertex->labels, label);
    }
    if (!held) {
      if (auto error = copy_properties()) return std::unexpected(*error);
      if (auto error = CopyKeyParts(transaction, vertex, *stub_vertex, KeyParts{.labels = {label}, .properties = {}})) {
        return std::unexpected(*error);
      }
      utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
      transaction->active_indices_->label_properties_->UpdateOnAddLabel(label, stub_vertex, *transaction);
    }
  }
  guard.Done();
  return unmarked;
}

storage::Result<bool> BranchContext::AddLabel(storage::VertexAccessor &vertex, storage::LabelId label) {
  return ToggleLabel(vertex, label, true);
}

storage::Result<bool> BranchContext::RemoveLabel(storage::VertexAccessor &vertex, storage::LabelId label) {
  return ToggleLabel(vertex, label, false);
}

template <typename Object>
storage::Result<storage::PropertyValue> BranchContext::SetPropertyAt(Object &object, storage::PropertyId key,
                                                                     const storage::PropertyValue &value) {
  if (IsNative(object)) {
    DMG_ASSERT(!IsStub(object), "a stub reached a branch write.");
    if (PropertyWritesDisabled(object)) return std::unexpected(storage::Error::PROPERTIES_DISABLED);
    return object.SetProperty(key, value);
  }
  auto stub = StubForWrite(object);
  if (!stub) return std::unexpected(stub.error());
  RecordPropertiesWrite(object);
  RecordPropertyWrite(object, key);
  IndexedStub indexed;
  if constexpr (std::is_same_v<std::remove_const_t<Object>, storage::VertexAccessor>)
    indexed = {&core_->key_parts(), stub->vertex_};
  return WriteStubProperty(txn().diff->GetTransaction(), object, *stub, key, value, indexed);
}

template <typename Object>
storage::Result<bool> BranchContext::InitPropertiesAt(Object &object, PropertyMap &properties) {
  if (IsNative(object)) {
    DMG_ASSERT(!IsStub(object), "a stub reached a branch write.");
    if (PropertyWritesDisabled(object)) return std::unexpected(storage::Error::PROPERTIES_DISABLED);
    return object.InitProperties(properties);
  }
  auto stub = StubForWrite(object);
  if (!stub) return std::unexpected(stub.error());
  auto existing = Properties(object, storage::View::NEW);
  if (!existing) return std::unexpected(existing.error());
  if (!existing->empty()) {
    if (!TouchStub(*stub)) return std::unexpected(storage::Error::SERIALIZATION_ERROR);
    return false;
  }
  if (properties.empty()) {
    if (!TouchStub(*stub)) return std::unexpected(storage::Error::SERIALIZATION_ERROR);
    return true;
  }
  RecordPropertiesWrite(object);
  for (auto const &entry : properties) RecordPropertyWrite(object, entry.first);
  auto *const transaction = txn().diff->GetTransaction();
  IndexedStub indexed;
  if constexpr (std::is_same_v<std::remove_const_t<Object>, storage::VertexAccessor>)
    indexed = {&core_->key_parts(), stub->vertex_};
  PartialWriteGuard guard{transaction};
  for (auto const &[key, value] : properties) {
    if (auto written = WriteStubProperty(transaction, object, *stub, key, value, indexed); !written) {
      return std::unexpected(written.error());
    }
  }
  guard.Done();
  return true;
}

template <typename Object>
storage::Result<std::vector<PropertyChange>> BranchContext::UpdatePropertiesAt(Object &object,
                                                                               PropertyMap &properties) {
  if (IsNative(object)) {
    DMG_ASSERT(!IsStub(object), "a stub reached a branch write.");
    if (PropertyWritesDisabled(object)) return std::unexpected(storage::Error::PROPERTIES_DISABLED);
    return object.UpdateProperties(properties);
  }
  auto stub = StubForWrite(object);
  if (!stub) return std::unexpected(stub.error());
  auto existing = Properties(object, storage::View::NEW);
  if (!existing) return std::unexpected(existing.error());
  if (properties.empty()) {
    if (!TouchStub(*stub)) return std::unexpected(storage::Error::SERIALIZATION_ERROR);
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
  auto *const transaction = txn().diff->GetTransaction();
  IndexedStub indexed;
  if constexpr (std::is_same_v<std::remove_const_t<Object>, storage::VertexAccessor>)
    indexed = {&core_->key_parts(), stub->vertex_};
  PartialWriteGuard guard{transaction};
  for (auto const &[key, value] : properties) {
    if (auto written = WriteStubProperty(transaction, object, *stub, key, value, indexed); !written) {
      return std::unexpected(written.error());
    }
  }
  guard.Done();
  // As storage's UpdateProperties: the properties the update left alone come back in the caller's map.
  for (auto &[key, old_value] : *existing) properties.emplace(key, std::move(old_value));
  return changes;
}

template <typename Object>
storage::Result<PropertyMap> BranchContext::ClearPropertiesAt(Object &object) {
  if (IsNative(object)) {
    DMG_ASSERT(!IsStub(object), "a stub reached a branch write.");
    if (PropertyWritesDisabled(object)) return std::unexpected(storage::Error::PROPERTIES_DISABLED);
    return object.ClearProperties();
  }
  auto stub = StubForWrite(object);
  if (!stub) return std::unexpected(stub.error());
  auto existing = Properties(object, storage::View::NEW);
  if (!existing) return std::unexpected(existing.error());
  if (existing->empty()) {
    if (!TouchStub(*stub)) return std::unexpected(storage::Error::SERIALIZATION_ERROR);
    return existing;
  }
  RecordPropertiesWrite(object);
  for (auto const &entry : *existing) RecordPropertyWrite(object, entry.first);
  auto *const transaction = txn().diff->GetTransaction();
  PartialWriteGuard guard{transaction};
  for (auto const &entry : *existing) {
    if (auto removed = WriteStubProperty(transaction, object, *stub, entry.first, storage::PropertyValue{}); !removed) {
      return std::unexpected(removed.error());
    }
  }
  guard.Done();
  return existing;
}

storage::Result<void> BranchContext::HideForkEdge(const storage::EdgeAccessor &fork_edge) {
  auto *const from = fork_edge.from_vertex_;
  auto *const to = fork_edge.to_vertex_;
  core_->RecordEdgeChange(from->gid);
  core_->RecordEdgeChange(to->gid);
  MarkBranched(from);
  MarkBranched(to);
  auto *const from_stub = stubs().GetOrCreateStub(from->gid.AsUint(), from);
  auto *const to_stub = stubs().GetOrCreateStub(to->gid.AsUint(), to);
  auto *const edge_stub = edge_stubs().GetOrCreate(fork_edge.Gid().AsUint(), fork_edge.edge_);
  // No check that the edge is not hidden already: a hide another transaction committed after this one began is not a
  // broken precondition but a conflict, and LinkEdgeMarker reports it as one.
  auto *const transaction = txn().diff->GetTransaction();
  auto const type = fork_edge.EdgeType();
  if (!stub::LinkEdgeMarker(
          transaction, diff_engine(), from_stub, storage::EdgeDirection::OUT, type, to_stub, edge_stub) ||
      !stub::LinkEdgeMarker(
          transaction, diff_engine(), to_stub, storage::EdgeDirection::IN, type, from_stub, edge_stub)) {
    return std::unexpected(storage::Error::SERIALIZATION_ERROR);
  }
  return {};
}

std::vector<storage::EdgeAccessor> BranchContext::VisibleForkEdges(const storage::VertexAccessor &fork) {
  auto const found = stubs().Find(fork.Gid().AsUint(), txn().stub_cursor);
  std::vector<storage::EdgeAccessor> visible;
  for (auto const direction : {storage::EdgeDirection::OUT, storage::EdgeDirection::IN}) {
    HiddenForkEdges hidden;
    if (found.stub != nullptr) hidden = ReadStubAdjacency(found.stub, direction, storage::View::NEW).hidden;
    auto expanded =
        direction == storage::EdgeDirection::OUT ? fork.OutEdges(storage::View::OLD) : fork.InEdges(storage::View::OLD);
    // The accessor is a resolved fork vertex, so it is visible and not deleted at the fork.
    DMG_ASSERT(expanded.has_value(), "expanding a fork vertex failed.");
    if (!expanded) continue;
    for (auto const &edge : expanded->edges) {
      // A self-loop is in both lists; it was taken as an out-edge.
      if (direction == storage::EdgeDirection::IN && edge.IsCycle()) continue;
      if (!hidden.Contains(edge.edge_)) visible.push_back(edge);
    }
  }
  return visible;
}

storage::Result<storage::PropertyValue> BranchContext::SetProperty(storage::VertexAccessor &vertex,
                                                                   storage::PropertyId key,
                                                                   const storage::PropertyValue &value) {
  return SetPropertyAt(vertex, key, value);
}

storage::Result<bool> BranchContext::InitProperties(storage::VertexAccessor &vertex, PropertyMap &properties) {
  return InitPropertiesAt(vertex, properties);
}

storage::Result<std::vector<PropertyChange>> BranchContext::UpdateProperties(const storage::VertexAccessor &vertex,
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

// Like main's DETACH DELETE, writes every edge it hides, so a concurrent writer of those edges or their other endpoints
// hits an ordinary stub conflict. Edges first: the stub's own marker is the one write left to conflict on the vertex.
storage::Result<bool> BranchContext::DeleteForkVertex(storage::Gid gid) {
  if (IsVertexDeleted(gid, storage::View::NEW)) return false;
  auto fork = FindForkVertex(gid);
  if (!fork) return std::unexpected(storage::Error::NONEXISTENT_OBJECT);
  // The edges the delete hides, taken before the markers hide them all.
  auto const hidden_edges = VisibleForkEdges(*fork);
  // MaterializeStub sets the branched bit before RecordVertexDeleted publishes the any-deleted flag, and the marker
  // comes last: every read that can see the marker is gated on the filter.
  auto stub = MaterializeStub(*fork);
  for (auto const &edge : hidden_edges) {
    if (auto marked = MarkForkEdgeDeleted(edge); !marked) return std::unexpected(marked.error());
  }
  core_->RecordVertexDeleted(gid);
  auto written = stub.AddLabel(stub::kDeleteLabel);
  if (!written) return std::unexpected(written.error());
  if (!CanDeleteAnchor(stub)) return std::unexpected(storage::Error::SERIALIZATION_ERROR);
  if (*written) {
    auto &deleted = txn().fork_deletes;
    ++deleted.vertices;
    deleted.edges += hidden_edges.size();
  }
  return *written;
}

// Marker first, as stock locks the edge before its endpoints: it is the edge-level conflict and the WAL's record. The
// filters are recorded before either write, so every read that can see a marker is gated on them.
storage::Result<void> BranchContext::MarkForkEdgeDeleted(const storage::EdgeAccessor &fork_edge) {
  core_->RecordForkEdgeDeleted(fork_edge.Gid());
  auto stub = MaterializeEdgeStub(fork_edge);
  if (auto marked = stub.SetProperty(stub::kEdgeDeletePid, storage::PropertyValue{true}); !marked) {
    return std::unexpected(marked.error());
  }
  return HideForkEdge(fork_edge);
}

// An endpoint delete hides the edge the same way, so the two conflict on the endpoint stubs and the edge's.
storage::Result<bool> BranchContext::DeleteForkEdge(const storage::EdgeAccessor &fork_edge) {
  DMG_ASSERT(!IsNative(fork_edge), "a native edge is deleted by the diff engine.");
  if (IsForkEdgeHidden(fork_edge, storage::View::NEW)) return false;
  if (auto marked = MarkForkEdgeDeleted(fork_edge); !marked) return std::unexpected(marked.error());
  ++txn().fork_deletes.edges;
  return true;
}

// ---- Adjacency

storage::Result<std::vector<storage::EdgeAccessor>> BranchContext::Edges(const storage::VertexAccessor &vertex,
                                                                         storage::EdgeDirection direction,
                                                                         storage::View view) {
  std::vector<storage::EdgeAccessor> edges;
  auto visited = VisitEdges(
      vertex,
      direction,
      view,
      [&](size_t count) { edges.reserve(edges.size() + count); },
      [&](const storage::EdgeAccessor &edge) {
        edges.push_back(edge);
        return true;
      });
  if (!visited) return std::unexpected(visited.error());
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

BranchContext::StubAdjacency BranchContext::ReadStubAdjacency(storage::Vertex *stub, storage::EdgeDirection direction,
                                                              storage::View view) {
  StubAdjacency adjacency;
  auto const stub_accessor = StubAccessor(stub);
  auto const expanded =
      direction == storage::EdgeDirection::OUT ? stub_accessor.OutEdges(view) : stub_accessor.InEdges(view);
  // Callers ruled out a deleted stub, and a delta-less stub is visible at every view.
  DMG_ASSERT(expanded.has_value(), "expanding a stub failed.");
  if (!expanded) return adjacency;
  auto const entries = expanded->edges.Triples();
  adjacency.hidden.Reserve(entries.size());
  for (size_t index = 0; index != entries.size(); ++index) {
    auto const &[edge_type, other, ref] = entries[index];
    if (!storage::IsReservedId(edge_type)) {
      adjacency.natives.push_back(expanded->edges.At(index));
      continue;
    }
    // A marker entry holds its edge's stub, which carries the fork edge the fork's adjacency holds.
    auto const fork = EdgeStubIndex::ForkOf(ref.ptr);
    DMG_ASSERT(fork.has_value(), "a marker's edge stub is created with its fork edge.");
    if (fork) adjacency.hidden.Add(*fork);
  }
  adjacency.hidden.Seal();
  return adjacency;
}

// Exact because a native edge is appended to the stub's in/out edges under the stub's lock before its transaction
// commits, so every edge this reader may see is in the stub's adjacency at `view`. The filters only gate the read: a
// clear bit means no native edge, no marker and no delete of this vertex.
std::optional<BranchContext::DirectExpansionPlan> BranchContext::DirectExpansion(const storage::VertexAccessor &vertex,
                                                                                 storage::EdgeDirection direction,
                                                                                 storage::View view) {
  if (vertex.storage_ == &diff_engine()) return DirectExpansionPlan{.view = view, .hidden = {}, .natives = {}};
  auto const gid = vertex.Gid();
  if (!IsForkCandidateVertex(gid)) return std::nullopt;
  bool const may_delete = core_->MayHaveVertexDeleted(gid);
  bool const may_have_edges = core_->MayHaveEdgeChange(gid);
  if (!may_delete && !may_have_edges)
    return DirectExpansionPlan{.view = storage::View::OLD, .hidden = {}, .natives = {}};
  auto *const stub = stubs().FindStub(gid.AsUint(), txn().stub_cursor);
  if (stub == nullptr) return DirectExpansionPlan{.view = storage::View::OLD, .hidden = {}, .natives = {}};
  if (may_delete && StubDeleted(stub, view)) return std::nullopt;
  if (!may_have_edges) return DirectExpansionPlan{.view = storage::View::OLD, .hidden = {}, .natives = {}};
  auto adjacency = ReadStubAdjacency(stub, direction, view);
  return DirectExpansionPlan{
      .view = storage::View::OLD, .hidden = std::move(adjacency.hidden), .natives = std::move(adjacency.natives)};
}

storage::Result<void> BranchContext::VisitEdges(const storage::VertexAccessor &vertex, storage::EdgeDirection direction,
                                                storage::View view, absl::FunctionRef<void(size_t)> reserve,
                                                absl::FunctionRef<bool(const storage::EdgeAccessor &)> emit) {
  if (vertex.storage_ == &diff_engine()) {
    // A native vertex's edges are all native, and it reports its own deletion.
    auto native = direction == storage::EdgeDirection::OUT ? vertex.OutEdges(view) : vertex.InEdges(view);
    if (!native) return std::unexpected(native.error());
    reserve(native->edges.size());
    for (auto const &edge : native->edges) {
      if (!emit(edge)) break;
    }
    return {};
  }
  bool deleted = false;
  ResolveEdgesInto(vertex.Gid(), &vertex, direction, view, &deleted, reserve, emit);
  if (deleted) return std::unexpected(storage::Error::DELETED_OBJECT);
  return {};
}

std::vector<storage::EdgeAccessor> BranchContext::ResolveEdges(storage::Gid vertex_gid,
                                                               storage::EdgeDirection direction, storage::View view,
                                                               bool *vertex_deleted) {
  std::vector<storage::EdgeAccessor> result;
  ResolveEdgesInto(
      vertex_gid,
      nullptr,
      direction,
      view,
      vertex_deleted,
      [&](size_t count) { result.reserve(count); },
      [&](const storage::EdgeAccessor &edge) {
        result.push_back(edge);
        return true;
      });
  return result;
}

// historical() is a frozen snapshot, so it is read with View::OLD throughout. `emit` order: the stub's native edges,
// then the fork's, less those the stub's markers hide.
void BranchContext::ResolveEdgesInto(storage::Gid vertex_gid, const storage::VertexAccessor *fork_hint,
                                     storage::EdgeDirection direction, storage::View view, bool *vertex_deleted,
                                     absl::FunctionRef<void(size_t)> reserve,
                                     absl::FunctionRef<bool(const storage::EdgeAccessor &)> emit) {
  auto &t = txn();
  if (vertex_deleted != nullptr) *vertex_deleted = false;
  bool const out = direction == storage::EdgeDirection::OUT;
  auto const expand = [&](const storage::VertexAccessor &vertex,
                          storage::View expand_view) -> std::optional<storage::EdgesVertexAccessorResult> {
    auto expanded = out ? vertex.OutEdges(expand_view) : vertex.InEdges(expand_view);
    // The accessor was just resolved through the same transaction and view, so it is visible and not deleted.
    DMG_ASSERT(expanded.has_value(), "expanding a resolved vertex failed.");
    if (!expanded) return std::nullopt;
    return std::move(*expanded);
  };

  if (!IsForkCandidateVertex(vertex_gid)) {
    if (auto native = t.diff->FindVertex(vertex_gid, view)) {
      if (auto expanded = expand(*native, view)) {
        reserve(expanded->edges.size());
        for (auto const &edge : expanded->edges) {
          if (!emit(edge)) return;
        }
      }
    }
    return;
  }

  // A native edge to a branch-deleted fork vertex cannot exist: the delete conflicts with the edge's creation on that
  // vertex's stub (stock first-writer-wins), and DETACH DELETE removes its native edges first.
  auto const found = stubs().Find(vertex_gid.AsUint(), t.stub_cursor);
  StubAdjacency adjacency;
  if (found.stub != nullptr) {
    if (core_->MayHaveVertexDeleted(vertex_gid) && StubDeleted(found.stub, view)) {
      if (vertex_deleted != nullptr) *vertex_deleted = true;
      return;
    }
    adjacency = ReadStubAdjacency(found.stub, direction, view);
  }

  std::optional<storage::VertexAccessor> looked_up;
  if (fork_hint == nullptr) {
    looked_up = ForkAccessor(found.fork, vertex_gid);
    if (looked_up) fork_hint = &*looked_up;
  }
  std::optional<storage::EdgesVertexAccessorResult> fork_edges;
  if (fork_hint != nullptr) fork_edges = expand(*fork_hint, storage::View::OLD);

  size_t const fork_count = fork_edges ? fork_edges->edges.size() : 0;
  reserve(adjacency.natives.size() + fork_count);

  for (auto const &edge : adjacency.natives) {
    if (!emit(edge)) return;
  }
  if (fork_count == 0) return;
  for (auto const &edge : fork_edges->edges) {
    if (!adjacency.hidden.empty() && adjacency.hidden.Contains(edge.edge_)) continue;
    if (!emit(edge)) return;
  }
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

// A fork vertex is emitted as the fork accessor (reads merge the branch's changes by gid) unless deleted on the branch.
void BranchContext::UnionVerticesIterable::Iterator::SeekNext() {
  while (hist_it_.has_value() && !(*hist_it_ == *hist_end_)) {
    const storage::VertexAccessor fork_vertex = **hist_it_;
    ++(*hist_it_);
    if (ctx_->IsForkVertexDeleted(fork_vertex, view_)) continue;
    current_ = fork_vertex;
    done_ = false;
    return;
  }

  while (diff_it_.has_value() && !(*diff_it_ == *diff_end_)) {
    const storage::VertexAccessor native = **diff_it_;
    DMG_ASSERT(!ctx_->IsStub(native), "the diff engine's vertices_ holds only native vertices.");
    ++(*diff_it_);
    // The diff iterator checked `native` when it stepped onto it, one emitted row ago: check again, as main's scan
    // checks the vertex it steps onto.
    if (!native.IsVisible(view_)) continue;
    current_ = native;
    done_ = false;
    return;
  }

  current_.reset();
  done_ = true;
}

}  // namespace memgraph::versioning
