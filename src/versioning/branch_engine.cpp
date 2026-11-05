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
#include <utility>

#include "memory/db_arena.hpp"
#include "storage/v2/edge_ref.hpp"
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

}  // namespace

std::expected<std::shared_ptr<BranchCore>, BranchCore::BuildError> BranchCore::Build(
    storage::InMemoryStorage &main, uint64_t fork_ts, std::optional<RecordedFloors> recorded) {
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

  // Read after the fork pin: every gid visible at fork_ts is below these floors, every later allocation (main's or a
  // branch's) at or above. Below them may also be main's post-fork objects, which fork_ts does not see.
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

// ---- Adjacency

storage::Result<std::vector<storage::EdgeAccessor>> BranchContext::Edges(const storage::VertexAccessor &vertex,
                                                                         storage::EdgeDirection direction,
                                                                         storage::View view) {
  bool const out = direction == storage::EdgeDirection::OUT;
  if (vertex.storage_ == &diff_engine()) {
    // A native vertex's edges are all native.
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

BranchContext::StubAdjacency BranchContext::ReadStubAdjacency(storage::Vertex *stub, storage::EdgeDirection direction,
                                                              storage::View view) {
  StubAdjacency adjacency;
  auto const stub_accessor = StubAccessor(stub);
  auto const expanded =
      direction == storage::EdgeDirection::OUT ? stub_accessor.OutEdges(view) : stub_accessor.InEdges(view);
  // Callers ruled out a deleted stub, and a delta-less stub is visible at every view.
  DMG_ASSERT(expanded.has_value(), "expanding a stub failed.");
  if (!expanded) return adjacency;
  for (auto const &edge : expanded->edges) {
    if (!storage::IsReservedId(edge.EdgeType())) {
      adjacency.natives.push_back(edge);
      continue;
    }
    // A marker entry holds its edge's stub, which carries the fork edge the fork's adjacency holds.
    auto const fork = EdgeStubIndex::ForkOf(edge.edge_.ptr);
    DMG_ASSERT(fork.has_value(), "a marker's edge stub is created with its fork edge.");
    if (fork) adjacency.hidden.Add(*fork);
  }
  adjacency.hidden.Seal();
  return adjacency;
}

std::vector<storage::EdgeAccessor> BranchContext::ResolveEdges(storage::Gid vertex_gid,
                                                               storage::EdgeDirection direction, storage::View view,
                                                               bool *vertex_deleted) {
  auto &t = txn();
  if (vertex_deleted != nullptr) *vertex_deleted = false;
  bool const out = direction == storage::EdgeDirection::OUT;
  std::vector<storage::EdgeAccessor> result;
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

  // A native edge to a branch-deleted fork vertex cannot exist: the delete conflicts with the edge's creation on that
  // vertex's stub (stock first-writer-wins), and DETACH DELETE removes its native edges first.
  auto const found = stubs().Find(vertex_gid.AsUint(), t.stub_cursor);
  HiddenForkEdges hidden;
  if (found.stub != nullptr) {
    if (core_->MayHaveVertexDeleted(vertex_gid) && StubDeleted(found.stub, view)) {
      if (vertex_deleted != nullptr) *vertex_deleted = true;
      return result;
    }
    auto adjacency = ReadStubAdjacency(found.stub, direction, view);
    result = std::move(adjacency.natives);
    hidden = std::move(adjacency.hidden);
  }

  if (auto const hist_vertex = ForkAccessor(found.fork, vertex_gid)) {
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
