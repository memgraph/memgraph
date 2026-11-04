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
#include <map>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "memory/db_arena.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_value.hpp"
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
  // Attribute the diff engine's allocations to main's tenant arena pool.
  const memory::DbArenaScope db_arena_scope{main.DbArenaPool()};

  // Pin main at fork_ts first so an unpinned fork fails before the diff engine is built. This accessor's READ guard
  // also blocks a concurrent CREATE ENUM while the enum store is mirrored below.
  auto hist_exp = main.HistoricalAccess(fork_ts);
  if (!hist_exp.has_value()) {
    return std::unexpected(
        BuildError{.message = fmt::format(
                       "Cannot open the branch: its fork point (timestamp {}) is no longer retained.", fork_ts)});
  }
  std::unique_ptr<storage::Storage::Accessor> historical = std::move(*hist_exp);

  // Shares main's NameIdMapper (historical vertices carry main's ids; a separate mapper would misdecode them).
  // Config is copied wholesale so future salient flags are inherited.
  storage::Config config = main.config_;
  // Fresh UUID avoids aliasing main in any registry. The NAME is main's too, read live from main below, because
  // DbAccessor::DatabaseName() (auth, FGA, SHOW, DUMP) reads it and main can be renamed.
  config.salient.uuid = utils::UUID{};
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
  auto diff_engine = std::make_unique<storage::InMemoryStorage>(config,
                                                                std::nullopt,
                                                                std::make_unique<storage::PlanInvalidatorDefault>(),
                                                                metric_handles,
                                                                nullptr,
                                                                main.DbArenaPool(),
                                                                main.DbEmbeddingMemoryTracker(),
                                                                main.GetSharedNameIdMapper());
  diff_engine->ReportNameOf(main);

  // Done before the core is visible to any session, so the diff engine never needs UNIQUE access afterwards.
  MirrorMainEnumsIntoDiffEngine(*diff_engine, historical->GetEnumStoreShared());

  // After the fork pin: every gid visible at fork_ts is below these floors, every later allocation (main or any
  // branch) is at or above them.
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

// The diff engine's deltas and indexes point at stubs, so it goes before stub_index_ (declared, hence destroyed, last).
BranchCore::~BranchCore() { diff_engine_.reset(); }

void BranchCore::RecordVertexDeleted(storage::Gid gid) {
  change_filters_.RecordVertexDeleted(gid);
  std::lock_guard lock(touched_mutex_);
  if (deleted_exact_.insert(gid.AsUint()).second) ++touched_version_;
}

void BranchCore::RecordLabelTouched(storage::LabelId label, storage::Gid gid) {
  // Before the set insert and the commit: readers that see the commit see the bit.
  label_touched_gate_[GateWord(label.AsUint())].fetch_or(GateBit(label.AsUint()), std::memory_order_release);
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

void BranchCore::RecordPropertyTouched(storage::PropertyId pid, storage::Gid gid) {
  property_touched_gate_[GateWord(pid.AsUint())].fetch_or(GateBit(pid.AsUint()), std::memory_order_release);
  std::lock_guard lock(touched_mutex_);
  if (property_touched_[pid].insert(gid.AsUint()).second) ++touched_version_;
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
  std::lock_guard lock(touched_mutex_);
  if (edge_property_touched_[pid].try_emplace(edge_gid.AsUint(), from_gid.AsUint()).second) ++touched_version_;
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
  auto core = BranchCore::Build(main, fork_ts);
  if (!core) return std::unexpected(std::move(core.error()));
  return std::make_unique<BranchContext>(std::move(*core));
}

std::expected<void, BranchContext::BuildError> BranchContext::OpenHistorical() {
  EndTransaction();
  auto hist_exp = core_->main().HistoricalAccess(core_->fork_ts());
  if (!hist_exp.has_value()) {
    return std::unexpected(
        BuildError{.message = fmt::format(
                       "The branch is no longer available: its fork point (timestamp {}) is no longer retained.",
                       core_->fork_ts())});
  }
  historical_ = std::move(*hist_exp);
  return {};
}

void BranchContext::SetCurrentDiffTxn(storage::Storage::Accessor *diff_txn) {
  current_diff_txn_ = diff_txn;
  if (diff_txn != nullptr) StartOverlayTransaction();
}

void BranchContext::StartOverlayTransaction() {
  auto const *txn = current_diff_txn_->GetTransaction();
  overlay_txn_ = std::make_unique<OverlayTransaction>(txn->start_timestamp, txn->transaction_id, txn->isolation_level);
}

void BranchContext::EndTransaction() noexcept {
  if (overlay_txn_ != nullptr) {
    if (!overlay_txn_->finished()) core_->overlay_store().Abort(*overlay_txn_);
    overlay_txn_.reset();
  }
  current_diff_txn_ = nullptr;
  historical_.reset();
  stub_cursor_.Reset();
}

void BranchContext::SyncOverlayCommand() noexcept {
  auto const command_id = current_diff_txn_->GetTransaction()->command_id;
  while (overlay_txn_->command_id() < command_id) overlay_txn_->AdvanceCommand();
}

const OverlayVersion *BranchContext::ReadVersion(uint64_t key, storage::View view) {
  MG_ASSERT(overlay_txn_ != nullptr, "BranchContext: no overlay transaction -- no branch transaction is open.");
  SyncOverlayCommand();
  return core_->overlay_store().Read(key, *overlay_txn_, view);
}

template <typename Mutator>
storage::Result<bool> BranchContext::WriteOverlay(uint64_t key, Mutator &&mutate) {
  MG_ASSERT(overlay_txn_ != nullptr, "BranchContext: no overlay transaction -- no branch transaction is open.");
  SyncOverlayCommand();
  auto written = core_->overlay_store().Write(key, *overlay_txn_, std::forward<Mutator>(mutate));
  if (!written) {
    if (written.error() == storage::Error::SERIALIZATION_ERROR) {
      current_diff_txn_->GetTransaction()->has_serialization_error = true;
    }
    return std::unexpected{written.error()};
  }
  return *written;
}

std::vector<uint64_t> BranchContext::OverlayWrittenGids() const {
  if (overlay_txn_ == nullptr) return {};
  std::vector<uint64_t> gids;
  for (auto const key : overlay_txn_->writes()) {
    if (!IsEdgeKey(key)) gids.push_back(key);
  }
  return gids;
}

void BranchContext::MarkMainObjectBranched(storage::Vertex *main_vertex) {
  auto guard = std::unique_lock{main_vertex->lock};
  main_vertex->SetBranched(true);
}

std::expected<storage::VertexAccessor, BranchContext::CowError> BranchContext::CowVertex(storage::Gid gid) {
  // MG_ASSERT not DMG_ASSERT: null current_diff_txn_ is an invariant break, not a debug-only condition (applies to
  // every such assert below).
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::CowVertex: no current diff transaction set -- "
            "CurrentDB must call OpenHistorical() and SetCurrentDiffTxn() before any query "
            "runs against a checked-out branch.");

  if (IsForkVertexGid(gid) && IsVertexDeleted(gid, storage::View::NEW)) {
    return std::unexpected(
        CowError{fmt::format("Cannot copy-on-write vertex {}: already deleted on this branch.", gid.AsUint())});
  }

  if (auto existing = FindStub(gid, storage::View::NEW)) return *existing;

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

std::expected<void, BranchContext::CowError> BranchContext::PrepareForkWrite(storage::Gid gid) {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::PrepareForkWrite: no current diff transaction set.");
  if (IsVertexDeleted(gid, storage::View::NEW)) {
    return std::unexpected(
        CowError{fmt::format("Cannot copy-on-write vertex {}: already deleted on this branch.", gid.AsUint())});
  }
  auto fork = FindForkVertex(gid);
  if (!fork) {
    return std::unexpected(
        CowError{fmt::format("Cannot copy-on-write vertex {}: not found in the branch's fork-state base either -- "
                             "this should be unreachable (callers resolve a vertex before mutating it).",
                             gid.AsUint())});
  }
  // Over-marking is safe: branched() is a hint, not a claim of divergence.
  if (!fork->vertex_->branched()) MarkMainObjectBranched(fork->vertex_);
  return {};
}

std::optional<storage::EdgeAccessor> BranchContext::FindForkEdge(storage::Gid edge_gid, storage::Gid from_gid) {
  return historical().FindEdge(edge_gid, from_gid, storage::View::OLD);
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

bool BranchContext::IsVertexDeleted(storage::Gid gid, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::IsVertexDeleted: no current diff transaction set.");
  if (!IsForkVertexGid(gid)) return false;
  if (!MayHaveVertexDeleted(gid)) return false;  // the delete is only written after RecordVertexDeleted
  auto const *version = ReadVersion(gid.AsUint(), view);
  return version != nullptr && version->deleted();
}

bool BranchContext::IsForkVertexDeleted(const storage::VertexAccessor &fork_vertex, storage::View view) {
  if (!core_->MayHaveAnyVertexDeleted() || !fork_vertex.vertex_->branched()) return false;
  return IsVertexDeleted(fork_vertex.Gid(), view);
}

bool BranchContext::IsForkEdgeHidden(const storage::EdgeAccessor &fork_edge, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::IsForkEdgeHidden: no current diff transaction set.");
  auto const edge_gid = fork_edge.Gid();
  if (!IsForkEdgeGid(edge_gid)) return false;
  if (IsForkEdgeDeleted(edge_gid, view)) return true;
  if (!core_->MayHaveAnyVertexDeleted()) return false;
  auto const from = fork_edge.FromVertex();
  if (IsForkVertexDeleted(from, view)) return true;
  auto const to = fork_edge.ToVertex();
  return to.Gid() != from.Gid() && IsForkVertexDeleted(to, view);
}

bool BranchContext::IsForkEdgeDeleted(storage::Gid edge_gid, storage::View view) {
  if (!MayHaveForkEdgeDeleted(edge_gid)) return false;
  auto const *version = ReadVersion(EdgeKey(edge_gid.AsUint()), view);
  return version != nullptr && version->deleted();
}

const OverlayVersion *BranchContext::ReadOverlayVersion(storage::Gid gid, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::ReadOverlayVersion: no current diff transaction set.");
  if (!IsForkVertexGid(gid)) return nullptr;
  return ReadVersion(gid.AsUint(), view);
}

bool BranchContext::HasVertexOverlay(storage::Gid gid) {
  return ReadVersion(gid.AsUint(), storage::View::NEW) != nullptr;
}

const OverlayVersion *BranchContext::ReadEdgeOverlayVersion(storage::Gid edge_gid, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::ReadEdgeOverlayVersion: no current diff transaction set.");
  if (!IsForkEdgeGid(edge_gid)) return nullptr;
  return ReadVersion(EdgeKey(edge_gid.AsUint()), view);
}

namespace {

// What a write mutator reports besides "changed": the error that made it refuse.
using MutateError = std::optional<storage::Error>;

using PropertyMap = std::map<storage::PropertyId, storage::PropertyValue>;

std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>> UpdateChanges(
    const PropertyMap &existing, const PropertyMap &properties) {
  std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>> changes;
  changes.reserve(properties.size());
  for (const auto &[pid, new_value] : properties) {
    auto it = existing.find(pid);
    changes.emplace_back(pid, it != existing.end() ? it->second : storage::PropertyValue{}, new_value);
  }
  return changes;
}

// Storage's UpdateProperties leaves the untouched old properties merged into the caller's map; mirror that.
void MergeOldBack(PropertyMap &existing, PropertyMap &properties) {
  for (auto &[pid, old_value] : existing) properties.emplace(pid, std::move(old_value));
}

PropertyMap MergedFromState(const OverlayState &state, const PropertyMap &fork_props) {
  auto merged = fork_props;
  for (auto const pid : state.removed_pids) merged.erase(pid);
  for (auto &[key, value] : state.properties.Properties()) merged.insert_or_assign(key, std::move(value));
  return merged;
}

// Applies one change (Null = remove). A removal is recorded only for a property the fork vertex has.
void ApplyChange(OverlayState &state, storage::PropertyId pid, const storage::PropertyValue &value, bool fork_has) {
  if (!value.IsNull()) {
    state.SetProperty(pid, value);
  } else if (fork_has) {
    state.RemoveProperty(pid);
  } else {
    state.properties.SetProperty(pid, storage::PropertyValue{});
  }
}

bool ForkHasProperty(const PropertyMap &fork_props, storage::PropertyId pid) {
  auto const it = fork_props.find(pid);
  return it != fork_props.end() && !it->second.IsNull();
}

}  // namespace

storage::Result<bool> BranchContext::OverlayToggleLabel(storage::Gid gid, storage::LabelId label, bool add) {
  auto fork = FindForkVertex(gid);
  if (!fork) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  auto in_fork = fork->HasLabel(label, storage::View::OLD);
  if (!in_fork) return std::unexpected{in_fork.error()};

  MutateError error;
  auto written = WriteOverlay(gid.AsUint(), [&](OverlayState &state) {
    if (state.deleted) {
      error = storage::Error::DELETED_OBJECT;
      return false;
    }
    bool const has = *in_fork ? !std::ranges::contains(state.removed_labels, label)
                              : std::ranges::contains(state.added_labels, label);
    if (has == add) return false;
    // Touching a label the fork vertex has only hides or un-hides it; otherwise the overlay adds or drops it.
    if (add) {
      *in_fork ? state.UnhideLabel(label) : state.AddLabel(label);
    } else {
      *in_fork ? state.RemoveLabel(label) : state.DropAddedLabel(label);
    }
    return true;
  });
  if (error) return std::unexpected{*error};
  return written;
}

storage::Result<bool> BranchContext::OverlayAddLabel(storage::Gid gid, storage::LabelId label) {
  return OverlayToggleLabel(gid, label, true);
}

storage::Result<bool> BranchContext::OverlayRemoveLabel(storage::Gid gid, storage::LabelId label) {
  return OverlayToggleLabel(gid, label, false);
}

storage::Result<storage::PropertyValue> BranchContext::OverlaySetPropertyAt(uint64_t key, storage::PropertyId pid,
                                                                            const storage::PropertyValue &value,
                                                                            const storage::PropertyValue &fork_value) {
  MutateError error;
  storage::PropertyValue old_value;
  auto written = WriteOverlay(key, [&](OverlayState &state) {
    if (state.deleted) {
      error = storage::Error::DELETED_OBJECT;
      return false;
    }
    auto own = state.properties.GetProperty(pid);
    if (!own.IsNull()) {
      old_value = std::move(own);
    } else if (!std::ranges::contains(state.removed_pids, pid)) {
      old_value = fork_value;
    }
    ApplyChange(state, pid, value, !fork_value.IsNull());
    return true;
  });
  if (error) return std::unexpected{*error};
  if (!written) return std::unexpected{written.error()};
  return old_value;
}

storage::Result<bool> BranchContext::OverlayInitPropertiesAt(
    uint64_t key, std::map<storage::PropertyId, storage::PropertyValue> &properties,
    const std::map<storage::PropertyId, storage::PropertyValue> &fork_props) {
  MutateError error;
  auto written = WriteOverlay(key, [&](OverlayState &state) {
    if (state.deleted) {
      error = storage::Error::DELETED_OBJECT;
      return false;
    }
    if (!MergedFromState(state, fork_props).empty()) return false;
    for (auto const &[pid, value] : properties) ApplyChange(state, pid, value, ForkHasProperty(fork_props, pid));
    return true;
  });
  if (error) return std::unexpected{*error};
  return written;
}

storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
BranchContext::OverlayUpdatePropertiesAt(uint64_t key,
                                         std::map<storage::PropertyId, storage::PropertyValue> &properties,
                                         const std::map<storage::PropertyId, storage::PropertyValue> &fork_props) {
  MutateError error;
  std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>> changes;
  auto written = WriteOverlay(key, [&](OverlayState &state) {
    if (state.deleted) {
      error = storage::Error::DELETED_OBJECT;
      return false;
    }
    auto existing = MergedFromState(state, fork_props);
    changes = UpdateChanges(existing, properties);
    for (auto const &[pid, value] : properties) ApplyChange(state, pid, value, ForkHasProperty(fork_props, pid));
    MergeOldBack(existing, properties);
    return true;
  });
  if (error) return std::unexpected{*error};
  if (!written) return std::unexpected{written.error()};
  return changes;
}

storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> BranchContext::OverlayClearPropertiesAt(
    uint64_t key, const std::map<storage::PropertyId, storage::PropertyValue> &fork_props) {
  MutateError error;
  PropertyMap existing;
  auto written = WriteOverlay(key, [&](OverlayState &state) {
    if (state.deleted) {
      error = storage::Error::DELETED_OBJECT;
      return false;
    }
    existing = MergedFromState(state, fork_props);
    state.properties.ClearProperties();
    state.removed_pids.clear();
    for (auto const &[pid, _] : fork_props) state.removed_pids.push_back(pid);  // ascending
    return true;
  });
  if (error) return std::unexpected{*error};
  if (!written) return std::unexpected{written.error()};
  return existing;
}

storage::Result<storage::PropertyValue> BranchContext::OverlaySetProperty(storage::Gid gid, storage::PropertyId key,
                                                                          const storage::PropertyValue &value) {
  auto fork = FindForkVertex(gid);
  if (!fork) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  auto fork_value = fork->GetProperty(key, storage::View::OLD);
  if (!fork_value) return std::unexpected{fork_value.error()};
  return OverlaySetPropertyAt(gid.AsUint(), key, value, *fork_value);
}

storage::Result<bool> BranchContext::OverlayInitProperties(
    storage::Gid gid, std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  auto fork = FindForkVertex(gid);
  if (!fork) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  auto fork_props = fork->Properties(storage::View::OLD);
  if (!fork_props) return std::unexpected{fork_props.error()};
  return OverlayInitPropertiesAt(gid.AsUint(), properties, *fork_props);
}

storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
BranchContext::OverlayUpdateProperties(storage::Gid gid,
                                       std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  auto fork = FindForkVertex(gid);
  if (!fork) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  auto fork_props = fork->Properties(storage::View::OLD);
  if (!fork_props) return std::unexpected{fork_props.error()};
  return OverlayUpdatePropertiesAt(gid.AsUint(), properties, *fork_props);
}

storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> BranchContext::OverlayClearProperties(
    storage::Gid gid) {
  auto fork = FindForkVertex(gid);
  if (!fork) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  auto fork_props = fork->Properties(storage::View::OLD);
  if (!fork_props) return std::unexpected{fork_props.error()};
  return OverlayClearPropertiesAt(gid.AsUint(), *fork_props);
}

storage::Result<bool> BranchContext::OverlayMarkDeleted(storage::Gid gid) {
  MG_ASSERT(current_diff_txn_ != nullptr, "BranchContext::OverlayMarkDeleted: no current diff transaction set.");
  if (IsVertexDeleted(gid, storage::View::NEW)) return false;
  if (auto prepared = PrepareForkWrite(gid); !prepared) return std::unexpected{storage::Error::NONEXISTENT_OBJECT};
  RecordVertexDeleted(gid);
  // The delete is a label and property change for the read gates, and flags the adjacency fast path.
  RecordVertexChange(gid, BranchChangeKind::kLabel);
  RecordVertexChange(gid, BranchChangeKind::kProperty);
  RecordEdgeChange(gid);
  auto written = WriteOverlay(gid.AsUint(), [](OverlayState &state) {
    if (state.deleted) return false;
    state.MarkDeleted();
    return true;
  });
  // The flag is published; now look for a branch-created edge attached to the vertex (see CreateNativeEdge).
  if (written && *written && !CanDeleteAnchor(gid)) return std::unexpected{storage::Error::SERIALIZATION_ERROR};
  return written;
}

// Pairs with the overlay check in CreateNativeEdge: whichever of "edge attached to the anchor" and "delete flag
// published" happens second sees the other. The lookup must take the stub shard's lock (a cursor could answer a miss
// lock-free), which orders the flag before any later insert of the anchor; an anchor found is ordered by its own lock.
bool BranchContext::CanDeleteAnchor(storage::Gid gid) {
  storage::Vertex *const anchor = core_->stub_index().FindStub(gid.AsUint());
  if (anchor == nullptr) return true;
  std::shared_lock guard{anchor->lock};
  auto *const txn = current_diff_txn_->GetTransaction();
  if (!storage::PrepareForWrite(txn, anchor)) return false;
  // Non-sequential edge deltas can leave a committed older head over a delta this snapshot cannot see, so the head
  // alone is not enough. The chain is intact while this transaction is active.
  for (const storage::Delta *delta = anchor->delta(); delta != nullptr;
       delta = delta->next.load(std::memory_order_acquire)) {
    const auto ts = delta->commit_info->timestamp.load(std::memory_order_acquire);
    if (ts != txn->transaction_id && ts >= txn->start_timestamp) {
      txn->has_serialization_error = true;
      return false;
    }
  }
  return true;
}

storage::VertexAccessor BranchContext::ForkIdentity(const storage::VertexAccessor &v) {
  if (!IsStub(v) || historical_ == nullptr) return v;
  auto fork = FindForkVertex(v.Gid());
  DMG_ASSERT(fork.has_value(), "Fork vertex missing for stub {}", v.Gid().AsUint());
  return fork ? *fork : v;
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
  if (IsForkVertexGid(gid)) return std::nullopt;  // fork anchors live in the stub index only
  return current_diff_txn_->FindVertex(gid, view);
}

std::optional<storage::VertexAccessor> BranchContext::ResolveVertex(storage::Gid gid, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::ResolveVertex: no current diff transaction set -- "
            "CurrentDB must call OpenHistorical() and SetCurrentDiffTxn() before any query "
            "runs against a checked-out branch.");
  if (IsForkVertexGid(gid)) {
    if (IsVertexDeleted(gid, view)) return std::nullopt;
    return FindForkVertex(gid);
  }
  return FindStub(gid, view);
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

  // Diff side first: it holds only branch-native edges. Fork edges come from the historical pass below, in fork
  // identity (readers overlay the branch's changes on them by gid).
  if (IsForkVertexGid(vertex_gid) && MayHaveVertexDeleted(vertex_gid)) {
    if (auto const *version = ReadVersion(vertex_gid.AsUint(), view); version != nullptr && version->deleted()) {
      if (vertex_deleted != nullptr) *vertex_deleted = true;
      return result;
    }
  }
  if (auto diff_vertex = FindStub(vertex_gid, view)) {
    auto maybe_result =
        out ? diff_vertex->OutEdges(view, edge_types, nullptr) : diff_vertex->InEdges(view, edge_types, nullptr);
    if (maybe_result.has_value()) {
      result.reserve(maybe_result->edges.size());
      for (auto edge : maybe_result->edges) {
        // A branch-created edge on a fork vertex whose other end the branch deleted is gone with it.
        if (IsVertexDeleted((out ? edge.ToVertex() : edge.FromVertex()).Gid(), view)) continue;
        result.push_back(edge);
      }
    }
  }

  if (auto hist_vertex = FindForkVertex(vertex_gid)) {
    auto maybe_result = out ? hist_vertex->OutEdges(storage::View::OLD, edge_types, nullptr)
                            : hist_vertex->InEdges(storage::View::OLD, edge_types, nullptr);
    if (maybe_result.has_value()) {
      // A deleted fork edge flags both endpoints' adjacency, so a clear flag skips the per-edge probes.
      bool const edges_may_be_deleted = MayHaveEdgeChange(vertex_gid);
      result.reserve(result.size() + maybe_result->edges.size());
      for (auto edge : maybe_result->edges) {
        if (edges_may_be_deleted && IsForkEdgeDeleted(edge.Gid(), view)) continue;
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

// Fork vertices are emitted as the fork accessor (readers overlay branch changes by gid) unless deleted on the branch;
// fork gids are all below the diff engine's, so the diff side follows once the fork side is exhausted.
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
    ++(*diff_it_);
    done_ = false;
    return;
  }

  current_.reset();
  done_ = true;
}

}  // namespace memgraph::versioning
