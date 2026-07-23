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

#pragma once

#include <optional>
#include <set>
#include <string>

#include "common_function_signatures.hpp"
#include "memory/db_arena_fwd.hpp"
#include "mg_procedure.h"
#include "storage/v2/access_type.hpp"
#include "storage/v2/async_indexer.hpp"
#include "storage/v2/commit_args.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/database_protector.hpp"
#include "storage/v2/description_store.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edges_chunked_iterable.hpp"
#include "storage/v2/edges_iterable.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/indices.hpp"
#include "storage/v2/indices/label_property_index_entry.hpp"
#include "storage/v2/indices/text_index.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/replication_client.hpp"
#include "storage/v2/replication/replication_storage_state.hpp"
#include "storage/v2/storage_error.hpp"
#include "storage/v2/ttl.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/vertices_chunked_iterable.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "utils/resource_lock.hpp"
#include "utils/synchronized_metadata_store.hpp"
#include "utils/worker_resume_event.hpp"

namespace memgraph::metrics {
struct DatabaseMetricHandles;
}  // namespace memgraph::metrics

namespace memgraph::utils {
class MemoryTracker;
}

namespace memgraph::storage {
class SharedAccessTimeout : public utils::BasicException {
 public:
  SharedAccessTimeout()
      : utils::BasicException(
            "Cannot get shared access to the storage. Try stopping other parallel queries. "
            "You can increase the timeout via SET DATABASE SETTINGS 'storage.access_timeout_sec' TO 'value'; "
            "or via the --storage-access-timeout-sec flag. Run SHOW CONFIG to return the current value. "
            "See: https://memgraph.com/docs/help-center/errors/transactions#storage-access-timeout") {}
  SPECIALIZE_GET_EXCEPTION_NAME(SharedAccessTimeout)
};

class UniqueAccessTimeout : public utils::BasicException {
 public:
  UniqueAccessTimeout()
      : utils::BasicException(
            "Cannot get unique access to the storage. Try stopping other parallel queries. "
            "Note: Periodic snapshots also hold storage access. "
            "You can increase the timeout via SET DATABASE SETTINGS 'storage.access_timeout_sec' TO 'value'; "
            "or via the --storage-access-timeout-sec flag. Run SHOW CONFIG to return the current value. "
            "See: https://memgraph.com/docs/help-center/errors/transactions#storage-access-timeout") {}
  SPECIALIZE_GET_EXCEPTION_NAME(UniqueAccessTimeout)
};

class ReadOnlyAccessTimeout : public utils::BasicException {
 public:
  ReadOnlyAccessTimeout()
      : utils::BasicException(
            "Cannot get read-only access to the storage. Try stopping other parallel queries. "
            "You can increase the timeout via SET DATABASE SETTINGS 'storage.access_timeout_sec' TO 'value'; "
            "or via the --storage-access-timeout-sec flag. Run SHOW CONFIG to return the current value. "
            "See: https://memgraph.com/docs/help-center/errors/transactions#storage-access-timeout") {}
  SPECIALIZE_GET_EXCEPTION_NAME(ReadOnlyAccessTimeout)
};

struct Transaction;
class EdgeAccessor;

// TODO: list status Populating/Ready
struct IndicesInfo {
  std::vector<LabelId> label;
  std::vector<LabelPropertyIndexEntry> label_properties;
  std::vector<EdgeTypeId> edge_type;
  std::vector<std::pair<EdgeTypeId, PropertyId>> edge_type_property;
  std::vector<PropertyId> edge_property;
  std::vector<TextIndexSpec> text_indices;
  std::vector<TextEdgeIndexSpec> text_edge_indices;
  std::vector<std::pair<LabelId, PropertyId>> point_label_property;
  std::vector<VectorIndexSpec> vector_indices_spec;
  std::vector<VectorEdgeIndexSpec> vector_edge_indices_spec;
};

struct ConstraintsInfo {
  std::vector<std::pair<LabelId, PropertyId>> existence;
  std::vector<std::pair<LabelId, std::set<PropertyId>>> unique;
  std::vector<std::tuple<LabelId, PropertyId, TypeConstraintKind>> type;
};

struct StorageInfo {
  uint64_t vertex_count;
  uint64_t edge_count;
  double average_degree;
  uint64_t memory_res;
  uint64_t peak_memory_res;
  uint64_t unreleased_delta_objects;
  uint64_t disk_usage;
  uint64_t label_indices;
  uint64_t label_property_indices;
  uint64_t text_indices;
  uint64_t vector_indices;
  uint64_t vector_edge_indices;
  uint64_t existence_constraints;
  uint64_t unique_constraints;
  uint64_t type_constraints;
  StorageMode storage_mode;
  IsolationLevel isolation_level;
  bool durability_snapshot_enabled;
  bool durability_wal_enabled;
  bool property_store_compression_enabled;
  utils::CompressionLevel property_store_compression_level;
  uint64_t schema_vertex_count;
  uint64_t schema_edge_count;

  friend bool operator==(const StorageInfo &, const StorageInfo &) = default;
};

// Single ordered list of StorageInfo's (de)serializable fields. Every persistence/wire path —
// the durability cold_stats JSON (Durability::StatsToJson/StatsFromJson) and the V3 SystemRecovery SLK
// (system_rpc.cpp Save/Load) — drives serialization through this one visitor, so adding a field to
// StorageInfo extends all of them at once instead of silently drifting (the field-drift hazard flagged
// in earlier reviews). `visit(key, ref)` is invoked once per field in declaration order; the caller's
// visitor handles scalars directly and enums via `if constexpr (std::is_enum_v<T>)`. Templated on Self so
// the SAME field list serves a const StorageInfo (save) and a mutable one (load).
template <typename Self, typename Visit>
void StorageInfoForEachField(Self &s, Visit &&visit) {
  visit("vertex_count", s.vertex_count);
  visit("edge_count", s.edge_count);
  visit("average_degree", s.average_degree);
  visit("memory_res", s.memory_res);
  visit("peak_memory_res", s.peak_memory_res);
  visit("unreleased_delta_objects", s.unreleased_delta_objects);
  visit("disk_usage", s.disk_usage);
  visit("label_indices", s.label_indices);
  visit("label_property_indices", s.label_property_indices);
  visit("text_indices", s.text_indices);
  visit("vector_indices", s.vector_indices);
  visit("vector_edge_indices", s.vector_edge_indices);
  visit("existence_constraints", s.existence_constraints);
  visit("unique_constraints", s.unique_constraints);
  visit("type_constraints", s.type_constraints);
  visit("storage_mode", s.storage_mode);
  visit("isolation_level", s.isolation_level);
  visit("durability_snapshot_enabled", s.durability_snapshot_enabled);
  visit("durability_wal_enabled", s.durability_wal_enabled);
  visit("property_store_compression_enabled", s.property_store_compression_enabled);
  visit("property_store_compression_level", s.property_store_compression_level);
  visit("schema_vertex_count", s.schema_vertex_count);
  visit("schema_edge_count", s.schema_edge_count);
}

// Hot/cold: the per-COLD-tenant recovery payload carried in SystemRecoveryReq V3 so a
// reconnecting/lagging replica converges to MAIN's authoritative {HOT ∪ COLD} set. Replaces the
// earlier two parallel (salient, stats) vectors; bundling them keeps the 1:1 pairing structural (no
// length-mismatch guard). Composed of storage:: types only, so it can sit in both the dbms and
// replication_handler signatures without a layer cycle. A resumed cold tenant trusts its own
// on-disk WAL/snapshot epoch (BuildDetached); no epoch is carried here (cold-tenant epoch machinery
// was intentionally removed — a cold tenant accumulates no divergent commits to reconcile).
struct ColdTenantRecovery {
  SalientConfig salient;
  StorageInfo stats{};  // value-init: a default-constructed recovery carries zeroed stats
};

static inline nlohmann::json ToJson(const StorageInfo &info) {
  nlohmann::json res;

  res["edges"] = info.edge_count;
  res["vertices"] = info.vertex_count;
  res["memory"] = info.memory_res;
  res["disk"] = info.disk_usage;
  res["label_indices"] = info.label_indices;
  res["label_prop_indices"] = info.label_property_indices;
  res["text_indices"] = info.text_indices;
  res["vector_indices"] = info.vector_indices;
  res["vector_edge_indices"] = info.vector_edge_indices;
  res["existence_constraints"] = info.existence_constraints;
  res["unique_constraints"] = info.unique_constraints;
  res["type_constraints"] = info.type_constraints;
  res["storage_mode"] = storage::StorageModeToString(info.storage_mode);
  res["isolation_level"] = storage::IsolationLevelToString(info.isolation_level);
  res["durability"] = {{"snapshot_enabled", info.durability_snapshot_enabled},
                       {"WAL_enabled", info.durability_wal_enabled}};
  res["property_store_compression_enabled"] = info.property_store_compression_enabled;
  res["property_store_compression_level"] = utils::CompressionLevelToString(info.property_store_compression_level);
  res["schema_vertex_count"] = info.schema_vertex_count;
  res["schema_edge_count"] = info.schema_edge_count;
  return res;
}

struct EdgeInfoForDeletion {
  std::unordered_set<Gid> partial_src_edge_ids{};
  std::unordered_set<Gid> partial_dest_edge_ids{};
  std::unordered_set<Vertex *> partial_src_vertices{};
  std::unordered_set<Vertex *> partial_dest_vertices{};
};

struct TTLReplicationArgs {
  bool is_main{true};
};

struct PlanInvalidator {
  virtual auto invalidate_for_timestamp_wrapper(std::function<bool(uint64_t)> func)
      -> std::function<bool(uint64_t)> = 0;
  virtual bool invalidate_now(std::function<bool()> func) = 0;
  virtual ~PlanInvalidator() = default;
};

struct PlanInvalidatorDefault : public PlanInvalidator {
  auto invalidate_for_timestamp_wrapper(std::function<bool(uint64_t)> func) -> std::function<bool(uint64_t)> override {
    return func;
  }

  bool invalidate_now(std::function<bool()> func) override { return func(); };
};

using PlanInvalidatorPtr = std::unique_ptr<PlanInvalidator>;

class Accessor;

class Storage {
  friend class ReplicationServer;
  friend class ReplicationStorageClient;
  friend class VectorIndex;

 public:
  Storage(Config config, StorageMode storage_mode, PlanInvalidatorPtr invalidator,
          metrics::DatabaseMetricHandles metric_handles = {}, memory::ArenaPool *db_arena_pool = nullptr,
          utils::MemoryTracker *db_embedding_memory_tracker = nullptr,
          std::function<std::unique_ptr<DatabaseProtector>()> database_protector_factory = nullptr);

  Storage(const Storage &) = delete;
  Storage(Storage &&) = delete;
  Storage &operator=(const Storage &) = delete;
  Storage &operator=(Storage &&) = delete;

  virtual ~Storage() = default;

  std::string name() const { return config_.salient.name.str(); }

  auto name_view() const { return config_.salient.name.str_view(); }

  auto uuid() const -> utils::UUID const & { return config_.salient.uuid; }

  auto uuid() -> utils::UUID & { return config_.salient.uuid; }

  // A storage is broken when it failed durability recovery on startup and was
  // brought up empty (see --storage-allow-recovery-failure). A broken storage
  // rejects data queries until recovered via RECOVER SNAPSHOT.
  bool IsBroken() const noexcept { return broken_.load(std::memory_order_acquire); }

  void SetBroken(bool value) noexcept { broken_.store(value, std::memory_order_release); }

  memory::ArenaPool *DbArenaPool() const noexcept { return db_arena_pool_; }

  using Accessor = memgraph::storage::Accessor;
  friend class memgraph::storage::Accessor;

  const std::string &LabelToName(LabelId label) const { return name_id_mapper_->IdToName(label.AsUint()); }

  const std::string &PropertyToName(PropertyId property) const { return name_id_mapper_->IdToName(property.AsUint()); }

  const std::string &EdgeTypeToName(EdgeTypeId edge_type) const {
    return name_id_mapper_->IdToName(edge_type.AsUint());
  }

  LabelId NameToLabel(const std::string_view name) const { return LabelId::FromUint(name_id_mapper_->NameToId(name)); }

  PropertyId NameToProperty(const std::string_view name) const {
    return PropertyId::FromUint(name_id_mapper_->NameToId(name));
  }

  std::optional<PropertyId> NameToPropertyIfExists(std::string_view name) const {
    const auto id = name_id_mapper_->NameToIdIfExists(name);
    if (!id) {
      return std::nullopt;
    }
    return PropertyId::FromUint(*id);
  }

  EdgeTypeId NameToEdgeType(const std::string_view name) const {
    return EdgeTypeId::FromUint(name_id_mapper_->NameToId(name));
  }

  StorageMode GetStorageMode() const noexcept;

  // True iff this storage's durability mode keeps BOTH periodic snapshots AND a WAL
  // chain — the precondition for hot/cold suspend (suspend tears down RAM and relies
  // on {snapshot + WAL} on disk to recover). PERIODIC_SNAPSHOT-only or DISABLED is
  // NOT suspendable.
  [[nodiscard]] bool IsDurabilityCompleteForSuspend() const {
    return config_.durability.snapshot_wal_mode == Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  }

  virtual void FreeMemory(std::unique_lock<utils::ResourceLock> main_guard, bool periodic) = 0;

  void FreeMemory() { FreeMemory(std::unique_lock{main_lock_, std::defer_lock}, false); }

  virtual std::unique_ptr<Accessor> Access(StorageAccessType rw_type,
                                           std::optional<IsolationLevel> override_isolation_level,
                                           std::optional<std::chrono::milliseconds> timeout) = 0;

  std::unique_ptr<Accessor> Access(StorageAccessType rw_type);

  virtual std::unique_ptr<Accessor> UniqueAccess(std::optional<IsolationLevel> override_isolation_level,
                                                 std::optional<std::chrono::milliseconds> timeout) = 0;

  std::unique_ptr<Accessor> UniqueAccess(std::optional<IsolationLevel> override_isolation_level);
  std::unique_ptr<Accessor> UniqueAccess();

  virtual std::unique_ptr<Accessor> ReadOnlyAccess(std::optional<IsolationLevel> override_isolation_level,
                                                   std::optional<std::chrono::milliseconds> timeout) = 0;

  std::unique_ptr<Accessor> ReadOnlyAccess(std::optional<IsolationLevel> override_isolation_level);
  std::unique_ptr<Accessor> ReadOnlyAccess();

  /// Non-blocking counterpart to Access(): returns std::nullopt (no transaction, no throw) if
  /// main_lock_ is not immediately available. WRITE/READ use a plain try_lock_shared; READ_ONLY
  /// uses a single-shot ReadOnlyPendingScope so a retrying caller keeps priority-over-new-WRITE.
  /// Base is a safe no-op stub (always nullopt, never touches main_lock_); InMemoryStorage
  /// overrides with the real implementation.
  virtual std::optional<std::unique_ptr<Accessor>> TryAccess(
      StorageAccessType rw_type, std::optional<IsolationLevel> override_isolation_level = std::nullopt);

  /// Non-blocking counterpart to UniqueAccess(); uses a single-shot UniquePendingScope so a
  /// retrying caller keeps writer-preference. See TryAccess() for the contract and safe-stub default.
  virtual std::optional<std::unique_ptr<Accessor>> TryUniqueAccess(
      std::optional<IsolationLevel> override_isolation_level = std::nullopt);

  /// Non-blocking counterpart to ReadOnlyAccess(). See TryAccess() for the contract and safe-stub default.
  virtual std::optional<std::unique_ptr<Accessor>> TryReadOnlyAccess(
      std::optional<IsolationLevel> override_isolation_level = std::nullopt);

  /// Whether this engine supports the coro-prepare park-acquire path (a caller-held pending scope
  /// kept across a retry campaign plus a real TryAccess*). False by default: DiskStorage never
  /// overrides TryAccess*, so parking against it would just spin to the deadline -- park is
  /// InMemory-only for v1. Overridden `true` in InMemoryStorage.
  virtual bool SupportsParkAcquire() const { return false; }

  enum class SetIsolationLevelError : uint8_t { DisabledForAnalyticalMode };

  std::expected<void, SetIsolationLevelError> SetIsolationLevel(IsolationLevel isolation_level);
  IsolationLevel GetIsolationLevel() const noexcept;

  /// Per-Storage wake event for a parked (coro-prepare) waiter blocked on `main_lock_`. ALL release
  /// sites (UNIQUE/READ_ONLY/WRITE/READ) must poke it via NotifyMainLockReleased().
  utils::WorkerResumeEvent &main_lock_resume_event() { return main_lock_resume_event_; }

  /// Wakes any parked waiter, only if the flag is on AND someone is parked (otherwise one relaxed
  /// load). MUST be called AFTER main_lock_ is actually released (C3), never while holding it.
  ///
  /// F5 fix: call for EVERY release mode, not just UNIQUE/READ_ONLY. Given main_lock_'s conflict
  /// matrix (UNIQUE excludes all; READ_ONLY excludes WRITE; READ/WRITE coexist), a parked
  /// READ_ONLY/UNIQUE waiter is unblocked when the last conflicting WRITE (or READ, for UNIQUE)
  /// holder releases -- so a plain READ/WRITE release can unblock a waiting acquirer of another
  /// mode. Only a genuinely parked waiter pays the extra wake-and-reprobe cost.
  void NotifyMainLockReleased();

  virtual StorageInfo GetBaseInfo() = 0;

  virtual StorageInfo GetInfo() = 0;

  size_t GetDescriptionCount() const { return description_store_.Size(); }

  virtual std::unordered_map<LabelId, uint64_t> GetLabelCounts() const = 0;

  virtual void UpdateLabelCount(LabelId label, int64_t change) = 0;

  virtual Transaction CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode) = 0;

  virtual void PrepareForNewEpoch() = 0;

  auto GetReplicaState(std::string_view name) const -> std::optional<replication::ReplicaState> {
    return repl_storage_state_.GetReplicaState(name);
  }

  // Snapshot-free read: the underlying `SynchronizedMetaDataStore::vectorize()`
  // takes a shared rwlock and returns a consistent copy of the set. The set
  // grows as new labels / edge-types are observed, and is cleared wholesale by
  // `InMemoryStorage::Clear()` on DROP GRAPH (which takes the matching write
  // lock). Callers therefore see either the pre-clear or post-clear contents
  // atomically; no storage accessor is needed.
  std::vector<EdgeTypeId> ListAllPossiblyPresentEdgeTypes() const;
  std::vector<LabelId> ListAllPossiblyPresentVertexLabels() const;

  /// Returns the current snapshot of active indices
  auto GetActiveIndices() const -> ActiveIndicesPtr { return indices_.active_indices_.ReadCopy(); }

  auto GetActiveConstraints() const -> ActiveConstraintsPtr { return constraints_.active_constraints_.ReadCopy(); }

  // Vector index counts are now accessed through ActiveIndices snapshots (shared_ptr + COW),
  // which provide both live counts and snapshot isolation.

  /// Check if async indexer is idle (no pending work)
  /// @return true if async indexer is idle, false if actively processing or has pending work
  /// @note For storage types without async indexing, this always returns true
  virtual bool IsAsyncIndexerIdle() const = 0;

  /// Check if async indexer thread has stopped
  /// @return true if async indexer thread has stopped (due to null protector or shutdown), false otherwise
  /// @note For storage types without async indexing, this always returns true
  virtual bool HasAsyncIndexerStopped() const = 0;

  virtual void StopAllBackgroundTasks() {
    stop_source.request_stop();

    ttl_.Shutdown();

    // Shutdown-latency/leak fix: drain any coro-prepare waiter still registered on THIS storage's
    // main_lock_resume_event_. A genuine park dual-registers the same ParkState in two registries
    // (the pool's DeadlineParkRegistry AND this event); only the pool side was ever drained, so an
    // undrained entry here kept its ParkState -- and the closure-captured Session and its
    // Gatekeeper<Database>::Accessor -- alive forever, pinning Gatekeeper::count_ > 0 and hanging
    // ~Gatekeeper() (5min cv wait) into a process-wide shutdown hang.
    //
    // Safe regardless of ordering vs the pool's drain: Drain() moves every entry out and invokes
    // on_resume only for one it still wins ClaimPark on, so an entry the pool already claimed is
    // pruned without a second (UAF-risking) resume. Once per DB at shutdown, so no flag gate.
    main_lock_resume_event_.Drain();
  }

  // TODO: make non-public
  ReplicationStorageState repl_storage_state_;

  // Main storage lock.
  // Accessors take a shared lock when starting, so it is possible to block
  // creation of new accessors by taking a unique lock. This is used when doing
  // operations on storage that affect the global state, for example index
  // creation.
  mutable utils::ResourceLock main_lock_;

  // Wake event for a coro-prepare waiter parked on main_lock_ (see main_lock_resume_event() /
  // NotifyMainLockReleased()). Pool-agnostic -- each waiter's ParkState carries its own closure.
  utils::WorkerResumeEvent main_lock_resume_event_;

  // Even though the edge count is already kept in the `edges_` SkipList, the
  // list is used only when properties are enabled for edges. Because of that we
  // keep a separate count of edges that is always updated. This counter is also used
  // for disk storage.
  std::atomic<uint64_t> edge_count_{0};

  // Set when durability recovery failed and the storage was brought up empty.
  std::atomic<bool> broken_{false};

  std::unique_ptr<NameIdMapper> name_id_mapper_;
  Config config_;

  // Transaction engine
  mutable utils::SpinLock engine_lock_;
  uint64_t timestamp_{kTimestampInitialId};
  uint64_t transaction_id_{kTransactionInitialId};

  IsolationLevel isolation_level_;
  StorageMode storage_mode_;
  memory::ArenaPool *db_arena_pool_{nullptr};

  metrics::DatabaseMetricHandles metric_handles_{};

  Indices indices_;
  Constraints constraints_;
  PlanInvalidatorPtr invalidator_;

  // Datastructures to provide fast retrieval of node-label and
  // edge-type related metadata.
  // Currently we should not remove any node-labels or edge-types even
  // if the set of given types are currently not present in the
  // database. This metadata is usually used by client side
  // applications that want to be aware of the kind of data that *may*
  // be present in the database.

  // TODO(gvolfing): check if this would be faster with flat_maps.
  utils::SynchronizedMetaDataStore<LabelId> stored_node_labels_;
  utils::SynchronizedMetaDataStore<EdgeTypeId> stored_edge_types_;

  std::atomic<uint64_t> vertex_id_{0};  // contains Vertex Gid that has not been used yet
  std::atomic<uint64_t> edge_id_{0};    // contains Edge Gid that has not been used yet

  // Mutable methods only safe if we have UniqueAccess to this storage
  EnumStore enum_store_;
  DescriptionStore description_store_;

  SchemaInfo schema_info_;

  // A way to tell async operation to stop
  std::stop_source stop_source;

  ttl::TTL ttl_{this, metric_handles_.deleted_nodes, metric_handles_.deleted_edges};  // TTL handler

  // Factory function to create database protectors for async operations
  // Used by async indexer and TTL system to get protectors for committing transactions
  std::function<std::unique_ptr<DatabaseProtector>()> database_protector_factory_;

  /// Creates a database protector for async operations
  /// @return DatabaseProtector instance for committing async transactions
  /// @note Never returns nullptr - always provides a valid protector
  auto make_database_protector() const -> std::unique_ptr<DatabaseProtector> { return database_protector_factory_(); }

  /// Gets the database protector factory for copying to new storage instances
  /// @return Copy of the factory function for preservation during storage transitions
  auto get_database_protector_factory() const -> std::function<std::unique_ptr<DatabaseProtector>()> {
    return database_protector_factory_;
  }
};

inline std::ostream &operator<<(std::ostream &os, StorageAccessType type) {
  switch (type) {
    using enum StorageAccessType;
    case NO_ACCESS:
      return os << "NO_ACCESS";
    case UNIQUE:
      return os << "UNIQUE";
    case WRITE:
      return os << "WRITE";
    case READ:
      return os << "READ";
    case READ_ONLY:
      return os << "READ_ONLY";
  }
  return os;
}

class Accessor {
 public:
  static constexpr struct SharedAccess {
  } shared_access;

  static constexpr struct UniqueAccess {
  } unique_access;

  static constexpr struct ReadOnlyAccess {
  } read_only_access;

  Accessor(SharedAccess /* tag */, Storage *storage, IsolationLevel isolation_level, StorageMode storage_mode,
           StorageAccessType rw_type = StorageAccessType::WRITE,
           std::optional<std::chrono::milliseconds> timeout = std::nullopt);
  Accessor(UniqueAccess /* tag */, Storage *storage, IsolationLevel isolation_level, StorageMode storage_mode,
           std::optional<std::chrono::milliseconds> timeout = std::nullopt);
  Accessor(ReadOnlyAccess /* tag */, Storage *storage, IsolationLevel isolation_level, StorageMode storage_mode,
           std::optional<std::chrono::milliseconds> timeout = std::nullopt);

  // "Owning" variants of the tags above, used by Storage::Try*Access once a non-blocking probe has
  // ALREADY acquired main_lock_: they adopt the already-locked guard by move rather than acquiring,
  // so construction can never block or fail (the reverse of ReleaseUniqueGuard()'s hand-off).
  static constexpr struct SharedAccessOwning {
  } shared_access_owning;

  static constexpr struct UniqueAccessOwning {
  } unique_access_owning;

  static constexpr struct ReadOnlyAccessOwning {
  } read_only_access_owning;

  Accessor(SharedAccessOwning /* tag */, Storage *storage, IsolationLevel isolation_level, StorageMode storage_mode,
           StorageAccessType rw_type, utils::SharedResourceLockGuard already_locked_guard);
  Accessor(UniqueAccessOwning /* tag */, Storage *storage, IsolationLevel isolation_level, StorageMode storage_mode,
           std::unique_lock<utils::ResourceLock> already_locked_guard);
  Accessor(ReadOnlyAccessOwning /* tag */, Storage *storage, IsolationLevel isolation_level, StorageMode storage_mode,
           utils::SharedResourceLockGuard already_locked_guard);

  Accessor(const Accessor &) = delete;
  Accessor &operator=(const Accessor &) = delete;
  Accessor &operator=(Accessor &&other) = delete;

  Accessor(Accessor &&other) noexcept;

  // Defined out-of-line (storage.cpp): on a UNIQUE/READ_ONLY accessor, explicitly releases the
  // guard here (before implicit member-destruction) so NotifyMainLockReleased() runs strictly after
  // main_lock_ is released (C3). READ/WRITE accessors skip this and fall through to the ordinary
  // implicit release -- zero extra cost on that hot path.
  virtual ~Accessor();

  StorageAccessType original_access_type() const { return original_access_type_; }

  StorageAccessType type() const {
    if (unique_guard_.owns_lock()) {
      return UNIQUE;
    }
    if (storage_guard_.owns_lock()) {
      switch (storage_guard_.type()) {
        case utils::SharedResourceLockGuard::Type::WRITE:
          return WRITE;
        case utils::SharedResourceLockGuard::Type::READ:
          return READ;
        case utils::SharedResourceLockGuard::Type::READ_ONLY:
          return READ_ONLY;
      }
    }
    return NO_ACCESS;
  }

  /// Hands off this accessor's UNIQUE lock to the caller (valid only while it holds UNIQUE). The
  /// returned unique_lock becomes the sole owner; the accessor no longer unlocks at destruction.
  /// Move the SAME unique_lock rather than adopting the mutex into a second one -- two adopters
  /// would each unlock once and double-unlock.
  auto ReleaseUniqueGuard() -> std::unique_lock<utils::ResourceLock> {
    MG_ASSERT(unique_guard_.owns_lock(), "ReleaseUniqueGuard requires the accessor to hold the UNIQUE lock");
    return std::move(unique_guard_);
  }

  virtual VertexAccessor CreateVertex() = 0;

  virtual std::optional<VertexAccessor> FindVertex(Gid gid, View view) = 0;

  virtual VerticesIterable Vertices(View view) = 0;

  virtual VerticesIterable Vertices(LabelId label, View view) = 0;

  virtual VerticesIterable Vertices(LabelId label, std::span<storage::PropertyPath const> properties,
                                    std::span<storage::PropertyValueRange const> property_ranges, View view,
                                    IndexOrder order = IndexOrder::ASC) = 0;

  virtual VerticesIterable Vertices(LabelId label, std::span<storage::PropertyPath const> properties, View view) {
    return Vertices(label, properties, std::vector(properties.size(), storage::PropertyValueRange::IsNotNull()), view);
  };

  virtual VerticesChunkedIterable ChunkedVertices(View view, size_t num_chunks) = 0;

  virtual VerticesChunkedIterable ChunkedVertices(LabelId label, View view, size_t num_chunks) = 0;

  virtual VerticesChunkedIterable ChunkedVertices(LabelId label, std::span<storage::PropertyPath const> properties,
                                                  std::span<storage::PropertyValueRange const> property_ranges,
                                                  View view, size_t num_chunks, IndexOrder order = IndexOrder::ASC) = 0;

  virtual std::optional<EdgeAccessor> FindEdge(Gid gid, View view) = 0;

  virtual std::optional<EdgeAccessor> FindEdge(Gid edge_gid, Gid from_vertex_gid, View view) = 0;

  virtual EdgesIterable Edges(EdgeTypeId edge_type, View view) = 0;

  virtual EdgesIterable Edges(EdgeTypeId edge_type, PropertyId property, View view) = 0;

  virtual EdgesIterable Edges(EdgeTypeId edge_type, PropertyId property, const PropertyValue &value, View view) = 0;

  virtual EdgesIterable Edges(EdgeTypeId edge_type, PropertyId property,
                              const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                              const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) = 0;

  virtual EdgesIterable Edges(PropertyId property, View view) = 0;

  virtual EdgesIterable Edges(PropertyId property, const PropertyValue &value, View view) = 0;

  virtual EdgesIterable Edges(PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                              const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) = 0;

  virtual EdgesChunkedIterable ChunkedEdges(EdgeTypeId edge_type, View view, size_t num_chunks) = 0;

  virtual EdgesChunkedIterable ChunkedEdges(EdgeTypeId edge_type, PropertyId property, View view,
                                            size_t num_chunks) = 0;

  virtual EdgesChunkedIterable ChunkedEdges(EdgeTypeId edge_type, PropertyId property,
                                            const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                            const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view,
                                            size_t num_chunks) = 0;

  virtual EdgesChunkedIterable ChunkedEdges(PropertyId property, View view, size_t num_chunks) = 0;

  virtual EdgesChunkedIterable ChunkedEdges(PropertyId property, const PropertyValue &value, View view,
                                            size_t num_chunks) = 0;

  virtual EdgesChunkedIterable ChunkedEdges(PropertyId property,
                                            const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                            const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view,
                                            size_t num_chunks) = 0;

  virtual auto DeleteVertex(VertexAccessor *vertex) -> Result<std::optional<VertexAccessor>>;

  virtual auto DetachDeleteVertex(VertexAccessor *vertex)
      -> Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>>;

  virtual auto DetachDelete(std::vector<VertexAccessor *> nodes, std::vector<EdgeAccessor *> edges, bool detach)
      -> Result<std::optional<std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>>>;

  virtual uint64_t ApproximateVertexCount() const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label) const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyPath const> properties) const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyPath const> properties,
                                          std::span<PropertyValue const> values) const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, std::span<PropertyPath const> properties,
                                          std::span<PropertyValueRange const> bounds) const = 0;

  virtual uint64_t ApproximateEdgeCount() const = 0;

  virtual uint64_t ApproximateEdgeCount(EdgeTypeId edge_type) const = 0;

  virtual uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property) const = 0;

  virtual uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property,
                                        const PropertyValue &value) const = 0;

  virtual uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property,
                                        const std::optional<utils::Bound<PropertyValue>> &lower,
                                        const std::optional<utils::Bound<PropertyValue>> &upper) const = 0;

  virtual uint64_t ApproximateEdgeCount(PropertyId property) const = 0;

  virtual uint64_t ApproximateEdgeCount(PropertyId property, const PropertyValue &value) const = 0;

  virtual uint64_t ApproximateEdgeCount(PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower,
                                        const std::optional<utils::Bound<PropertyValue>> &upper) const = 0;

  virtual std::optional<uint64_t> ApproximateVerticesPointCount(LabelId label, PropertyId property) const = 0;

  virtual std::optional<uint64_t> ApproximateVerticesVectorCount(std::string_view index_name) const = 0;

  virtual std::optional<uint64_t> ApproximateEdgesVectorCount(std::string_view index_name) const = 0;

  virtual std::optional<uint64_t> ApproximateVerticesTextCount(std::string_view index_name) const = 0;

  virtual std::optional<uint64_t> ApproximateEdgesTextCount(std::string_view index_name) const = 0;

  virtual auto GetIndexStats(const storage::LabelId &label) const -> std::optional<storage::LabelIndexStats> = 0;

  virtual auto GetIndexStats(const storage::LabelId &label, std::span<storage::PropertyPath const> properties) const
      -> std::optional<storage::LabelPropertyIndexStats> = 0;

  virtual void SetIndexStats(const storage::LabelId &label, const LabelIndexStats &stats) = 0;

  virtual void SetIndexStats(const storage::LabelId &label, std::span<storage::PropertyPath const> property,
                             const LabelPropertyIndexStats &stats) = 0;

  virtual auto DeleteLabelPropertyIndexStats(const storage::LabelId &label)
      -> std::vector<std::pair<LabelId, std::vector<PropertyPath>>> = 0;

  virtual bool DeleteLabelIndexStats(const storage::LabelId &label) = 0;

  virtual Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type) = 0;

  virtual std::optional<EdgeAccessor> FindEdge(Gid gid, View view, EdgeTypeId edge_type, VertexAccessor *from_vertex,
                                               VertexAccessor *to_vertex) = 0;

  virtual auto DeleteEdge(EdgeAccessor *edge) -> Result<std::optional<EdgeAccessor>>;

  virtual bool LabelIndexReady(LabelId label) const = 0;

  virtual bool LabelPropertyIndexReady(LabelId label, std::span<PropertyPath const> properties) const = 0;

  virtual bool LabelPropertyIndexExists(LabelId label, std::span<PropertyPath const> properties) const = 0;

  auto RelevantLabelPropertiesIndicesInfo(std::span<LabelId const> labels,
                                          std::span<PropertyPath const> properties) const
      -> std::vector<LabelPropertiesIndicesInfo> {
    return transaction_.active_indices_->label_properties_->RelevantLabelPropertiesIndicesInfo(labels, properties);
  };

  virtual bool EdgeTypeIndexReady(EdgeTypeId edge_type) const = 0;

  virtual bool EdgeTypePropertyIndexReady(EdgeTypeId edge_type, PropertyId property) const = 0;

  virtual bool EdgePropertyIndexReady(PropertyId property) const = 0;

  virtual bool EdgePropertyIndexExists(PropertyId property) const = 0;

  bool TextIndexExists(const std::string &index_name) const {
    return transaction_.active_indices_->text_->IndexExists(index_name);
  }

  std::vector<TextSearchResult> TextIndexSearch(const std::string &index_name, const std::string &search_query,
                                                text_search_mode search_mode, const TextSearchConfig &config) const {
    return transaction_.active_indices_->text_->Search(index_name, search_query, search_mode, config, transaction_);
  }

  std::string TextIndexAggregate(const std::string &index_name, const std::string &search_query,
                                 const std::string &aggregation_query) const {
    return transaction_.active_indices_->text_->Aggregate(index_name, search_query, aggregation_query);
  }

  std::string TextEdgeIndexAggregate(const std::string &index_name, const std::string &search_query,
                                     const std::string &aggregation_query) const {
    return transaction_.active_indices_->text_edge_->Aggregate(index_name, search_query, aggregation_query);
  }

  std::vector<TextEdgeSearchResult> SearchEdgeTextIndex(const std::string &index_name, const std::string &search_query,
                                                        text_search_mode search_mode,
                                                        const TextSearchConfig &config) const {
    return transaction_.active_indices_->text_edge_->Search(
        index_name, search_query, search_mode, config, transaction_);
  }

  virtual bool PointIndexExists(LabelId label, PropertyId property) const = 0;

  virtual IndicesInfo ListAllIndices() const = 0;

  virtual ConstraintsInfo ListAllConstraints() const = 0;

  virtual void DropAllIndexes() = 0;

  virtual void DropAllConstraints() = 0;

  // NOLINTNEXTLINE(google-default-arguments)
  virtual std::expected<void, StorageManipulationError> PrepareForCommitPhase(CommitArgs commit_args) = 0;

  // NOLINTNEXTLINE(google-default-arguments)
  virtual std::expected<void, StorageManipulationError> PeriodicCommit(CommitArgs commit_args) = 0;

  virtual void Abort() = 0;

  virtual void FinalizeTransaction() = 0;

  // Stable per-query id; preserved across PERIODIC COMMIT.
  std::optional<uint64_t> GetStartTimestamp() const;

  utils::QueryMemoryTracker &GetTransactionMemoryTracker();

  void AdvanceCommand();

  const std::string &LabelToName(LabelId label) const { return storage_->LabelToName(label); }

  const std::string &PropertyToName(PropertyId property) const { return storage_->PropertyToName(property); }

  const std::string &EdgeTypeToName(EdgeTypeId edge_type) const { return storage_->EdgeTypeToName(edge_type); }

  LabelId NameToLabel(std::string_view name) { return storage_->NameToLabel(name); }

  PropertyId NameToProperty(std::string_view name) { return storage_->NameToProperty(name); }

  std::optional<PropertyId> NameToPropertyIfExists(std::string_view name) const {
    return storage_->NameToPropertyIfExists(name);
  }

  EdgeTypeId NameToEdgeType(std::string_view name) { return storage_->NameToEdgeType(name); }

  StorageMode GetCreationStorageMode() const noexcept;

  std::string id() const { return storage_->name(); }

  auto id_view() const { return storage_->name_view(); }

  auto const &uuid() const { return storage_->uuid(); }

  virtual std::expected<void, StorageIndexDefinitionError> CreateIndex(LabelId label,
                                                                       CheckCancelFunction cancel_check) = 0;

  virtual std::expected<void, StorageIndexDefinitionError> CreateIndex(LabelId label, PropertiesPaths properties,
                                                                       IndexOrder order,
                                                                       CheckCancelFunction cancel_check) = 0;

  virtual std::expected<void, StorageIndexDefinitionError> CreateIndex(EdgeTypeId edge_type,
                                                                       CheckCancelFunction cancel_check) = 0;

  virtual std::expected<void, StorageIndexDefinitionError> CreateIndex(EdgeTypeId edge_type, PropertyId property,
                                                                       CheckCancelFunction cancel_check) = 0;

  virtual std::expected<void, StorageIndexDefinitionError> CreateGlobalEdgeIndex(PropertyId property,
                                                                                 CheckCancelFunction cancel_check) = 0;

  // Convenience overloads with default cancel check
  auto CreateIndex(LabelId label) -> std::expected<void, StorageIndexDefinitionError> {
    return CreateIndex(label, neverCancel);
  }

  auto CreateIndex(LabelId label, PropertiesPaths properties, IndexOrder order = IndexOrder::ASC)
      -> std::expected<void, StorageIndexDefinitionError> {
    return CreateIndex(label, std::move(properties), order, neverCancel);
  }

  auto CreateIndex(EdgeTypeId edge_type) -> std::expected<void, StorageIndexDefinitionError> {
    return CreateIndex(edge_type, neverCancel);
  }

  auto CreateIndex(EdgeTypeId edge_type, PropertyId property) -> std::expected<void, StorageIndexDefinitionError> {
    return CreateIndex(edge_type, property, neverCancel);
  }

  auto CreateGlobalEdgeIndex(PropertyId property) -> std::expected<void, StorageIndexDefinitionError> {
    return CreateGlobalEdgeIndex(property, neverCancel);
  }

  virtual std::expected<void, StorageIndexDefinitionError> DropIndex(LabelId label) = 0;

  virtual std::expected<void, StorageIndexDefinitionError> DropIndex(
      LabelId label, std::vector<storage::PropertyPath> &&properties,
      std::optional<IndexOrder> order = std::nullopt) = 0;

  virtual std::expected<void, StorageIndexDefinitionError> DropIndex(EdgeTypeId edge_type) = 0;

  virtual std::expected<void, StorageIndexDefinitionError> DropIndex(EdgeTypeId edge_type, PropertyId property) = 0;

  virtual std::expected<void, StorageIndexDefinitionError> DropGlobalEdgeIndex(PropertyId property) = 0;

  virtual std::expected<void, storage::StorageIndexDefinitionError> CreatePointIndex(storage::LabelId label,
                                                                                     storage::PropertyId property) = 0;

  virtual std::expected<void, storage::StorageIndexDefinitionError> DropPointIndex(storage::LabelId label,
                                                                                   storage::PropertyId property) = 0;

  std::expected<void, storage::StorageIndexDefinitionError> CreateTextIndex(const TextIndexSpec &text_index_info);

  std::expected<void, storage::StorageIndexDefinitionError> DropTextIndex(const std::string &index_name);

  std::expected<void, storage::StorageIndexDefinitionError> CreateTextEdgeIndex(
      const TextEdgeIndexSpec &text_edge_index_info);

  virtual std::expected<void, storage::StorageIndexDefinitionError> CreateVectorIndex(VectorIndexSpec spec) = 0;

  virtual std::expected<void, storage::StorageIndexDefinitionError> DropVectorIndex(std::string_view index_name) = 0;

  virtual utils::small_vector<uint64_t> GetVectorIndexIdsForVertex(Vertex *vertex, PropertyId property) = 0;

  virtual utils::small_vector<float> GetVectorFromVectorIndex(Vertex *vertex, std::string_view index_name) const = 0;

  virtual std::expected<void, storage::StorageIndexDefinitionError> CreateVectorEdgeIndex(VectorEdgeIndexSpec spec) = 0;

  virtual std::expected<void, StorageExistenceConstraintDefinitionError> CreateExistenceConstraint(
      LabelId label, PropertyId property) = 0;

  virtual std::expected<void, StorageExistenceConstraintDroppingError> DropExistenceConstraint(LabelId label,
                                                                                               PropertyId property) = 0;

  virtual std::expected<UniqueConstraints::CreationStatus, StorageUniqueConstraintDefinitionError>
  CreateUniqueConstraint(LabelId label, const std::set<PropertyId> &properties) = 0;

  virtual UniqueConstraints::DeletionStatus DropUniqueConstraint(LabelId label,
                                                                 const std::set<PropertyId> &properties) = 0;

  virtual std::expected<void, StorageExistenceConstraintDefinitionError> CreateTypeConstraint(
      LabelId label, PropertyId property, TypeConstraintKind type) = 0;

  virtual std::expected<void, StorageExistenceConstraintDroppingError> DropTypeConstraint(LabelId label,
                                                                                          PropertyId property,
                                                                                          TypeConstraintKind type) = 0;

  virtual void DropGraph() = 0;

  auto GetTransaction() -> Transaction * { return std::addressof(transaction_); }

  auto GetEnumStoreUnique() -> EnumStore & {
    DMG_ASSERT(unique_guard_.owns_lock());
    return storage_->enum_store_;
  }

  auto GetEnumStoreShared() const -> EnumStore const & { return storage_->enum_store_; }

  auto CreateEnum(std::string_view name, std::span<std::string const> values)
      -> std::expected<EnumTypeId, EnumStorageError> {
    auto res = storage_->enum_store_.RegisterEnum(name, values);
    if (res) {
      transaction_.md_deltas.emplace_back(MetadataDelta::enum_create, res.value());
    }
    return res;
  }

  auto EnumAlterAdd(std::string_view name, std::string_view value)
      -> std::expected<storage::Enum, storage::EnumStorageError> {
    auto res = storage_->enum_store_.AddValue(name, value);
    if (res) {
      transaction_.md_deltas.emplace_back(MetadataDelta::enum_alter_add, res.value());
    }
    return res;
  }

  auto EnumAlterUpdate(std::string_view name, std::string_view old_value, std::string_view new_value)
      -> std::expected<storage::Enum, storage::EnumStorageError> {
    auto res = storage_->enum_store_.UpdateValue(name, old_value, new_value);
    if (res) {
      transaction_.md_deltas.emplace_back(MetadataDelta::enum_alter_update, res.value(), std::string{old_value});
    }
    return res;
  }

  auto ShowEnums() { return storage_->enum_store_.AllRegistered(); }

  void SetLabelDescription(std::span<std::string const> label_names, std::string_view desc) {
    auto labels = ResolveLabels(label_names);
    storage_->description_store_.SetLabel(labels, desc);
    transaction_.md_deltas.emplace_back(MetadataDelta::description_set,
                                        DescriptionTargetKind::LABEL,
                                        std::move(labels),
                                        EdgeTypeId{},
                                        PropertyId{},
                                        std::string{desc});
  }

  bool DeleteLabelDescription(std::span<std::string const> label_names) {
    auto labels = ResolveLabels(label_names);
    auto deleted = storage_->description_store_.DeleteLabel(labels);
    if (deleted) {
      transaction_.md_deltas.emplace_back(MetadataDelta::description_delete,
                                          DescriptionTargetKind::LABEL,
                                          std::move(labels),
                                          EdgeTypeId{},
                                          PropertyId{});
    }
    return deleted;
  }

  std::optional<std::string> GetLabelDescription(std::span<std::string const> label_names) const {
    return storage_->description_store_.GetLabel(ResolveLabels(label_names));
  }

  void SetEdgeTypeDescription(std::string_view name, std::string_view desc) {
    auto edge_type = storage_->NameToEdgeType(name);
    storage_->description_store_.SetEdgeType(edge_type, desc);
    transaction_.md_deltas.emplace_back(MetadataDelta::description_set,
                                        DescriptionTargetKind::EDGE_TYPE,
                                        std::vector<LabelId>{},
                                        edge_type,
                                        PropertyId{},
                                        std::string{desc});
  }

  bool DeleteEdgeTypeDescription(std::string_view name) {
    auto edge_type = storage_->NameToEdgeType(name);
    auto deleted = storage_->description_store_.DeleteEdgeType(edge_type);
    if (deleted) {
      transaction_.md_deltas.emplace_back(MetadataDelta::description_delete,
                                          DescriptionTargetKind::EDGE_TYPE,
                                          std::vector<LabelId>{},
                                          edge_type,
                                          PropertyId{});
    }
    return deleted;
  }

  std::optional<std::string> GetEdgeTypeDescription(std::string_view name) const {
    return storage_->description_store_.GetEdgeType(storage_->NameToEdgeType(name));
  }

  void SetLabelPropertyDescription(std::span<std::string const> label_qualifier, std::string_view prop_name,
                                   std::string_view desc) {
    auto labels = ResolveLabels(label_qualifier);
    auto prop = storage_->NameToProperty(prop_name);
    storage_->description_store_.SetLabelProperty(labels, prop, desc);
    transaction_.md_deltas.emplace_back(MetadataDelta::description_set,
                                        DescriptionTargetKind::LABEL_PROPERTY,
                                        std::move(labels),
                                        EdgeTypeId{},
                                        prop,
                                        std::string{desc});
  }

  bool DeleteLabelPropertyDescription(std::span<std::string const> label_qualifier, std::string_view prop_name) {
    auto labels = ResolveLabels(label_qualifier);
    auto prop = storage_->NameToProperty(prop_name);
    bool deleted = storage_->description_store_.DeleteLabelProperty(labels, prop);
    if (deleted) {
      transaction_.md_deltas.emplace_back(MetadataDelta::description_delete,
                                          DescriptionTargetKind::LABEL_PROPERTY,
                                          std::move(labels),
                                          EdgeTypeId{},
                                          prop);
    }
    return deleted;
  }

  std::optional<std::string> GetLabelPropertyDescription(std::span<std::string const> label_qualifier,
                                                         std::string_view prop_name) const {
    return storage_->description_store_.GetLabelProperty(ResolveLabels(label_qualifier),
                                                         storage_->NameToProperty(prop_name));
  }

  void SetEdgeTypePropertyDescription(std::string_view edge_type_name, std::string_view prop_name,
                                      std::string_view desc) {
    auto edge_type = storage_->NameToEdgeType(edge_type_name);
    auto prop = storage_->NameToProperty(prop_name);
    storage_->description_store_.SetEdgeTypeProperty(edge_type, prop, desc);
    transaction_.md_deltas.emplace_back(MetadataDelta::description_set,
                                        DescriptionTargetKind::EDGE_TYPE_PROPERTY,
                                        std::vector<LabelId>{},
                                        edge_type,
                                        prop,
                                        std::string{desc});
  }

  bool DeleteEdgeTypePropertyDescription(std::string_view edge_type_name, std::string_view prop_name) {
    auto edge_type = storage_->NameToEdgeType(edge_type_name);
    auto prop = storage_->NameToProperty(prop_name);
    bool deleted = storage_->description_store_.DeleteEdgeTypeProperty(edge_type, prop);
    if (deleted) {
      transaction_.md_deltas.emplace_back(MetadataDelta::description_delete,
                                          DescriptionTargetKind::EDGE_TYPE_PROPERTY,
                                          std::vector<LabelId>{},
                                          edge_type,
                                          prop);
    }
    return deleted;
  }

  std::optional<std::string> GetEdgeTypePropertyDescription(std::string_view edge_type_name,
                                                            std::string_view prop_name) const {
    return storage_->description_store_.GetEdgeTypeProperty(storage_->NameToEdgeType(edge_type_name),
                                                            storage_->NameToProperty(prop_name));
  }

  void SetPropertyDescription(std::string_view prop_name, std::string_view desc) {
    auto prop = storage_->NameToProperty(prop_name);
    storage_->description_store_.SetProperty(prop, desc);
    transaction_.md_deltas.emplace_back(MetadataDelta::description_set,
                                        DescriptionTargetKind::PROPERTY,
                                        std::vector<LabelId>{},
                                        EdgeTypeId{},
                                        prop,
                                        std::string{desc});
  }

  bool DeletePropertyDescription(std::string_view prop_name) {
    auto prop = storage_->NameToProperty(prop_name);
    bool deleted = storage_->description_store_.DeleteProperty(prop);
    if (deleted) {
      transaction_.md_deltas.emplace_back(MetadataDelta::description_delete,
                                          DescriptionTargetKind::PROPERTY,
                                          std::vector<LabelId>{},
                                          EdgeTypeId{},
                                          prop);
    }
    return deleted;
  }

  std::optional<std::string> GetPropertyDescription(std::string_view prop_name) const {
    return storage_->description_store_.GetProperty(storage_->NameToProperty(prop_name));
  }

  void SetEdgeTypePatternDescription(std::span<std::string const> from_label_names, std::string_view edge_type_name,
                                     std::span<std::string const> to_label_names, std::string_view desc) {
    auto from_labels = ResolveLabels(from_label_names);
    auto edge_type = storage_->NameToEdgeType(edge_type_name);
    auto to_labels = ResolveLabels(to_label_names);
    storage_->description_store_.SetEdgeTypePattern(from_labels, edge_type, to_labels, desc);
    transaction_.md_deltas.emplace_back(MetadataDelta::description_set,
                                        DescriptionTargetKind::EDGE_TYPE_PATTERN,
                                        std::vector<LabelId>{},
                                        edge_type,
                                        PropertyId{},
                                        std::string{desc},
                                        std::move(from_labels),
                                        std::move(to_labels));
  }

  bool DeleteEdgeTypePatternDescription(std::span<std::string const> from_label_names, std::string_view edge_type_name,
                                        std::span<std::string const> to_label_names) {
    auto from_labels = ResolveLabels(from_label_names);
    auto edge_type = storage_->NameToEdgeType(edge_type_name);
    auto to_labels = ResolveLabels(to_label_names);
    auto deleted = storage_->description_store_.DeleteEdgeTypePattern(from_labels, edge_type, to_labels);
    if (deleted) {
      transaction_.md_deltas.emplace_back(MetadataDelta::description_delete,
                                          DescriptionTargetKind::EDGE_TYPE_PATTERN,
                                          std::vector<LabelId>{},
                                          edge_type,
                                          PropertyId{},
                                          std::move(from_labels),
                                          std::move(to_labels));
    }
    return deleted;
  }

  std::optional<std::string> GetEdgeTypePatternDescription(std::span<std::string const> from_label_names,
                                                           std::string_view edge_type_name,
                                                           std::span<std::string const> to_label_names) const {
    return storage_->description_store_.GetEdgeTypePattern(
        ResolveLabels(from_label_names), storage_->NameToEdgeType(edge_type_name), ResolveLabels(to_label_names));
  }

  void SetEdgeTypePatternPropertyDescription(std::span<std::string const> from_label_names,
                                             std::string_view edge_type_name,
                                             std::span<std::string const> to_label_names, std::string_view prop_name,
                                             std::string_view desc) {
    auto from_labels = ResolveLabels(from_label_names);
    auto edge_type = storage_->NameToEdgeType(edge_type_name);
    auto to_labels = ResolveLabels(to_label_names);
    auto prop = storage_->NameToProperty(prop_name);
    storage_->description_store_.SetEdgeTypePatternProperty(from_labels, edge_type, to_labels, prop, desc);
    transaction_.md_deltas.emplace_back(MetadataDelta::description_set,
                                        DescriptionTargetKind::EDGE_TYPE_PATTERN_PROPERTY,
                                        std::vector<LabelId>{},
                                        edge_type,
                                        prop,
                                        std::string{desc},
                                        std::move(from_labels),
                                        std::move(to_labels));
  }

  bool DeleteEdgeTypePatternPropertyDescription(std::span<std::string const> from_label_names,
                                                std::string_view edge_type_name,
                                                std::span<std::string const> to_label_names,
                                                std::string_view prop_name) {
    auto from_labels = ResolveLabels(from_label_names);
    auto edge_type = storage_->NameToEdgeType(edge_type_name);
    auto to_labels = ResolveLabels(to_label_names);
    auto prop = storage_->NameToProperty(prop_name);
    auto deleted = storage_->description_store_.DeleteEdgeTypePatternProperty(from_labels, edge_type, to_labels, prop);
    if (deleted) {
      transaction_.md_deltas.emplace_back(MetadataDelta::description_delete,
                                          DescriptionTargetKind::EDGE_TYPE_PATTERN_PROPERTY,
                                          std::vector<LabelId>{},
                                          edge_type,
                                          prop,
                                          std::move(from_labels),
                                          std::move(to_labels));
    }
    return deleted;
  }

  std::optional<std::string> GetEdgeTypePatternPropertyDescription(std::span<std::string const> from_label_names,
                                                                   std::string_view edge_type_name,
                                                                   std::span<std::string const> to_label_names,
                                                                   std::string_view prop_name) const {
    return storage_->description_store_.GetEdgeTypePatternProperty(ResolveLabels(from_label_names),
                                                                   storage_->NameToEdgeType(edge_type_name),
                                                                   ResolveLabels(to_label_names),
                                                                   storage_->NameToProperty(prop_name));
  }

  void SetDatabaseDescription(std::string_view desc) {
    storage_->description_store_.SetDatabase(desc);
    transaction_.md_deltas.emplace_back(MetadataDelta::description_set,
                                        DescriptionTargetKind::DATABASE,
                                        std::vector<LabelId>{},
                                        EdgeTypeId{},
                                        PropertyId{},
                                        std::string{desc});
  }

  bool DeleteDatabaseDescription() {
    bool deleted = storage_->description_store_.DeleteDatabase();
    if (deleted) {
      transaction_.md_deltas.emplace_back(MetadataDelta::description_delete,
                                          DescriptionTargetKind::DATABASE,
                                          std::vector<LabelId>{},
                                          EdgeTypeId{},
                                          PropertyId{});
    }
    return deleted;
  }

  std::optional<std::string> GetDatabaseDescription() const { return storage_->description_store_.GetDatabase(); }

  std::vector<DescriptionEntry> GetAllDescriptions() const { return storage_->description_store_.GetAll(); }

 private:
  std::vector<LabelId> ResolveLabels(std::span<std::string const> names) const {
    return names | ranges::views::transform([this](std::string_view name) { return storage_->NameToLabel(name); }) |
           ranges::to<std::vector>();
  }

 public:
  auto GetEnumValue(std::string_view name, std::string_view value) const -> std::expected<Enum, EnumStorageError> {
    return storage_->enum_store_.ToEnum(name, value);
  }

  auto GetEnumValue(std::string_view enum_str) -> std::expected<Enum, EnumStorageError> {
    return storage_->enum_store_.ToEnum(enum_str);
  }

  virtual auto PointVertices(LabelId label, PropertyId property, CoordinateReferenceSystem crs,
                             PropertyValue const &point_value, PropertyValue const &boundary_value,
                             PointDistanceCondition condition) -> PointIterable = 0;

  virtual auto PointVertices(LabelId label, PropertyId property, CoordinateReferenceSystem crs,
                             PropertyValue const &bottom_left, PropertyValue const &top_right,
                             WithinBBoxCondition condition) -> PointIterable = 0;

  virtual std::vector<std::tuple<VertexAccessor, double, double>> VectorIndexSearchOnNodes(
      const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector) = 0;

  virtual std::vector<std::tuple<EdgeAccessor, double, double>> VectorIndexSearchOnEdges(
      const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector) = 0;

  virtual std::vector<VectorIndexInfo> ListAllVectorIndices() const = 0;

  virtual std::vector<VectorEdgeIndexInfo> ListAllVectorEdgeIndices() const = 0;

  auto GetNameIdMapper() const -> NameIdMapper * { return storage_->name_id_mapper_.get(); }

  bool CheckIndicesAreReady(IndicesCollection const &required_indices) const {
    return transaction_.active_indices_->CheckIndicesAreReady(required_indices);
  }

  bool TransactionHasSerializationError() const { return transaction_.has_serialization_error; }

  // TTL methods
  ttl::TTL &ttl() { return storage_->ttl_; }

#ifdef MG_ENTERPRISE
  // TTL management methods
  virtual void StartTtl(TTLReplicationArgs repl_args = {}) = 0;
  virtual void DisableTtl(TTLReplicationArgs repl_args = {}) = 0;
  virtual void StopTtl() = 0;
  virtual void ConfigureTtl(const storage::ttl::TtlInfo &ttl_info, TTLReplicationArgs repl_args = {}) = 0;
  virtual storage::ttl::TtlInfo GetTtlConfig() const = 0;
#endif
 protected:
  Storage *storage_;
  utils::SharedResourceLockGuard storage_guard_;
  std::unique_lock<utils::ResourceLock> unique_guard_;  // TODO: Split the accessor into Shared/Unique
  /// IMPORTANT: transaction_ has to be constructed after the guards (so that destruction is in correct order)
  Transaction transaction_;
  std::optional<uint64_t> commit_timestamp_;
  bool is_transaction_active_;
  StorageAccessType original_access_type_;

  // Detach delete private methods
  Result<std::optional<std::unordered_set<Vertex *>>> PrepareDeletableNodes(
      const std::vector<VertexAccessor *> &vertices);
  EdgeInfoForDeletion PrepareDeletableEdges(const std::unordered_set<Vertex *> &vertices,
                                            const std::vector<EdgeAccessor *> &edges, bool detach) noexcept;
  Result<std::optional<std::vector<EdgeAccessor>>> ClearEdgesOnVertices(
      const std::unordered_set<Vertex *> &vertices, std::unordered_set<Gid> &deleted_edge_ids,
      std::optional<SchemaInfo::ModifyingAccessor> &schema_acc);
  Result<std::optional<std::vector<EdgeAccessor>>> DetachRemainingEdges(
      EdgeInfoForDeletion info, std::unordered_set<Gid> &partially_detached_edge_ids,
      std::optional<SchemaInfo::ModifyingAccessor> &schema_acc);
  Result<std::vector<VertexAccessor>> TryDeleteVertices(const std::unordered_set<Vertex *> &vertices,
                                                        std::optional<SchemaInfo::ModifyingAccessor> &schema_acc);
  void MarkEdgeAsDeleted(Edge *edge);

 private:
  StorageMode creation_storage_mode_;
};

}  // namespace memgraph::storage
