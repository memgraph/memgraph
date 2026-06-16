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

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "kvstore/kvstore.hpp"

namespace memgraph::storage {

// A single overlay change recorded against the master graph for one version. Overlay deltas are
// *forward* operations (what the version did), applied to master at query time and unwound afterwards.
// Object/label/property/edge-type ids are stored as raw integers (the caller converts to/from the
// strongly-typed ids); `properties` is an opaque, already-serialized PropertyStore buffer (a single
// property for Set*Property, the full property set for Create*).
enum class OverlayOp : uint8_t {
  kCreateVertex,
  kDeleteVertex,
  kAddLabel,
  kRemoveLabel,
  kSetVertexProperty,
  kCreateEdge,
  kDeleteEdge,
  kSetEdgeProperty,
};

struct OverlayDelta {
  OverlayOp op{};
  uint64_t gid{0};               // the vertex (or edge) being changed
  uint32_t label_id{0};          // kAddLabel / kRemoveLabel
  uint32_t property_id{0};       // kSet*Property
  uint32_t edge_type_id{0};      // kCreateEdge / kDeleteEdge
  uint64_t from_gid{0};          // kCreateEdge
  uint64_t to_gid{0};            // kCreateEdge
  std::string properties;        // opaque PropertyStore buffer (kSet*Property, kCreate*)
  std::vector<uint32_t> labels;  // kCreateVertex

  // --- Provenance: who/when produced this delta. Same value across all deltas captured from one query. ---
  uint64_t txn_start_timestamp{0};  // MVCC logical start timestamp of the producing transaction
  uint64_t ledger_time_ns{0};       // wall-clock UTC nanoseconds-since-epoch when appended to the ledger
  std::string query;                // the originating (unstripped) query text
};

// Persistent, ordered op-log of a single version's overlay, backed by its own RocksDB instance under
// <db_dir>/versions/<name>/. Records are iterated in insertion order, which is exactly what
// `SHOW CHANGES` dumps and what the replay engine applies to master.
//
// NOTE: opening a store takes RocksDB's exclusive directory lock; only one VersionDeltaStore may be
// open per path at a time. Callers currently open transiently per operation.
class VersionDeltaStore {
 public:
  // Opens (creating if necessary) the version's RocksDB instance at `path`.
  explicit VersionDeltaStore(std::filesystem::path path);

  VersionDeltaStore(VersionDeltaStore &&) noexcept = default;
  VersionDeltaStore &operator=(VersionDeltaStore &&) noexcept = default;
  VersionDeltaStore(const VersionDeltaStore &) = delete;
  VersionDeltaStore &operator=(const VersionDeltaStore &) = delete;
  ~VersionDeltaStore() = default;

  // Appends one overlay delta to the end of the log.
  void Append(const OverlayDelta &delta);

  // Returns every recorded overlay delta, in insertion order.
  std::vector<OverlayDelta> ReadAll() const;

  // Replaces the entire op-log with `deltas` (clear + re-append). Used by the capture path,
  // which recomputes a version's full overlay after each write query.
  void ReplaceAll(const std::vector<OverlayDelta> &deltas);

  // Number of recorded overlay deltas.
  uint64_t Size() const { return next_seq_; }

  // Removes a version's on-disk store entirely. The store must not be open elsewhere.
  static void Drop(const std::filesystem::path &path);

 private:
  std::unique_ptr<kvstore::KVStore> store_;
  uint64_t next_seq_{0};  // monotonic record counter; also the count of stored deltas
};

}  // namespace memgraph::storage
