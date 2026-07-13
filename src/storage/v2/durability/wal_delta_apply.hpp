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
#include <functional>
#include <optional>
#include <tuple>
#include <unordered_map>

#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storagefwd.hpp"

// Graph Versioning v1 durability, slice S3b (see opencode-work/versioning-v1/
// 2026-07-13--durability-S2S3-design-v4.html §6). Pure extraction, no behavior change: partitions
// InMemoryReplicationHandlers::ReadAndApplyDeltasSingleTxn's single big Overloaded into
//   (A) the per-delta DATA-plane ops (this file), reusable by any caller that has already opened its
//       own write accessor and wants to replay a WAL transaction's vertex/edge mutations onto it, and
//   (B) transaction FRAMING (WalTransactionStart/End -- accessor open/commit, res_builder/RPC
//       progress, 2PC caching), which stays entirely caller-side because it differs per caller
//       (replica-apply today; the future S3d windowed branch-replay will supply its own framing that
//       forces the original commit timestamp and suppresses the WAL write -- see CommitArgs::
//       make_recovery_replay, commit_args.hpp).
//
// S3b only creates this core and switches the replica-apply caller (replication_handlers.cpp) onto
// it; window-replay (S3d) is NOT wired up yet.
namespace memgraph::storage {

// Cache key for the (edge_gid, delta_timestamp) -> EdgeAccessor memoization used while applying a
// single WAL transaction's data-plane deltas: WalEdgeCreate pre-fills it, WalEdgeSetProperty reuses
// it for any subsequent SET_PROPERTY deltas on the same edge within the same transaction, avoiding a
// repeated FindEdge lookup. Moved here (out of ReadAndApplyDeltasSingleTxn, where it used to be a
// function-local type) so the caller and ApplyWalDataDelta (below) share one definition of the cache
// across the whole per-transaction call.
struct EdgeSetPropertyCacheKey {
  uint64_t edge_gid{};
  uint64_t delta_timestamp{};
  bool operator==(EdgeSetPropertyCacheKey const &) const = default;
};

struct EdgeSetPropertyCacheKeyHash {
  size_t operator()(EdgeSetPropertyCacheKey const &key) const;
};

using EdgeSetPropertyCache = std::unordered_map<EdgeSetPropertyCacheKey, EdgeAccessor, EdgeSetPropertyCacheKeyHash>;

// Result type for InMemoryStorage's PRIVATE FindEdge(Gid)/FindEdge(Gid, Gid) lookups (structurally
// identical to InMemoryStorage's own `EdgeInfo` alias -- redeclared here so this header doesn't need
// to pull in the full inmemory/storage.hpp just to name the type). See FindEdgeFallback below.
using FindEdgeResult = std::optional<std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *>>;

// Caller-supplied resolver for the two WalEdgeSetProperty cases that need InMemoryStorage's PRIVATE
// FindEdge(Gid)/FindEdge(Gid, Gid) overloads (the from-vertex-assisted and full-scan fallbacks for
// older WAL formats that didn't carry to_gid/edge_type -- see the WalEdgeSetProperty arm in
// wal_delta_apply.cpp). ApplyWalDataDelta is a free function: friending it to reach a private
// InMemoryStorage member doesn't extend to a non-friend helper it calls into (the private FindEdge
// calls actually live one function deeper, inside the WalEdgeSetProperty arm's own helper), so the
// lookup is done by the caller -- which already has friend access via its own class (e.g.
// InMemoryReplicationHandlers) -- and threaded in here as this callback instead.
//
// `from_vertex_gid` is empty for the gid-only case (oldest WAL format); the callback should invoke
// `storage->FindEdge(edge_gid)` then. When set (gid + from-vertex case), the callback should invoke
// `storage->FindEdge(edge_gid, *from_vertex_gid)`.
using FindEdgeFallback = std::function<FindEdgeResult(Gid edge_gid, std::optional<Gid> from_vertex_gid)>;

// Applies a single pure DATA-plane WAL delta -- WalVertexCreate, WalVertexDelete, WalVertexAddLabel,
// WalVertexRemoveLabel, WalVertexSetProperty, WalEdgeCreate, WalEdgeDelete, WalEdgeSetProperty -- to
// an already-open write `accessor`. `delta` must hold one of those eight alternatives; any other
// WalDeltaData alternative (transaction framing, or a schema/index/constraint/enum/TTL/description
// op) throws utils::BasicException -- callers are expected to dispatch those themselves and never
// reach this function with them (see the file header comment for why those stay caller-side).
//
// `delta_timestamp` is the WAL delta's commit timestamp (used for the edge-property cache key and
// for trace logging, exactly as ReadAndApplyDeltasSingleTxn logged before this extraction);
// `current_delta_idx` is used only for trace-log messages, matching today's wording byte-for-byte.
//
// Throws utils::BasicException on every failure condition the original inline Overloaded arms threw
// on (missing vertex/edge, gid collision, disabled properties-on-edges, ...) -- same messages, same
// call sites relative to one another -- so a catching caller's behavior is unchanged.
//
// `find_edge_fallback` resolves the two WalEdgeSetProperty cases that need InMemoryStorage's PRIVATE
// FindEdge(Gid)/FindEdge(Gid, Gid) overloads -- see FindEdgeFallback above for the exact contract.
// Ignored by every other delta kind; the caller can build it once per transaction and pass it through
// unconditionally.
//
// AccessorT is `storage::ReplicationAccessor` today (the sole caller: replica-apply). A future
// window-replay accessor (S3d) that publicly exposes the same CreateVertexEx / CreateEdgeEx /
// FindVertex / FindEdge / DeleteVertex / DeleteEdge / NameToLabel / NameToProperty / NameToEdgeType /
// GetTransaction surface can reuse this core by adding its own overload (or templating this
// function) at that time -- deliberately not generalized now, per S3b's scope.
void ApplyWalDataDelta(ReplicationAccessor *accessor, InMemoryStorage *storage, durability::WalDeltaData const &delta,
                       uint64_t delta_timestamp, uint64_t current_delta_idx,
                       EdgeSetPropertyCache &edge_set_property_cache, FindEdgeFallback const &find_edge_fallback);

}  // namespace memgraph::storage
