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
#include <optional>
#include <set>
#include <unordered_map>
#include <vector>

#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::versioning {

// Graph Versioning CHUNK 5a: the production branch OVERLAY CONTAINER (B1/R38).
//
// A BranchOverlay is a delta-scale private store holding ONLY the objects a branch created or
// modified (bounded by D5) -- it is NOT a main-scale clone. It is rebuilt per-query from the
// branch's own change-log (versioning::BranchLog::ReadAll, chunk 3a) via Materialize(): create
// ops insert directly, modify ops on a fork-existing object first copy-on-write that object's
// fork-state OUT of main (via a HistoricalAccess accessor, chunk 4) into the overlay and then
// apply the mutation there, and delete ops leave a tombstone. Main's live objects are NEVER
// mutated (R35) -- fork_base is used strictly as a read-only COW source.
//
// An overlay Vertex/Edge is a plain materialized object: it carries no live MVCC delta chain
// (delta_ == nullptr) because the overlay itself has no intra-branch MVCC -- it is rebuilt fresh
// (single materialization pass) each time a query needs it.
//
// Scope of this chunk: the container + materialize-from-changelog only. The union read-path that
// merges this overlay with main during query execution (ScanAll/expand/etc.) is chunk 5b, as is
// wiring live branch-write capture into a real transaction -- neither happens here. The read
// helpers below (LookupVertex/LookupEdge/VerticesAccessor/EdgesAccessor/EdgeEndpoints) exist
// purely as the surface 5b's streaming gid-ordered merge will consume; nothing here calls them.
//
// Design note (flagged, not specified by the plan): storage::Edge itself carries no
// endpoint/type fields (main reconstructs from/to only by walking vertex adjacency). Wiring live
// Vertex::in_edges/out_edges adjacency across the overlay/main boundary is exactly the "union"
// problem chunk 5b owns, so this chunk deliberately leaves every overlay Vertex's in_edges/
// out_edges EMPTY and instead records each overlay edge's (type, from, to) in a side table
// (EdgeEndpoints()) for 5b to consume when it builds the merged adjacency view.
//
// Per-query transient: no durability, no background threads, no locking beyond what the
// underlying SkipLists already provide for their own accessors; simply discarded on destruction.
class BranchOverlay {
 public:
  enum class Status : uint8_t { kAbsent, kPresent, kTombstoned };

  // Endpoint/type metadata for an overlay-resident edge -- see the class comment above for why
  // this lives in a side table rather than on storage::Edge or in vertex adjacency.
  struct EdgeEndpoints {
    storage::EdgeTypeId edge_type;
    storage::Gid from_vertex;
    storage::Gid to_vertex;
  };

  // `name_id_mapper` must outlive this BranchOverlay (mirrors BranchLog's own constructor
  // contract) -- it resolves the WAL change-log's label/property/edge-type NAMES back to ids.
  explicit BranchOverlay(storage::NameIdMapper *name_id_mapper) : name_id_mapper_(name_id_mapper) {}

  BranchOverlay(const BranchOverlay &) = delete;
  BranchOverlay &operator=(const BranchOverlay &) = delete;
  BranchOverlay(BranchOverlay &&) = delete;
  BranchOverlay &operator=(BranchOverlay &&) = delete;
  ~BranchOverlay() = default;

  // Applies every forward WAL record from a branch's change-log onto this overlay, in order.
  // Intended to be called once against a freshly-constructed overlay.
  //
  // `fork_base` must be an accessor opened over main as-of the branch's fork timestamp
  // (InMemoryStorage::HistoricalAccess) -- it is consulted ONLY as a read-only COW source for
  // MODIFY ops that target a vertex/edge the branch did not itself CREATE earlier in this same
  // change-log; main is never mutated through it (R35).
  //
  // Non-graph-data records (index/constraint/enum/text-index/ttl/etc.) are silently ignored --
  // out of scope for a branch overlay, which materializes graph data only.
  //
  // Gid lifecycle within one pass: a delete followed later (same pass) by a create of the SAME
  // gid is legitimate re-use -- the create clears the tombstone, so the recreated object reads
  // back as kPresent with its fresh (create-origin) data, not kTombstoned. The reverse anomaly --
  // a mutation (SetProperty/AddLabel/RemoveLabel) targeting a gid that is currently tombstoned
  // and has NOT been recreated since -- has no live object left to mutate and is rejected the
  // same way a duplicate create is: by throwing.
  //
  // @throw utils::BasicException if the change-log references a vertex/edge that is neither
  // already present in the overlay nor findable in fork_base (corrupt changelog or a caller error
  // such as a fork_base opened at the wrong timestamp), creates the same gid twice, or mutates a
  // gid that is currently tombstoned.
  void Materialize(const std::vector<storage::durability::WalDeltaData> &changelog,
                   storage::Storage::Accessor &fork_base);

  // Read surface for chunk 5b's union merge.
  //
  // `**out` is left unset (kAbsent/kTombstoned) rather than nulled if `out == nullptr`; callers
  // that only need the status may pass nullptr. Returned pointers are valid only for as long as
  // this BranchOverlay is alive and not further mutated (single-materialization container).
  Status LookupVertex(storage::Gid gid, storage::Vertex **out = nullptr);
  Status LookupEdge(storage::Gid gid, storage::Edge **out = nullptr);

  // Endpoint/type lookup for an overlay-resident edge (see EdgeEndpoints above). Returns nullptr
  // if `gid` is not an overlay-resident edge.
  const EdgeEndpoints *LookupEdgeEndpoints(storage::Gid gid) const;

  // Gid-ordered accessors over the overlay's materialized objects, for the streaming merge (5b).
  // Vertex/Edge already order by gid (operator< in vertex.hpp/edge.hpp), so plain SkipList
  // iteration is already gid-ordered.
  auto VerticesAccessor() { return vertices_.access(); }

  auto EdgesAccessor() { return edges_.access(); }

  const std::set<storage::Gid> &DeletedVertices() const { return deleted_vertices_; }

  const std::set<storage::Gid> &DeletedEdges() const { return deleted_edges_; }

 private:
  // Returns the overlay-resident Vertex for `gid`, COW'ing it out of `fork_base` first if this is
  // the branch's first-ever touch of that vertex. `fork_base` is read-only (R35).
  storage::Vertex *EnsureOverlayVertex(storage::Gid gid, storage::Storage::Accessor &fork_base);

  // Same as EnsureOverlayVertex but for edges. `from_gid`/`to_gid`/`edge_type_name` are the
  // optional WalEdgeSetProperty hint fields (kEdgeSetDeltaWithVertexInfo/kExtendedEdgeSetProperty)
  // used to resolve the edge in fork_base efficiently when they're available; falls back to
  // gid-only lookup otherwise (mirrors dbms/inmemory/replication_handlers.cpp's WalEdgeSetProperty
  // resolution strategy).
  storage::Edge *EnsureOverlayEdge(storage::Gid gid, storage::Storage::Accessor &fork_base,
                                   const std::optional<storage::Gid> &from_gid,
                                   const std::optional<storage::Gid> &to_gid,
                                   const std::optional<std::string> &edge_type_name);

  storage::LabelId NameToLabel(std::string_view name) const {
    return storage::LabelId::FromUint(name_id_mapper_->NameToId(name));
  }

  storage::PropertyId NameToProperty(std::string_view name) const {
    return storage::PropertyId::FromUint(name_id_mapper_->NameToId(name));
  }

  storage::EdgeTypeId NameToEdgeType(std::string_view name) const {
    return storage::EdgeTypeId::FromUint(name_id_mapper_->NameToId(name));
  }

  storage::NameIdMapper *name_id_mapper_;

  // Delta-scale object stores -- reuses the exact SkipListDb<T> alias main's InMemoryStorage
  // vertices_/edges_ use (utils::SkipListDb<T> = SkipList<T, memory::DbAwareAllocator<char>>).
  // DbAwareAllocator degrades to the process-default allocator when no DB arena is TLS-pinned
  // (tls_db_arena_state.arena == 0), and attributes to the current DB's jemalloc arena when one
  // IS pinned -- which is the ordinary case here, since Materialize() runs on a query thread
  // already inside that DB's DbArenaScope. So this container needs no bespoke arena wiring: it
  // naturally inherits whatever scope its caller is already running under, and its (bounded,
  // delta-scale) memory shows up in that DB's existing tracker for free.
  utils::SkipListDb<storage::Vertex> vertices_;
  utils::SkipListDb<storage::Edge> edges_;

  // Side table: overlay edge gid -> (type, from, to). See the class comment for why this can't
  // live on storage::Edge or in vertex adjacency at this chunk.
  std::unordered_map<storage::Gid, EdgeEndpoints> edge_endpoints_;

  // Tombstones: objects the branch has deleted. Checked ahead of vertices_/edges_ by
  // LookupVertex/LookupEdge (a delete always wins, whether or not the object was ever COW'd into
  // the overlay).
  std::set<storage::Gid> deleted_vertices_;
  std::set<storage::Gid> deleted_edges_;
};

}  // namespace memgraph::versioning
