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

#include "storage/v2/delta.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "utils/logging.hpp"
#include "utils/pointer_pack.hpp"
#include "utils/rw_spin_lock.hpp"

namespace memgraph::storage {

struct Vertex;

struct Edge {
  Edge(Gid gid, Delta *delta) noexcept : gid(gid), delta_(delta) {
    MG_ASSERT(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT ||
                  delta->action == Delta::Action::DELETE_DESERIALIZED_OBJECT,
              "Edge must be created with an initial DELETE_OBJECT delta!");
  }

  Gid gid{};

  PropertyStore properties{};

  mutable utils::RWSpinLock lock;

  [[nodiscard]] Delta *delta() const { return delta_.GetPtr(); }

  void SetDelta(Delta *d) { delta_.SetPtr(d); }

  [[nodiscard]] bool deleted() const { return delta_.Get<kDeletedBit>() != 0; }

  void SetDeleted(bool b) { delta_.Set<kDeletedBit>(b ? 1 : 0); }

  // Graph Versioning v1 -- phase 1 (write-side only) of a per-object "touched by any branch" hint.
  // Mirrors `storage::Vertex::branched()`/`SetBranched()` (vertex.hpp) exactly -- see its own
  // doc-comment for the full monotonic/never-clear policy, the invariant this maintains, and the
  // concurrency contract (`SetBranched` under `lock`; `branched()` a lock-free relaxed-atomic
  // read via `PointerPack::GetRelaxed`). For an Edge specifically, this bit is BEST-EFFORT/
  // supplementary -- the two endpoint Vertex objects' own `branched()` bits are what the
  // phase-2 traversal fast-path is documented to actually key on (see
  // versioning::BranchContext::CowEdge, branch_engine.cpp, for why: an edge is only reachable at
  // all with `--storage-properties-on-edges=true`, i.e. a real `Edge` object to mark; with it
  // false edges are REFERENCE-ONLY (`EdgeRef` holds just a gid) and there is no `Edge` object here
  // to set this bit on in the first place).
  bool branched() const { return delta_.GetRelaxed<kBranchedBit>(); }

  // Caller MUST hold `lock` (see `branched()`'s own doc-comment above for the full concurrency
  // contract).
  void SetBranched(bool b) { delta_.Set<kBranchedBit>(b ? 1 : 0); }

 private:
  static constexpr int kDeletedBit = 0;
  static constexpr int kBranchedBit = 1;

  utils::PointerPack<Delta, 2> delta_{};
};

static_assert(alignof(Edge) >= 8, "The Edge should be aligned to at least 8!");

inline bool operator==(const Edge &first, const Edge &second) { return first.gid == second.gid; }

inline bool operator<(const Edge &first, const Edge &second) { return first.gid < second.gid; }

inline bool operator==(const Edge &first, const Gid &second) { return first.gid == second; }

inline bool operator<(const Edge &first, const Gid &second) { return first.gid < second; }

struct EdgeMetadata {
  EdgeMetadata(Gid gid, Vertex *from_vertex) : gid(gid), from_vertex(from_vertex) {}

  Gid gid;
  Vertex *from_vertex;
};

static_assert(alignof(Edge) >= 8, "The Edge should be aligned to at least 8!");

inline bool operator==(const EdgeMetadata &first, const EdgeMetadata &second) { return first.gid == second.gid; }

inline bool operator<(const EdgeMetadata &first, const EdgeMetadata &second) { return first.gid < second.gid; }

inline bool operator==(const EdgeMetadata &first, const Gid &second) { return first.gid == second; }

inline bool operator<(const EdgeMetadata &first, const Gid &second) { return first.gid < second; }

}  // namespace memgraph::storage
