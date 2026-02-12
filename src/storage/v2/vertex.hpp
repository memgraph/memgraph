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

#include <tuple>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/small_vector.hpp"

namespace memgraph::storage {

struct Vertex {
  Vertex(Gid gid, Delta *delta) : gid(gid), delta_(delta) {
    MG_ASSERT(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT ||
                  delta->action == Delta::Action::DELETE_DESERIALIZED_OBJECT,
              "Vertex must be created with an initial DELETE_OBJECT delta!");
  }

  const Gid gid;

  utils::small_vector<LabelId> labels;

  using EdgeTriple = std::tuple<EdgeTypeId, Vertex *, EdgeRef>;

  utils::small_vector<EdgeTriple> in_edges;
  utils::small_vector<EdgeTriple> out_edges;

  PropertyStore properties;
  mutable utils::RWSpinLock lock;

  Delta *delta() const { return ::memgraph::storage::get(delta_); }

  void set_delta(Delta *d) { ::memgraph::storage::set_delta(delta_, d); }

  bool deleted() const { return ::memgraph::storage::deleted(delta_); }

  void set_deleted(bool b) { ::memgraph::storage::set_deleted(delta_, b); }

  bool has_uncommitted_non_sequential_deltas() const {
    return ::memgraph::storage::has_uncommitted_non_sequential_deltas(delta_);
  }

  void set_has_uncommitted_non_sequential_deltas(bool b) {
    ::memgraph::storage::set_has_uncommitted_non_sequential_deltas(delta_, b);
  }

 private:
  DeltaPtrPack delta_;
};

static constexpr std::size_t kEdgeTypeIdPos = 0U;
static constexpr std::size_t kVertexPos = 1U;
static constexpr std::size_t kEdgeRefPos = 2U;

static_assert(alignof(Vertex) >= 8, "The Vertex should be aligned to at least 8!");
static_assert(sizeof(Vertex) == 80,
              "If this changes documentation needs changing (deleted + has_uncommitted_non_seq packed into delta ptr)");

inline bool operator==(const Vertex &first, const Vertex &second) { return first.gid == second.gid; }

inline bool operator<(const Vertex &first, const Vertex &second) { return first.gid < second.gid; }

inline bool operator==(const Vertex &first, const Gid &second) { return first.gid == second; }

inline bool operator<(const Vertex &first, const Gid &second) { return first.gid < second; }

}  // namespace memgraph::storage
