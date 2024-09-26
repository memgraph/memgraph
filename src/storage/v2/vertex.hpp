// Copyright 2024 Memgraph Ltd.
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

#include <alloca.h>
#include <boost/container_hash/hash_fwd.hpp>
#include <functional>
#include <iterator>
#include <limits>
#include <tuple>
#include <vector>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/small_vector.hpp"

namespace memgraph::storage {

struct Vertex {
  Vertex(Gid gid, Delta *delta) : gid(gid), deleted(false), delta(delta) {
    MG_ASSERT(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT ||
                  delta->action == Delta::Action::DELETE_DESERIALIZED_OBJECT,
              "Vertex must be created with an initial DELETE_OBJECT delta!");
  }

  const Gid gid;

  utils::small_vector<LabelId> labels;

  utils::small_vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> in_edges;
  utils::small_vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> out_edges;

  PropertyStore properties;
  mutable utils::RWSpinLock lock;
  bool deleted;
  // uint8_t PAD;
  // uint16_t PAD;

  Delta *delta;
};

static_assert(alignof(Vertex) >= 8, "The Vertex should be aligned to at least 8!");
static_assert(sizeof(Vertex) == 88, "If this changes documentation needs changing");

inline bool operator==(const Vertex &first, const Vertex &second) { return first.gid == second.gid; }
inline bool operator<(const Vertex &first, const Vertex &second) { return first.gid < second.gid; }
inline bool operator==(const Vertex &first, const Gid &second) { return first.gid == second; }
inline bool operator<(const Vertex &first, const Gid &second) { return first.gid < second; }

struct PostProcessPOC {
  EdgeRef edge_ref;
  EdgeTypeId edge_type;
  Vertex *from;
  Vertex *to;
};

}  // namespace memgraph::storage

namespace std {
template <>
class hash<memgraph::storage::PostProcessPOC> {
 public:
  size_t operator()(const memgraph::storage::PostProcessPOC &pp) const {
    return pp.edge_ref.gid.AsUint();  // Both ptr and gid are the same size and unique
  }
};

template <>
class equal_to<memgraph::storage::PostProcessPOC> {
 public:
  bool operator()(const memgraph::storage::PostProcessPOC &lhs, const memgraph::storage::PostProcessPOC &rhs) const {
    // Edge ref is a pointer or gid, both are unique and should completely define the edge
    return lhs.edge_ref == rhs.edge_ref;
  }
};
}  // namespace std
