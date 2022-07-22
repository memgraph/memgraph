// Copyright 2022 Memgraph Ltd.
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

#include <limits>
#include <tuple>
#include <vector>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "utils/algorithm.hpp"
#include "utils/spin_lock.hpp"

namespace memgraph::storage {

struct Vertex {
  Vertex(Gid gid, Delta *delta, LabelId primary_label)
      : gid(gid), primary_label{primary_label}, deleted(false), delta(delta) {
    MG_ASSERT(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT,
              "Vertex must be created with an initial DELETE_OBJECT delta!");
  }

  // TODO remove this when import replication is solved
  Vertex(Gid gid, LabelId primary_label) : gid(gid), primary_label{primary_label}, deleted(false) {
    MG_ASSERT(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT,
              "Vertex must be created with an initial DELETE_OBJECT delta!");
  }

  // TODO remove this when import csv is solved
  [[deprecated]] Vertex(Gid gid, Delta *delta) : gid(gid), deleted(false), delta(delta) {
    MG_ASSERT(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT,
              "Vertex must be created with an initial DELETE_OBJECT delta!");
  }

  // TODO remove this when import replication is solved
  [[deprecated]] explicit Vertex(Gid gid) : gid(gid), deleted(false) {
    MG_ASSERT(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT,
              "Vertex must be created with an initial DELETE_OBJECT delta!");
  }

  Gid gid;

  LabelId primary_label;
  std::vector<LabelId> labels;
  PropertyStore properties;

  std::vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> in_edges;
  std::vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> out_edges;

  mutable utils::SpinLock lock;
  bool deleted;
  // uint8_t PAD;
  // uint16_t PAD;

  Delta *delta;
};

static_assert(alignof(Vertex) >= 8, "The Vertex should be aligned to at least 8!");

inline bool operator==(const Vertex &first, const Vertex &second) { return first.gid == second.gid; }
inline bool operator<(const Vertex &first, const Vertex &second) { return first.gid < second.gid; }
inline bool operator==(const Vertex &first, const Gid &second) { return first.gid == second; }
inline bool operator<(const Vertex &first, const Gid &second) { return first.gid < second; }

inline bool VertexHasLabel(const Vertex &vertex, const LabelId label) {
  return vertex.primary_label == label || utils::Contains(vertex.labels, label);
}

}  // namespace memgraph::storage
