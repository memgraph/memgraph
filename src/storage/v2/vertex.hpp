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
#include <iterator>
#include <limits>
#include <tuple>
#include <vector>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/tco_delta.hpp"
#include "storage/v2/tco_vector.hpp"
#include "utils/rw_spin_lock.hpp"

namespace memgraph::storage {

struct Vertex {
  Vertex(Gid gid, Delta *delta) : gid(gid), tco_delta(delta) {
    tco_delta.deleted(false);
    MG_ASSERT(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT ||
                  delta->action == Delta::Action::DELETE_DESERIALIZED_OBJECT,
              "Vertex must be created with an initial DELETE_OBJECT delta!");
  }

  const Gid gid;

  std::vector<LabelId> labels;

  TcoVector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> in_edges;
  TcoVector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> out_edges;

  mutable utils::RWSpinLock lock;
  PropertyStore properties;
  // bool deleted; 88 vs 112 == 24
  // // uint8_t PAD;
  // // uint16_t PAD;

  // Delta *delta;

  TcoDelta tco_delta;

  bool deleted() const { return tco_delta.deleted(); }
  void deleted(bool in) { tco_delta.deleted(in); }
  Delta *delta() const { return tco_delta.delta(); }
  void delta(Delta *in) { tco_delta.delta(in); }
};

static_assert(alignof(Vertex) >= 8, "The Vertex should be aligned to at least 8!");
static_assert(sizeof(Vertex) == 8 + 24 + 16 + 16 + 12 + 4 + 8);

inline bool operator==(const Vertex &first, const Vertex &second) { return first.gid == second.gid; }
inline bool operator<(const Vertex &first, const Vertex &second) { return first.gid < second.gid; }
inline bool operator==(const Vertex &first, const Gid &second) { return first.gid == second; }
inline bool operator<(const Vertex &first, const Gid &second) { return first.gid < second; }

}  // namespace memgraph::storage
