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

#include "storage/v3/delta.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_store.hpp"
#include "utils/logging.hpp"
#include "utils/spin_lock.hpp"

namespace memgraph::storage::v3 {

struct Vertex;

struct Edge {
  Edge(Gid gid, Delta *delta) : gid(gid), deleted(false), delta(delta) {
    MG_ASSERT(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT,
              "Edge must be created with an initial DELETE_OBJECT delta!");
  }

  Gid gid;

  PropertyStore properties;

  mutable utils::SpinLock lock;
  bool deleted;
  // uint8_t PAD;
  // uint16_t PAD;

  Delta *delta;
};

static_assert(alignof(Edge) >= 8, "The Edge should be aligned to at least 8!");

inline bool operator==(const Edge &first, const Edge &second) { return first.gid == second.gid; }
inline bool operator<(const Edge &first, const Edge &second) { return first.gid < second.gid; }
inline bool operator==(const Edge &first, const Gid &second) { return first.gid == second; }
inline bool operator<(const Edge &first, const Gid &second) { return first.gid < second; }

}  // namespace memgraph::storage::v3
