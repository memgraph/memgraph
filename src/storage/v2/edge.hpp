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
#include "utils/rw_spin_lock.hpp"

namespace memgraph::storage {

struct Vertex;

struct Edge {
  Edge(Gid gid, Delta *delta) : gid_ref_(gid.AsUint()), delta_(delta) {
    MG_ASSERT(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT ||
                  delta->action == Delta::Action::DELETE_DESERIALIZED_OBJECT,
              "Edge must be created with an initial DELETE_OBJECT delta!");
  }

  Gid Gid() const { return Gid::FromUint(gid_ref_); }

  PropertyStore properties{};

  mutable utils::RWSpinLock lock;

  Delta *delta() const { return ::memgraph::storage::get(delta_); }

  void set_delta(Delta *d) { ::memgraph::storage::set_delta(delta_, d); }

  bool deleted() const { return ::memgraph::storage::deleted(delta_); }

  void set_deleted(bool b) { ::memgraph::storage::set_deleted(delta_, b); }

  bool has_uncommitted_non_sequential_deltas() const { return false; }

  void set_has_uncommitted_non_sequential_deltas(bool) {}

 private:
  uint64_t gid_ref_{};
  DeltaPtrPack delta_{};
};

static_assert(alignof(Edge) >= 8, "The Edge should be aligned to at least 8!");
static_assert(sizeof(Edge) == 32, "The Edge should be 32 bytes!");

inline bool operator==(const Edge &first, const Edge &second) { return first.Gid() == second.Gid(); }

inline bool operator<(const Edge &first, const Edge &second) { return first.Gid() < second.Gid(); }

inline bool operator==(const Edge &first, const Gid &second) { return first.Gid() == second; }

inline bool operator<(const Edge &first, const Gid &second) { return first.Gid() < second; }

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
