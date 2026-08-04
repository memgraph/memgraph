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

#include "storage/v2/id_types.hpp"
#include "storage/v2/index_impact.hpp"
#include "utils/id_bitmap.hpp"

namespace memgraph::storage {

/// Which indexes a set of writes could have left stale or duplicated entries in, so that a
/// collection cycle can sweep those and leave the rest alone. A sweep visits every entry of an
/// index rather than only the stale ones, so an index with nothing to collect costs its whole
/// size to walk.
///
/// Names the writes rather than the indexes: a write knows the label it touched, not which
/// indexes stand on that label, and the set of indexes can change between the write and the
/// cycle that acts on it. An index created in between is simply swept if its label was written.
///
/// Grouped by index family, because the two do not arm on the same things: a vertex index turns
/// on labels, which are mutable, while an edge index cannot turn on its edge type, which is not.
///
/// Also carries which index families are affected at all, the coarser question that index types
/// not yet consulting the ids are still gated on.
class IndexArming {
 public:
  /// Sweep every vertex index regardless of what was written. Used where entries may point at
  /// objects about to be freed: no delta names those, and leaving one behind is a dangling
  /// pointer rather than a missed saving. Named per family so that a deleted edge, which says
  /// nothing about any vertex index, does not cost the vertex indexes their narrowing.
  void arm_every_vertex_index() { vertex_.all = true; }

  void note_label(LabelId label) { vertex_.labels.Set(label); }

  /// Which families the writes could have touched at all, taken from the transaction where a
  /// delta cannot say it on its own.
  void note_families(IndexImpact impact) { families_ |= impact; }

  bool armed(LabelId label) const { return vertex_.all || vertex_.labels.Test(label); }

  IndexImpact const &families() const { return families_; }

  bool any() const { return vertex_.all || families_.any(); }

  IndexArming &operator|=(IndexArming const &other) {
    vertex_.Merge(other.vertex_);
    families_ |= other.families_;
    return *this;
  }

  /// Empties without giving up the memory, so that a holder reused across collection cycles
  /// keeps its allocation instead of growing back to the same size every time.
  void Clear() {
    vertex_.Clear();
    families_ = {};
  }

 private:
  struct VertexIndexes {
    bool all{false};
    utils::IdBitmap<LabelId> labels{};

    void Merge(VertexIndexes const &other) {
      all |= other.all;
      labels.Merge(other.labels);
    }

    void Clear() {
      all = false;
      labels.Clear();
    }
  };

  VertexIndexes vertex_{};
  IndexImpact families_{};
};

}  // namespace memgraph::storage
