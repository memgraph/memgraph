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

#include <algorithm>
#include <cstdint>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/index_impact.hpp"

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
/// Also carries which index families are affected at all, which is the coarser question the
/// families that do not yet consult the ids are still gated on.
class IndexArming {
 public:
  /// Sweep every vertex index regardless of what was written. Used where entries may point at
  /// objects about to be freed: no delta names those, and leaving one behind is a dangling
  /// pointer rather than a missed saving. Named per side so that a deleted edge, which says
  /// nothing about any vertex index, does not cost the vertex side its narrowing.
  void arm_every_vertex_index() { all_vertex_indexes_ = true; }

  void note_label(LabelId label) { Set(label.AsUint()); }

  /// Which families the writes could have touched at all, taken from the transaction where a
  /// delta cannot say it on its own.
  void note_families(IndexImpact impact) { families_ |= impact; }

  bool armed(LabelId label) const { return all_vertex_indexes_ || Test(label.AsUint()); }

  IndexImpact const &families() const { return families_; }

  bool any() const { return all_vertex_indexes_ || families_.any(); }

  IndexArming &operator|=(IndexArming const &other) {
    all_vertex_indexes_ |= other.all_vertex_indexes_;
    families_ |= other.families_;
    if (labels_.size() < other.labels_.size()) labels_.resize(other.labels_.size(), 0);
    for (size_t i = 0; i != other.labels_.size(); ++i) labels_[i] |= other.labels_[i];
    return *this;
  }

 private:
  // Ids are dense and allocated monotonically by the name-id mapper, so a bitmap over them costs
  // a word per sixty-four ids ever created and answers in constant time, which a per-delta hot
  // loop needs more than it needs the space back.
  static constexpr auto kBitsPerWord = 64U;

  void Set(uint32_t id) {
    auto const word = id / kBitsPerWord;
    if (word >= labels_.size()) labels_.resize(word + 1, 0);
    labels_[word] |= uint64_t{1} << (id % kBitsPerWord);
  }

  bool Test(uint32_t id) const {
    auto const word = id / kBitsPerWord;
    return word < labels_.size() && (labels_[word] & (uint64_t{1} << (id % kBitsPerWord))) != 0;
  }

  bool all_vertex_indexes_{false};
  IndexImpact families_{};
  std::vector<uint64_t> labels_;
};

}  // namespace memgraph::storage
