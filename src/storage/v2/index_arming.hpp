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
#include <set>

#include "storage/v2/delta.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/property_writes.hpp"
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
class IndexArming {
 public:
  /// One transaction's worth of deltas. A property delta records the property but not whether it
  /// belonged to a vertex or an edge, and only the transaction knows that, so it is resolved once
  /// here rather than asked again for every delta. Opening the scope is also what makes recording
  /// a delta outside a transaction impossible to write.
  class TransactionScope {
   public:
    // Takes the whole delta rather than its action: the action says an index family may hold
    // something to collect, the payload says which index within it. A delta holds the inverse of
    // the write that made it, so the action here is the opposite of what the writer called, but
    // the id is the same either way.
    void note(Delta const &delta) const {
      switch (delta.action) {
        using enum Delta::Action;
        case DELETE_DESERIALIZED_OBJECT:
        case DELETE_OBJECT:
        case RECREATE_OBJECT: {
          // can impact correctness, but does not matter for performance
          return;
        }
        case SET_PROPERTY: {
          if (writes_vertex_properties_) arming_->note_vertex_property(delta.property.key);
          if (writes_edge_properties_) arming_->note_edge_property(delta.property.key);
          return;
        }
        case ADD_LABEL:
        case REMOVE_LABEL: {
          arming_->note_label(delta.label.value);
          return;
        }
        case ADD_IN_EDGE:
        case ADD_OUT_EDGE:
        case REMOVE_IN_EDGE:
        case REMOVE_OUT_EDGE: {
          arming_->note_edge_structure();
          return;
        }
      }
    }

   private:
    friend class IndexArming;

    // Neither side is noted here. A transaction reports having written a property on one only
    // because it created a delta saying so, and that delta is in the buffer about to be read,
    // where it names the property.
    TransactionScope(IndexArming &arming, PropertyWrites property_writes)
        : arming_{&arming},
          writes_vertex_properties_{property_writes.on_vertices},
          writes_edge_properties_{property_writes.on_edges} {}

    IndexArming *arming_;
    bool writes_vertex_properties_;
    bool writes_edge_properties_;
  };

  /// @param property_writes what this transaction's property writes belonged to, which its
  ///                        deltas cannot say on their own.
  TransactionScope for_transaction(PropertyWrites property_writes) { return {*this, property_writes}; }

  /// Sweep every vertex index regardless of what was written. Used where entries may point at
  /// objects about to be freed: no delta names those, and leaving one behind is a dangling
  /// pointer rather than a missed saving. Named per family so that a deleted edge, which says
  /// nothing about any vertex index, does not cost the vertex indexes their narrowing.
  void arm_all_vertex_indexes() { vertex_.armed_all = true; }

  /// The edge counterpart; see above.
  void arm_all_edge_indexes() { edge_.armed_all = true; }

  void note_label(LabelId label) { vertex_.labels.set(label); }

  void note_vertex_property(PropertyId property) { vertex_.properties.set(property); }

  void note_edge_property(PropertyId property) { edge_.properties.set(property); }

  /// An edge was created or removed. Unlike a write to a vertex, this names no index: an edge
  /// index turns on its edge type, and the delta that carries the type cannot say which of the
  /// indexes on it hold the edge.
  void note_edge_structure() { edge_.structural = true; }

  bool arms_vertex_indexes() const { return vertex_.armed(); }

  bool arms_edge_indexes() const { return edge_.armed(); }

  bool arms_anything() const { return arms_vertex_indexes() || arms_edge_indexes(); }

  bool arms_vertex_index_on(LabelId label) const { return vertex_.armed_all || vertex_.labels.test(label); }

  /// An index on a label and a set of properties goes stale when the label is taken off a vertex
  /// it covers or when one of the properties is written, so either arms it. A path names a
  /// property reached through others, and a write names the one it starts from.
  bool arms_vertex_index_on(LabelId label, PropertiesPaths const &properties) const {
    return arms_vertex_index_on(label) || std::ranges::any_of(properties, [this](PropertyPath const &path) {
             return !path.empty() && vertex_.properties.test(path[0]);
           });
  }

  /// An edge index keyed on a property goes stale when that property is written. Its edge type
  /// arms nothing: an edge cannot change type, so the only way such an entry goes stale is the
  /// edge being removed, which sweeps every edge index for correctness anyway.
  bool arms_edge_index_on(PropertyId property) const { return edge_.armed_all || edge_.properties.test(property); }

  /// An index keyed on an edge type alone holds an entry per edge of that type, so the only way
  /// one goes stale is the edge being removed. An edge cannot change type, and a property write
  /// cannot invalidate an entry that no property is part of.
  bool arms_edge_type_index() const { return edge_.armed_all || edge_.structural; }

  /// A unique constraint keeps a skiplist keyed the same way an index does, and it goes stale on
  /// the same conditions, so the same question answers for it. Its key is a set of properties
  /// rather than paths into them.
  bool arms_vertex_index_on(LabelId label, std::set<PropertyId> const &properties) const {
    return arms_vertex_index_on(label) || std::ranges::any_of(properties, [this](PropertyId const property) {
             return vertex_.properties.test(property);
           });
  }

  IndexArming &operator|=(IndexArming const &other) {
    vertex_ |= other.vertex_;
    edge_ |= other.edge_;
    return *this;
  }

  /// Empties without giving up the memory, so that a holder reused across collection cycles
  /// keeps its allocation instead of growing back to the same size every time.
  void reset() {
    vertex_.reset();
    edge_.reset();
  }

 private:
  struct VertexIndexes {
    /// Which of them is not known, so every one is swept.
    bool armed_all{false};
    utils::IdBitmap<LabelId> labels{};
    utils::IdBitmap<PropertyId> properties{};

    /// Whether any vertex index may hold something to collect. Read once per collection cycle
    /// and derived rather than carried, because what would carry it is a store on every write's
    /// delta, which is the side that runs often.
    bool armed() const { return armed_all || labels.any() || properties.any(); }

    VertexIndexes &operator|=(VertexIndexes const &other) {
      armed_all |= other.armed_all;
      labels |= other.labels;
      properties |= other.properties;
      return *this;
    }

    void reset() {
      armed_all = false;
      labels.reset();
      properties.reset();
    }
  };

  struct EdgeIndexes {
    /// Which of them is not known, so every one is swept.
    bool armed_all{false};
    /// An edge was created or removed, which no id narrows; see note_edge_structure.
    bool structural{false};
    utils::IdBitmap<PropertyId> properties{};

    /// See VertexIndexes::armed for why this is derived rather than carried.
    bool armed() const { return armed_all || structural || properties.any(); }

    EdgeIndexes &operator|=(EdgeIndexes const &other) {
      armed_all |= other.armed_all;
      structural |= other.structural;
      properties |= other.properties;
      return *this;
    }

    void reset() {
      armed_all = false;
      structural = false;
      properties.reset();
    }
  };

  VertexIndexes vertex_{};
  EdgeIndexes edge_{};
};

}  // namespace memgraph::storage
