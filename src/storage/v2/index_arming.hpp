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
#include "storage/v2/property_write_targets.hpp"
#include "utils/id_bitmap.hpp"

namespace memgraph::storage {

/// Which indexes a set of writes could have left stale or duplicated entries in. An index is
/// "armed" when some write may have left it something to clean up, and garbage collection sweeps
/// the armed ones and skips the rest. Skipping is worth doing because a sweep looks at every
/// entry of an index, so even an index with nothing to clean up costs its whole size to walk.
///
/// This records what was written rather than which indexes were affected: a write knows the label
/// or property it touched, and indexes can be created or dropped before garbage collection gets
/// to it. An index created in between is simply swept if its label was written.
///
/// Vertex and edge indexes are kept apart because they are armed by different things: a vertex
/// index is armed when one of its labels is added or removed, but an edge index is never armed by
/// its edge type, because an edge's type cannot change. No label or property is shared between a
/// vertex index key and an edge index key, so any one write can only affect one of the two.
class IndexArming {
 public:
  /// One transaction's worth of deltas. A property delta records which property was written but
  /// not whether it was on a vertex or an edge; only the transaction knows that, so it is looked
  /// up once here rather than for every delta. Having to open a scope also means a delta cannot
  /// be recorded without saying which transaction it came from.
  class TransactionScope {
   public:
    // Takes the whole delta rather than just its action: the action says whether vertex or edge
    // indexes may need cleaning, and the rest of the delta says which ones. Note that a delta
    // undoes the write that made it, so the action is the opposite of what the writer called; the
    // label or property id it carries is the same either way.
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
          if (wrote_properties_on_.vertices) arming_->note_vertex_property(delta.property.key);
          if (wrote_properties_on_.edges) arming_->note_edge_property(delta.property.key);
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

    // Opening a scope arms nothing by itself. A transaction only reports writing vertex or edge
    // properties because it made a delta saying so, and that delta is in the buffer about to be
    // read, where it names the property.
    TransactionScope(IndexArming &arming, PropertyWriteTargets wrote_properties_on)
        : arming_{&arming}, wrote_properties_on_{wrote_properties_on} {}

    IndexArming *arming_;
    PropertyWriteTargets wrote_properties_on_;
  };

  /// Opens a scope for one transaction's deltas, given the one thing the deltas cannot say on
  /// their own: whether its property writes were on vertices or on edges.
  TransactionScope for_deltas_of(PropertyWriteTargets wrote_properties_on) { return {*this, wrote_properties_on}; }

  /// Sweep every vertex index whatever was written. Needed when entries may point at objects
  /// about to be freed: no delta says which indexes hold those, and missing one leaves a dangling
  /// pointer rather than just wasting some space. Separate from the edge version so that a
  /// deleted edge, which cannot affect any vertex index, does not force them all to be swept.
  void arm_all_vertex_indexes() { vertex_.armed_all = true; }

  /// The edge counterpart; see above.
  void arm_all_edge_indexes() { edge_.armed_all = true; }

  void note_label(LabelId label) { vertex_.labels.set(label); }

  void note_vertex_property(PropertyId property) { vertex_.properties.set(property); }

  void note_edge_property(PropertyId property) { edge_.properties.set(property); }

  /// An edge was created or removed. Unlike a write to a vertex, this cannot say which index is
  /// affected, so every edge index has to be swept.
  void note_edge_structure() { edge_.structural = true; }

  bool arms_vertex_indexes() const { return vertex_.armed(); }

  bool arms_edge_indexes() const { return edge_.armed(); }

  bool arms_anything() const { return arms_vertex_indexes() || arms_edge_indexes(); }

  bool arms_vertex_index_on(LabelId label) const { return vertex_.armed_all || vertex_.labels.test(label); }

  /// Such an index can go stale either by the label coming off a vertex it covers or by one of
  /// its properties being written, so either one arms it. Its key can be a path into a nested
  /// property, and a write names only the property the path starts from.
  bool arms_vertex_index_on(LabelId label, PropertiesPaths const &properties) const {
    return arms_vertex_index_on(label) || std::ranges::any_of(properties, [this](PropertyPath const &path) {
             return !path.empty() && vertex_.properties.test(path[0]);
           });
  }

  /// An index keyed on a property alone holds an entry per vertex carrying that property,
  /// whatever labels the vertex has, so only writing the property can stale one. A label is not
  /// part of the key and no entry is touched when one comes or goes, which is why this asks
  /// something narrower than the label-property version above.
  bool arms_vertex_property_index_on(PropertyId property) const {
    return vertex_.armed_all || vertex_.properties.test(property);
  }

  /// Such an index can only go stale by the property being written, or by the edge itself being
  /// removed. The edge type never arms it, because an edge's type cannot change, and a removed
  /// edge sweeps every edge index anyway.
  bool arms_edge_index_on(PropertyId property) const { return edge_.armed_all || edge_.properties.test(property); }

  /// An entry in such an index holds no property, and an edge's type cannot change, so the only
  /// thing that can go stale is the edge being removed.
  bool arms_edge_type_index() const { return edge_.armed_all || edge_.structural; }

  /// A unique constraint is keyed the way a label-property index is and goes stale under the same
  /// conditions, so the same question answers for it. Its key is a plain set of properties.
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

  /// Empties without freeing the memory, so one reused across collection cycles does not have to
  /// grow back to the same size each time.
  void reset() {
    vertex_.reset();
    edge_.reset();
  }

 private:
  struct VertexIndexes {
    /// Which of them are affected is not known, so every one is swept.
    bool armed_all{false};
    utils::IdBitmap<LabelId> labels{};
    utils::IdBitmap<PropertyId> properties{};

    /// Worked out on demand rather than kept up to date, because keeping it up to date means a
    /// write per delta, and this is only read once per collection cycle.
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
    /// Which of them are affected is not known, so every one is swept.
    bool armed_all{false};
    /// An edge was created or removed, which does not point at any particular index.
    bool structural{false};
    utils::IdBitmap<PropertyId> properties{};

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
