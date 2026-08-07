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

// The arming decides which indexes a collection cycle sweeps. Getting it wrong in one direction
// costs a sweep of an index that had nothing to collect; in the other it leaves stale entries, or
// entries pointing at freed objects, behind. Both are answered here by asking the arming
// directly, rather than through a collection cycle and the count it reports.

#include "gtest/gtest.h"

#include "storage/v2/delta_container.hpp"
#include "storage/v2/index_arming.hpp"

#include <set>

using memgraph::storage::Delta;
using memgraph::storage::delta_container;
using memgraph::storage::EdgeRef;
using memgraph::storage::EdgeTypeId;
using memgraph::storage::IndexArming;
using memgraph::storage::LabelId;
using memgraph::storage::PropertiesPaths;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyPath;
using memgraph::storage::PropertyValue;
using memgraph::storage::PropertyWriteTargets;

namespace {

LabelId Label(uint64_t id) { return LabelId::FromUint(id); }

PropertyId Property(uint64_t id) { return PropertyId::FromUint(id); }

constexpr auto kOnVertices = PropertyWriteTargets{.vertices = true, .edges = false};
constexpr auto kOnEdges = PropertyWriteTargets{.vertices = false, .edges = true};

}  // namespace

///// NOTHING WRITTEN

TEST(IndexArming, AFreshArmingArmsNothing) {
  auto const arming = IndexArming{};

  EXPECT_FALSE(arming.arms_anything());
  EXPECT_FALSE(arming.arms_vertex_indexes());
  EXPECT_FALSE(arming.arms_edge_indexes());
  EXPECT_FALSE(arming.arms_vertex_index_on(Label(1)));
  EXPECT_FALSE(arming.arms_vertex_index_on(Label(1), PropertiesPaths{PropertyPath{Property(1)}}));
  EXPECT_FALSE(arming.arms_vertex_property_index_on(Property(1)));
  EXPECT_FALSE(arming.arms_edge_index_on(Property(1)));
  EXPECT_FALSE(arming.arms_edge_type_index());
}

///// LABELS

TEST(IndexArming, ALabelWriteArmsOnlyThatLabelsIndex) {
  auto arming = IndexArming{};
  arming.note_label(Label(1));

  EXPECT_TRUE(arming.arms_vertex_index_on(Label(1)));
  EXPECT_FALSE(arming.arms_vertex_index_on(Label(2)));
}

// The two families are disjoint: no label appears in an edge index key, so a workload writing
// only labels must not pay to walk the edge indexes.
TEST(IndexArming, ALabelWriteArmsNoEdgeIndex) {
  auto arming = IndexArming{};
  arming.note_label(Label(1));

  EXPECT_TRUE(arming.arms_vertex_indexes());
  EXPECT_FALSE(arming.arms_edge_indexes());
  EXPECT_FALSE(arming.arms_edge_index_on(Property(1)));
  EXPECT_FALSE(arming.arms_edge_type_index());
}

///// VERTEX PROPERTIES

// An index on a label and properties goes stale when the label comes off a vertex it covers or
// when one of the properties is written, so either arms it and neither is required.
TEST(IndexArming, ALabelPropertyIndexArmsOnEitherHalfOfItsKey) {
  auto const key = PropertiesPaths{PropertyPath{Property(10)}};

  auto on_label = IndexArming{};
  on_label.note_label(Label(1));
  EXPECT_TRUE(on_label.arms_vertex_index_on(Label(1), key));

  auto on_property = IndexArming{};
  on_property.note_vertex_property(Property(10));
  EXPECT_TRUE(on_property.arms_vertex_index_on(Label(1), key));

  auto on_neither = IndexArming{};
  on_neither.note_label(Label(2));
  on_neither.note_vertex_property(Property(11));
  EXPECT_FALSE(on_neither.arms_vertex_index_on(Label(1), key));
}

TEST(IndexArming, ACompositeKeyArmsOnAnyOneOfItsProperties) {
  auto const key = PropertiesPaths{PropertyPath{Property(10)}, PropertyPath{Property(11)}};

  auto arming = IndexArming{};
  arming.note_vertex_property(Property(11));

  EXPECT_TRUE(arming.arms_vertex_index_on(Label(1), key));
}

// A path names a property reached through others, and a write names the one it starts from, so
// only the root of the path can arm the index that stands on it.
TEST(IndexArming, ANestedPathArmsOnThePropertyItStartsFrom) {
  auto const key = PropertiesPaths{PropertyPath{Property(10), Property(11)}};

  auto on_root = IndexArming{};
  on_root.note_vertex_property(Property(10));
  EXPECT_TRUE(on_root.arms_vertex_index_on(Label(1), key));

  auto on_leaf = IndexArming{};
  on_leaf.note_vertex_property(Property(11));
  EXPECT_FALSE(on_leaf.arms_vertex_index_on(Label(1), key));
}

TEST(IndexArming, AnEmptyPathArmsNothing) {
  auto const key = PropertiesPaths{PropertyPath{}};

  auto arming = IndexArming{};
  arming.note_vertex_property(Property(10));

  EXPECT_FALSE(arming.arms_vertex_index_on(Label(1), key));
}

// A unique constraint keeps a skiplist keyed the way an index is and is swept the same way; its
// key is a set of properties rather than paths into them.
TEST(IndexArming, AConstraintArmsOnEitherHalfOfItsKey) {
  auto const key = std::set<PropertyId>{Property(10), Property(11)};

  auto on_label = IndexArming{};
  on_label.note_label(Label(1));
  EXPECT_TRUE(on_label.arms_vertex_index_on(Label(1), key));

  auto on_property = IndexArming{};
  on_property.note_vertex_property(Property(11));
  EXPECT_TRUE(on_property.arms_vertex_index_on(Label(1), key));

  auto on_neither = IndexArming{};
  on_neither.note_vertex_property(Property(12));
  EXPECT_FALSE(on_neither.arms_vertex_index_on(Label(1), key));
}

// An index keyed on a property alone holds an entry per vertex carrying it, whatever labels that
// vertex has, so only writing the property can stale one.
TEST(IndexArming, AGlobalVertexIndexArmsOnItsPropertyAlone) {
  auto on_property = IndexArming{};
  on_property.note_vertex_property(Property(10));
  EXPECT_TRUE(on_property.arms_vertex_property_index_on(Property(10)));
  EXPECT_FALSE(on_property.arms_vertex_property_index_on(Property(11)));
}

// Such an index is not scoped to a label, and no entry of it is touched when one comes or goes,
// so a workload writing only labels must not pay to walk it.
TEST(IndexArming, ALabelWriteArmsNoGlobalVertexIndex) {
  auto arming = IndexArming{};
  arming.note_label(Label(1));

  EXPECT_FALSE(arming.arms_vertex_property_index_on(Property(10)));
}

TEST(IndexArming, AVertexPropertyWriteArmsNoEdgeIndex) {
  auto arming = IndexArming{};
  arming.note_vertex_property(Property(10));

  EXPECT_TRUE(arming.arms_vertex_indexes());
  EXPECT_FALSE(arming.arms_edge_indexes());
  EXPECT_FALSE(arming.arms_edge_index_on(Property(10)));
}

///// EDGE PROPERTIES AND STRUCTURE

TEST(IndexArming, AnEdgePropertyWriteArmsOnlyThatPropertysIndex) {
  auto arming = IndexArming{};
  arming.note_edge_property(Property(10));

  EXPECT_TRUE(arming.arms_edge_index_on(Property(10)));
  EXPECT_FALSE(arming.arms_edge_index_on(Property(11)));
  EXPECT_FALSE(arming.arms_vertex_indexes());
}

// An index keyed on an edge type alone holds an entry per edge of that type, and no property is
// part of that entry, so a property write cannot invalidate one.
TEST(IndexArming, AnEdgePropertyWriteArmsNoEdgeTypeIndex) {
  auto arming = IndexArming{};
  arming.note_edge_property(Property(10));

  EXPECT_FALSE(arming.arms_edge_type_index());
}

// An edge coming or going is the only thing that stales an edge-type index, and it names no
// property, so it arms every edge index rather than one.
TEST(IndexArming, AStructuralEdgeWriteArmsTheEdgeTypeIndex) {
  auto arming = IndexArming{};
  arming.note_edge_structure();

  EXPECT_TRUE(arming.arms_edge_type_index());
  EXPECT_TRUE(arming.arms_edge_indexes());
  EXPECT_FALSE(arming.arms_vertex_indexes());
}

///// ARMING EVERYTHING

// Where entries may point at objects about to be freed, no delta names those, and leaving one
// behind is a dangling pointer rather than a missed saving.
TEST(IndexArming, ArmingAllVertexIndexesArmsEveryVertexKey) {
  auto arming = IndexArming{};
  arming.arm_all_vertex_indexes();

  EXPECT_TRUE(arming.arms_vertex_indexes());
  EXPECT_TRUE(arming.arms_vertex_index_on(Label(1)));
  EXPECT_TRUE(arming.arms_vertex_index_on(Label(999), PropertiesPaths{PropertyPath{Property(999)}}));
  EXPECT_TRUE(arming.arms_vertex_index_on(Label(999), std::set<PropertyId>{Property(999)}));
  EXPECT_TRUE(arming.arms_vertex_property_index_on(Property(999)));
}

// Named per family so that a deleted edge, which says nothing about any vertex index, does not
// cost the vertex indexes their narrowing.
TEST(IndexArming, ArmingAllOfOneFamilyLeavesTheOtherNarrowed) {
  auto vertices = IndexArming{};
  vertices.arm_all_vertex_indexes();
  EXPECT_FALSE(vertices.arms_edge_indexes());
  EXPECT_FALSE(vertices.arms_edge_index_on(Property(1)));

  auto edges = IndexArming{};
  edges.arm_all_edge_indexes();
  EXPECT_TRUE(edges.arms_edge_index_on(Property(1)));
  EXPECT_TRUE(edges.arms_edge_type_index());
  EXPECT_FALSE(edges.arms_vertex_indexes());
  EXPECT_FALSE(edges.arms_vertex_index_on(Label(1)));
  EXPECT_FALSE(edges.arms_vertex_property_index_on(Property(1)));
}

///// MERGING AND REUSE

TEST(IndexArming, MergingTakesTheUnionOfBothFamilies) {
  auto lhs = IndexArming{};
  lhs.note_label(Label(1));
  auto rhs = IndexArming{};
  rhs.note_edge_property(Property(10));

  lhs |= rhs;

  EXPECT_TRUE(lhs.arms_vertex_index_on(Label(1)));
  EXPECT_TRUE(lhs.arms_edge_index_on(Property(10)));
  EXPECT_FALSE(lhs.arms_vertex_index_on(Label(2)));
}

TEST(IndexArming, MergingCarriesAnArmAll) {
  auto lhs = IndexArming{};
  auto rhs = IndexArming{};
  rhs.arm_all_vertex_indexes();

  lhs |= rhs;

  EXPECT_TRUE(lhs.arms_vertex_index_on(Label(1)));
}

// A cycle claims the published holder by swapping its own reset one in, so a holder that has been
// emptied must not report the writes it used to hold.
TEST(IndexArming, ResetForgetsEverything) {
  auto arming = IndexArming{};
  arming.note_label(Label(1));
  arming.note_vertex_property(Property(10));
  arming.note_edge_property(Property(11));
  arming.note_edge_structure();
  arming.arm_all_vertex_indexes();
  arming.arm_all_edge_indexes();
  ASSERT_TRUE(arming.arms_anything());

  arming.reset();

  EXPECT_FALSE(arming.arms_anything());
  EXPECT_FALSE(arming.arms_vertex_index_on(Label(1)));
  EXPECT_FALSE(arming.arms_edge_index_on(Property(11)));
  EXPECT_FALSE(arming.arms_edge_type_index());
}

TEST(IndexArming, AResetHolderStillRecordsWhatComesNext) {
  auto arming = IndexArming{};
  arming.note_label(Label(1));
  arming.reset();

  arming.note_label(Label(2));

  EXPECT_TRUE(arming.arms_vertex_index_on(Label(2)));
  EXPECT_FALSE(arming.arms_vertex_index_on(Label(1)));
}

///// READING DELTAS

// A delta holds the inverse of the write that made it, so the action noted is the opposite of
// what the writer called; the id is the same either way.
TEST(IndexArming, ALabelDeltaArmsTheIndexOnItsLabel) {
  auto deltas = delta_container{};
  auto const &delta = deltas.emplace(Delta::RemoveLabelTag{}, Label(1), nullptr, 0);

  auto arming = IndexArming{};
  arming.for_deltas_of(kOnVertices).note(delta);

  EXPECT_TRUE(arming.arms_vertex_index_on(Label(1)));
  EXPECT_FALSE(arming.arms_vertex_index_on(Label(2)));
}

// A property delta records the property but not whether it belonged to a vertex or an edge; only
// the transaction knows, and it says so when the scope is opened.
TEST(IndexArming, APropertyDeltaIsArmedOnTheSideTheTransactionWroteTo) {
  auto deltas = delta_container{};
  auto const &delta = deltas.emplace(Delta::SetPropertyTag{}, Property(10), PropertyValue{}, nullptr, 0);

  auto on_vertices = IndexArming{};
  on_vertices.for_deltas_of(kOnVertices).note(delta);
  EXPECT_TRUE(on_vertices.arms_vertex_index_on(Label(1), PropertiesPaths{PropertyPath{Property(10)}}));
  EXPECT_FALSE(on_vertices.arms_edge_index_on(Property(10)));

  auto on_edges = IndexArming{};
  on_edges.for_deltas_of(kOnEdges).note(delta);
  EXPECT_TRUE(on_edges.arms_edge_index_on(Property(10)));
  EXPECT_FALSE(on_edges.arms_vertex_indexes());
}

// A transaction that wrote properties on both sides cannot tell which delta belonged to which, so
// each arms both.
TEST(IndexArming, APropertyDeltaFromATransactionWritingBothSidesArmsBoth) {
  auto deltas = delta_container{};
  auto const &delta = deltas.emplace(Delta::SetPropertyTag{}, Property(10), PropertyValue{}, nullptr, 0);

  auto arming = IndexArming{};
  arming.for_deltas_of(PropertyWriteTargets{.vertices = true, .edges = true}).note(delta);

  EXPECT_TRUE(arming.arms_vertex_index_on(Label(1), PropertiesPaths{PropertyPath{Property(10)}}));
  EXPECT_TRUE(arming.arms_edge_index_on(Property(10)));
}

// A scope opened for a transaction that set no properties has nothing to say about a property
// delta. Were one to reach it, the sweep that would collect its own stale entries would be
// silently skipped, so pin that this combination cannot arise unnoticed.
TEST(IndexArming, APropertyDeltaArmsNothingWhenTheTransactionWroteNoProperties) {
  auto deltas = delta_container{};
  auto const &delta = deltas.emplace(Delta::SetPropertyTag{}, Property(10), PropertyValue{}, nullptr, 0);

  auto arming = IndexArming{};
  arming.for_deltas_of(PropertyWriteTargets{}).note(delta);

  EXPECT_FALSE(arming.arms_anything());
}

// Stands for all four edge actions: added or removed, in or out, they collapse to the same
// structural note, so there is one rule here rather than four.
TEST(IndexArming, AnEdgeDeltaArmsTheEdgeTypeIndex) {
  auto deltas = delta_container{};
  auto const &delta =
      deltas.emplace(Delta::RemoveOutEdgeTag{}, EdgeTypeId::FromUint(1), nullptr, EdgeRef{nullptr}, nullptr, 0);

  auto arming = IndexArming{};
  arming.for_deltas_of(kOnVertices).note(delta);

  EXPECT_TRUE(arming.arms_edge_type_index());
  EXPECT_FALSE(arming.arms_vertex_indexes());
}

// A deleted object matters for correctness, but no id narrows which indexes hold its entries, so
// the delta saying so arms nothing: the cycle arms every index of the family instead. Stands for
// all three object actions, which return without noting anything.
TEST(IndexArming, AnObjectDeletionDeltaArmsNothingByItself) {
  auto deltas = delta_container{};
  auto const &delta = deltas.emplace(Delta::DeleteObjectTag{}, nullptr, 0);

  auto arming = IndexArming{};
  arming.for_deltas_of(kOnVertices).note(delta);

  EXPECT_FALSE(arming.arms_anything());
}

TEST(IndexArming, ASingleScopeReadsAWholeDeltaBuffer) {
  auto deltas = delta_container{};
  deltas.emplace(Delta::AddLabelTag{}, Label(1), nullptr, 0);
  deltas.emplace(Delta::SetPropertyTag{}, Property(10), PropertyValue{}, nullptr, 0);
  deltas.emplace(Delta::RemoveLabelTag{}, Label(2), nullptr, 0);

  auto arming = IndexArming{};
  auto const scope = arming.for_deltas_of(kOnVertices);
  for (auto const &delta : deltas) scope.note(delta);

  EXPECT_TRUE(arming.arms_vertex_index_on(Label(1)));
  EXPECT_TRUE(arming.arms_vertex_index_on(Label(2)));
  EXPECT_TRUE(arming.arms_vertex_index_on(Label(3), PropertiesPaths{PropertyPath{Property(10)}}));
  EXPECT_FALSE(arming.arms_vertex_index_on(Label(3)));
  EXPECT_FALSE(arming.arms_edge_indexes());
}
