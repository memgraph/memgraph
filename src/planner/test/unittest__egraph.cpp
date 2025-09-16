// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>

#include "planner/core/egraph.hpp"
#include "planner/core/processing_context.hpp"

namespace memgraph::planner::core {

// Simple test symbol type using enum to be trivially copyable
// TODO: both unittest__enode and this unittest__egraph use TestSymbols, we should unify into a common test helper
// header
enum class TestSymbol : uint32_t {
  A,
  B,
  C,
  D,
  X,
  Y,
  Plus,
  Mul,
  F,
  Test,
  Node,
  // For numbered nodes
  Node0 = 1000,
  // We can generate more by adding to Node0
};

struct NoAnalysis {};

TEST(EGraphBasicOperations, EmptyEGraph) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  EXPECT_TRUE(egraph.empty());
  EXPECT_EQ(egraph.num_classes(), 0);
  EXPECT_EQ(egraph.num_nodes(), 0);
}

TEST(EGraphBasicOperations, AddSimpleENodes) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  // Add leaf nodes
  auto id1 = egraph.emplace(TestSymbol::A);
  auto id2 = egraph.emplace(TestSymbol::B);

  EXPECT_EQ(egraph.num_classes(), 2);
  EXPECT_EQ(egraph.num_nodes(), 2);
  EXPECT_NE(id1, id2);
  EXPECT_TRUE(egraph.has_class(id1));
  EXPECT_TRUE(egraph.has_class(id2));
}

TEST(EGraphBasicOperations, AddNodesWithChildren) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  // Add leaf nodes first
  auto a = egraph.emplace(TestSymbol::A);
  auto b = egraph.emplace(TestSymbol::B);

  // Add node with children
  EClassId plus = egraph.emplace(TestSymbol::Plus, {a, b});

  EXPECT_EQ(egraph.num_classes(), 3);
  EXPECT_EQ(egraph.num_nodes(), 3);
  EXPECT_TRUE(egraph.has_class(plus));
  auto const &enode = egraph.get_enode(plus);
  EXPECT_EQ(enode.arity(), 2);

  auto const &eclass = egraph.eclass(plus);
  EXPECT_EQ(eclass.size(), 1);
  EXPECT_EQ(eclass.representative_id(), plus);
}

TEST(EGraphBasicOperations, DuplicateNodesReturnSameEClass) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  // Add same node twice
  auto id1 = egraph.emplace(TestSymbol::A, 0);
  auto id2 = egraph.emplace(TestSymbol::A, 0);

  EXPECT_EQ(id1, id2);
  EXPECT_EQ(egraph.num_classes(), 1);
  EXPECT_EQ(egraph.num_nodes(), 1);
}

TEST(EGraphMergingOperations, MergeTwoDifferentEClasses) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  auto id1 = egraph.emplace(TestSymbol::A);
  auto id2 = egraph.emplace(TestSymbol::B);

  EXPECT_EQ(egraph.num_classes(), 2);

  auto merged = egraph.merge(id1, id2);

  EXPECT_EQ(egraph.num_classes(), 1);
  EXPECT_EQ(egraph.find(id1), egraph.find(id2));
  EXPECT_TRUE(merged == id1 || merged == id2);
}

TEST(EGraphMergingOperations, MergeSameEClassIsNoOp) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  auto id1 = egraph.emplace(TestSymbol::A);
  auto merged = egraph.merge(id1, id1);

  EXPECT_EQ(merged, id1);
  EXPECT_EQ(egraph.num_classes(), 1);
}

TEST(EGraphMergingOperations, CongruenceAfterMergeAndRebuild) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  // Create: f(a), f(b), merge a and b
  auto a = egraph.emplace(TestSymbol::A);
  auto b = egraph.emplace(TestSymbol::B);
  auto fa = egraph.emplace(TestSymbol::F, {a});
  auto fb = egraph.emplace(TestSymbol::F, {b});

  EXPECT_EQ(egraph.num_classes(), 4);
  EXPECT_NE(egraph.find(fa), egraph.find(fb));

  // Merge a and b
  egraph.merge(a, b);

  // a and b should be same e-class
  EXPECT_EQ(egraph.find(a), egraph.find(b));

  // f(a) and f(b) should not yet be congruent
  EXPECT_NE(egraph.find(fa), egraph.find(fb));

  // Need to rebuild to update congruence
  egraph.rebuild(ctx);

  // f(a) and f(b) should now be congruent
  EXPECT_EQ(egraph.find(fa), egraph.find(fb));

  EXPECT_EQ(egraph.num_classes(), 2);  // One for a=b, one for f(a)=f(b)
}

TEST(EGraphMergingOperations, CongruenceAfterMergeAndRebuildSelfReference) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  auto a = egraph.emplace(TestSymbol::A);
  auto b = egraph.emplace(TestSymbol::B);
  auto fab = egraph.emplace(TestSymbol::F, {a, b});

  EXPECT_EQ(egraph.num_classes(), 3);

  egraph.merge(a, b);
  egraph.merge(a, fab);
  egraph.rebuild(ctx);

  EXPECT_EQ(egraph.num_classes(), 1);
}

TEST(EGraphEClassAccess, GetEClassByID) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  auto id = egraph.emplace(TestSymbol::Test);

  const auto &eclass = egraph.eclass(id);
  EXPECT_EQ(eclass.size(), 1);

  auto repr_id = eclass.representative_id();
  const auto &repr = egraph.get_enode(repr_id);
  EXPECT_EQ(repr.symbol(), TestSymbol::Test);
}

TEST(EGraphClearAndReserve, ClearEmptiesTheGraph) {
  EGraph<TestSymbol, NoAnalysis> egraph;

  egraph.emplace(TestSymbol::A);
  egraph.emplace(TestSymbol::B);

  EXPECT_FALSE(egraph.empty());

  egraph.clear();

  EXPECT_TRUE(egraph.empty());
  EXPECT_EQ(egraph.num_classes(), 0);
  EXPECT_EQ(egraph.num_nodes(), 0);
}

TEST(EGraphRebuildHashconsConsistency, SingleParentHashconsCheck) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  // Create a scenario where an e-class has a single parent
  // This tests the corner case in process_class_parents_for_rebuild
  // where parent_ids.size() == 1
  auto a = egraph.emplace(TestSymbol::A);
  auto fa = egraph.emplace(TestSymbol::F, {a});  // f(a) - single parent of a

  // Create another independent e-class
  auto b = egraph.emplace(TestSymbol::B);

  EXPECT_EQ(egraph.num_classes(), 3);

  // Merge a with b - this will trigger rebuild processing
  // During rebuild, f(a) will be processed as a single parent
  egraph.merge(a, b);

  // The runtime checks should pass - hashcons should already have
  // the correct mapping for the canonicalized f(a) -> fa e-class
  EXPECT_NO_THROW(egraph.rebuild(ctx));

  // Verify the merge worked correctly
  EXPECT_EQ(egraph.find(a), egraph.find(b));
  EXPECT_EQ(egraph.num_classes(), 2);
}

TEST(EGraphRebuildHashconsConsistency, MultipleParentsAfterMerge) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  // Create scenario with multiple parents that become congruent after merge
  auto a = egraph.emplace(TestSymbol::A);
  auto b = egraph.emplace(TestSymbol::B);
  auto c = egraph.emplace(TestSymbol::C);

  // Create f(a, b) and f(c, b) - these will become congruent when a=c
  auto fab = egraph.emplace(TestSymbol::F, {a, b});
  auto fcb = egraph.emplace(TestSymbol::F, {c, b});

  EXPECT_EQ(egraph.num_classes(), 5);
  EXPECT_NE(egraph.find(fab), egraph.find(fcb));

  // Merge a with c - this should make f(a,b) and f(c,b) congruent
  egraph.merge(a, c);

  // Rebuild should handle the multiple congruent parents case
  EXPECT_NO_THROW(egraph.rebuild(ctx));

  // Verify congruence after rebuild
  EXPECT_EQ(egraph.find(fab), egraph.find(fcb));
  EXPECT_EQ(egraph.num_classes(), 3);  // a=c, b, f(a,b)=f(c,b)
}

TEST(EGraphRebuildHashconsConsistency, DeepRebuildSingleParentChain) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  // Create a chain: a -> f(a) -> g(f(a)) where each has single parent
  auto a = egraph.emplace(TestSymbol::A);
  auto fa = egraph.emplace(TestSymbol::F, {a});
  auto gfa = egraph.emplace(TestSymbol::Plus, {fa});  // Using Plus as second function

  // Create another chain: b -> h(b)
  auto b = egraph.emplace(TestSymbol::B);
  auto hb = egraph.emplace(TestSymbol::Mul, {b});  // Using Mul as third function

  EXPECT_EQ(egraph.num_classes(), 5);

  // Merge a chain element - this will cascade through single parents
  egraph.merge(a, b);

  // Each level should be processed with single parent logic
  EXPECT_NO_THROW(egraph.rebuild(ctx));

  // Verify the deep merge worked
  EXPECT_EQ(egraph.find(a), egraph.find(b));
  EXPECT_EQ(egraph.num_classes(), 4);
}

// Helper function to validate congruence closure using canonical ENode objects
bool ValidateCongruenceClosure(EGraph<TestSymbol, NoAnalysis> &egraph) {
  std::unordered_map<ENode<TestSymbol>, EClassId> canonical_forms;

  for (auto const &[class_id, eclass] : egraph.canonical_classes()) {
    for (auto enode_id : eclass.nodes()) {
      auto canonical_enode = egraph.get_enode(enode_id).canonicalize(egraph.union_find());
      auto [it, inserted] = canonical_forms.try_emplace(canonical_enode, class_id);
      if (!inserted && it->second != class_id) {
        return false;  // Same canonical form in different e-classes
      }
    }
  }
  return true;
}

TEST(EGraphCongruenceClosureBug, MissingHashconsUpdateForSingleParent) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  // Create scenario to trigger single parent hashcons bug:
  // We need a case where after deduplication, a canonical e-node has exactly ONE parent

  auto a = egraph.emplace(TestSymbol::A);
  auto f_a = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{a});

  EXPECT_EQ(egraph.num_classes(), 2);  // a, F(a)

  // Trigger a rebuild cycle where F(a) needs its hashcons updated
  // Create another leaf that will merge with 'a'
  auto b = egraph.emplace(TestSymbol::B);
  egraph.merge(a, b);  // Now a≡b

  // During rebuild, F(a) canonicalizes to F(canonical_ab)
  // This creates a single parent scenario for the canonical form
  egraph.rebuild(ctx);

  // Now create a new F node with the merged class - should be found via hashcons
  auto f_merged = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{egraph.find(a)});

  // This should return the same class as f_a because F(canonical_ab) should be in hashcons
  EXPECT_EQ(egraph.find(f_a), f_merged)
      << "F(a) should be found via hashcons lookup, but single parent hashcons update may be missing";

  EXPECT_TRUE(ValidateCongruenceClosure(egraph));
  EXPECT_EQ(egraph.num_classes(), 2);  // (a≡b), F(a≡b)
}

TEST(EGraphCongruenceClosureBug, MissingHashconsUpdateForMultipleParents) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  auto a = egraph.emplace(TestSymbol::A);
  auto b = egraph.emplace(TestSymbol::B);

  auto f_a = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{a});
  auto f_b = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{b});

  EXPECT_EQ(egraph.num_classes(), 4);  // a, b, F(a), F(b)

  egraph.merge(a, b);  // Now a≡b
  egraph.rebuild(ctx);
  EXPECT_EQ(egraph.num_classes(), 2);  // a≡b, F(a≡b)

  egraph.merge(a, f_a);
  egraph.rebuild(ctx);

  EXPECT_EQ(egraph.num_classes(), 1);
  EXPECT_EQ(egraph.find(f_a), egraph.find(a));
}

TEST(EGraphCongruenceClosureBug, ComplexFuzzSequenceInvariantViolation) {
  /*
  This test reproduces a specific fuzz sequence that causes an invariant violation:
  "Non-canonical class ID 9 in canonical_class_ids()"

  The sequence involves multiple CREATE_CONGRUENT, MERGE_CLASSES, CREATE_LEAF, and REBUILD operations
  that eventually lead to a state where canonical_class_ids() contains non-canonical IDs.
  */

  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;
  std::vector<EClassId> created_ids;

  // Operation #1: CREATE_CONGRUENT (raw: 49)
  // Creates two distinct leaf nodes and compound nodes, then merges the leaves
  auto leaf_a1 = egraph.emplace(TestSymbol::A, 0);  // A(D0)
  auto leaf_b1 = egraph.emplace(TestSymbol::B, 1);  // B(D1)
  created_ids.push_back(leaf_a1);
  created_ids.push_back(leaf_b1);

  auto f_a1 = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{leaf_a1});
  auto f_b1 = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{leaf_b1});
  created_ids.push_back(f_a1);
  created_ids.push_back(f_b1);

  egraph.merge(leaf_a1, leaf_b1);  // This should make f_a1 and f_b1 congruent after rebuild

  // Operation #2: MERGE_CLASSES (raw: 112) - Merging class 2 with 1
  // Merge f_a1 (class 2) with leaf_b1 (class 1)
  egraph.merge(f_a1, leaf_b1);

  // Operation #3: CREATE_CONGRUENT (raw: 119)
  auto leaf_c1 = egraph.emplace(TestSymbol::C, 2);  // C(D2)
  auto leaf_d1 = egraph.emplace(TestSymbol::D, 3);  // D(D3)
  created_ids.push_back(leaf_c1);
  created_ids.push_back(leaf_d1);

  auto test_c1 = egraph.emplace(TestSymbol::Test, utils::small_vector<EClassId>{leaf_c1});
  auto test_d1 = egraph.emplace(TestSymbol::Test, utils::small_vector<EClassId>{leaf_d1});
  created_ids.push_back(test_c1);
  created_ids.push_back(test_d1);

  egraph.merge(leaf_c1, leaf_d1);

  // Operation #4: CREATE_CONGRUENT (raw: 39)
  auto leaf_x1 = egraph.emplace(TestSymbol::X, 4);  // X(D4)
  auto leaf_a2 = egraph.emplace(TestSymbol::A, 5);  // A(D5)
  created_ids.push_back(leaf_x1);
  created_ids.push_back(leaf_a2);

  auto node_x1 = egraph.emplace(TestSymbol::Node, utils::small_vector<EClassId>{leaf_x1});
  auto node_a2 = egraph.emplace(TestSymbol::Node, utils::small_vector<EClassId>{leaf_a2});
  created_ids.push_back(node_x1);
  created_ids.push_back(node_a2);

  egraph.merge(leaf_x1, leaf_a2);

  // Operation #5: CREATE_CONGRUENT (raw: 4)
  auto leaf_b2 = egraph.emplace(TestSymbol::B, 6);  // B(D6)
  auto leaf_c2 = egraph.emplace(TestSymbol::C, 7);  // C(D7)
  created_ids.push_back(leaf_b2);
  created_ids.push_back(leaf_c2);

  auto plus_b2 = egraph.emplace(TestSymbol::Plus, utils::small_vector<EClassId>{leaf_b2});
  auto plus_c2 = egraph.emplace(TestSymbol::Plus, utils::small_vector<EClassId>{leaf_c2});
  created_ids.push_back(plus_b2);
  created_ids.push_back(plus_c2);

  egraph.merge(leaf_b2, leaf_c2);

  // Operation #6: CREATE_CONGRUENT (raw: 39)
  auto leaf_d2 = egraph.emplace(TestSymbol::D, 8);  // D(D8)
  auto leaf_y2 = egraph.emplace(TestSymbol::Y, 9);  // Y(D9)
  created_ids.push_back(leaf_d2);
  created_ids.push_back(leaf_y2);

  auto mul_d2 = egraph.emplace(TestSymbol::Mul, utils::small_vector<EClassId>{leaf_d2});
  auto mul_y2 = egraph.emplace(TestSymbol::Mul, utils::small_vector<EClassId>{leaf_y2});
  created_ids.push_back(mul_d2);
  created_ids.push_back(mul_y2);

  egraph.merge(leaf_d2, leaf_y2);

  // Operation #8: MERGE_CLASSES (raw: 12) - Merging class 2 with 12
  if (created_ids.size() > 12) {
    egraph.merge(created_ids[2], created_ids[12]);
  }

  // Operation #9: MERGE_CLASSES (raw: 17) - Merging class 3 with 18
  if (created_ids.size() > 18) {
    egraph.merge(created_ids[3], created_ids[18]);
  }

  // Operation #14: MERGE_CLASSES (raw: 57) - Merging class 8 with 19
  if (created_ids.size() > 19) {
    egraph.merge(created_ids[8], created_ids[19]);
  }

  // Operation #17: MERGE_CLASSES (raw: 42) - Merging class 12 with 17
  if (created_ids.size() > 17) {
    egraph.merge(created_ids[12], created_ids[17]);
  }

  // Operation #18: REBUILD (raw: 3) - This should trigger the invariant violation
  // The issue is that after all these operations, canonical_class_ids() contains
  // non-canonical class IDs, which violates the invariant.

  // Before rebuild, let's check the current state
  size_t classes_before = egraph.num_classes();
  size_t nodes_before = egraph.num_nodes();

  // This rebuild should detect the invariant violation
  egraph.rebuild(ctx);

  // After rebuild, verify the invariants are satisfied
  size_t classes_after = egraph.num_classes();
  size_t nodes_after = egraph.num_nodes();

  // The rebuild should have cleaned up the state
  EXPECT_LE(classes_after, classes_before);  // Classes should not increase after rebuild
  EXPECT_EQ(nodes_after, nodes_before);      // Nodes should remain the same

  // Verify that all canonical class IDs are actually canonical
  for (auto id : egraph.canonical_class_ids()) {
    EXPECT_EQ(egraph.find(id), id) << "Non-canonical class ID " << id << " found in canonical_class_ids()";
  }
}
}  // namespace memgraph::planner::core
