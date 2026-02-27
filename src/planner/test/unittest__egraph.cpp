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

#include <gtest/gtest.h>

#include "planner/egraph/egraph.hpp"
#include "planner/egraph/processing_context.hpp"
#include "test_symbols.hpp"

import memgraph.planner.core.eids;

namespace memgraph::planner::core {

using namespace test;

TEST(EGraphBasicOperations, EmptyEGraph) {
  auto const egraph = EGraph<Op, NoAnalysis>{};

  EXPECT_TRUE(egraph.empty());
  EXPECT_EQ(egraph.num_classes(), 0);
  EXPECT_EQ(egraph.num_nodes(), 0);
}

TEST(EGraphBasicOperations, AddSimpleENodes) {
  EGraph<Op, NoAnalysis> egraph;

  // Add leaf nodes
  auto [info1, info1_node, inserted1] = egraph.emplace(Op::A);
  auto [info2, info2_node, inserted2] = egraph.emplace(Op::B);

  EXPECT_EQ(egraph.num_classes(), 2);
  EXPECT_EQ(egraph.num_nodes(), 2);
  EXPECT_NE(info1, info2);
  EXPECT_TRUE(egraph.has_class(info1));
  EXPECT_TRUE(egraph.has_class(info2));
  EXPECT_TRUE(inserted1);
  EXPECT_TRUE(inserted2);
}

TEST(EGraphBasicOperations, AddNodesWithChildren) {
  EGraph<Op, NoAnalysis> egraph;

  // Add leaf nodes first
  auto [a, a_node, a_inserted] = egraph.emplace(Op::A);
  auto [b, b_node, b_inserted] = egraph.emplace(Op::B);

  // Add node with children
  auto [plus, plus_node, plus_inserted] = egraph.emplace(Op::Add, {a, b});

  EXPECT_EQ(egraph.num_classes(), 3);
  EXPECT_EQ(egraph.num_nodes(), 3);
  EXPECT_TRUE(egraph.has_class(plus));
  auto const &enode = egraph.get_enode(plus_node);
  EXPECT_EQ(enode.arity(), 2);

  auto const &eclass = egraph.eclass(plus);
  EXPECT_EQ(eclass.size(), 1);
}

TEST(EGraphBasicOperations, DuplicateNodesReturnSameEClass) {
  EGraph<Op, NoAnalysis> egraph;

  // Add same node twice
  auto [info1, info1_node, inserted1] = egraph.emplace(Op::A, 0);
  auto [info2, info2_node, inserted2] = egraph.emplace(Op::A, 0);

  EXPECT_EQ(info1, info2);
  EXPECT_EQ(egraph.num_classes(), 1);
  EXPECT_EQ(egraph.num_nodes(), 1);
  EXPECT_TRUE(inserted1);
  EXPECT_FALSE(inserted2);  // Second emplace finds existing node
}

TEST(EGraphMergingOperations, MergeTwoDifferentEClasses) {
  EGraph<Op, NoAnalysis> egraph;

  auto [info1, info1_node, ins1] = egraph.emplace(Op::A);
  auto [info2, info2_node, ins2] = egraph.emplace(Op::B);
  auto id1 = info1;
  auto id2 = info2;

  EXPECT_EQ(egraph.num_classes(), 2);

  auto [merged, did_merge] = egraph.merge(id1, id2);

  EXPECT_EQ(egraph.num_classes(), 1);
  EXPECT_EQ(egraph.find(id1), egraph.find(id2));
  EXPECT_TRUE(merged == id1 || merged == id2);
  EXPECT_TRUE(did_merge);
}

TEST(EGraphMergingOperations, MergeSameEClassIsNoOp) {
  EGraph<Op, NoAnalysis> egraph;

  auto [info1, info1_node, ins1] = egraph.emplace(Op::A);
  auto id1 = info1;
  auto [merged, did_merge] = egraph.merge(id1, id1);

  EXPECT_EQ(merged, id1);
  EXPECT_EQ(egraph.num_classes(), 1);
  EXPECT_FALSE(did_merge);  // Same e-class, no merge needed
}

TEST(EGraphMergingOperations, CongruenceAfterMergeAndRebuild) {
  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  // Create: f(a), f(b), merge a and b
  auto [a, a_node, a_ins] = egraph.emplace(Op::A);
  auto [b, b_node, b_ins] = egraph.emplace(Op::B);
  auto [fa, fa_node, fa_ins] = egraph.emplace(Op::F, {a});
  auto [fb, fb_node, fb_ins] = egraph.emplace(Op::F, {b});

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
  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  auto [a, a_node, a_ins] = egraph.emplace(Op::A);
  auto [b, b_node, b_ins] = egraph.emplace(Op::B);
  auto [fab, fab_node, fab_ins] = egraph.emplace(Op::F, {a, b});

  EXPECT_EQ(egraph.num_classes(), 3);

  egraph.merge(a, b);
  egraph.merge(a, fab);
  egraph.rebuild(ctx);

  EXPECT_EQ(egraph.num_classes(), 1);
}

TEST(EGraphBasicOperations, GetEClassByID) {
  EGraph<Op, NoAnalysis> egraph;

  auto [info, info_node, ins] = egraph.emplace(Op::Test);
  auto id = info;

  const auto &eclass = egraph.eclass(id);
  EXPECT_EQ(eclass.size(), 1);

  auto repr_id = *eclass.nodes().begin();
  const auto &repr = egraph.get_enode(repr_id);
  EXPECT_EQ(repr.symbol(), Op::Test);
}

TEST(EGraphBasicOperations, ClearEmptiesTheGraph) {
  EGraph<Op, NoAnalysis> egraph;

  egraph.emplace(Op::A);
  egraph.emplace(Op::B);

  EXPECT_FALSE(egraph.empty());

  egraph.clear();

  EXPECT_TRUE(egraph.empty());
  EXPECT_EQ(egraph.num_classes(), 0);
  EXPECT_EQ(egraph.num_nodes(), 0);
}

TEST(EGraphRebuildHashconsConsistency, SingleParentHashconsCheck) {
  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  // Create a scenario where an e-class has a single parent
  // This tests the corner case in process_class_parents_for_rebuild
  // where parent_ids.size() == 1
  auto a = egraph.emplace(Op::A).eclass_id;
  egraph.emplace(Op::F, {a});  // f(a) - single parent of a

  // Create another independent e-class
  auto b = egraph.emplace(Op::B).eclass_id;

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
  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  // Create scenario with multiple parents that become congruent after merge
  auto a = egraph.emplace(Op::A).eclass_id;
  auto b = egraph.emplace(Op::B).eclass_id;
  auto c = egraph.emplace(Op::C).eclass_id;

  // Create f(a, b) and f(c, b) - these will become congruent when a=c
  auto fab = egraph.emplace(Op::F, {a, b}).eclass_id;
  auto fcb = egraph.emplace(Op::F, {c, b}).eclass_id;

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
  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  // Create a chain: a -> f(a) -> g(f(a)) where each has single parent
  auto a = egraph.emplace(Op::A).eclass_id;
  auto fa = egraph.emplace(Op::F, {a}).eclass_id;
  egraph.emplace(Op::Add, {fa});  // Using Plus as second function

  // Create another chain: b -> h(b)
  auto b = egraph.emplace(Op::B).eclass_id;
  egraph.emplace(Op::Mul, {b});  // Using Mul as third function

  EXPECT_EQ(egraph.num_classes(), 5);

  // Merge a chain element - this will cascade through single parents
  egraph.merge(a, b);

  // Each level should be processed with single parent logic
  EXPECT_NO_THROW(egraph.rebuild(ctx));

  // Verify the deep merge worked
  EXPECT_EQ(egraph.find(a), egraph.find(b));
  EXPECT_EQ(egraph.num_classes(), 4);
}

TEST(EGraphCongruenceClosureBug, MissingHashconsUpdateForSingleParent) {
  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  // Create scenario to trigger single parent hashcons bug:
  // We need a case where after deduplication, a canonical e-node has exactly ONE parent

  auto a = egraph.emplace(Op::A).eclass_id;
  auto f_a = egraph.emplace(Op::F, {a}).eclass_id;

  EXPECT_EQ(egraph.num_classes(), 2);  // a, F(a)

  // Trigger a rebuild cycle where F(a) needs its hashcons updated
  // Create another leaf that will merge with 'a'
  auto b = egraph.emplace(Op::B).eclass_id;
  egraph.merge(a, b);  // Now a≡b

  // During rebuild, F(a) canonicalizes to F(canonical_ab)
  // This creates a single parent scenario for the canonical form
  egraph.rebuild(ctx);

  // Now create a new F node with the merged class - should be found via hashcons
  auto f_merged = egraph.emplace(Op::F, {egraph.find(a)}).eclass_id;

  // This should return the same class as f_a because F(canonical_ab) should be in hashcons
  EXPECT_EQ(egraph.find(f_a), f_merged)
      << "F(a) should be found via hashcons lookup, but single parent hashcons update may be missing";

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
  EXPECT_EQ(egraph.num_classes(), 2);  // (a≡b), F(a≡b)
}

TEST(EGraphCongruenceClosureBug, MissingHashconsUpdateForMultipleParents) {
  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  auto a = egraph.emplace(Op::A).eclass_id;
  auto b = egraph.emplace(Op::B).eclass_id;

  auto f_a = egraph.emplace(Op::F, {a}).eclass_id;
  egraph.emplace(Op::F, {b});

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
  // This case came for a fuzz test and found a complex example where cogruence was broken
  // Fix has been applied but this test remains to ensure we do not redo the bug
  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  auto leaf_0 = egraph.emplace(Op::A, 0).eclass_id;
  auto leaf_1 = egraph.emplace(Op::A, 1).eclass_id;
  auto f_leaf_0 = egraph.emplace(Op::F, {leaf_0}).eclass_id;
  auto f_leaf_1 = egraph.emplace(Op::F, {leaf_1}).eclass_id;
  auto leaf_2 = egraph.emplace(Op::A, 2).eclass_id;
  auto leaf_3 = egraph.emplace(Op::A, 3).eclass_id;
  auto leaf_4 = egraph.emplace(Op::A, 4).eclass_id;
  auto leaf_5 = egraph.emplace(Op::A, 5).eclass_id;
  auto leaf_6 = egraph.emplace(Op::A, 6).eclass_id;
  auto f2_leaf_5 = egraph.emplace(Op::F2, {leaf_5}).eclass_id;
  auto f2_leaf_6 = egraph.emplace(Op::F2, {leaf_6}).eclass_id;

  egraph.merge(leaf_0, leaf_1);
  egraph.merge(f_leaf_0, leaf_1);
  egraph.merge(leaf_2, leaf_3);
  egraph.merge(leaf_5, leaf_6);
  egraph.merge(f_leaf_0, leaf_4);
  egraph.merge(f_leaf_1, f2_leaf_5);
  egraph.merge(leaf_2, f2_leaf_6);
  egraph.merge(leaf_4, leaf_6);

  egraph.rebuild(ctx);

  EXPECT_EQ(egraph.num_classes(), 1);
  for (auto id : egraph.canonical_class_ids()) {
    EXPECT_EQ(egraph.find(id), id) << "Non-canonical class ID " << id << " found in canonical_class_ids()";
  }
}

TEST(EGraphCongruenceClosureBug, InfiniteSelfReference) {
  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  auto zero_1 = egraph.emplace(Op::A, 0).eclass_id;  // First zero
  auto zero_2 = egraph.emplace(Op::A, 1).eclass_id;  // Second zero
  auto add = egraph.emplace(Op::F, {zero_1, zero_2}).eclass_id;
  egraph.merge(zero_1, add);  // infinate self reference
  egraph.rebuild(ctx);        // ensure graph is correct
  EXPECT_EQ(egraph.num_classes(), 2);

  egraph.merge(zero_1, zero_2);  // actually only one zero now
  egraph.rebuild(ctx);           // ensure graph is correct
  EXPECT_EQ(egraph.num_classes(), 1);
}

// ============================================================================
// Duplicate E-Node Removal Tests
// ============================================================================
//
// When congruence causes multiple e-nodes to have the same canonical form,
// duplicates are detected via hashcons collision and removed.

TEST(EGraphDuplicateRemoval, CongruenceRemovesDuplicateENodes) {
  // When e-nodes become identical after canonicalization, duplicates are removed.
  //
  //   F(a) and F(b) are separate e-nodes
  //   Merge a≡b → both canonicalize to F(canonical)
  //   One is kept, one is removed as duplicate

  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto b = egraph.emplace(Op::A, 1).eclass_id;
  egraph.emplace(Op::F, {a});
  egraph.emplace(Op::F, {b});

  EXPECT_EQ(egraph.num_nodes(), 4);       // 2 leaves + 2 F nodes
  EXPECT_EQ(egraph.num_dead_nodes(), 0);  // no duplicates yet
  EXPECT_EQ(egraph.num_classes(), 4);     // all separate

  egraph.merge(a, b);
  egraph.rebuild(ctx);

  EXPECT_EQ(egraph.num_nodes(), 4);       // total created unchanged
  EXPECT_EQ(egraph.num_dead_nodes(), 1);  // one F was duplicate
  EXPECT_EQ(egraph.num_live_nodes(), 3);  // 1 merged leaf + 1 F
  EXPECT_EQ(egraph.num_classes(), 2);     // a≡b, F(a)≡F(b)
}

TEST(EGraphDuplicateRemoval, MultipleDuplicatesRemoved) {
  // When 3+ e-nodes canonicalize to the same form, all but one are removed.
  //
  //   F(a), F(b), F(c) where a, b, c are separate
  //   Merge a≡b≡c → all three F nodes canonicalize to F(canonical)
  //   Two duplicates removed, one kept

  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto b = egraph.emplace(Op::A, 1).eclass_id;
  auto c = egraph.emplace(Op::A, 2).eclass_id;
  egraph.emplace(Op::F, {a});
  egraph.emplace(Op::F, {b});
  egraph.emplace(Op::F, {c});

  EXPECT_EQ(egraph.num_nodes(), 6);
  EXPECT_EQ(egraph.num_dead_nodes(), 0);

  egraph.merge(a, b);
  egraph.merge(a, c);
  egraph.rebuild(ctx);

  EXPECT_EQ(egraph.num_nodes(), 6);       // total unchanged
  EXPECT_EQ(egraph.num_dead_nodes(), 2);  // two F nodes were duplicates
  EXPECT_EQ(egraph.num_live_nodes(), 4);  // 1 merged leaf + 1 F
  EXPECT_EQ(egraph.num_classes(), 2);

  // Invariant: live count matches sum of e-nodes across canonical classes
  std::size_t sum = 0;
  for (auto const &[id, eclass] : egraph.canonical_classes()) {
    sum += eclass.nodes().size();
  }
  EXPECT_EQ(sum, egraph.num_live_nodes());
}

TEST(EGraphDuplicateRemoval, CascadingCongruenceRemovesDuplicates) {
  // Test deferred duplicate detection: when parent e-nodes are in different
  // e-classes, they're merged first, then duplicates detected in subsequent
  // rebuild iteration.
  //
  //   G(F(a)) and G(F(b)) where F(a), F(b) are in different e-classes
  //   Merge a≡b → F(a)≡F(b) by congruence, then G(F(a))≡G(F(b))
  //   Duplicates removed at each level

  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto b = egraph.emplace(Op::A, 1).eclass_id;
  auto fa = egraph.emplace(Op::F, {a}).eclass_id;
  auto fb = egraph.emplace(Op::F, {b}).eclass_id;
  egraph.emplace(Op::F2, {fa});  // G(F(a))
  egraph.emplace(Op::F2, {fb});  // G(F(b))

  EXPECT_EQ(egraph.num_nodes(), 6);
  EXPECT_EQ(egraph.num_dead_nodes(), 0);
  EXPECT_EQ(egraph.num_classes(), 6);

  egraph.merge(a, b);
  egraph.rebuild(ctx);

  // F(a)≡F(b) removes one F duplicate
  // G(F(a))≡G(F(b)) removes one G duplicate
  EXPECT_EQ(egraph.num_nodes(), 6);
  EXPECT_EQ(egraph.num_dead_nodes(), 2);
  EXPECT_EQ(egraph.num_live_nodes(), 4);
  EXPECT_EQ(egraph.num_classes(), 3);  // a≡b, F(a)≡F(b), G(F)

  // Invariant: live count matches sum of e-nodes across canonical classes
  std::size_t sum = 0;
  for (auto const &[id, eclass] : egraph.canonical_classes()) {
    sum += eclass.nodes().size();
  }
  EXPECT_EQ(sum, egraph.num_live_nodes());
}

TEST(EGraphDuplicateRemoval, ClearResetsDuplicateCount) {
  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto b = egraph.emplace(Op::A, 1).eclass_id;
  egraph.emplace(Op::F, {a});
  egraph.emplace(Op::F, {b});

  egraph.merge(a, b);
  egraph.rebuild(ctx);

  EXPECT_EQ(egraph.num_dead_nodes(), 1);

  egraph.clear();

  EXPECT_EQ(egraph.num_nodes(), 0);
  EXPECT_EQ(egraph.num_dead_nodes(), 0);
}

// ============================================================================
// Duplicate Detection Triggers E-Class Merge Tests
// ============================================================================
//
// When duplicate e-nodes are detected during hashcons repair, their e-classes
// must be merged. This tests the scenario where:
//   1. E-nodes in different e-classes become congruent after a merge
//   2. During rebuild, hashcons repair detects duplicates
//   3. The e-classes containing the duplicates must be merged

TEST(EGraphCongruenceClosureBug, DuplicateDetectionMergesEClasses) {
  // Reproduces the bug from crash-19575df5f7b4c24248ece3ea22ac44956c4bacb9
  //
  // Scenario:
  //   n0 = A      -> class 0
  //   n1 = F(n0)  -> class 1  (n1 is parent of class 0)
  //   n2 = F(n1)  -> class 2  (n2 is parent of class 1)
  //   merge(n0, n1)           (creates self-referential class)
  //   n3 = F(n2)  -> class 3  (created AFTER merge, BEFORE rebuild)
  //   rebuild()
  //
  // After merge(n0, n1), n1's children [class 0] canonicalize to [class 1].
  // So n1 = F(class 1). But n2 = F(class 1) also exists in class 2!
  // These are congruent duplicates, so class 1 and class 2 must merge.
  // This cascades to merge class 3 as well.
  //
  // Expected: All F nodes end up in the same e-class.

  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  // n0 = A -> class 0
  auto n0 = egraph.emplace(Op::A, 203).eclass_id;
  EXPECT_EQ(egraph.num_classes(), 1);

  // n1 = F(n0) -> class 1
  auto n1 = egraph.emplace(Op::F, {n0}).eclass_id;
  EXPECT_EQ(egraph.num_classes(), 2);

  // n2 = F(n1) -> class 2
  auto n2 = egraph.emplace(Op::F, {n1}).eclass_id;
  EXPECT_EQ(egraph.num_classes(), 3);

  // merge(n0, n1) - creates self-referential class
  egraph.merge(n0, n1);
  // Don't rebuild yet - simulate creating more nodes before rebuild
  // After merge, num_classes is 2 (class 0 merged into class 1, class 2 remains)
  EXPECT_EQ(egraph.num_classes(), 2);

  // n3 = F(n2) -> new class
  auto n3 = egraph.emplace(Op::F, {n2}).eclass_id;
  EXPECT_EQ(egraph.num_classes(), 3);

  // Now rebuild - this should trigger congruence closure
  egraph.rebuild(ctx);

  // After rebuild:
  // - n1 = F(class 1) (since n0 merged with n1, and n1's child was n0 = class 0 -> class 1)
  // - n2 = F(class 1) (n2's child was n1 = class 1)
  // - n3 = F(class 1) (n3's child was n2, which is now congruent to n1)
  // All F nodes should be in the same e-class

  // The merged class should contain n0 and all F nodes
  EXPECT_EQ(egraph.find(n1), egraph.find(n2));
  EXPECT_EQ(egraph.find(n2), egraph.find(n3));

  // Should have only 1 e-class (all merged together)
  EXPECT_EQ(egraph.num_classes(), 1);

  // Validate congruence closure
  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

TEST(EGraphCongruenceClosureBug, DuplicateDetectionWithMultipleLayers) {
  // Similar to above but with deeper nesting to ensure cascading works
  //
  //   a, b separate
  //   F(a), F(b) separate
  //   F2(F(a)), F2(F(b)) separate
  //   merge(a, b)
  //   rebuild -> F(a)≡F(b), then F2(F(a))≡F2(F(b))

  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto b = egraph.emplace(Op::A, 1).eclass_id;
  auto fa = egraph.emplace(Op::F, {a}).eclass_id;
  auto fb = egraph.emplace(Op::F, {b}).eclass_id;
  auto f2fa = egraph.emplace(Op::F2, {fa}).eclass_id;
  auto f2fb = egraph.emplace(Op::F2, {fb}).eclass_id;

  EXPECT_EQ(egraph.num_classes(), 6);

  egraph.merge(a, b);
  egraph.rebuild(ctx);

  // After merge(a,b) and rebuild:
  // - a≡b
  // - F(a)≡F(b) by congruence
  // - F2(F(a))≡F2(F(b)) by congruence
  EXPECT_EQ(egraph.find(a), egraph.find(b));
  EXPECT_EQ(egraph.find(fa), egraph.find(fb));
  EXPECT_EQ(egraph.find(f2fa), egraph.find(f2fb));

  EXPECT_EQ(egraph.num_classes(), 3);  // a≡b, F(a)≡F(b), F2(F(a))≡F2(F(b))

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

TEST(EGraphCongruenceClosureBug, IndirectCongruenceViaChildMerges) {
  // Regression test for congruence closure bug where two e-nodes become
  // congruent via child merges but aren't detected as duplicates.
  //
  // The bug: When two e-nodes have different children that become equivalent
  // through separate merge chains, the rebuild may fail to detect that the
  // e-nodes are now congruent and should be merged.
  //
  // Scenario:
  //   a, b are separate leaves
  //   c = F(a), d = F(b) - will become self-referential
  //   merge(c, a) - a's e-class now contains F(a)
  //   merge(d, b) - b's e-class now contains F(b)
  //
  //   e = G(c, b) - after canonicalization: G(find(c), find(b)) = G(E_a, E_b)
  //   f = G(a, d) - after canonicalization: G(find(a), find(d)) = G(E_a, E_b)
  //
  //   Both e and f canonicalize to G(E_a, E_b), so they must be merged!
  //
  // This is a minimal reproduction of the bug found by the fuzzer in
  // RepeatedVarInNestedPatternWithSelfReferentialEClass where n11 and n13
  // should have been merged but weren't.

  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  // Create two separate leaves
  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto b = egraph.emplace(Op::A, 1).eclass_id;

  // Create F(a) and F(b)
  auto c = egraph.emplace(Op::F, {a}).eclass_id;  // F(a)
  auto d = egraph.emplace(Op::F, {b}).eclass_id;  // F(b)

  // Create G(c, b) and G(a, d) BEFORE the merges
  // These will become congruent after the merges
  auto e = egraph.emplace(Op::F2, {c, b}).eclass_id;  // G(F(a), b)
  auto f = egraph.emplace(Op::F2, {a, d}).eclass_id;  // G(a, F(b))

  EXPECT_EQ(egraph.num_classes(), 6);
  EXPECT_NE(egraph.find(e), egraph.find(f));  // Not yet congruent

  // Make a and b self-referential:
  // merge(F(a), a) => a's e-class contains {a, F(a)}, so find(c) = find(a)
  // merge(F(b), b) => b's e-class contains {b, F(b)}, so find(d) = find(b)
  egraph.merge(c, a);
  egraph.merge(d, b);

  // Before rebuild, e and f are in separate e-classes
  EXPECT_NE(egraph.find(e), egraph.find(f));

  egraph.rebuild(ctx);

  // After rebuild:
  // - e = G(c, b) canonicalizes to G(find(c), find(b)) = G(E_a, E_b)
  // - f = G(a, d) canonicalizes to G(find(a), find(d)) = G(E_a, E_b)
  // Both canonicalize to the same form, so they MUST be merged!

  EXPECT_EQ(egraph.find(e), egraph.find(f)) << "e and f should be in the same e-class after rebuild because both "
                                               "canonicalize to G(E_a, E_b)";

  // Should have 3 e-classes: {a, F(a)}, {b, F(b)}, {G(E_a, E_b)}
  EXPECT_EQ(egraph.num_classes(), 3);

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

TEST(EGraphCongruenceClosureBug, SelfRefWithIndirectChildCongruence) {
  // Exact reproduction from the failing fuzzer test
  // RepeatedVarInNestedPatternWithSelfReferentialEClass
  //
  // This copies the EXACT sequence of operations that triggers the bug.
  // The bug is that n11 and n13 should be in the same e-class after rebuild
  // because both canonicalize to F2(E_n1, E_n0), but they aren't.

  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  // Exact reproduction from unittest__ematch.cpp
  auto n0 = egraph.emplace(Op::A, 0).eclass_id;
  auto n1 = egraph.emplace(Op::A, 1).eclass_id;
  auto n2 = egraph.emplace(Op::F, {n1, n1}).eclass_id;
  auto n3 = egraph.emplace(Op::F, {n0, n0}).eclass_id;
  auto n4 = egraph.emplace(Op::F, {n3, n3}).eclass_id;
  auto n5 = egraph.emplace(Op::F, {n0, n0}).eclass_id;
  auto n6 = egraph.emplace(Op::F, {n5, n5}).eclass_id;
  auto n7 = egraph.emplace(Op::F, {n1, n1}).eclass_id;
  auto n8 = egraph.emplace(Op::F, {n7, n7}).eclass_id;
  auto n9 = egraph.emplace(Op::F, {n6, n6}).eclass_id;
  auto n10 = egraph.emplace(Op::F, {n5, n5}).eclass_id;
  auto n11 = egraph.emplace(Op::F, {n8, n0}).eclass_id;
  auto n12 = egraph.emplace(Op::F, {n10, n5}).eclass_id;
  auto n13 = egraph.emplace(Op::F, {n7, n10}).eclass_id;
  auto n14 = egraph.emplace(Op::F, {n1, n1}).eclass_id;
  auto n15 = egraph.emplace(Op::F, {n5, n5}).eclass_id;

  // First batch of Add merges (using F2 since we don't have Add)
  auto n16 = egraph.emplace(Op::F2, {n0, n0}).eclass_id;
  egraph.merge(n16, n0);
  auto n17 = egraph.emplace(Op::F2, {n8, n8}).eclass_id;
  egraph.merge(n17, n8);
  auto n18 = egraph.emplace(Op::F2, {n14, n14}).eclass_id;
  egraph.merge(n18, n14);
  auto n19 = egraph.emplace(Op::F2, {n11, n11}).eclass_id;
  egraph.merge(n19, n11);
  auto n20 = egraph.emplace(Op::F2, {n15, n15}).eclass_id;
  egraph.merge(n20, n15);
  auto n21 = egraph.emplace(Op::F2, {n13, n13}).eclass_id;
  egraph.merge(n21, n13);
  auto n22 = egraph.emplace(Op::F2, {n1, n1}).eclass_id;
  egraph.merge(n22, n1);
  auto n23 = egraph.emplace(Op::F2, {n9, n9}).eclass_id;
  egraph.merge(n23, n9);

  // First batch of F merges (self-referential)
  auto n24 = egraph.emplace(Op::F, {n0, n0}).eclass_id;
  egraph.merge(n24, n0);
  auto n25 = egraph.emplace(Op::F, {n8, n8}).eclass_id;
  egraph.merge(n25, n8);
  auto n26 = egraph.emplace(Op::F, {n14, n14}).eclass_id;
  egraph.merge(n26, n14);
  auto n27 = egraph.emplace(Op::F, {n11, n11}).eclass_id;
  egraph.merge(n27, n11);
  auto n28 = egraph.emplace(Op::F, {n15, n15}).eclass_id;
  egraph.merge(n28, n15);
  auto n29 = egraph.emplace(Op::F, {n13, n13}).eclass_id;
  egraph.merge(n29, n13);
  auto n30 = egraph.emplace(Op::F, {n1, n1}).eclass_id;
  egraph.merge(n30, n1);
  auto n31 = egraph.emplace(Op::F, {n15, n15}).eclass_id;
  egraph.merge(n31, n15);

  // Second batch of F2 merges
  auto n32 = egraph.emplace(Op::F2, {n30, n30}).eclass_id;
  egraph.merge(n32, n30);
  auto n33 = egraph.emplace(Op::F2, {n11, n11}).eclass_id;
  egraph.merge(n33, n11);
  auto n34 = egraph.emplace(Op::F2, {n31, n31}).eclass_id;
  egraph.merge(n34, n31);
  auto n35 = egraph.emplace(Op::F2, {n13, n13}).eclass_id;
  egraph.merge(n35, n13);
  auto n36 = egraph.emplace(Op::F2, {n32, n32}).eclass_id;
  egraph.merge(n36, n32);
  auto n37 = egraph.emplace(Op::F2, {n33, n33}).eclass_id;
  egraph.merge(n37, n33);
  auto n38 = egraph.emplace(Op::F2, {n34, n34}).eclass_id;
  egraph.merge(n38, n34);
  auto n39 = egraph.emplace(Op::F2, {n35, n35}).eclass_id;
  egraph.merge(n39, n35);

  // Final nodes
  auto n40 = egraph.emplace(Op::F, {n38, n21}).eclass_id;
  auto n41 = egraph.emplace(Op::F, {n17, n28}).eclass_id;

  // Suppress unused variable warnings
  (void)n2;
  (void)n4;
  (void)n12;
  (void)n40;
  (void)n41;

  egraph.rebuild(ctx);

  // After rebuild:
  // n11 = F(n8, n0), and find(n8) should be in n1's e-class, find(n0) is E_n0
  // n13 = F(n7, n10), and find(n7) = n1's e-class, find(n10) = n0's e-class
  // Both should canonicalize to F(E_n1, E_n0) and be merged!

  EXPECT_EQ(egraph.find(n11), egraph.find(n13)) << "n11 and n13 should be in the same e-class because both "
                                                   "canonicalize to F(E_n1, E_n0). find(n11)="
                                                << egraph.find(n11) << ", find(n13)=" << egraph.find(n13);

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

TEST(EGraphCongruenceClosureBug, DuplicateInSameEClass) {
  // Test: When two e-nodes in the SAME e-class canonicalize to the same form,
  // one is removed as a duplicate but no merge is triggered.
  //
  // Scenario:
  //   a = leaf
  //   f1 = F(a)
  //   f2 = F(a)  <- same canonical form as f1, but different e-node
  //   merge(f1, f2)  <- now both e-nodes are in the same e-class
  //   rebuild should detect f2 as duplicate of f1 and remove it,
  //   but no inter-class merge is needed

  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto f1 = egraph.emplace(Op::F, {a}).eclass_id;
  auto f2 = egraph.emplace(Op::F, {a}).eclass_id;  // Same canonical form as f1

  // f1 and f2 are already the same due to hashcons deduplication at emplace time
  EXPECT_EQ(f1, f2) << "Hashcons should deduplicate at emplace time";
  EXPECT_EQ(egraph.num_classes(), 2);  // a, F(a)

  // To actually test duplicate removal within same e-class, we need to create
  // two e-nodes that become duplicates AFTER a merge changes their children
  auto b = egraph.emplace(Op::A, 1).eclass_id;
  auto fa = egraph.emplace(Op::F, {a}).eclass_id;  // F(a)
  auto fb = egraph.emplace(Op::F, {b}).eclass_id;  // F(b) - different from F(a)

  EXPECT_NE(fa, fb);
  EXPECT_EQ(egraph.num_classes(), 4);  // a, b, F(a), F(b)

  // Merge a and b - now F(a) and F(b) should become congruent
  egraph.merge(a, b);

  // Also merge fa and fb manually BEFORE rebuild
  // This puts two e-nodes that will canonicalize to the same form in the same e-class
  egraph.merge(fa, fb);

  EXPECT_EQ(egraph.num_classes(), 2);  // {a,b}, {F(a),F(b)}

  egraph.rebuild(ctx);

  // After rebuild, the e-class should have had duplicate e-nodes removed
  // but since they're in the same class, no additional merges needed
  EXPECT_EQ(egraph.num_classes(), 2);
  EXPECT_EQ(egraph.find(a), egraph.find(b));
  EXPECT_EQ(egraph.find(fa), egraph.find(fb));

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

TEST(EGraphCongruenceClosureBug, DuplicateInDifferentEClass) {
  // Test: When two e-nodes in DIFFERENT e-classes canonicalize to the same form,
  // a merge must be triggered.
  //
  // This is the core case that was broken before the fix.
  //
  // Scenario:
  //   a, b = separate leaves
  //   fa = F(a), fb = F(b) - in separate e-classes
  //   merge(a, b) - now a≡b, but fa and fb are still separate
  //   rebuild should detect that fa and fb both canonicalize to F(E_ab)
  //   and merge their e-classes

  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto b = egraph.emplace(Op::A, 1).eclass_id;
  auto fa = egraph.emplace(Op::F, {a}).eclass_id;
  auto fb = egraph.emplace(Op::F, {b}).eclass_id;

  EXPECT_EQ(egraph.num_classes(), 4);
  EXPECT_NE(egraph.find(fa), egraph.find(fb));

  // Merge a and b
  egraph.merge(a, b);

  // Before rebuild, fa and fb are still in different e-classes
  EXPECT_NE(egraph.find(fa), egraph.find(fb));

  egraph.rebuild(ctx);

  // After rebuild, fa and fb should be merged because both canonicalize to F(E_ab)
  EXPECT_EQ(egraph.find(fa), egraph.find(fb))
      << "fa and fb should be merged after rebuild because both canonicalize to F(E_ab)";

  EXPECT_EQ(egraph.num_classes(), 2);  // {a,b}, {fa,fb}

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

TEST(EGraphCongruenceClosureBug, MultipleParentsFromDifferentClasses) {
  // Test: Multiple parent e-nodes from different e-classes all canonicalize
  // to the same form, triggering a multi-way merge.
  //
  // This tests the `canonical_eclass_ids.size() > 1` branch in process_parents.
  //
  // Scenario:
  //   a, b, c = separate leaves
  //   fa = F(a), fb = F(b), fc = F(c) - all in separate e-classes
  //   merge(a, b), merge(b, c) - now a≡b≡c
  //   rebuild should merge fa, fb, fc because all canonicalize to F(E_abc)

  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto b = egraph.emplace(Op::A, 1).eclass_id;
  auto c = egraph.emplace(Op::A, 2).eclass_id;
  auto fa = egraph.emplace(Op::F, {a}).eclass_id;
  auto fb = egraph.emplace(Op::F, {b}).eclass_id;
  auto fc = egraph.emplace(Op::F, {c}).eclass_id;

  EXPECT_EQ(egraph.num_classes(), 6);

  // Merge all leaves together
  egraph.merge(a, b);
  egraph.merge(b, c);

  // Before rebuild, fa, fb, fc are still separate
  EXPECT_NE(egraph.find(fa), egraph.find(fb));
  EXPECT_NE(egraph.find(fb), egraph.find(fc));

  egraph.rebuild(ctx);

  // After rebuild, all F nodes should be merged
  EXPECT_EQ(egraph.find(fa), egraph.find(fb));
  EXPECT_EQ(egraph.find(fb), egraph.find(fc));

  EXPECT_EQ(egraph.num_classes(), 2);  // {a,b,c}, {fa,fb,fc}

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

TEST(EGraphCongruenceClosureBug, AlreadyCanonicalNoChange) {
  // Test: When an e-node's children are already canonical, repair_hashcons_enode
  // should just update the eclass_id in the hashcons entry without triggering
  // any merges or duplicate detection.
  //
  // Scenario:
  //   a = leaf
  //   f = F(a)
  //   Rebuild with no pending merges - f's children are already canonical
  //   No merges should occur, e-graph structure unchanged

  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto f = egraph.emplace(Op::F, {a}).eclass_id;
  auto g = egraph.emplace(Op::F2, {f, a}).eclass_id;

  EXPECT_EQ(egraph.num_classes(), 3);

  // No merges, just rebuild
  egraph.rebuild(ctx);

  // Structure should be unchanged
  EXPECT_EQ(egraph.num_classes(), 3);
  EXPECT_NE(egraph.find(a), egraph.find(f));
  EXPECT_NE(egraph.find(f), egraph.find(g));

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

TEST(EGraphCongruenceClosureBug, ChainedCongruencePropagation) {
  // Test: Congruence propagates through multiple levels in a single rebuild.
  //
  // Scenario:
  //   a, b = separate leaves
  //   fa = F(a), fb = F(b)
  //   gfa = G(fa), gfb = G(fb)
  //   hgfa = H(gfa), hgfb = H(gfb)
  //   merge(a, b)
  //   rebuild should propagate: a≡b -> fa≡fb -> gfa≡gfb -> hgfa≡hgfb

  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto b = egraph.emplace(Op::A, 1).eclass_id;
  auto fa = egraph.emplace(Op::F, {a}).eclass_id;
  auto fb = egraph.emplace(Op::F, {b}).eclass_id;
  auto gfa = egraph.emplace(Op::F2, {fa}).eclass_id;
  auto gfb = egraph.emplace(Op::F2, {fb}).eclass_id;
  auto hgfa = egraph.emplace(Op::Neg, {gfa}).eclass_id;
  auto hgfb = egraph.emplace(Op::Neg, {gfb}).eclass_id;

  EXPECT_EQ(egraph.num_classes(), 8);

  egraph.merge(a, b);
  egraph.rebuild(ctx);

  // All corresponding nodes should be merged
  EXPECT_EQ(egraph.find(a), egraph.find(b));
  EXPECT_EQ(egraph.find(fa), egraph.find(fb));
  EXPECT_EQ(egraph.find(gfa), egraph.find(gfb));
  EXPECT_EQ(egraph.find(hgfa), egraph.find(hgfb));

  EXPECT_EQ(egraph.num_classes(), 4);

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

// ============================================================================
// Coverage tests for utility functions and edge cases
// ============================================================================

TEST(EGraphCoverage, NeedsRebuildAndWorklistSize) {
  // Test: needs_rebuild() and worklist_size() utility functions
  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  // Initially no rebuild needed
  EXPECT_FALSE(egraph.needs_rebuild());
  EXPECT_EQ(egraph.worklist_size(), 0);

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto b = egraph.emplace(Op::A, 1).eclass_id;

  // Still no rebuild needed (no merges)
  EXPECT_FALSE(egraph.needs_rebuild());
  EXPECT_EQ(egraph.worklist_size(), 0);

  // Merge adds to worklist
  egraph.merge(a, b);

  EXPECT_TRUE(egraph.needs_rebuild());
  EXPECT_GT(egraph.worklist_size(), 0);

  // After rebuild, worklist is empty
  egraph.rebuild(ctx);

  EXPECT_FALSE(egraph.needs_rebuild());
  EXPECT_EQ(egraph.worklist_size(), 0);
}

TEST(EGraphCoverage, HasClassWithInvalidId) {
  // Test: has_class() returns false for invalid e-class ID
  EGraph<Op, NoAnalysis> egraph;

  auto a = egraph.emplace(Op::A, 0).eclass_id;

  // Valid class ID
  EXPECT_TRUE(egraph.has_class(a));

  // Invalid class ID (beyond union-find size)
  EXPECT_FALSE(egraph.has_class(EClassId{999999}));

  // Another invalid ID
  EXPECT_FALSE(egraph.has_class(EClassId{egraph.num_nodes() + 100}));
}

TEST(EGraphCoverage, RepairHashconsEclassMergesAwayOriginal) {
  // Test: Line 475-478 branch where canonical_after_repair != eclass_id
  //
  // This happens when repair_hashcons_eclass causes the ORIGINAL e-class
  // to be merged into a DIFFERENT e-class (the other becomes the root).
  //
  // Scenario:
  //   a, b = separate leaves
  //   fa = F(a), fb = F(b) - in separate e-classes
  //   ga = G(fa), gb = G(fb) - ga has child fa, gb has child fb
  //   merge(a, b) - this triggers rebuild
  //
  // During rebuild of fa's e-class:
  //   - fa and fb become congruent (both canonicalize to F(E_ab))
  //   - repair_hashcons_eclass finds fb is a duplicate
  //   - Depending on union-find, fa's class might be merged INTO fb's class
  //   - So canonical_after_repair = fb's class != fa's original class
  //
  // To ensure we hit this branch, we need the merge to go "the other way"
  // We can influence this by creating fb BEFORE fa (lower ID = more likely root)

  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  // Create b first so it gets lower ID
  auto b = egraph.emplace(Op::A, 1).eclass_id;
  auto a = egraph.emplace(Op::A, 0).eclass_id;

  // Create fb before fa - fb will have lower eclass ID
  auto fb = egraph.emplace(Op::F, {b}).eclass_id;
  auto fa = egraph.emplace(Op::F, {a}).eclass_id;

  // Verify fa has higher ID than fb
  EXPECT_GT(fa.value_of(), fb.value_of());

  // Create parent nodes to ensure fa's class is in the worklist
  auto ga = egraph.emplace(Op::F2, {fa}).eclass_id;
  auto gb = egraph.emplace(Op::F2, {fb}).eclass_id;

  EXPECT_EQ(egraph.num_classes(), 6);

  // Merge a and b - this will cause fa and fb to become congruent during rebuild
  egraph.merge(a, b);
  egraph.rebuild(ctx);

  // After rebuild, fa and fb should be merged
  EXPECT_EQ(egraph.find(fa), egraph.find(fb));
  // ga and gb should also be merged (congruence propagation)
  EXPECT_EQ(egraph.find(ga), egraph.find(gb));

  EXPECT_EQ(egraph.num_classes(), 3);  // {a,b}, {fa,fb}, {ga,gb}
  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

TEST(EGraphCoverage, RepairHashconsWithHigherRankTarget) {
  // Test: Line 475-478 branch where canonical_after_repair != eclass_id
  //
  // This happens when the e-class being repaired gets merged INTO another
  // e-class (the other becomes canonical due to higher union-find rank).
  //
  // Strategy: Build rank in one e-class through preliminary merges, then
  // trigger congruence that merges a lower-rank e-class into the higher one.

  EGraph<Op, NoAnalysis> egraph;
  ProcessingContext<Op> ctx;

  // Create leaf nodes - we'll merge c1, c2, c3 to build rank
  auto c1 = egraph.emplace(Op::A, 1).eclass_id;
  auto c2 = egraph.emplace(Op::A, 2).eclass_id;
  auto c3 = egraph.emplace(Op::A, 3).eclass_id;
  auto d = egraph.emplace(Op::A, 4).eclass_id;

  // Merge c1, c2, c3 to increase rank of the merged class
  egraph.merge(c1, c2);
  egraph.rebuild(ctx);
  auto merged_c = egraph.find(c1);
  egraph.merge(merged_c, c3);
  egraph.rebuild(ctx);
  merged_c = egraph.find(c1);

  // Create F nodes - f_high has child with higher union-find rank
  auto f_high = egraph.emplace(Op::F, {merged_c}).eclass_id;
  auto f_low = egraph.emplace(Op::F, {d}).eclass_id;

  // Create parent nodes to ensure both F classes get processed
  egraph.emplace(Op::F2, {f_high});
  egraph.emplace(Op::F2, {f_low});

  // Merge the leaves - this triggers congruence between f_high and f_low
  egraph.merge(merged_c, d);
  egraph.rebuild(ctx);

  // After rebuild, both F nodes should be in same class
  EXPECT_EQ(egraph.find(f_high), egraph.find(f_low));
  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

TEST(EGraphCoverage, ValidateCongruenceClosureDetectsFailure) {
  // Test: ValidateCongruenceClosure returns false when congruence is broken
  //
  // We need to create a state where two e-nodes in different e-classes
  // have the same canonical form. This requires bypassing normal e-graph
  // operations that maintain congruence.
  //
  // Strategy: Create congruent e-nodes, merge their children, but DON'T
  // call rebuild. The validation should detect the inconsistency.

  EGraph<Op, NoAnalysis> egraph;

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto b = egraph.emplace(Op::A, 1).eclass_id;
  auto fa = egraph.emplace(Op::F, {a}).eclass_id;
  auto fb = egraph.emplace(Op::F, {b}).eclass_id;

  // Before merge, congruence closure is valid
  EXPECT_TRUE(egraph.ValidateCongruenceClosure());

  // Merge a and b - now fa and fb SHOULD be congruent but we don't rebuild
  egraph.merge(a, b);

  // Without rebuild, fa and fb are in different e-classes but have same
  // canonical form F(E_ab). This violates congruence closure.
  EXPECT_FALSE(egraph.ValidateCongruenceClosure())
      << "Congruence closure should be broken: fa and fb have same canonical "
         "form but are in different e-classes";

  // After rebuild, congruence is restored
  ProcessingContext<Op> ctx;
  egraph.rebuild(ctx);

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());

  // fa and fb should now be in the same e-class (they became congruent)
  EXPECT_EQ(egraph.find(fa), egraph.find(fb));
}

}  // namespace memgraph::planner::core
