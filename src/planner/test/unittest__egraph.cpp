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

TEST(EGraphEClassAccess, GetEClassByID) {
  EGraph<Op, NoAnalysis> egraph;

  auto [info, info_node, ins] = egraph.emplace(Op::Test);
  auto id = info;

  const auto &eclass = egraph.eclass(id);
  EXPECT_EQ(eclass.size(), 1);

  auto repr_id = *eclass.nodes().begin();
  const auto &repr = egraph.get_enode(repr_id);
  EXPECT_EQ(repr.symbol(), Op::Test);
}

TEST(EGraphClearAndReserve, ClearEmptiesTheGraph) {
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

TEST(EGraphCongruenceClosureBug, InfinateSelfReference) {
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

}  // namespace memgraph::planner::core
