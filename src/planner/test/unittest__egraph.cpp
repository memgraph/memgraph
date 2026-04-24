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

#include "test_symbols.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core {

using namespace test;
using TestProcessingContext = ProcessingContext<Op>;

TEST(EGraph_Basic, EmptyEGraph) {
  auto const egraph = EGraph<Op, NoAnalysis>{};

  EXPECT_TRUE(egraph.empty());
  EXPECT_EQ(egraph.num_classes(), 0);
  EXPECT_EQ(egraph.num_nodes(), 0);
}

TEST(EGraph_Basic, AddSimpleENodes) {
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

TEST(EGraph_Basic, AddNodesWithChildren) {
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

TEST(EGraph_Basic, DuplicateNodesReturnSameEClass) {
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

TEST(EGraph_Basic, GetEClassByID) {
  EGraph<Op, NoAnalysis> egraph;

  auto [info, info_node, ins] = egraph.emplace(Op::Test);
  auto id = info;

  auto const &eclass = egraph.eclass(id);
  EXPECT_EQ(eclass.size(), 1);

  auto repr_id = *eclass.nodes().begin();
  auto const &repr = egraph.get_enode(repr_id);
  EXPECT_EQ(repr.symbol(), Op::Test);
}

TEST(EGraph_Basic, ClearEmptiesTheGraph) {
  EGraph<Op, NoAnalysis> egraph;

  egraph.emplace(Op::A);
  egraph.emplace(Op::B);

  EXPECT_FALSE(egraph.empty());

  egraph.clear();

  EXPECT_TRUE(egraph.empty());
  EXPECT_EQ(egraph.num_classes(), 0);
  EXPECT_EQ(egraph.num_nodes(), 0);
}

TEST(EGraph_Merge, MergeTwoDifferentEClasses) {
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

TEST(EGraph_Merge, MergeSameEClassIsNoOp) {
  EGraph<Op, NoAnalysis> egraph;

  auto [info1, info1_node, ins1] = egraph.emplace(Op::A);
  auto id1 = info1;
  auto [merged, did_merge] = egraph.merge(id1, id1);

  EXPECT_EQ(merged, id1);
  EXPECT_EQ(egraph.num_classes(), 1);
  EXPECT_FALSE(did_merge);  // Same e-class, no merge needed
}

TEST(EGraph_DuplicateRemoval, CongruenceRemovesDuplicateENodes) {
  // When e-nodes become identical after canonicalization, duplicates are removed
  // and their e-classes are merged.
  //
  //   F(a) and F(b) are separate e-nodes in separate e-classes
  //   Merge a≡b → both canonicalize to F(canonical)
  //   One is kept, one is removed as duplicate, e-classes merge

  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto b = egraph.emplace(Op::A, 1).eclass_id;
  auto fa = egraph.emplace(Op::F, {a}).eclass_id;
  auto fb = egraph.emplace(Op::F, {b}).eclass_id;

  EXPECT_EQ(egraph.num_nodes(), 4);       // 2 leaves + 2 F nodes
  EXPECT_EQ(egraph.num_dead_nodes(), 0);  // no duplicates yet
  EXPECT_EQ(egraph.num_classes(), 4);     // all separate
  EXPECT_NE(egraph.find(fa), egraph.find(fb));

  egraph.merge(a, b);

  // Merge is immediate for leaves, congruence requires rebuild
  EXPECT_EQ(egraph.find(a), egraph.find(b));
  EXPECT_NE(egraph.find(fa), egraph.find(fb));

  egraph.rebuild(ctx);

  // Congruence: both F nodes canonicalize to F(E_ab), e-classes merged
  EXPECT_EQ(egraph.find(fa), egraph.find(fb));
  EXPECT_EQ(egraph.num_classes(), 2);  // {a,b}, {fa,fb}

  // Bookkeeping: one duplicate removed
  EXPECT_EQ(egraph.num_nodes(), 4);       // total created unchanged
  EXPECT_EQ(egraph.num_dead_nodes(), 1);  // one F was duplicate
  EXPECT_EQ(egraph.num_live_nodes(), 3);  // 1 merged leaf + 1 F

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

TEST(EGraph_DuplicateRemoval, MultipleDuplicatesRemoved) {
  // When 3+ e-nodes canonicalize to the same form, all but one are removed.
  //
  //   F(a), F(b), F(c) where a, b, c are separate
  //   Merge a≡b≡c → all three F nodes canonicalize to F(canonical)
  //   Two duplicates removed, one kept

  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

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

TEST(EGraph_DuplicateRemoval, CascadingCongruenceRemovesDuplicates) {
  // Congruence propagates through multiple levels in a single rebuild,
  // removing duplicates at each level.
  //
  //   G(F(a)) and G(F(b)) where F(a), F(b) are in different e-classes
  //   Merge a≡b → F(a)≡F(b) by congruence, then G(F(a))≡G(F(b))
  //   Duplicates removed at each level

  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto b = egraph.emplace(Op::A, 1).eclass_id;
  auto fa = egraph.emplace(Op::F, {a}).eclass_id;
  auto fb = egraph.emplace(Op::F, {b}).eclass_id;
  auto f2fa = egraph.emplace(Op::F2, {fa}).eclass_id;
  auto f2fb = egraph.emplace(Op::F2, {fb}).eclass_id;

  EXPECT_EQ(egraph.num_nodes(), 6);
  EXPECT_EQ(egraph.num_dead_nodes(), 0);
  EXPECT_EQ(egraph.num_classes(), 6);

  egraph.merge(a, b);
  egraph.rebuild(ctx);

  // Congruence: all corresponding nodes merged at each level
  EXPECT_EQ(egraph.find(a), egraph.find(b));
  EXPECT_EQ(egraph.find(fa), egraph.find(fb));
  EXPECT_EQ(egraph.find(f2fa), egraph.find(f2fb));
  EXPECT_EQ(egraph.num_classes(), 3);  // a≡b, F(a)≡F(b), G(F)

  // Bookkeeping: one duplicate removed per level
  EXPECT_EQ(egraph.num_nodes(), 6);
  EXPECT_EQ(egraph.num_dead_nodes(), 2);
  EXPECT_EQ(egraph.num_live_nodes(), 4);

  // Invariant: live count matches sum of e-nodes across canonical classes
  std::size_t sum = 0;
  for (auto const &[id, eclass] : egraph.canonical_classes()) {
    sum += eclass.nodes().size();
  }
  EXPECT_EQ(sum, egraph.num_live_nodes());

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

TEST(EGraph_DuplicateRemoval, ClearResetsDuplicateCount) {
  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

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

TEST(EGraph_DuplicateRemoval, ManyDuplicatesInSameEclass) {
  // Multiple e-nodes in one eclass that all canonicalize to the same form.
  // Duplicate removal is deferred until after iteration of eclass.nodes()
  // completes, so all duplicates are found regardless of removal order.
  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

  auto a0 = egraph.emplace(Op::A, 0).eclass_id;
  auto a1 = egraph.emplace(Op::A, 1).eclass_id;
  auto a2 = egraph.emplace(Op::A, 2).eclass_id;
  auto a3 = egraph.emplace(Op::A, 3).eclass_id;

  auto f0 = egraph.emplace(Op::F, {a0}).eclass_id;
  auto f1 = egraph.emplace(Op::F, {a1}).eclass_id;
  auto f2 = egraph.emplace(Op::F, {a2}).eclass_id;
  auto f3 = egraph.emplace(Op::F, {a3}).eclass_id;

  egraph.merge(f0, f1);
  egraph.merge(f1, f2);
  egraph.merge(f2, f3);

  egraph.merge(a0, a1);
  egraph.merge(a1, a2);
  egraph.merge(a2, a3);

  egraph.rebuild(ctx);

  EXPECT_EQ(egraph.num_dead_nodes(), 3);
  EXPECT_EQ(egraph.num_classes(), 2);
  EXPECT_EQ(egraph.eclass(egraph.find(f0)).size(), 1);
  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

TEST(EGraph_DuplicateRemoval, ManyDuplicatesExceedingSBO) {
  // 8 nodes exceeds small_vector SBO capacity (4).
  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

  constexpr int N = 8;
  std::vector<EClassId> leaves;
  std::vector<EClassId> fnodes;

  for (int i = 0; i < N; ++i) {
    leaves.push_back(egraph.emplace(Op::A, static_cast<uint64_t>(i)).eclass_id);
  }
  for (int i = 0; i < N; ++i) {
    fnodes.push_back(egraph.emplace(Op::F, {leaves[static_cast<size_t>(i)]}).eclass_id);
  }

  for (int i = 1; i < N; ++i) {
    egraph.merge(fnodes[0], fnodes[static_cast<size_t>(i)]);
  }
  for (int i = 1; i < N; ++i) {
    egraph.merge(leaves[0], leaves[static_cast<size_t>(i)]);
  }

  egraph.rebuild(ctx);

  EXPECT_EQ(egraph.num_dead_nodes(), N - 1);
  EXPECT_EQ(egraph.num_classes(), 2);
  EXPECT_EQ(egraph.eclass(egraph.find(fnodes[0])).size(), 1);
  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

TEST(EGraph_DuplicateRemoval, TwoCongruentParentsCollapseAfterChildMerge) {
  // Targets the scenario in process_parents() where two parent e-nodes end up in the
  // SAME canonical_to_parents group. Static analysis argues the assert inside
  // repair_hashcons_enode (hashcons_.find(ENodeRef{enode}) != end) cannot fire: each
  // parent has a DISTINCT pre-canonical hashcons key (otherwise add_enode would have
  // deduplicated at insertion), and repair only erases the key of the enode currently
  // being repaired — it never removes a sibling's entry.
  //
  // This test empirically confirms that: two F(a_i) nodes are forced into one
  // canonical_to_parents group by merging their children, rebuild must not trip the
  // assert (Debug build), and congruence closure holds.
  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;
  auto a0 = egraph.emplace(Op::A, 0).eclass_id;
  auto a1 = egraph.emplace(Op::A, 1).eclass_id;
  auto p0 = egraph.emplace(Op::F, {a0}).eclass_id;
  auto p1 = egraph.emplace(Op::F, {a1}).eclass_id;
  ASSERT_NE(p0, p1);

  egraph.merge(a0, a1);
  egraph.rebuild(ctx);

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
  EXPECT_EQ(egraph.find(p0), egraph.find(p1));
  EXPECT_EQ(egraph.num_dead_nodes(), 1);
}

TEST(EGraph_SelfReference, ChainCollapseWithDeferredNodes) {
  // Self-referential chain F(F(F(a))) where nodes are created between merge and rebuild.
  //
  //   n0 = A, n1 = F(n0), n2 = F(n1)
  //   merge(n0, n1) — creates self-referential class
  //   n3 = F(n2) — created AFTER merge, BEFORE rebuild
  //   rebuild()
  //
  // After merge, n1 = F(class_01) and n2 = F(class_01) become congruent duplicates.
  // This cascades: n3's child also collapses, merging everything into one class.

  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

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

TEST(EGraph_SelfReference, IndirectCongruenceViaChildMerges) {
  // Two separate self-referential merges cause parent nodes to become
  // indirectly congruent.
  //
  //   c = F(a), d = F(b)
  //   merge(c, a) — a's class now contains {a, F(a)}, so find(c) = find(a)
  //   merge(d, b) — b's class now contains {b, F(b)}, so find(d) = find(b)
  //
  //   e = G(c, b) → G(E_a, E_b)
  //   f = G(a, d) → G(E_a, E_b)
  //
  //   Both canonicalize to the same form via different self-ref paths,
  //   so they must be merged.

  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

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

TEST(EGraph_SelfReference, TransitiveSelfRefCreatesIndirectCongruence) {
  // Multi-hop self-referential collapse leading to indirect congruence.
  //
  // Unlike IndirectCongruenceViaChildMerges (single-hop self-ref), this tests
  // that rebuild propagates self-referential collapse through TWO levels before
  // detecting that parent nodes are congruent.
  //
  //   a, b = leaves
  //   fa = F(a, a),  fb = F(b, b)       — level 1
  //   ffa = F(fa, fa), ffb = F(fb, fb)  — level 2
  //   p = G(ffa, b),  q = G(a, ffb)     — parents using nodes from different chains
  //
  //   merge(fa, a) — a's class absorbs F(a,a), so fa collapses into a
  //   merge(fb, b) — b's class absorbs F(b,b), so fb collapses into b
  //
  // After rebuild (transitive collapse, 2 hops each):
  //   fa → E_a, ffa = F(fa,fa) → F(E_a,E_a) → E_a
  //   fb → E_b, ffb = F(fb,fb) → F(E_b,E_b) → E_b
  //   p = G(ffa, b) → G(E_a, E_b)
  //   q = G(a, ffb) → G(E_a, E_b)  — congruent with p

  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto b = egraph.emplace(Op::A, 1).eclass_id;

  // Level 1: F(x, x) for each leaf
  auto fa = egraph.emplace(Op::F, {a, a}).eclass_id;
  auto fb = egraph.emplace(Op::F, {b, b}).eclass_id;

  // Level 2: F(F(x,x), F(x,x)) — two hops from leaf
  auto ffa = egraph.emplace(Op::F, {fa, fa}).eclass_id;
  auto ffb = egraph.emplace(Op::F, {fb, fb}).eclass_id;

  // Parents that use nodes from different depths of each chain
  auto p = egraph.emplace(Op::F2, {ffa, b}).eclass_id;  // G(ffa, b)
  auto q = egraph.emplace(Op::F2, {a, ffb}).eclass_id;  // G(a, ffb)

  EXPECT_EQ(egraph.num_classes(), 8);
  EXPECT_NE(egraph.find(p), egraph.find(q));

  // Self-referential merges: F(x,x) → x
  egraph.merge(fa, a);  // fa collapses into a
  egraph.merge(fb, b);  // fb collapses into b

  egraph.rebuild(ctx);

  // After rebuild, transitive collapse:
  //   fa → E_a, ffa = F(fa,fa) → F(E_a,E_a) → E_a (2-hop collapse)
  //   fb → E_b, ffb = F(fb,fb) → F(E_b,E_b) → E_b (2-hop collapse)
  //   p = G(ffa, b) → G(E_a, E_b)
  //   q = G(a, ffb) → G(E_a, E_b)
  EXPECT_EQ(egraph.find(p), egraph.find(q)) << "p and q should be congruent after transitive self-ref collapse";

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

TEST(EGraph_SelfReference, CongruenceAfterMergeAndRebuildSelfReference) {
  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

  auto [a, a_node, a_ins] = egraph.emplace(Op::A);
  auto [b, b_node, b_ins] = egraph.emplace(Op::B);
  auto [fab, fab_node, fab_ins] = egraph.emplace(Op::F, {a, b});

  EXPECT_EQ(egraph.num_classes(), 3);

  egraph.merge(a, b);
  egraph.merge(a, fab);
  egraph.rebuild(ctx);

  EXPECT_EQ(egraph.num_classes(), 1);
}

TEST(EGraph_SelfReference, CollapseAfterMergeWithParent) {
  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

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

TEST(EGraph_SelfReference, BatchedMergesCollapseToSingleClass) {
  // Multiple leaves and unary nodes with mixed symbols (F, F2) are merged in a single
  // batch before rebuild. Cross-symbol self-references cause everything to collapse
  // into a single e-class.
  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

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
  for (auto id : egraph.canonical_eclass_ids()) {
    EXPECT_EQ(egraph.find(id), id) << "Non-canonical class ID " << id << " found in canonical_eclass_ids()";
  }
}

TEST(EGraph_SelfReference, BasicCycle) {
  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

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

TEST(EGraph_Congruence, DuplicateInSameEClass) {
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
  TestProcessingContext ctx;

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

TEST(EGraph_Congruence, MultipleParentsFromDifferentClasses) {
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
  TestProcessingContext ctx;

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

TEST(EGraph_Congruence, AlreadyCanonicalNoChange) {
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
  TestProcessingContext ctx;

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

TEST(EGraph_Congruence, ChainedPropagation) {
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
  TestProcessingContext ctx;

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

TEST(EGraph_Congruence, HashconsUpdatedAfterChildMerge) {
  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

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

TEST(EGraph_Congruence, MultipleParentsAfterMerge) {
  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

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
  egraph.rebuild(ctx);

  // Verify congruence after rebuild
  EXPECT_EQ(egraph.find(fab), egraph.find(fcb));
  EXPECT_EQ(egraph.num_classes(), 3);  // a=c, b, f(a,b)=f(c,b)
  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
}

TEST(EGraph_Congruence, DeepRebuildSingleParentChain) {
  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

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

TEST(EGraph_Congruence, SwappedChildrenBecomeCongruent) {
  // F2(a,b) and F2(b,a) in the same eclass. After merge(a,b), both
  // canonicalize to F2(E_ab, E_ab) and the duplicate is removed.
  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto b = egraph.emplace(Op::A, 1).eclass_id;

  auto g = egraph.emplace(Op::F2, {a, b}).eclass_id;
  auto h = egraph.emplace(Op::F2, {b, a}).eclass_id;

  egraph.merge(g, h);
  egraph.rebuild(ctx);
  ASSERT_EQ(egraph.num_classes(), 3);

  egraph.merge(a, b);
  egraph.rebuild(ctx);

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
  EXPECT_EQ(egraph.num_classes(), 2);
}

TEST(EGraph_Congruence, ThreeWayChildMergeWithSharedParents) {
  // Three parents with different child pairs from {a,b,c}, all in the same
  // eclass. After merging all leaves, all canonicalize to the same form.
  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

  auto a = egraph.emplace(Op::A, 0).eclass_id;
  auto b = egraph.emplace(Op::A, 1).eclass_id;
  auto c = egraph.emplace(Op::A, 2).eclass_id;

  auto p1 = egraph.emplace(Op::F2, {a, b}).eclass_id;
  auto p2 = egraph.emplace(Op::F2, {b, c}).eclass_id;
  auto p3 = egraph.emplace(Op::F2, {c, a}).eclass_id;

  egraph.merge(p1, p2);
  egraph.merge(p2, p3);
  egraph.rebuild(ctx);

  egraph.merge(a, b);
  egraph.merge(b, c);
  egraph.rebuild(ctx);

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());
  EXPECT_EQ(egraph.num_classes(), 2);
}

TEST(EGraph_EdgeCases, NeedsRebuildAndWorklistSize) {
  // Test: needs_rebuild() and worklist_size() utility functions
  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

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

TEST(EGraph_EdgeCases, HasClassWithInvalidId) {
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

TEST(EGraph_EdgeCases, RepairHashconsEclassMergesAwayOriginal) {
  // During hashcons repair, the e-class being repaired may get merged INTO
  // another e-class (rather than absorbing it). This tests that congruence
  // propagation still works correctly when the original e-class loses its
  // identity during repair.
  //
  // Setup: fb is created before fa (lower ID), so when fa and fb become
  // congruent, fa's class is merged into fb's class. Parent nodes ga and gb
  // must still propagate correctly through this "reversed" merge.

  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

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

TEST(EGraph_EdgeCases, RepairHashconsWithHigherRankTarget) {
  // Same scenario as RepairHashconsEclassMergesAwayOriginal, but using
  // union-find rank (built through preliminary merges) to force the merge
  // direction, rather than relying on creation order.

  EGraph<Op, NoAnalysis> egraph;
  TestProcessingContext ctx;

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

TEST(EGraph_EdgeCases, ValidateCongruenceClosureDetectsFailure) {
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
  TestProcessingContext ctx;
  egraph.rebuild(ctx);

  EXPECT_TRUE(egraph.ValidateCongruenceClosure());

  // fa and fb should now be in the same e-class (they became congruent)
  EXPECT_EQ(egraph.find(fa), egraph.find(fb));
}

}  // namespace memgraph::planner::core
