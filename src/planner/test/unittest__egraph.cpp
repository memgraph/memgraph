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

// Helper function to create canonical signatures for validation
std::string CreateCanonicalSignature(const ENode<TestSymbol> &enode, EGraph<TestSymbol, NoAnalysis> &egraph) {
  std::string sig = std::to_string(static_cast<uint32_t>(enode.symbol()));

  for (auto child_id : enode.children()) {
    auto canonical_child = egraph.find(child_id);
    sig += "_" + std::to_string(canonical_child);
  }

  if (enode.is_leaf()) {
    sig += "_D" + std::to_string(enode.disambiguator());
  }

  return sig;
}

// Helper function to validate congruence closure
bool ValidateCongruenceClosure(EGraph<TestSymbol, NoAnalysis> &egraph) {
  std::unordered_map<std::string, EClassId> canonical_forms;

  for (auto const &[class_id, eclass] : egraph.canonical_classes()) {
    for (auto enode_id : eclass.nodes()) {
      const auto &enode = egraph.get_enode(enode_id);
      std::string sig = CreateCanonicalSignature(enode, egraph);

      if (canonical_forms.contains(sig)) {
        if (canonical_forms[sig] != class_id) {
          std::cout << "Congruent e-nodes in different e-classes: " << canonical_forms[sig] << " vs " << class_id
                    << "\n";
          std::cout << "Signature: " << sig << "\n";
          return false;
        }
      } else {
        canonical_forms[sig] = class_id;
      }
    }
  }
  return true;
}

TEST(EGraphCongruenceClosureBug, ReproduceFuzzerFailure) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  // This test case doesn't reproduce the bug - the simple case works
  // The real bug is likely in the missing hashcons update for single parents
  // after deduplication, or in more complex rebuild scenarios

  auto leaf_a = egraph.emplace(TestSymbol::A, 1);
  auto leaf_b = egraph.emplace(TestSymbol::B, 2);
  auto f_a = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{leaf_a});
  auto f_b = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{leaf_b});

  egraph.merge(leaf_a, leaf_b);
  egraph.rebuild(ctx);

  // This should pass - demonstrating this simple case works
  EXPECT_TRUE(ValidateCongruenceClosure(egraph));
  EXPECT_EQ(egraph.find(f_a), egraph.find(f_b));
}

TEST(EGraphCongruenceClosureBug, MissingHashconsUpdateForSingleParent) {
  EGraph<TestSymbol, NoAnalysis> egraph;
  ProcessingContext<TestSymbol> ctx;

  // Try to reproduce the actual bug: missing hashcons update when parent_ids.size() == 1
  // This happens after deduplication leaves only one parent

  // Create a more complex scenario that might trigger the bug
  auto a = egraph.emplace(TestSymbol::A, 1);
  auto b = egraph.emplace(TestSymbol::B, 2);
  auto c = egraph.emplace(TestSymbol::C, 3);

  // Create multiple compound nodes that will need hashcons updates
  auto f_a = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{a});  // F(a)
  auto f_b = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{b});  // F(b)
  auto f_c = egraph.emplace(TestSymbol::F, utils::small_vector<EClassId>{c});  // F(c)

  // Create higher-level compounds that reference the F nodes
  auto g_fa = egraph.emplace(TestSymbol::Plus, utils::small_vector<EClassId>{f_a});  // Plus(F(a))
  auto g_fb = egraph.emplace(TestSymbol::Plus, utils::small_vector<EClassId>{f_b});  // Plus(F(b))

  EXPECT_EQ(egraph.num_classes(), 8);  // a, b, c, F(a), F(b), F(c), Plus(F(a)), Plus(F(b))

  // First merge a and b - this should make F(a) ≡ F(b)
  egraph.merge(a, b);
  egraph.rebuild(ctx);

  EXPECT_TRUE(ValidateCongruenceClosure(egraph));
  EXPECT_EQ(egraph.find(f_a), egraph.find(f_b));    // F(a) ≡ F(b)
  EXPECT_EQ(egraph.find(g_fa), egraph.find(g_fb));  // Plus(F(a)) ≡ Plus(F(b))

  // Now merge with c - this creates a scenario where we have one parent after deduplication
  egraph.merge(egraph.find(a), c);  // Merge (a≡b) with c
  egraph.rebuild(ctx);

  // After this rebuild, all F nodes should be congruent
  EXPECT_TRUE(ValidateCongruenceClosure(egraph));
  EXPECT_EQ(egraph.find(f_a), egraph.find(f_c));  // F(a) ≡ F(c)
  EXPECT_EQ(egraph.find(f_b), egraph.find(f_c));  // F(b) ≡ F(c)
}

}  // namespace memgraph::planner::core
