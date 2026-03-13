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

// VM bytecode compilation tests: verifies correct instruction generation.

#include <gtest/gtest.h>

#include "planner/pattern/vm/compiler.hpp"
#include "test_egraph_fixture.hpp"
#include "test_patterns.hpp"
#include "test_vm_bytecode.hpp"

namespace memgraph::planner::core {

using namespace test;
using namespace pattern;
using namespace pattern::vm;

// ============================================================================
// Data-driven compiler tests
// ============================================================================
//
// Each case builds patterns, compiles them, and verifies:
//   1. Structural invariants (ExpectValidBytecode)
//   2. Approved bytecode snapshot (VerifyBytecode)

struct CompilerTestCase {
  std::string name;
  std::function<std::vector<TestPattern>()> make_patterns;
};

// clang-format off
static auto const kCompilerCases = std::vector<CompilerTestCase>{
    // --- Single patterns ---
    {"Single_Wildcard",                                                          // _
     [] { return std::vector{make_wildcard_pattern()}; }},
    {"Single_Var",                                                               // ?x
     [] { return std::vector{make_var_pattern(kVarX)}; }},
    {"Single_Neg_x",                                                             // ?r=Neg(?x)
     [] { return std::vector{TestPattern::build(kTestRoot, Op::Neg, {Var{kVarX}})}; }},
    {"Single_Neg_Neg_x",                                                         // ?r=Neg(Neg(?x))
     [] { return std::vector{TestPattern::build(kTestRoot, Op::Neg, {Sym(Op::Neg, Var{kVarX})})}; }},
    {"Single_Add_x_y",                                                           // ?r=Add(?x,?y)
     [] { return std::vector{TestPattern::build(kTestRoot, Op::Add, {Var{kVarX}, Var{kVarY}})}; }},
    {"Single_Mul_Neg_x_y",                                                       // Mul(Neg(?x),?y)
     [] { return std::vector{TestPattern::build(Op::Mul, {Sym(Op::Neg, Var{kVarX}), Var{kVarY}})}; }},
    {"Single_Leaf",                                                               // ?r=A()
     [] { return std::vector{TestPattern::build(kTestRoot, Op::A)}; }},
    {"Single_SelfRef",                                                            // ?x=Neg(?x)
     [] { return std::vector{TestPattern::build(kVarX, Op::Neg, {Var{kVarX}})}; }},
    {"Single_TwoSymbolChildren",                                                  // F(G(?x),H(?y),?z)
     [] { return std::vector{TestPattern::build(Op::F, {Sym(Op::G, Var{kVarX}), Sym(Op::H, Var{kVarY}), Var{kVarZ}})}; }},

    // --- Deep entry: no root binding forces entry from deepest symbol ---
    {"DeepEntry_Neg_Neg_x",                                                      // Neg(Neg(?x))
     [] { return std::vector{TestPattern::build(Op::Neg, {Sym(Op::Neg, Var{kVarX})})}; }},
    {"DeepEntry_Neg_Neg_Neg_x",                                                  // Neg(Neg(Neg(?x)))
     [] { return std::vector{TestPattern::build(Op::Neg, {Sym(Op::Neg, Sym(Op::Neg, Var{kVarX}))})}; }},
    {"DeepEntry_Add_y_Neg_x",                                                    // Add(?y,Neg(?x))
     [] { return std::vector{TestPattern::build(Op::Add, {Var{kVarY}, Sym(Op::Neg, Var{kVarX})})}; }},
    {"DeepEntry_MultiStepBinding",                                                // Neg(?x=Neg(Neg(?y)))
     [] { return std::vector{TestPattern::build(Op::Neg, {BoundSym(kVarX, Op::Neg, Sym(Op::Neg, Var{kVarY}))})}; }},
    {"DeepEntry_WildcardSiblings",                                                // F(_,?x,_)
     [] { return std::vector{TestPattern::build(Op::F, {Wildcard{}, Var{kVarX}, Wildcard{}})}; }},

    // --- Join: shared variable ---
    {"Join_SharedVar_Bind_Ident",                                                // Bind(_,?x,?y), ?z=Ident(?x)
     [] {
       return std::vector{
           TestPattern::build(Op::Bind, {Wildcard{}, Var{kVarX}, Var{kVarY}}),
           TestPattern::build(kVarZ, Op::Ident, {Var{kVarX}})};
     }},
    {"Join_DeepChain_Neg4_x",                                                    // A(?x), Neg(Neg(Neg(Neg(?x))))
     [] {
       return std::vector{
           TestPattern::build(Op::A, {Var{kVarX}}),
           TestPattern::build(Op::Neg, {Sym(Op::Neg, Sym(Op::Neg, Sym(Op::Neg, Var{kVarX})))})};
     }},
    {"Join_NestedSharedVar",                                                     // F(G(?x)), H(?x)
     [] {
       return std::vector{
           TestPattern::build(Op::F, {Sym(Op::G, Var{kVarX})}),
           TestPattern::build(Op::H, {Var{kVarX}})};
     }},
    {"Join_DeepestSharedVar",                                                    // F(?x,?y), G(?x,H(?y))
     [] {
       return std::vector{
           TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(Op::G, {Var{kVarX}, Sym(Op::H, Var{kVarY})})};
     }},
    {"Join_ThreeWayShared",                                                      // F(?x,?y), G(?x,_), H(?y,_)
     [] {
       return std::vector{
           TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(Op::G, {Var{kVarX}, Wildcard{}}),
           TestPattern::build(Op::H, {Var{kVarY}, Wildcard{}})};
     }},

    // --- Join: disjoint (no shared variables) ---
    {"Join_Disjoint_Add_Neg",                                                    // Add(?x,?y), Neg(?z)
     [] {
       return std::vector{
           TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(Op::Neg, {Var{kVarZ}})};
     }},
    {"Join_Disjoint_Add_VarOnly",                                                // Add(?x,?y), ?z
     [] {
       return std::vector{
           TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}),
           make_var_pattern(kVarZ)};
     }},

    // --- Join: root and intermediate bindings ---
    {"Join_RootBinding_G_x",                                                     // F(?x,?y), ?z=G(?x)
     [] {
       return std::vector{
           TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(kVarZ, Op::G, {Var{kVarX}})};
     }},
    {"Join_IntermediateBinding_A_B_x",                                           // F(?x,?y), A(?z=B(?x))
     [] {
       return std::vector{
           TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(Op::A, {BoundSym(kVarZ, Op::B, Var{kVarX})})};
     }},

    // --- Join: symbol binding as shared variable ---
    {"Join_SharedBindingAtRoot",                                                 // Add(?x,?y), ?x=Neg(?z)
     [] {
       return std::vector{
           TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(kVarX, Op::Neg, {Var{kVarZ}})};
     }},
    {"Join_SharedBindingNested",                                                 // Add(?x,?y), Mul(?x=Neg(?z),?w)
     [] {
       return std::vector{
           TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(Op::Mul, {BoundSym(kVarX, Op::Neg, Var{kVarZ}), Var{kVarW}})};
     }},
    {"Join_SharedBindingMiddle",                                                 // Add(?x,?y), Mul(?x=Neg(_,?y),?z)
     [] {
       return std::vector{
           TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(Op::Mul, {BoundSym(kVarX, Op::Neg, Wildcard{}, Var{kVarY}), Var{kVarZ}})};
     }},
    {"Join_NestedBoundSymbol",                                                   // F(?x,?y), G(?x=Neg(?z))
     [] {
       return std::vector{
           TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(Op::G, {BoundSym(kVarX, Op::Neg, Var{kVarZ})})};
     }},
    {"Join_Mul_Neg_z_Add_z",                                                     // Mul(?z=Neg(?x),?w), Add(?z,?y)
     [] {
       return std::vector{
           TestPattern::build(Op::Mul, {BoundSym(kVarZ, Op::Neg, Var{kVarX}), Var{kVarW}}),
           TestPattern::build(Op::Add, {Var{kVarZ}, Var{kVarY}})};
     }},

    // --- Join: edge cases (degenerate patterns) ---
    {"Join_TwoVarOnly",                                                          // ?x, ?y
     [] { return std::vector{make_var_pattern(kVarX), make_var_pattern(kVarY)}; }},
    {"Join_SymbolAndWildcard",                                                   // F(?x), _
     [] {
       return std::vector{
           TestPattern::build(Op::F, {Var{kVarX}}),
           make_wildcard_pattern()};
     }},
    {"Join_AlreadyBoundVar",                                                     // F(?x,?y), ?x
     [] {
       return std::vector{
           TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}),
           make_var_pattern(kVarX)};
     }},

    // --- Join: chains (linear propagation through shared vars) ---
    {"Join_LinearChain_3",                                                        // F(?x,?y), G(?y,?z), H(?z,?w)
     [] {
       return std::vector{
           TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(Op::G, {Var{kVarY}, Var{kVarZ}}),
           TestPattern::build(Op::H, {Var{kVarZ}, Var{kVarW}})};
     }},
    {"Join_LinearChain_4",                                                        // F(?x,?y), G(?y,?z), H(?z,?w), K(?w,?a)
     [] {
       return std::vector{
           TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(Op::G, {Var{kVarY}, Var{kVarZ}}),
           TestPattern::build(Op::H, {Var{kVarZ}, Var{kVarW}}),
           TestPattern::build(Op::F2, {Var{kVarW}, Var{kVarA}})};
     }},

    // --- Join: triangles (three patterns sharing vars pairwise) ---
    {"Join_Triangle",                                                             // F(?x,?y), G(?x,?z), H(?y,?z)
     [] {
       return std::vector{
           TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(Op::G, {Var{kVarX}, Var{kVarZ}}),
           TestPattern::build(Op::H, {Var{kVarY}, Var{kVarZ}})};
     }},
    {"Join_Triangle_Swapped",                                                     // F(?x,?y), G(?y,?z), H(?x,?z)
     [] {
       return std::vector{
           TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(Op::G, {Var{kVarY}, Var{kVarZ}}),
           TestPattern::build(Op::H, {Var{kVarX}, Var{kVarZ}})};
     }},

    // --- Hoist: eclass-level join hoisting ---
    {"Hoist_RootBinding_F_G_H",                                                  // F(?x), ?y=G(?x), H(?y)
     [] {
       return std::vector{
           TestPattern::build(Op::F, {Var{kVarX}}),
           TestPattern::build(kVarY, Op::G, {Var{kVarX}}),
           TestPattern::build(Op::H, {Var{kVarY}})};
     }},
    {"Hoist_Cartesian_H_z",                                                      // F(?x,?y), G(?x), H(?z)
     [] {
       return std::vector{
           TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(Op::G, {Var{kVarX}}),
           TestPattern::build(Op::H, {Var{kVarZ}})};
     }},
    {"Hoist_Transitive_F_G_H",                                                   // ?a=F(?x), ?b=G(?a), H(?b)
     [] {
       return std::vector{
           TestPattern::build(kVarA, Op::F, {Var{kVarX}}),
           TestPattern::build(kVarB, Op::G, {Var{kVarA}}),
           TestPattern::build(Op::H, {Var{kVarB}})};
     }},
    {"Hoist_TwoIndependent_G_H",                                                 // ?a=F(?x,?y), G(?a), H(?a)
     [] {
       return std::vector{
           TestPattern::build(kVarA, Op::F, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(kVarB, Op::G, {Var{kVarA}}),
           TestPattern::build(kVarC, Op::H, {Var{kVarA}})};
     }},
    {"Hoist_Blocked_EnodeVar",                                                   // F(?x,?y), G(?x,?z), H(?z)
     [] {
       return std::vector{
           TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(Op::G, {Var{kVarX}, Var{kVarZ}}),
           TestPattern::build(Op::H, {Var{kVarZ}})};
     }},
    {"Hoist_DeepEntry_Mul_Neg",                                                  // Mul(?a=Neg(?z),?w), G(?a)
     [] {
       return std::vector{
           TestPattern::build(Op::Mul, {BoundSym(kVarA, Op::Neg, Var{kVarZ}), Var{kVarW}}),
           TestPattern::build(Op::G, {Var{kVarA}})};
     }},
    {"Hoist_DeepEntryInHoisted",                                                 // ?a=F(?x), Neg(Neg(?a))
     [] {
       return std::vector{
           TestPattern::build(kVarA, Op::F, {Var{kVarX}}),
           TestPattern::build(Op::Neg, {Sym(Op::Neg, Var{kVarA})})};
     }},
    {"Hoist_MixedCartesianAndShared",                                            // ?a=F(?x,?y), G(?a), H(?z)
     [] {
       return std::vector{
           TestPattern::build(kVarA, Op::F, {Var{kVarX}, Var{kVarY}}),
           TestPattern::build(kVarB, Op::G, {Var{kVarA}}),
           TestPattern::build(Op::H, {Var{kVarZ}})};
     }},
    {"Hoist_AllIndependent",                                                      // F(?x), G(?y), H(?z)
     [] {
       return std::vector{
           TestPattern::build(Op::F, {Var{kVarX}}),
           TestPattern::build(Op::G, {Var{kVarY}}),
           TestPattern::build(Op::H, {Var{kVarZ}})};
     }},
    {"Hoist_IndependentAmongConnected",                                           // ?a=F(?x), G(?y), ?b=H(?a), K(?y,?b)
     [] {
       return std::vector{
           TestPattern::build(kVarA, Op::F, {Var{kVarX}}),
           TestPattern::build(Op::G, {Var{kVarY}}),
           TestPattern::build(kVarB, Op::H, {Var{kVarA}}),
           TestPattern::build(Op::F2, {Var{kVarY}, Var{kVarB}})};
     }},

    // Deep
    {"Deep_NestedNeg35",                                                           // Neg(Neg(...Neg(?x)...)) depth 35
     [] {
       auto b = TestPattern::Builder{};
       auto cur = b.var(kVarX);
       for (int i = 0; i < 35; ++i) cur = b.sym(Op::Neg, {cur});
       return std::vector{std::move(b).build()};
     }},
};
// clang-format on

class PatternVM_Compiler : public EGraphTestBase {
 protected:
  TestPatternCompiler compiler;
};

class Compile : public EGraphTestBase, public testing::WithParamInterface<CompilerTestCase> {
 protected:
  TestPatternCompiler compiler;
};

TEST_P(Compile, Verify) {
  auto patterns = GetParam().make_patterns();
  auto compiled = compiler.compile(patterns);
  auto code = compiled.code();
  ExpectValidBytecode(code, compiled.symbols());
  VerifyBytecode(code, compiled.symbols());
}

INSTANTIATE_TEST_SUITE_P(PatternVM, Compile, testing::ValuesIn(kCompilerCases),
                         [](auto const &info) { return info.param.name; });

// ============================================================================
// Permutation tests (not data-driven — test stability across input orderings)
// ============================================================================
//
// Verifies that all permutations of a pattern set produce valid bytecode with
// the same structure: same instruction count, no IterAllEClasses, IterParents.

template <std::size_t N>
void ExpectStableAcrossPermutations(std::array<TestPattern, N> const &patterns) {
  TestPatternCompiler canonical_compiler;
  auto canonical_compiled = canonical_compiler.compile(patterns);
  auto canonical_size = canonical_compiled.code().size();
  ExpectValidBytecode(canonical_compiled.code(), canonical_compiled.symbols());

  auto perm = [] {
    std::array<int, N> a;
    std::iota(a.begin(), a.end(), 0);
    return a;
  }();

  std::size_t count = 0;
  do {
    auto permuted = [&]<std::size_t... I>(std::index_sequence<I...>) {
      return std::array<TestPattern, N>{patterns[perm[I]]...};
    }(std::make_index_sequence<N>{});

    TestPatternCompiler perm_compiler;
    auto compiled = perm_compiler.compile(permuted);
    auto code = compiled.code();
    auto bytecode = disassemble(code, compiled.symbols());

    ExpectValidBytecode(code, compiled.symbols());
    EXPECT_EQ(code.size(), canonical_size) << "Permutation " << count << "\n" << bytecode;

    bool has_iter_all = std::ranges::any_of(code, [](auto const &i) { return i.op == VMOp::IterAllEClasses; });
    EXPECT_FALSE(has_iter_all) << "Permutation " << count << " should not use IterAllEClasses\n" << bytecode;

    bool has_iter_parents = std::ranges::any_of(code, [](auto const &i) { return i.op == VMOp::IterParents; });
    EXPECT_TRUE(has_iter_parents) << "Permutation " << count << " should use IterParents\n" << bytecode;

    ++count;
  } while (std::ranges::next_permutation(perm).found);
}

TEST_F(PatternVM_Compiler, JoinOrder_HubsAndLeaves_AllPermutations) {
  // Hub-and-leaf topology: A(?a,?b), B(?b,?c), C(?c,?a), X(?a), Y(?b), Z(?c)
  ExpectStableAcrossPermutations(std::array{
      TestPattern::build(Op::A, {Var{kVarA}, Var{kVarB}}),
      TestPattern::build(Op::B, {Var{kVarB}, Var{kVarC}}),
      TestPattern::build(Op::C, {Var{kVarC}, Var{kVarA}}),
      TestPattern::build(Op::X, {Var{kVarA}}),
      TestPattern::build(Op::Y, {Var{kVarB}}),
      TestPattern::build(Op::F3, {Var{kVarC}}),
  });
}

TEST_F(PatternVM_Compiler, JoinOrder_LinearChain_AllPermutations) {
  // Linear chain: F(?x) — G(?x,?y) — H(?y)
  ExpectStableAcrossPermutations(std::array{
      TestPattern::build(Op::F, {Var{kVarX}}),
      TestPattern::build(Op::G, {Var{kVarX}, Var{kVarY}}),
      TestPattern::build(Op::H, {Var{kVarY}}),
  });
}

// ============================================================================
// Validation: Error handling and constraint enforcement
// ============================================================================

TEST_F(PatternVM_Compiler, Validation_DuplicateSymbolBindingInSinglePattern) {
  EXPECT_THROW(TestPattern::build(kVarX, Op::Add, {BoundSym(kVarX, Op::Neg)}), std::invalid_argument);
}

TEST_F(PatternVM_Compiler, Validation_DuplicateSymbolBindingAcrossPatterns) {
  auto pattern1 = TestPattern::build(kVarX, Op::Add, {Var{kVarY}});
  auto pattern2 = TestPattern::build(kVarX, Op::Neg, {Var{kVarZ}});

  EXPECT_THROW(compiler.compile(pattern1, pattern2), std::invalid_argument);
}

}  // namespace memgraph::planner::core
