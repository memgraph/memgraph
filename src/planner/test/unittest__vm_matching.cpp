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

#include <random>

#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"
#include "test_matcher_fixture.hpp"
#include "test_patterns.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::planner::core {

using namespace test;
using namespace pattern;
using namespace pattern::vm;

// ============================================================================
// Parameterized Matching Tests
// ============================================================================
//
// Most matching tests follow a common structure:
//   1. Build e-graph (leaf, node, merge, rebuild)
//   2. Set patterns (use_patterns)
//   3. Execute (run_compiled)
//   4. Assert (verify exact bindings, verify_empty, or check count)
//
// This data-driven framework captures that pattern. Each test case provides
// a setup callback that builds the e-graph, sets patterns, and returns
// the expected result.

struct MatchingExpected {
  std::vector<Bindings> bindings;  // exact expected bindings (empty = verify_empty)
};

// Orthogonal tag bits describing what each test exercises.
enum MatchTag : uint16_t {
  // Pattern cardinality (mutually exclusive)
  Single = 1 << 0,
  Multi = 1 << 1,
  // E-graph shape
  Merge = 1 << 2,
  SelfLoop = 1 << 3,
  // Pattern feature
  Nesting = 1 << 4,
  LeafSym = 1 << 5,
  SameVar = 1 << 6,
  BoundSym_ = 1 << 7,  // trailing underscore to avoid collision with pattern::dsl::BoundSym
  Wildcard_ = 1 << 8,  // trailing underscore to avoid collision with pattern::Wildcard
  // VM / infrastructure mechanism
  Join = 1 << 9,
  Dedup = 1 << 10,
  Hoisting = 1 << 11,
  ParentChain = 1 << 12,
  Concurrent = 1 << 13,
  // Test purpose
  Codegen = 1 << 14,
};

constexpr auto operator|(MatchTag a, MatchTag b) -> MatchTag {
  return static_cast<MatchTag>(static_cast<uint16_t>(a) | static_cast<uint16_t>(b));
}

constexpr auto operator&(MatchTag a, MatchTag b) -> bool {
  return (static_cast<uint16_t>(a) & static_cast<uint16_t>(b)) != 0;
}

struct MatchingTestCase {
  MatchTag tags;
  std::string name;
  std::function<MatchingExpected(PatternVM_Matching &)> run;
};

class PatternVM_MatchingParam : public PatternVM_Matching, public ::testing::WithParamInterface<MatchingTestCase> {};

TEST_P(PatternVM_MatchingParam, Case) {
  auto expected = GetParam().run(*this);
  rebuild_egraph();
  rebuild_index();
  // Resolve all EClassIds to canonical form after rebuilds.
  for (auto &b : expected.bindings) {
    for (auto &[var, id] : b) {
      id = egraph.find(id);
    }
  }
  run_compiled();
  if (expected.bindings.empty()) {
    verify_empty();
  } else {
    verify(expected.bindings);
  }
}

// clang-format off
auto const kMatchingCases = std::to_array<MatchingTestCase>({

// ============================================================================
// Single Pattern: Basics
// ============================================================================

{Single, "VarBindsAll", [](auto &t) -> MatchingExpected {
  t.use_patterns(make_var_pattern(kVarX));
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.node(Op::Add, a, b);
  return {{{{kVarX, a}}, {{kVarX, b}}, {{kVarX, c}}}};
}},

{Single, "SymbolExactMatch", [](auto &t) -> MatchingExpected {
  t.use_patterns(TestPattern::build(kTestRoot, Op::F, {Var{kVarX}, Var{kVarY}}));
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  t.node(Op::F, a, b, t.leaf(Op::C));  // 3-ary, won't match 2-ary pattern
  auto correct_f = t.node(Op::F, a, b);
  return {{{{kTestRoot, correct_f}, {kVarX, a}, {kVarY, b}}}};
}},

{Single, "NoMatch", [](auto &t) -> MatchingExpected {
  t.use_patterns(TestPattern::build(kTestRoot, Op::Add, {Var{kVarX}, Var{kVarY}}));
  auto a = t.leaf(Op::A);
  t.node(Op::Mul, a, t.leaf(Op::B));
  t.node(Op::Neg, a);
  return {};
}},

{Single | Wildcard_, "SkipsBinding", [](auto &t) -> MatchingExpected {
  t.use_patterns(TestPattern::build(Op::Add, {Wildcard{}, Var{kVarX}}));
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  t.node(Op::Add, a, b);
  t.node(Op::Add, b, a);
  return {{{{kVarX, b}}, {{kVarX, a}}}};
}},

{Single | Wildcard_, "NoVarsNoResults", [](auto &t) -> MatchingExpected {
  t.use_patterns(make_wildcard_pattern());
  t.leaf(Op::A);
  t.leaf(Op::B);
  return {};
}},

// ============================================================================
// Single Pattern: Nesting
// ============================================================================

{Single | Nesting, "AllLevels", [](auto &t) -> MatchingExpected {
  t.use_patterns(
      TestPattern::build(kTestRoot, Op::Add, {Sym(Op::Mul, Sym(Op::Neg, Var{kVarX}), Var{kVarY}), Var{kVarZ}}));
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto expr = t.node(Op::Add, t.node(Op::Mul, t.node(Op::Neg, a), b), c);
  return {{{{kTestRoot, expr}, {kVarX, a}, {kVarY, b}, {kVarZ, c}}}};
}},

{Single | Nesting, "WideBranches", [](auto &t) -> MatchingExpected {
  t.use_patterns(TestPattern::build(
      kTestRoot, Op::F, {Sym(Op::Add, Var{kVarW}, Var{kVarX}), Sym(Op::Mul, Var{kVarY}, Var{kVarZ})}));
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto d = t.leaf(Op::D);
  auto f = t.node(Op::F, t.node(Op::Add, a, b), t.node(Op::Mul, c, d));
  return {{{{kTestRoot, f}, {kVarW, a}, {kVarX, b}, {kVarY, c}, {kVarZ, d}}}};
}},

{Single | Nesting, "MultipleRoots", [](auto &t) -> MatchingExpected {
  t.use_patterns(TestPattern::build(kTestRoot, Op::Add, {Var{kVarX}, Var{kVarY}}));
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto inner = t.node(Op::Add, b, c);
  auto outer = t.node(Op::Add, a, inner);
  return {{{{kTestRoot, outer}, {kVarX, a}, {kVarY, inner}},
           {{kTestRoot, inner}, {kVarX, b}, {kVarY, c}}}};
}},

// ============================================================================
// Single Pattern: Leaf Symbol
// ============================================================================

{Single | LeafSym | Merge, "PerEClassNotEnode", [](auto &t) -> MatchingExpected {
  auto x = t.leaf(Op::X);
  auto a0 = t.leaf(Op::A);
  auto a1 = t.leaf(Op::A, 1);
  auto f = t.node(Op::F, x, a0);
  t.merge(a0, a1);
  t.use_patterns(TestPattern::build(kTestRoot, Op::F, {Var{kVarX}, Sym(Op::A)}));
  return {{{{kTestRoot, f}, {kVarX, x}}}};
}},

// ============================================================================
// Multi-Pattern: Joins
// ============================================================================

{Multi | Join, "WrongSymbolFiltered", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  t.node(Op::F, a);
  t.use_patterns(TestPattern::build(Op::F, {Var{kVarX}}), TestPattern::build(Op::F2, {Var{kVarX}}));
  return {};
}},

{Multi | Join, "SharedVar", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  t.node(Op::F, a);
  t.node(Op::F2, a);
  t.use_patterns(TestPattern::build(Op::F, {Var{kVarX}}), TestPattern::build(Op::F2, {Var{kVarX}}));
  return {{{{kVarX, a}}}};
}},

{Multi | Join, "VarPatternSharedVar", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  t.node(Op::Neg, a);
  t.node(Op::Neg, b);
  t.use_patterns(TestPattern::build(Op::Neg, {Var{kVarX}}), make_var_pattern(kVarX));
  return {{{{kVarX, a}}, {{kVarX, b}}}};
}},

{Multi, "DisjointProduct", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto d = t.leaf(Op::D);
  t.node(Op::Neg, a);
  t.node(Op::Neg, b);
  t.node(Op::Add, c, d);
  t.use_patterns(TestPattern::build(Op::Neg, {Var{kVarX}}), TestPattern::build(Op::Add, {Var{kVarY}, Var{kVarZ}}));
  return {{{{kVarX, a}, {kVarY, c}, {kVarZ, d}}, {{kVarX, b}, {kVarY, c}, {kVarZ, d}}}};
}},

{Multi | Join | BoundSym_ | Merge, "ViaParent", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto d = t.leaf(Op::D);
  t.node(Op::Add, a, b);
  auto neg = t.node(Op::Neg, c);
  t.node(Op::Mul, neg, d);
  t.merge(a, neg);
  t.use_patterns(TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}),
                 TestPattern::build(Op::Mul, {BoundSym(kVarX, Op::Neg, Var{kVarZ}), Var{kVarW}}));
  return {{{{kVarX, a}, {kVarY, b},
             {kVarZ, c}, {kVarW, d}}}};
}},

{Multi | Join | BoundSym_, "RejectsNoMatch", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto d = t.leaf(Op::D);
  t.node(Op::Add, a, b);
  auto neg = t.node(Op::Neg, c);
  t.node(Op::Mul, neg, d);
  // No merge — ?x from Add won't have a Neg in its e-class
  t.use_patterns(TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}),
                 TestPattern::build(Op::Mul, {BoundSym(kVarX, Op::Neg, Var{kVarZ}), Var{kVarW}}));
  return {};
}},

{Multi | Join, "RootBindingFeedsNext", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  t.node(Op::F, a);
  auto g = t.node(Op::G, a);
  t.node(Op::H, g);
  t.use_patterns(TestPattern::build(Op::F, {Var{kVarX}}),
                 TestPattern::build(kVarY, Op::G, {Var{kVarX}}),
                 TestPattern::build(Op::H, {Var{kVarY}}));
  return {{{{kVarX, a}, {kVarY, g}}}};
}},

{Multi | Join, "Diamond", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  t.node(Op::F, a, b);
  t.node(Op::G, b, c);
  t.node(Op::H, a, c);
  t.use_patterns(TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}),
                 TestPattern::build(Op::G, {Var{kVarY}, Var{kVarZ}}),
                 TestPattern::build(Op::H, {Var{kVarX}, Var{kVarZ}}));
  return {{{{kVarX, a}, {kVarY, b}, {kVarZ, c}}}};
}},

{Multi | Join | BoundSym_ | ParentChain, "IntermediateBinding", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  t.node(Op::F, a, b);
  auto c = t.node(Op::C, a);
  t.node(Op::X, c);
  t.use_patterns(TestPattern::build(Op::F, {Var{kVarZ}, Var{kVarW}}),
                 TestPattern::build(Op::X, {BoundSym(kVarY, Op::C, Var{kVarZ})}));
  return {{{{kVarZ, a}, {kVarW, b}, {kVarY, c}}}};
}},

{Multi | Join | Wildcard_ | ParentChain, "Basic", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto bind = t.node(Op::Bind, c, a, b);
  auto ident = t.node(Op::Ident, a);
  t.use_patterns(TestPattern::build(kTestRoot, Op::Bind, {Wildcard{}, Var{kVarX}, Var{kVarY}}),
                 TestPattern::build(kVarZ, Op::Ident, {Var{kVarX}}));
  return {{{{kTestRoot, bind},
             {kVarX, a},
             {kVarY, b},
             {kVarZ, ident}}}};
}},

{Multi | Join | Wildcard_, "NoMatch", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto d = t.leaf(Op::D);
  t.node(Op::Bind, d, a, c);
  t.node(Op::Ident, b);
  t.use_patterns(TestPattern::build(kTestRoot, Op::Bind, {Wildcard{}, Var{kVarX}, Var{kVarY}}),
                 TestPattern::build(kVarZ, Op::Ident, {Var{kVarX}}));
  return {};
}},

{Multi | Join | Nesting | Wildcard_, "Nested", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto neg = t.node(Op::Neg, b);
  t.node(Op::F, a, neg);
  auto bind = t.node(Op::Bind, t.leaf(Op::A), a, t.leaf(Op::B));
  t.use_patterns(TestPattern::build(kTestRoot, Op::Bind, {Wildcard{}, Var{kVarX}, Wildcard{}}),
                 TestPattern::build(Op::F, {Var{kVarX}, Sym(Op::Neg, Var{kVarY})}));
  return {{{{kTestRoot, bind}, {kVarX, a}, {kVarY, b}}}};
}},

{Multi | Join | Wildcard_, "ThreePattern", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto bind = t.node(Op::Bind, c, a, b);
  auto ident = t.node(Op::Ident, a);
  auto neg = t.node(Op::Neg, a);
  t.use_patterns(TestPattern::build(kTestRoot, Op::Bind, {Wildcard{}, Var{kVarX}, Wildcard{}}),
                 TestPattern::build(kVarY, Op::Ident, {Var{kVarX}}),
                 TestPattern::build(kVarZ, Op::Neg, {Var{kVarX}}));
  return {{{{kTestRoot, bind},
             {kVarX, a},
             {kVarY, ident},
             {kVarZ, neg}}}};
}},

{Multi | Join | Wildcard_ | ParentChain, "ManyTraversals", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto bind = t.node(Op::Bind, c, a, b);
  auto f2_1 = t.node(Op::F2, a, t.leaf(Op::D, 1));
  auto f2_2 = t.node(Op::F2, a, t.leaf(Op::D, 2));
  auto f2_3 = t.node(Op::F2, a, t.leaf(Op::D, 3));
  auto neg = t.node(Op::Neg, a);
  t.use_patterns(TestPattern::build(kTestRoot, Op::Bind, {Wildcard{}, Var{kVarX}, Wildcard{}}),
                 TestPattern::build(kVarY, Op::F2, {Var{kVarX}, Wildcard{}}),
                 TestPattern::build(kVarZ, Op::Neg, {Var{kVarX}}));
  return {{{{kTestRoot, bind}, {kVarX, a}, {kVarY, f2_1}, {kVarZ, neg}},
           {{kTestRoot, bind}, {kVarX, a}, {kVarY, f2_2}, {kVarZ, neg}},
           {{kTestRoot, bind}, {kVarX, a}, {kVarY, f2_3}, {kVarZ, neg}}}};
}},

{Multi | Join | Dedup, "PrefixChange", [](auto &t) -> MatchingExpected {
  auto a1 = t.leaf(Op::A);
  auto a2 = t.leaf(Op::A, 1);
  auto b1 = t.leaf(Op::B);
  auto c1 = t.leaf(Op::C);
  auto c2 = t.leaf(Op::C, 1);
  auto f1 = t.node(Op::F, a1, b1);
  auto f2 = t.node(Op::F, a2, b1);
  auto f2_c1 = t.node(Op::F2, b1, c1);
  auto f2_c2 = t.node(Op::F2, b1, c2);
  t.use_patterns(TestPattern::build(kVarW, Op::F, {Var{kVarX}, Var{kVarY}}),
                 TestPattern::build(kVarA, Op::F2, {Var{kVarY}, Var{kVarZ}}));
  return {{{{kVarW, f1}, {kVarX, a1}, {kVarY, b1},
             {kVarA, f2_c1}, {kVarZ, c1}},
           {{kVarW, f1}, {kVarX, a1}, {kVarY, b1},
             {kVarA, f2_c2}, {kVarZ, c2}},
           {{kVarW, f2}, {kVarX, a2}, {kVarY, b1},
             {kVarA, f2_c1}, {kVarZ, c1}},
           {{kVarW, f2}, {kVarX, a2}, {kVarY, b1},
             {kVarA, f2_c2}, {kVarZ, c2}}}};
}},

{Multi | Merge, "SkipsNonCanonical", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto d = t.leaf(Op::D);
  t.merge(a, b);
  t.use_patterns(make_var_pattern(kVarX));
  return {{{{kVarX, a}}, {{kVarX, c}}, {{kVarX, d}}}};
}},

{Multi | Nesting | Merge | SelfLoop, "MatchesSelfRef", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto f_a = t.node(Op::F, a);
  auto f_f_a = t.node(Op::F, f_a);
  t.merge(f_a, f_f_a);
  t.use_patterns(TestPattern::build(kTestRoot, Op::F, {Sym(Op::F, Var{kVarX})}));
  return {{{{kTestRoot, f_a}, {kVarX, a}}, {{kTestRoot, f_a}, {kVarX, f_a}}}};
}},

// ============================================================================
// Deduplication
// ============================================================================

{Single | Dedup | LeafSym, "ByFullTuple", [](auto &t) -> MatchingExpected {
  auto a0 = t.leaf(Op::A);
  auto a1 = t.leaf(Op::A, 1);
  auto t1 = t.node(Op::F, a0, a1, a1);
  auto t2 = t.node(Op::F, a1, a1, a1);
  t.use_patterns(TestPattern::build(kTestRoot, Op::F, {Sym(Op::A), Var{kVarX}, Var{kVarY}}));
  return {{{{kTestRoot, t1}, {kVarX, a1}, {kVarY, a1}},
           {{kTestRoot, t2}, {kVarX, a1}, {kVarY, a1}}}};
}},

{Single | Dedup | Merge, "SameBindingMultiEnode", [](auto &t) -> MatchingExpected {
  auto a0 = t.leaf(Op::A);
  auto a1 = t.leaf(Op::A, 1);
  auto a2 = t.leaf(Op::A, 2);
  t.merge(a0, a1);
  t.merge(a0, a2);
  t.node(Op::Neg, a0);
  t.use_patterns(TestPattern::build(Op::Neg, {Var{kVarX}}));
  return {{{{kVarX, a0}}}};
}},

{Single | Dedup | Merge | SelfLoop, "ReportedOnce", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto f_a = t.node(Op::F, a);
  t.merge(f_a, a);
  t.use_patterns(TestPattern::build(Op::F, {Var{kVarX}}));
  return {{{{kVarX, a}}}};
}},

{Single | Dedup | Merge | SelfLoop, "SelfRef", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto n0 = t.node(Op::F, a);
  auto n1 = t.node(Op::F, n0);
  t.merge(n0, n1);
  t.use_patterns(TestPattern::build(kTestRoot, Op::F, {Var{kVarX}}));
  return {{{{kTestRoot, n0}, {kVarX, a}}, {{kTestRoot, n0}, {kVarX, n0}}}};
}},

{Single | Dedup | Merge, "DifferentSymbols", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto f_a = t.node(Op::F, a);
  auto f2_a = t.node(Op::F2, a);
  t.merge(f_a, f2_a);
  t.use_patterns(TestPattern::build(kTestRoot, Op::F, {Var{kVarX}}));
  return {{{{kTestRoot, f_a}, {kVarX, a}}}};
}},

{Single | Dedup | SameVar | Nesting, "PrefixChange", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto f_ab = t.node(Op::F, a, b);
  auto f_ac = t.node(Op::F, a, c);
  auto add1 = t.node(Op::Add, a, f_ab);
  auto add2 = t.node(Op::Add, a, f_ac);
  t.use_patterns(TestPattern::build(kTestRoot, Op::Add, {Var{kVarX}, Sym(Op::F, Var{kVarX}, Var{kVarY})}));
  return {{{{kTestRoot, add1}, {kVarX, a}, {kVarY, b}}, {{kTestRoot, add2}, {kVarX, a}, {kVarY, c}}}};
}},

{Multi | Dedup | ParentChain | Wildcard_, "NoIntermediateBindings", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  t.node(Op::Neg, c);
  t.node(Op::F, c, a);
  t.node(Op::F, c, b);
  t.use_patterns(TestPattern::build(Op::Neg, {Var{kVarX}}), TestPattern::build(Op::F, {Var{kVarX}, Wildcard{}}));
  return {{{{kVarX, c}}}};
}},

// ============================================================================
// Single Pattern: Unary Chains
// ============================================================================

{Single | Nesting, "UnaryChain4", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c0 = t.node(Op::Neg, t.node(Op::Neg, t.node(Op::Neg, t.node(Op::Neg, a))));
  auto c1 = t.node(Op::Neg, t.node(Op::Neg, t.node(Op::Neg, t.node(Op::Neg, b))));
  t.use_patterns(TestPattern::build(kTestRoot, Op::Neg, {Sym(Op::Neg, Sym(Op::Neg, Sym(Op::Neg, Var{kVarX})))}));
  return {{{{kTestRoot, c0}, {kVarX, a}},
           {{kTestRoot, c1}, {kVarX, b}}}};
}},

{Single | SameVar, "RejectsUnequal", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto aa = t.node(Op::Add, a, a);
  auto bb = t.node(Op::Add, b, b);
  t.node(Op::Add, a, b);
  t.node(Op::Add, b, c);
  t.node(Op::Add, c, a);
  t.use_patterns(TestPattern::build(kTestRoot, Op::Add, {Var{kVarX}, Var{kVarX}}));
  return {{{{kTestRoot, aa}, {kVarX, a}},
           {{kTestRoot, bb}, {kVarX, b}}}};
}},

{Single | Merge, "DistinctChildren", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto n0 = t.node(Op::Neg, a);
  auto n1 = t.node(Op::Neg, b);
  auto n2 = t.node(Op::Neg, c);
  t.merge(n0, n1);
  t.merge(n0, n2);
  t.use_patterns(TestPattern::build(kTestRoot, Op::Neg, {Var{kVarX}}));
  return {{{{kTestRoot, n0}, {kVarX, a}},
           {{kTestRoot, n0}, {kVarX, b}},
           {{kTestRoot, n0}, {kVarX, c}}}};
}},

// ============================================================================
// Single Pattern: Deep Nesting
// ============================================================================

{Single | Nesting, "TernaryBindsAll", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto neg_a = t.node(Op::Neg, a);
  auto neg_b = t.node(Op::Neg, b);
  auto neg_c = t.node(Op::Neg, c);
  auto f = t.node(Op::F, neg_a, neg_b, neg_c);
  t.use_patterns(TestPattern::build(
      kTestRoot, Op::F, {Sym(Op::Neg, Var{kVarX}), Sym(Op::Neg, Var{kVarY}), Sym(Op::Neg, Var{kVarZ})}));
  return {{{{kTestRoot, f},
             {kVarX, a}, {kVarY, b}, {kVarZ, c}}}};
}},

// ============================================================================
// Single Pattern: Same Variable (CheckSlot)
// ============================================================================

{Single | SameVar | Merge, "MergedEClass", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto neg_a = t.node(Op::Neg, a);
  auto neg_b = t.node(Op::Neg, b);
  auto n0 = t.node(Op::Add, a, neg_a);
  auto n1 = t.node(Op::Add, a, neg_b);
  t.merge(n0, n1);
  t.use_patterns(TestPattern::build(kTestRoot, Op::Add, {Var{kVarX}, Sym(Op::Neg, Var{kVarX})}));
  return {{{{kTestRoot, n0}, {kVarX, a}}}};
}},

{Single | SameVar | Merge, "MultipleEnodes", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto neg_b = t.node(Op::Neg, b);
  auto neg_a = t.node(Op::Neg, a);
  t.merge(neg_b, neg_a);
  auto add = t.node(Op::Add, a, neg_b);
  t.use_patterns(TestPattern::build(kTestRoot, Op::Add, {Var{kVarX}, Sym(Op::Neg, Var{kVarX})}));
  return {{{{kTestRoot, add}, {kVarX, a}}}};
}},

{Single | SameVar | Merge, "MultipleMerged", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto neg_a = t.node(Op::Neg, a);
  auto neg_b = t.node(Op::Neg, b);
  auto n0 = t.node(Op::Add, a, neg_a);
  auto n1 = t.node(Op::Add, a, neg_b);
  auto n2 = t.node(Op::Add, b, neg_b);
  t.merge(n0, n1);
  t.merge(n1, n2);
  t.use_patterns(TestPattern::build(kTestRoot, Op::Add, {Var{kVarX}, Sym(Op::Neg, Var{kVarX})}));
  return {{{{kTestRoot, n0}, {kVarX, a}}, {{kTestRoot, n0}, {kVarX, b}}}};
}},

// ============================================================================
// Parent Chain Backtrack
// ============================================================================

{Multi | ParentChain | Nesting | Wildcard_, "InnerExhaustion", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto d = t.leaf(Op::D);
  t.node(Op::Neg, d);
  t.node(Op::F, d, a);  // F-parent of d, no F-parent above
  auto f2 = t.node(Op::F, d, b);  // F-parent of d, has F-parent above
  t.node(Op::F, f2, c);
  t.use_patterns(TestPattern::build(Op::Neg, {Var{kVarX}}),
                 TestPattern::build(Op::F, {Sym(Op::F, Var{kVarX}, Wildcard{}), Wildcard{}}));
  return {{{{kVarX, d}}}};
}},

// ============================================================================
// Codegen Correctness Baselines
// ============================================================================

{Multi | Codegen | Join | Merge, "MultipleEnodes", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto g1 = t.node(Op::G, a);
  auto g2 = t.leaf(Op::G, 99);
  t.node(Op::F, a);
  t.merge(g1, g2);
  t.node(Op::H, g1);
  t.use_patterns(TestPattern::build(Op::F, {Var{kVarX}}),
                 TestPattern::build(kVarY, Op::G, {Var{kVarX}}),
                 TestPattern::build(Op::H, {Var{kVarY}}));
  return {{{{kVarX, a}, {kVarY, g1}}}};
}},

{Multi | Codegen | Join, "MultipleAnchors", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto g = t.node(Op::G, a);
  t.node(Op::F, a);
  t.node(Op::F, b);
  t.node(Op::H, g);
  t.use_patterns(TestPattern::build(Op::F, {Var{kVarX}}),
                 TestPattern::build(kVarY, Op::G, {Var{kVarX}}),
                 TestPattern::build(Op::H, {Var{kVarY}}));
  return {{{{kVarX, a}, {kVarY, g}}}};
}},

{Single | Codegen | Nesting | Merge, "SiblingLoadChild", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto neg1 = t.node(Op::Neg, a);
  auto neg2 = t.leaf(Op::Neg, 99);
  t.merge(neg1, neg2);
  t.node(Op::Mul, neg1, b);
  t.use_patterns(TestPattern::build(Op::Mul, {Sym(Op::Neg, Var{kVarX}), Var{kVarY}}));
  return {{{{kVarX, a}, {kVarY, b}}}};
}},

{Multi | Codegen | Join | Nesting, "SiblingLoadChildWithJoin", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  t.node(Op::Neg, a);
  t.node(Op::Mul, t.node(Op::Neg, a), b);
  t.node(Op::Add, a, c);
  t.use_patterns(TestPattern::build(Op::Mul, {Sym(Op::Neg, Var{kVarZ}), Var{kVarW}}),
                 TestPattern::build(Op::Add, {Var{kVarZ}, Var{kVarY}}));
  return {{{{kVarZ, a}, {kVarW, b}, {kVarY, c}}}};
}},

// ============================================================================
// Multi-Pattern: Concurrent Iteration
// ============================================================================

{Multi | Concurrent, "SameSymbol", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  t.node(Op::F, a);
  t.node(Op::F, b);
  t.use_patterns(TestPattern::build(Op::F, {Var{kVarX}}), TestPattern::build(Op::F, {Var{kVarY}}));
  return {{{{kVarX, a}, {kVarY, a}}, {{kVarX, a}, {kVarY, b}},
           {{kVarX, b}, {kVarY, a}}, {{kVarX, b}, {kVarY, b}}}};
}},

// ============================================================================
// Multi-Pattern: Hoisting
// ============================================================================

{Multi | Hoisting | Join, "Transitive", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto f = t.node(Op::F, a);
  auto g = t.node(Op::G, f);
  t.node(Op::H, g);
  t.use_patterns(TestPattern::build(kVarA, Op::F, {Var{kVarX}}),
                 TestPattern::build(kVarB, Op::G, {Var{kVarA}}),
                 TestPattern::build(Op::H, {Var{kVarB}}));
  return {{{{kVarX, a}, {kVarA, f}, {kVarB, g}}}};
}},

{Multi | Hoisting | Join, "TransitiveNoMatch", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto f = t.node(Op::F, a);
  t.node(Op::G, f);
  // No H node
  t.use_patterns(TestPattern::build(kVarA, Op::F, {Var{kVarX}}),
                 TestPattern::build(kVarB, Op::G, {Var{kVarA}}),
                 TestPattern::build(Op::H, {Var{kVarB}}));
  return {};
}},

{Multi | Hoisting | Join, "MultiplePatterns", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto f = t.node(Op::F, a, b);
  auto g = t.node(Op::G, f);
  auto h = t.node(Op::H, f);
  t.use_patterns(TestPattern::build(kVarA, Op::F, {Var{kVarX}, Var{kVarY}}),
                 TestPattern::build(kVarB, Op::G, {Var{kVarA}}),
                 TestPattern::build(kVarC, Op::H, {Var{kVarA}}));
  return {{{{kVarX, a}, {kVarY, b},
             {kVarA, f}, {kVarB, g}, {kVarC, h}}}};
}},

{Multi | Hoisting | Join, "MultiplePatternsMultiMatch", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c0 = t.leaf(Op::C);
  auto c1 = t.leaf(Op::C, 1);
  auto d0 = t.leaf(Op::D);
  auto d1 = t.leaf(Op::D, 1);
  auto d2 = t.leaf(Op::D, 2);
  auto f = t.node(Op::F, a, b);
  t.node(Op::G, f, c0);
  t.node(Op::G, f, c1);
  t.node(Op::H, f, d0);
  t.node(Op::H, f, d1);
  t.node(Op::H, f, d2);
  t.use_patterns(TestPattern::build(kVarA, Op::F, {Var{kVarX}, Var{kVarY}}),
                 TestPattern::build(Op::G, {Var{kVarA}, Var{kVarB}}),
                 TestPattern::build(Op::H, {Var{kVarA}, Var{kVarC}}));
  // 2 G parents × 3 H parents = 6 matches. Each differs in ?b (G's child) and ?c (H's child).
  return {{{{kVarX, a}, {kVarY, b}, {kVarA, f}, {kVarB, c0}, {kVarC, d0}},
           {{kVarX, a}, {kVarY, b}, {kVarA, f}, {kVarB, c0}, {kVarC, d1}},
           {{kVarX, a}, {kVarY, b}, {kVarA, f}, {kVarB, c0}, {kVarC, d2}},
           {{kVarX, a}, {kVarY, b}, {kVarA, f}, {kVarB, c1}, {kVarC, d0}},
           {{kVarX, a}, {kVarY, b}, {kVarA, f}, {kVarB, c1}, {kVarC, d1}},
           {{kVarX, a}, {kVarY, b}, {kVarA, f}, {kVarB, c1}, {kVarC, d2}}}};
}},

{Multi | Hoisting | Join, "SharedVarEntry", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  t.node(Op::F, a, b);
  t.node(Op::G, a, c);
  t.node(Op::H, c);
  t.use_patterns(TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}),
                 TestPattern::build(Op::G, {Var{kVarX}, Var{kVarZ}}),
                 TestPattern::build(Op::H, {Var{kVarZ}}));
  return {{{{kVarX, a}, {kVarY, b}, {kVarZ, c}}}};
}},

{Multi | Hoisting | Join, "SharedVarEntryNoFP", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  t.node(Op::F, a, b);
  t.node(Op::G, a, c);
  // No H(c)
  t.use_patterns(TestPattern::build(Op::F, {Var{kVarX}, Var{kVarY}}),
                 TestPattern::build(Op::G, {Var{kVarX}, Var{kVarZ}}),
                 TestPattern::build(Op::H, {Var{kVarZ}}));
  return {};
}},

{Multi | Hoisting | Join | BoundSym_, "DeepEntry", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto neg = t.node(Op::Neg, a);
  t.node(Op::Mul, neg, b);
  t.node(Op::G, neg);
  t.use_patterns(TestPattern::build(Op::Mul, {BoundSym(kVarA, Op::Neg, Var{kVarZ}), Var{kVarW}}),
                 TestPattern::build(Op::G, {Var{kVarA}}));
  return {{{{kVarA, neg}, {kVarZ, a}, {kVarW, b}}}};
}},

{Multi | Hoisting | Join | BoundSym_, "DeepEntryNoMatch", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto neg = t.node(Op::Neg, a);
  t.node(Op::Mul, neg, b);
  // No G(neg)
  t.use_patterns(TestPattern::build(Op::Mul, {BoundSym(kVarA, Op::Neg, Var{kVarZ}), Var{kVarW}}),
                 TestPattern::build(Op::G, {Var{kVarA}}));
  return {};
}},

{Multi | Hoisting | Join, "MixedCartesian", [](auto &t) -> MatchingExpected {
  auto a = t.leaf(Op::A);
  auto b = t.leaf(Op::B);
  auto c = t.leaf(Op::C);
  auto f = t.node(Op::F, a);
  t.node(Op::G, f);
  t.node(Op::H, b);
  t.node(Op::H, c);
  t.use_patterns(TestPattern::build(kVarA, Op::F, {Var{kVarX}}),
                 TestPattern::build(Op::G, {Var{kVarA}}),
                 TestPattern::build(Op::H, {Var{kVarZ}}));
  return {{{{kVarX, a}, {kVarA, f}, {kVarZ, b}},
           {{kVarX, a}, {kVarA, f}, {kVarZ, c}}}};
}},

});
// clang-format on

INSTANTIATE_TEST_SUITE_P(PatternVM_Matching, PatternVM_MatchingParam, ::testing::ValuesIn(kMatchingCases),
                         [](auto const &info) {
                           auto t = info.param.tags;
                           std::string prefix;
                           // Cardinality
                           if (t & Single) prefix += "S_";
                           if (t & Multi) prefix += "M_";
                           // Tags (alphabetical)
                           if (t & BoundSym_) prefix += "Bsym_";
                           if (t & Codegen) prefix += "Cgen_";
                           if (t & Concurrent) prefix += "Conc_";
                           if (t & Dedup) prefix += "Ded_";
                           if (t & Hoisting) prefix += "Hoist_";
                           if (t & Join) prefix += "Join_";
                           if (t & LeafSym) prefix += "Lsym_";
                           if (t & Merge) prefix += "Mrg_";
                           if (t & Nesting) prefix += "Nest_";
                           if (t & ParentChain) prefix += "Par_";
                           if (t & SameVar) prefix += "Svar_";
                           if (t & SelfLoop) prefix += "Loop_";
                           if (t & Wildcard_) prefix += "Wc_";
                           return prefix + info.param.name;
                         });

// ============================================================================
// Complex Tests (multi-phase, conditional, loops, fuzzer reproductions)
// ============================================================================
//
// These tests cannot be expressed as simple setup→verify because they:
// - Run multiple phases (use_patterns + run_compiled called multiple times)
// - Have conditional assertions based on runtime state
// - Use loops to build structures
// - Are fuzzer reproductions with exact sequences that should not be simplified

TEST_F(PatternVM_Matching, Merge_ExposesAllEnodes) {
  auto x = leaf(Op::Var, 1);
  auto y = leaf(Op::Var, 2);
  auto add = node(Op::Add, x, y);
  auto mul = node(Op::Mul, x, y);

  auto merged = merge(add, mul);
  rebuild_egraph();
  rebuild_index();

  use_patterns(TestPattern::build(kTestRoot, Op::Add, {Var{kVarX}, Var{kVarY}}));
  run_compiled();
  verify({{{kTestRoot, merged}, {kVarX, x}, {kVarY, y}}});

  use_patterns(TestPattern::build(kTestRoot, Op::Mul, {Var{kVarX}, Var{kVarY}}));
  run_compiled();
  verify({{{kTestRoot, merged}, {kVarX, x}, {kVarY, y}}});
}

TEST_F(PatternVM_Matching, Merge_UnifiesSharedVar) {
  use_patterns(TestPattern::build(kTestRoot, Op::Add, {Var{kVarX}, Var{kVarX}}));

  auto a = leaf(Op::Var, 1);
  auto b = leaf(Op::Var, 2);
  auto add = node(Op::Add, a, b);
  rebuild_index();

  // Before merge: no match (a != b)
  run_compiled();
  verify_empty();

  // After merge: matches (a == b)
  merge(a, b);
  rebuild_egraph();
  rebuild_index();

  run_compiled();
  verify({{{kTestRoot, add}, {kVarX, egraph.find(a)}}});
}

TEST_F(PatternVM_Matching, Merge_SelfLoopYieldsBothBindings) {
  auto n0 = leaf(Op::B, 64);
  auto n1 = node(Op::F, n0);
  auto n2 = node(Op::F, n1);

  merge(n1, n2);
  rebuild_egraph();
  rebuild_index();

  auto ec1 = egraph.find(n1);
  ASSERT_EQ(egraph.find(n2), ec1) << "n1 and n2 should be in same e-class";

  use_patterns(TestPattern::build(kTestRoot, Op::F, {Sym(Op::F, Sym(Op::F, Var{kVarX}))}));

  run_compiled();
  verify({
      {{kTestRoot, ec1}, {kVarX, n0}},
      {{kTestRoot, ec1}, {kVarX, ec1}},
  });
}

TEST_F(PatternVM_Matching, Merge_ChildCreatesSelfLoop) {
  auto n0 = leaf(Op::A, 10);
  auto n1 = node(Op::F, n0);
  leaf(Op::A, 0);

  merge(n0, n1);
  rebuild_egraph();

  auto n3 = node(Op::F, n1);
  node(Op::F, n3);
  rebuild_egraph();
  rebuild_index();

  use_patterns(TestPattern::build(kTestRoot, Op::F, {Var{kVarX}}));

  auto ec = egraph.find(n0);
  ASSERT_EQ(egraph.find(n1), ec) << "n0 and n1 should be in the same e-class after union";

  run_compiled();
  verify({{{kTestRoot, ec}, {kVarX, ec}}});
}

TEST_F(PatternVM_Matching, Index_IncrementalRebuildExposesNewNodes) {
  use_patterns(TestPattern::build(kTestRoot, Op::Const));

  rebuild_index();
  run_compiled();
  verify_empty();

  auto c = leaf(Op::Const, 42);
  run_compiled();
  verify_empty();

  rebuild_index_with(c);
  run_compiled();
  verify({{{kTestRoot, c}}});
}

TEST_F(PatternVM_Matching, Index_CongruenceDeduplicatesEnodes) {
  use_patterns(TestPattern::build(Op::Add, {Var{kVarX}, Var{kVarY}}));

  auto a = leaf(Op::Var, 0);
  auto b = leaf(Op::Var, 1);
  auto c = leaf(Op::Var, 2);
  auto d = leaf(Op::Var, 3);
  node(Op::Add, a, b);
  node(Op::Add, c, d);
  rebuild_index();

  run_compiled();
  verify({
      {{kVarX, a}, {kVarY, b}},
      {{kVarX, c}, {kVarY, d}},
  });

  auto x = merge(a, c);
  auto y = merge(b, d);
  rebuild_egraph();
  rebuild_index();

  run_compiled();
  verify({{{kVarX, x}, {kVarY, y}}});
}

TEST_F(PatternVM_Matching, MultiPattern_MultipleJoinMatches) {
  auto placeholder = leaf(Op::Const, 0);
  auto sym_val = leaf(Op::Const, 1);
  auto expr_val = leaf(Op::Const, 2);
  node(Op::Bind, placeholder, sym_val, expr_val);
  auto ident1 = node(Op::Ident, sym_val);
  auto ident2 = node(Op::Ident, sym_val);
  rebuild_index();

  use_patterns(TestPattern::build(kTestRoot, Op::Bind, {Wildcard{}, Var{kVarX}, Var{kVarY}}),
               TestPattern::build(kVarZ, Op::Ident, {Var{kVarX}}));
  run_compiled();

  bool idents_same = (egraph.find(ident1) == egraph.find(ident2));
  EXPECT_EQ(matches.size(), idents_same ? 1u : 2u);
}

TEST_F(PatternVM_Matching, Dedup_WideEClass) {
  auto a = leaf(Op::Const, 0);

  constexpr std::size_t kNumVariants = 50;
  std::vector<EClassId> f_nodes;
  for (std::size_t i = 1; i <= kNumVariants; ++i) {
    auto x = leaf(Op::Const, i);
    f_nodes.push_back(node(Op::F, a, x));
  }

  for (std::size_t i = 1; i < f_nodes.size(); ++i) {
    merge(f_nodes[0], f_nodes[i]);
  }
  rebuild_egraph();
  rebuild_index();

  use_patterns(TestPattern::build(kTestRoot, Op::F, {Var{kVarX}, Var{kVarY}}));
  run_compiled();
  EXPECT_EQ(matches.size(), kNumVariants) << "Should find " << kNumVariants << " unique matches";
}

TEST_F(PatternVM_Matching, SelfLoop_SharedVarExploresAllPaths) {
  // Exact fuzzer structure — do not simplify.
  auto n0 = leaf(Op::D, 1653159021);
  auto n1 = leaf(Op::D, 2573693081);
  [[maybe_unused]] auto n2 = node(Op::Mul, n1, n1);
  auto n3 = node(Op::Mul, n0, n0);
  [[maybe_unused]] auto n4 = node(Op::Mul, n3, n3);
  auto n5 = node(Op::Mul, n0, n0);
  auto n6 = node(Op::Mul, n5, n5);
  auto n7 = node(Op::Mul, n1, n1);
  auto n8 = node(Op::Mul, n7, n7);
  auto n9 = node(Op::Mul, n6, n6);
  auto n10 = node(Op::Mul, n5, n5);
  auto n11 = node(Op::Mul, n8, n0);
  [[maybe_unused]] auto n12 = node(Op::Mul, n10, n5);
  auto n13 = node(Op::Mul, n7, n10);
  auto n14 = node(Op::Mul, n1, n1);
  auto n15 = node(Op::Mul, n5, n5);

  auto n16 = node(Op::Add, n0, n0);
  merge(n16, n0);
  auto n17 = node(Op::Add, n8, n8);
  merge(n17, n8);
  auto n18 = node(Op::Add, n14, n14);
  merge(n18, n14);
  auto n19 = node(Op::Add, n11, n11);
  merge(n19, n11);
  auto n20 = node(Op::Add, n15, n15);
  merge(n20, n15);
  auto n21 = node(Op::Add, n13, n13);
  merge(n21, n13);
  auto n22 = node(Op::Add, n1, n1);
  merge(n22, n1);
  auto n23 = node(Op::Add, n9, n9);
  merge(n23, n9);

  auto n24 = node(Op::Mul, n0, n0);
  merge(n24, n0);
  auto n25 = node(Op::Mul, n8, n8);
  merge(n25, n8);
  auto n26 = node(Op::Mul, n14, n14);
  merge(n26, n14);
  auto n27 = node(Op::Mul, n11, n11);
  merge(n27, n11);
  auto n28 = node(Op::Mul, n15, n15);
  merge(n28, n15);
  auto n29 = node(Op::Mul, n13, n13);
  merge(n29, n13);
  auto n30 = node(Op::Mul, n1, n1);
  merge(n30, n1);
  auto n31 = node(Op::Mul, n15, n15);
  merge(n31, n15);

  auto n32 = node(Op::Add, n30, n30);
  merge(n32, n30);
  auto n33 = node(Op::Add, n11, n11);
  merge(n33, n11);
  auto n34 = node(Op::Add, n31, n31);
  merge(n34, n31);
  auto n35 = node(Op::Add, n13, n13);
  merge(n35, n13);
  auto n36 = node(Op::Add, n32, n32);
  merge(n36, n32);
  auto n37 = node(Op::Add, n33, n33);
  merge(n37, n33);
  auto n38 = node(Op::Add, n34, n34);
  merge(n38, n34);
  auto n39 = node(Op::Add, n35, n35);
  merge(n39, n35);

  [[maybe_unused]] auto n40 = node(Op::Mul, n38, n21);
  [[maybe_unused]] auto n41 = node(Op::Mul, n17, n28);

  rebuild_egraph();
  rebuild_index();

  use_patterns(TestPattern::build(Op::Mul, {Var{kVarX}, Sym(Op::Mul, Var{kVarX}, Var{kVarY})}));

  run_compiled();
  EXPECT_EQ(matches.size(), 3u);
}

TEST_F(PatternVM_Matching, Random_FAddNegFindsMatches) {
  constexpr std::size_t kNumNodes = 20;

  std::vector<EClassId> leaves;
  for (std::size_t i = 0; i < 10; ++i) {
    leaves.push_back(leaf(Op::Const, i));
  }

  std::mt19937 rng(42);
  for (std::size_t i = 0; i < kNumNodes; ++i) {
    auto x = leaves[rng() % leaves.size()];
    auto y = leaves[rng() % leaves.size()];
    auto z = leaves[rng() % leaves.size()];
    auto add_xy = node(Op::Add, x, y);
    auto neg_z = node(Op::Neg, z);
    node(Op::F, add_xy, neg_z);
  }
  rebuild_egraph();
  rebuild_index();

  use_patterns(TestPattern::build(kTestRoot, Op::F, {Sym(Op::Add, Var{kVarX}, Var{kVarY}), Sym(Op::Neg, Var{kVarZ})}));

  run_compiled();
  EXPECT_GT(matches.size(), 0u) << "Should find at least one match";
}

TEST_F(PatternVM_Matching, Random_AddFindsMatches) {
  std::vector<EClassId> leaves;
  for (std::size_t i = 0; i < 20; ++i) {
    leaves.push_back(leaf(Op::Const, i));
  }

  std::mt19937 rng(42);
  for (std::size_t i = 0; i < 50; ++i) {
    auto a = leaves[rng() % leaves.size()];
    auto b = leaves[rng() % leaves.size()];
    node(Op::Add, a, b);
  }
  rebuild_egraph();
  rebuild_index();

  use_patterns(TestPattern::build(kTestRoot, Op::Add, {Var{kVarX}, Var{kVarY}}));

  run_compiled();
  EXPECT_GT(matches.size(), 0u) << "Should find at least one match";
}

TEST_F(PatternVM_Matching, Fuzzer_SharedVarAcrossSiblingsInSelfLoop) {
  auto a = leaf(Op::A, 0);
  auto neg_a = node(Op::Neg, a);
  merge(neg_a, a);
  rebuild_egraph();
  rebuild_index();

  auto f_a = node(Op::F, a);
  auto mul = node(Op::Mul, neg_a, f_a);
  rebuild_egraph();
  rebuild_index();

  use_patterns(TestPattern::build(kTestRoot, Op::Mul, {Sym(Op::Neg, Var{kVarX}), Sym(Op::F, Var{kVarX})}));

  run_compiled();
  verify({{{kTestRoot, egraph.find(mul)}, {kVarX, egraph.find(a)}}});
}

TEST_F(PatternVM_Matching, Fuzzer_SiblingSymbolVerifiedInParentChain) {
  auto b0 = leaf(Op::B, 0);
  auto a0 = leaf(Op::A, 0);
  auto a1 = leaf(Op::A, 1);
  auto a2 = leaf(Op::A, 2);

  auto f_b0 = node(Op::F, b0);
  auto ff_b0 = node(Op::F, f_b0);
  merge(ff_b0, b0);

  auto f_a0 = node(Op::F, a0);
  auto ff_a0 = node(Op::F, f_a0);
  merge(ff_a0, a0);

  auto f_a1 = node(Op::F, a1);
  auto ff_a1 = node(Op::F, f_a1);
  merge(ff_a1, a1);

  auto f_a2 = node(Op::F, a2);
  auto ff_a2 = node(Op::F, f_a2);
  merge(ff_a2, a2);

  rebuild_egraph();
  rebuild_index();

  for (auto ec : {b0, a0, a1, a2}) {
    auto canonical = egraph.find(ec);
    auto f_ec = node(Op::F, canonical);
    auto ff_ec = node(Op::F, f_ec);
    merge(ff_ec, canonical);
  }
  rebuild_egraph();
  rebuild_index();

  for (auto ec : {b0, a0, a1, a2}) {
    auto canonical = egraph.find(ec);
    auto plus_xx = node(Op::Plus, canonical, canonical);
    merge(plus_xx, canonical);
  }
  rebuild_egraph();
  rebuild_index();

  use_patterns(TestPattern::build(Op::F, {Var{kVarX}}),
               TestPattern::build(Op::Plus, {Sym(Op::F, Var{kVarX}), Sym(Op::H, Var{kVarX})}));

  run_compiled();
  verify(std::initializer_list<Bindings>{});
}

TEST_F(PatternVM_Matching, Fuzzer_SelfRefMultiPatternRejectsWrongChild) {
  auto b0 = leaf(Op::B, 0);

  auto plus_b0 = node(Op::Plus, b0, b0);
  merge(plus_b0, b0);
  rebuild_egraph();
  rebuild_index();

  auto mul_node = node(Op::Mul, b0, b0);
  rebuild_egraph();
  rebuild_index();

  auto a0 = leaf(Op::A, 0);
  auto a1 = leaf(Op::A, 1);
  auto a2 = leaf(Op::A, 2);

  for (auto ec : {b0, mul_node, a0, a1, a2}) {
    auto h_ec = node(Op::H, ec);
    merge(h_ec, ec);
  }
  rebuild_egraph();
  rebuild_index();

  auto f_b0 = node(Op::F, b0);
  rebuild_egraph();
  rebuild_index();

  for (auto ec : {b0, mul_node, a0, a1, a2, f_b0}) {
    auto f_ec = node(Op::F, ec);
    auto ff_ec = node(Op::F, f_ec);
    merge(ff_ec, ec);
  }
  rebuild_egraph();
  rebuild_index();

  use_patterns(TestPattern::build(Op::F, {Var{kVarX}}),
               make_var_pattern(kVarX),
               TestPattern::build(Op::Mul, {Sym(Op::H, Var{kVarX}), Sym(Op::F, Var{kVarX})}));

  run_compiled();
  verify(std::initializer_list<Bindings>{});
}

TEST_F(PatternVM_Matching, Fuzzer_IdenticalPatternsMatchBothEClasses) {
  auto a0 = leaf(Op::A, 0);
  auto b0 = leaf(Op::B, 0);

  auto mul_bb = node(Op::Mul, b0, b0);

  for (auto ec : {a0, mul_bb, b0}) {
    auto h_ec = node(Op::H, ec);
    merge(h_ec, ec);
  }
  rebuild_egraph();
  rebuild_index();

  for (auto ec : {a0, mul_bb, b0}) {
    auto f_ec = node(Op::F, ec);
    auto ff_ec = node(Op::F, f_ec);
    merge(ff_ec, ec);
  }
  rebuild_egraph();
  rebuild_index();

  for (auto ec : {a0, mul_bb, b0}) {
    auto canonical = egraph.find(ec);
    auto g_ec = node(Op::G, canonical);
    merge(g_ec, canonical);
  }
  rebuild_egraph();
  rebuild_index();

  for (auto ec : {a0, b0}) {
    auto canonical = egraph.find(ec);
    auto plus_xx = node(Op::Plus, canonical, canonical);
    merge(plus_xx, canonical);
  }
  rebuild_egraph();
  rebuild_index();

  auto canonical_a = egraph.find(a0);
  auto mul_aa = node(Op::Mul, canonical_a, canonical_a);
  rebuild_egraph();
  rebuild_index();

  for (auto ec : {a0, mul_aa, b0}) {
    auto canonical = egraph.find(ec);
    auto g_ec = node(Op::G, canonical);
    merge(g_ec, canonical);
  }
  rebuild_egraph();
  rebuild_index();

  use_patterns(TestPattern::build(Op::Mul, {Sym(Op::G, Var{kVarX}), Sym(Op::G, Var{kVarX})}),
               TestPattern::build(Op::Mul, {Sym(Op::G, Var{kVarX}), Sym(Op::G, Var{kVarX})}));

  run_compiled();
  verify({{{kVarX, egraph.find(b0)}}, {{kVarX, egraph.find(a0)}}});
}

TEST_F(PatternVM_Matching, Deep_NestedPattern) {
  constexpr int kDepth = 10;

  auto x = leaf(Op::Const, 0);
  auto current = x;
  for (int i = 0; i < kDepth; ++i) {
    current = node(Op::Neg, current);
  }
  rebuild_index();

  use_patterns(TestPattern::build(kTestRoot, Op::Neg, {Sym(Op::Neg, Sym(Op::Neg, Var{kVarX}))}));
  run_compiled();
  EXPECT_EQ(matches.size(), kDepth - 3 + 1) << "Expected 8 matches for depth-3 pattern on depth-10 chain";
}

TEST_F(PatternVM_Matching, Deep_VeryNestedPattern) {
  constexpr int kDepth = 50;
  constexpr int kPatternDepth = 10;

  auto x = leaf(Op::Const, 0);
  auto current = x;
  for (int i = 0; i < kDepth; ++i) {
    current = node(Op::Neg, current);
  }
  rebuild_index();

  auto b = TestPattern::Builder{};
  auto cur = b.var(kVarX);
  for (int i = 0; i < kPatternDepth; ++i) {
    cur = b.sym(Op::Neg, {cur});
  }
  use_patterns(std::move(b).build());
  run_compiled();
  EXPECT_EQ(matches.size(), kDepth - kPatternDepth + 1) << "Expected 41 matches for depth-10 pattern on depth-50 chain";
}

}  // namespace memgraph::planner::core
