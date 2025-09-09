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

#include <doctest/doctest.h>

#include <stdexcept>

#include "ese/core/egraph.hpp"
#include "ese/core/processing_context.hpp"
#include "test_symbols.hpp"

TEST_CASE("EGraph basic operations") {
  using TestSymbol = test_symbols::TestSymbol;
  ese::core::EGraph<TestSymbol, void> egraph;

  SUBCASE("empty e-graph") {
    CHECK(egraph.empty());
    CHECK_EQ(egraph.num_classes(), 0);
    CHECK_EQ(egraph.num_nodes(), 0);
  }

  SUBCASE("add simple e-nodes") {
    // Add leaf nodes
    auto id1 = egraph.emplace("a", {});
    auto id2 = egraph.emplace("b", {});

    CHECK_EQ(egraph.num_classes(), 2);
    CHECK_EQ(egraph.num_nodes(), 2);
    CHECK_NE(id1, id2);
    CHECK(egraph.has_class(id1));
    CHECK(egraph.has_class(id2));
  }

  SUBCASE("add nodes with children") {
    // Add leaf nodes first
    auto a = egraph.emplace("a", {});
    auto b = egraph.emplace("b", {});

    // Add node with children
    auto plus = egraph.emplace("plus", {a, b});

    CHECK_EQ(egraph.num_classes(), 3);
    CHECK_EQ(egraph.num_nodes(), 3);
    CHECK(egraph.has_class(plus));
  }

  SUBCASE("duplicate nodes return same e-class") {
    // Add same node twice
    auto id1 = egraph.emplace("a", {});
    auto id2 = egraph.emplace("a", {});

    CHECK_EQ(id1, id2);
    CHECK_EQ(egraph.num_classes(), 1);
    CHECK_EQ(egraph.num_nodes(), 1);
  }
}

TEST_CASE("EGraph merging operations") {
  using TestSymbol = test_symbols::TestSymbol;
  ese::core::EGraph<TestSymbol, void> egraph;
  ese::core::ProcessingContext<TestSymbol> ctx;

  SUBCASE("merge two different e-classes") {
    auto id1 = egraph.emplace("a", {});
    auto id2 = egraph.emplace("b", {});

    CHECK_EQ(egraph.num_classes(), 2);

    auto merged = egraph.merge(id1, id2, ctx);

    CHECK_EQ(egraph.num_classes(), 1);
    CHECK_EQ(egraph.find(id1, ctx), egraph.find(id2, ctx));
    CHECK((merged == id1 || merged == id2));
  }

  SUBCASE("merge same e-class is no-op") {
    auto id1 = egraph.emplace("a", {});
    auto merged = egraph.merge(id1, id1, ctx);

    CHECK_EQ(merged, id1);
    CHECK_EQ(egraph.num_classes(), 1);
  }

  SUBCASE("congruence after merge") {
    // Create: f(a), f(b), merge a and b
    auto a = egraph.emplace("a", {});
    auto b = egraph.emplace("b", {});
    auto fa = egraph.emplace("f", {a});
    auto fb = egraph.emplace("f", {b});

    CHECK_EQ(egraph.num_classes(), 4);
    CHECK_NE(egraph.find(fa, ctx), egraph.find(fb, ctx));

    // Merge a and b
    egraph.merge(a, b, ctx);

    // f(a) and f(b) should now be congruent
    CHECK_EQ(egraph.find(fa, ctx), egraph.find(fb, ctx));
    CHECK_EQ(egraph.num_classes(), 2);  // One for a=b, one for f(a)=f(b)
  }
}

TEST_CASE("EGraph e-class access") {
  using TestSymbol = test_symbols::TestSymbol;
  ese::core::EGraph<TestSymbol, void> egraph;

  SUBCASE("get e-class by ID") {
    auto id = egraph.emplace("test", {});

    const auto &eclass = egraph.eclass(id);
    CHECK_EQ(eclass.size(), 1);

    auto repr_id = eclass.representative_id();
    const auto &repr = egraph.get_enode(repr_id);
    CHECK_EQ(repr.symbol, TestSymbol("test"));
  }

  SUBCASE("get e-class with invalid ID throws") {
#ifndef NDEBUG
    CHECK_THROWS_AS(egraph.eclass(999), std::out_of_range);
#else
    // In release builds, range checking is disabled for performance
    // These operations would segfault, so we skip testing them
    MESSAGE("Range checking disabled in release builds");
    return;
#endif
  }
}

TEST_CASE("EGraph congruence detailed") {
  using TestSymbol = test_symbols::TestSymbol;
  ese::core::EGraph<TestSymbol, void> egraph;
  ese::core::ProcessingContext<TestSymbol> ctx;

  SUBCASE("congruence with parent tracking") {
    // This covers debug_congruence.cpp scenarios
    auto a = egraph.emplace("a", {});
    auto b = egraph.emplace("b", {});
    auto fa = egraph.emplace("f", {a});
    auto fb = egraph.emplace("f", {b});

    // Verify initial state
    CHECK_EQ(egraph.num_classes(), 4);
    CHECK_NE(egraph.find(fa, ctx), egraph.find(fb, ctx));

    // Check parent tracking before merge
    const auto &eclass_a = egraph.eclass(a);
    bool has_parent_fa = false;
    for (const auto &[parent_enode_id, parent_id] : eclass_a.parents) {
      if (parent_id == fa) has_parent_fa = true;
    }
    CHECK(has_parent_fa);

    // Merge and verify congruence
    egraph.merge(a, b, ctx);
    CHECK_EQ(egraph.find(fa, ctx), egraph.find(fb, ctx));
    CHECK_EQ(egraph.num_classes(), 2);
  }

  SUBCASE("rebuilding after merge") {
    // This covers debug_rebuilding.cpp scenarios
    auto a = egraph.emplace("a", {});
    auto b = egraph.emplace("b", {});
    auto fa = egraph.emplace("f", {a});
    auto fb = egraph.emplace("f", {b});

    // Merge triggers rebuild internally
    egraph.merge(a, b, ctx);

    // Verify congruence is maintained
    CHECK_EQ(egraph.find(fa, ctx), egraph.find(fb, ctx));
  }

  SUBCASE("enode counting accuracy") {
    // This covers debug_enode_count.cpp scenarios
    CHECK_EQ(egraph.num_enodes(), 0);

    auto a = egraph.emplace("a", {});
    CHECK_EQ(egraph.num_enodes(), 1);

    auto b = egraph.emplace("b", {});
    CHECK_EQ(egraph.num_enodes(), 2);

    auto c = egraph.emplace("c", {});
    auto d = egraph.emplace("d", {});
    CHECK_EQ(egraph.num_enodes(), 4);

    auto f = egraph.emplace("f", {c, d});
    CHECK_EQ(egraph.num_enodes(), 5);

    // Adding duplicate should not increase count
    auto f2 = egraph.emplace("f", {c, d});
    CHECK_EQ(f, f2);
    CHECK_EQ(egraph.num_enodes(), 5);
  }
}

TEST_CASE("EGraph clear and reserve") {
  using TestSymbol = test_symbols::TestSymbol;
  ese::core::EGraph<TestSymbol, void> egraph;

  SUBCASE("clear empties the graph") {
    egraph.emplace("a", {});
    egraph.emplace("b", {});

    CHECK_FALSE(egraph.empty());

    egraph.clear();

    CHECK(egraph.empty());
    CHECK_EQ(egraph.num_classes(), 0);
    CHECK_EQ(egraph.num_nodes(), 0);
  }

  SUBCASE("reserve allocates capacity") {
    egraph.reserve(1000);

    // Add many nodes - should not trigger reallocations
    for (int i = 0; i < 100; ++i) {
      egraph.emplace("node" + std::to_string(i), {});
    }

    CHECK_EQ(egraph.num_classes(), 100);
  }
}

TEST_CASE("EGraph string representation") {
  using TestSymbol = test_symbols::TestSymbol;
  ese::core::EGraph<TestSymbol, void> egraph;

  SUBCASE("empty graph string") {
    auto str = egraph.to_string();
    CHECK_NE(str.find("0 classes"), std::string::npos);
    CHECK_NE(str.find("0 nodes"), std::string::npos);
  }

  SUBCASE("non-empty graph string") {
    egraph.emplace("test", {});

    auto str = egraph.to_string();
    CHECK_NE(str.find("1 classes"), std::string::npos);
    CHECK_NE(str.find("1 nodes"), std::string::npos);
    CHECK_NE(str.find("test"), std::string::npos);
  }
}

TEST_CASE("EGraph complex operations") {
  using TestSymbol = test_symbols::TestSymbol;
  ese::core::EGraph<TestSymbol, void> egraph;
  ese::core::ProcessingContext<TestSymbol> ctx;

  SUBCASE("build arithmetic expression tree") {
    // Build: (x + y) * (x + y)
    auto x = egraph.emplace("x", {});
    auto y = egraph.emplace("y", {});
    auto plus1 = egraph.emplace("+", {x, y});
    auto plus2 = egraph.emplace("+", {x, y});
    auto mult = egraph.emplace("*", {plus1, plus2});

    CHECK_EQ(egraph.num_classes(), 4);                           // x, y, +(x,y), *(+(x,y), +(x,y))
    CHECK_EQ(egraph.find(plus1, ctx), egraph.find(plus2, ctx));  // Congruent +

    // Verify structure
    const auto &mult_class = egraph.eclass(mult);
    auto mult_node_id = mult_class.representative_id();
    const auto &mult_node = egraph.get_enode(mult_node_id);
    CHECK_EQ(mult_node.symbol, TestSymbol("*"));
    CHECK_EQ(mult_node.arity(), 2);

    // Both children should point to the same e-class
    CHECK_EQ(egraph.find(mult_node.children[0], ctx), egraph.find(mult_node.children[1], ctx));
  }

  SUBCASE("associativity merge") {
    // Build: (a + b) + c and a + (b + c)
    auto a = egraph.emplace("a", {});
    auto b = egraph.emplace("b", {});
    auto c = egraph.emplace("c", {});

    auto ab = egraph.emplace("+", {a, b});
    auto abc1 = egraph.emplace("+", {ab, c});

    auto bc = egraph.emplace("+", {b, c});
    auto abc2 = egraph.emplace("+", {a, bc});

    CHECK_NE(egraph.find(abc1, ctx), egraph.find(abc2, ctx));

    // Merge them to represent associativity
    egraph.merge(abc1, abc2, ctx);

    CHECK_EQ(egraph.find(abc1, ctx), egraph.find(abc2, ctx));
  }

  SUBCASE("next_class_id is monotonic") {
    // next_class_id should never decrease, even after merges reduce num_classes

    // Initially empty
    auto initial_next = egraph.next_class_id();
    CHECK_EQ(initial_next, ese::core::EClassId(0));  // 0 is the first ID that will be assigned

    // Add first class
    auto id1 = egraph.emplace("a", {});
    auto next1 = egraph.next_class_id();
    CHECK_EQ(next1, ese::core::EClassId(1));  // Next ID after 0
    CHECK_EQ(egraph.num_classes(), 1);

    // Add second class
    auto id2 = egraph.emplace("b", {});
    auto next2 = egraph.next_class_id();
    CHECK_EQ(next2, ese::core::EClassId(2));  // Next ID after 0,1
    CHECK_GT(next2, next1);                   // Monotonic increase
    CHECK_EQ(egraph.num_classes(), 2);

    // Add third class
    auto id3 = egraph.emplace("c", {});
    auto next3 = egraph.next_class_id();
    CHECK_EQ(next3, ese::core::EClassId(3));  // Next ID after 0,1,2
    CHECK_GT(next3, next2);                   // Monotonic increase
    CHECK_EQ(egraph.num_classes(), 3);

    // Merge classes - this reduces num_classes but next_class_id should NOT decrease
    egraph.merge(id1, id2, ctx);
    auto next_after_merge1 = egraph.next_class_id();
    CHECK_EQ(next_after_merge1, next3);  // Should still be 3, not decreased!
    CHECK_EQ(egraph.num_classes(), 2);   // Only 2 classes now

    // Another merge
    egraph.merge(id2, id3, ctx);
    auto next_after_merge2 = egraph.next_class_id();
    CHECK_EQ(next_after_merge2, next3);  // Should still be 3, not decreased!
    CHECK_EQ(egraph.num_classes(), 1);   // Only 1 class now

    // Add a new class after merges
    auto id4 = egraph.emplace("d", {});
    auto next4 = egraph.next_class_id();
    CHECK_EQ(next4, ese::core::EClassId(4));  // Next ID after 0,1,2,3
    CHECK_GT(next4, next_after_merge2);       // Monotonic increase from previous next
    CHECK_EQ(egraph.num_classes(), 2);        // 2 classes again

    // Verify monotonic property held throughout
    CHECK_LE(initial_next, next1);
    CHECK_LE(next1, next2);
    CHECK_LE(next2, next3);
    CHECK_LE(next3, next_after_merge1);
    CHECK_LE(next_after_merge1, next_after_merge2);
    CHECK_LE(next_after_merge2, next4);
  }
}
