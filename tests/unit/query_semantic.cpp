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

#include <memory>
#include <sstream>
#include <variant>

#include "disk_test_utils.hpp"
#include "gtest/gtest.h"

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/preprocess.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"

#include "query_common.hpp"

using namespace memgraph::query;

template <typename StorageType>
class TestSymbolGenerator : public ::testing::Test {
 protected:
  const std::string testSuite = "query_semantic";
  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> db{new StorageType(config)};
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba{db->Access(memgraph::storage::WRITE)};
  memgraph::query::DbAccessor dba{storage_dba.get()};
  AstStorage storage;

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_SUITE(TestSymbolGenerator, StorageTypes);

TYPED_TEST(TestSymbolGenerator, MatchNodeReturn) {
  // MATCH (node_atom_1) RETURN node_atom_1
  auto query_ast = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("node_atom_1"))), RETURN("node_atom_1")));
  auto symbol_table = memgraph::query::MakeSymbolTable(query_ast);
  // symbols for pattern, node_atom_1 and named_expr in return
  EXPECT_EQ(symbol_table.max_position(), 3);
  auto *match = dynamic_cast<Match *>(query_ast->single_query_->clauses_[0]);
  auto *pattern = match->patterns_[0];
  auto pattern_sym = symbol_table.at(*pattern->identifier_);
  EXPECT_EQ(pattern_sym.type(), Symbol::Type::PATH);
  EXPECT_FALSE(pattern_sym.user_declared());
  auto *node_atom = dynamic_cast<NodeAtom *>(pattern->atoms_[0]);
  auto node_sym = symbol_table.at(*node_atom->identifier_);
  EXPECT_EQ(node_sym.name(), "node_atom_1");
  EXPECT_EQ(node_sym.type(), Symbol::Type::VERTEX);
  auto *ret = dynamic_cast<Return *>(query_ast->single_query_->clauses_[1]);
  auto *named_expr = ret->body_.named_expressions[0];
  auto column_sym = symbol_table.at(*named_expr);
  EXPECT_EQ(node_sym.name(), column_sym.name());
  EXPECT_NE(node_sym, column_sym);
  auto ret_sym = symbol_table.at(*dynamic_cast<Identifier *>(named_expr->expression_));
  EXPECT_EQ(node_sym, ret_sym);
}

TYPED_TEST(TestSymbolGenerator, MatchNamedPattern) {
  // MATCH p = (node_atom_1) RETURN node_atom_1
  auto query_ast = QUERY(SINGLE_QUERY(MATCH(NAMED_PATTERN("p", NODE("node_atom_1"))), RETURN("p")));
  auto symbol_table = memgraph::query::MakeSymbolTable(query_ast);
  // symbols for p, node_atom_1 and named_expr in return
  EXPECT_EQ(symbol_table.max_position(), 3);
  auto *match = dynamic_cast<Match *>(query_ast->single_query_->clauses_[0]);
  auto *pattern = match->patterns_[0];
  auto pattern_sym = symbol_table.at(*pattern->identifier_);
  EXPECT_EQ(pattern_sym.type(), Symbol::Type::PATH);
  EXPECT_EQ(pattern_sym.name(), "p");
  EXPECT_TRUE(pattern_sym.user_declared());
}

TYPED_TEST(TestSymbolGenerator, MatchUnboundMultiReturn) {
  // AST using variable in return bound by naming the previous return
  // expression. This is treated as an unbound variable.
  // MATCH (node_atom_1) RETURN node_atom_1 AS n, n
  auto query_ast = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("node_atom_1"))), RETURN("node_atom_1", AS("n"), "n")));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query_ast), UnboundVariableError);
}

TYPED_TEST(TestSymbolGenerator, MatchNodeUnboundReturn) {
  // AST with unbound variable in return: MATCH (n) RETURN x
  auto query_ast = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), RETURN("x")));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query_ast), UnboundVariableError);
}

TYPED_TEST(TestSymbolGenerator, CreatePropertyUnbound) {
  // AST with unbound variable in create: CREATE ({prop: x})
  auto node = NODE("anon");
  std::get<0>(node->properties_)[this->storage.GetPropertyIx("prop")] = IDENT("x");
  auto *query_ast = QUERY(SINGLE_QUERY(CREATE(PATTERN(node))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query_ast), UnboundVariableError);
}

TYPED_TEST(TestSymbolGenerator, CreateNodeReturn) {
  // Simple AST returning a created node: CREATE (n) RETURN n
  auto query_ast = QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n"))), RETURN("n")));
  auto symbol_table = memgraph::query::MakeSymbolTable(query_ast);
  // symbols for pattern, `n` and named_expr
  EXPECT_EQ(symbol_table.max_position(), 3);
  auto *create = dynamic_cast<Create *>(query_ast->single_query_->clauses_[0]);
  auto *pattern = create->patterns_[0];
  auto *node_atom = dynamic_cast<NodeAtom *>(pattern->atoms_[0]);
  auto node_sym = symbol_table.at(*node_atom->identifier_);
  EXPECT_EQ(node_sym.name(), "n");
  EXPECT_EQ(node_sym.type(), Symbol::Type::VERTEX);
  auto *ret = dynamic_cast<Return *>(query_ast->single_query_->clauses_[1]);
  auto *named_expr = ret->body_.named_expressions[0];
  auto column_sym = symbol_table.at(*named_expr);
  EXPECT_EQ(node_sym.name(), column_sym.name());
  EXPECT_NE(node_sym, column_sym);
  auto ret_sym = symbol_table.at(*dynamic_cast<Identifier *>(named_expr->expression_));
  EXPECT_EQ(node_sym, ret_sym);
}

TYPED_TEST(TestSymbolGenerator, CreateRedeclareNode) {
  // AST with redeclaring a variable when creating nodes: CREATE (n), (n)
  auto query_ast = QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n")), PATTERN(NODE("n")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query_ast), RedeclareVariableError);
}

TYPED_TEST(TestSymbolGenerator, MultiCreateRedeclareNode) {
  // AST with redeclaring a variable when creating nodes with multiple creates:
  // CREATE (n) CREATE (n)
  auto query_ast = QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n"))), CREATE(PATTERN(NODE("n")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query_ast), RedeclareVariableError);
}

TYPED_TEST(TestSymbolGenerator, MatchCreateRedeclareNode) {
  // AST with redeclaring a match node variable in create: MATCH (n) CREATE (n)
  auto query_ast = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), CREATE(PATTERN(NODE("n")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query_ast), RedeclareVariableError);
}

TYPED_TEST(TestSymbolGenerator, MatchCreateRedeclareEdge) {
  // AST with redeclaring a match edge variable in create:
  // MATCH (n) -[r]- (m) CREATE (n) -[r :relationship]-> (l)
  const auto *relationship = "relationship";
  auto query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
                         CREATE(PATTERN(NODE("n"), EDGE("r", EdgeAtom::Direction::OUT, {relationship}), NODE("l")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), RedeclareVariableError);
}

TYPED_TEST(TestSymbolGenerator, MatchTypeMismatch) {
  // Using an edge variable as a node causes a type mismatch.
  // MATCH (n) -[r]-> (r)
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("r")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), TypeMismatchError);
}

TYPED_TEST(TestSymbolGenerator, MatchCreateTypeMismatch) {
  // Using an edge variable as a node causes a type mismatch.
  // MATCH (n1) -[r1]- (n2) CREATE (r1) -[r2]-> (n2)
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n1"), EDGE("r1"), NODE("n2"))),
                                  CREATE(PATTERN(NODE("r1"), EDGE("r2", EdgeAtom::Direction::OUT), NODE("n2")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), TypeMismatchError);
}

TYPED_TEST(TestSymbolGenerator, CreateMultipleEdgeType) {
  // Multiple edge relationship are not allowed when creating edges.
  // CREATE (n) -[r :rel1 | :rel2]-> (m)
  const auto *rel1 = "rel1";
  const auto *rel2 = "rel2";
  auto edge = EDGE("r", EdgeAtom::Direction::OUT, {rel1});
  edge->edge_types_.emplace_back(this->storage.GetEdgeTypeIx(rel2));
  auto query = QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n"), edge, NODE("m")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, CreateBidirectionalEdge) {
  // Bidirectional relationships are not allowed when creating edges.
  // CREATE (n) -[r :rel1]- (m)
  const auto *rel1 = "rel1";
  auto query = QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n"), EDGE("r", EdgeAtom::Direction::BOTH, {rel1}), NODE("m")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, MatchWhereUnbound) {
  // Test MATCH (n) WHERE missing < 42 RETURN n
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), WHERE(LESS(IDENT("missing"), LITERAL(42))), RETURN("n")));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), UnboundVariableError);
}

TYPED_TEST(TestSymbolGenerator, CreateDelete) {
  // Test CREATE (n) DELETE n
  auto node = NODE("n");
  auto ident = IDENT("n");
  auto query = QUERY(SINGLE_QUERY(CREATE(PATTERN(node)), DELETE(ident)));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // symbols for pattern and `n`
  EXPECT_EQ(symbol_table.max_position(), 2);
  auto node_symbol = symbol_table.at(*node->identifier_);
  auto ident_symbol = symbol_table.at(*ident);
  EXPECT_EQ(node_symbol.type(), Symbol::Type::VERTEX);
  EXPECT_EQ(node_symbol, ident_symbol);
}

TYPED_TEST(TestSymbolGenerator, CreateDeleteUnbound) {
  // Test CREATE (n) DELETE missing
  auto query = QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n"))), DELETE(IDENT("missing"))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), UnboundVariableError);
}

TYPED_TEST(TestSymbolGenerator, MatchWithReturn) {
  // Test MATCH (old) WITH old AS n RETURN n AS n
  auto node = NODE("old");
  auto old_ident = IDENT("old");
  auto with_as_n = AS("n");
  auto n_ident = IDENT("n");
  auto ret_as_n = AS("n");
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node)), WITH(old_ident, with_as_n), RETURN(n_ident, ret_as_n)));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // symbols for pattern, `old`, `n` and named_expr in return
  EXPECT_EQ(symbol_table.max_position(), 4);
  auto node_symbol = symbol_table.at(*node->identifier_);
  auto old = symbol_table.at(*old_ident);
  EXPECT_EQ(node_symbol, old);
  auto with_n = symbol_table.at(*with_as_n);
  EXPECT_NE(old, with_n);
  auto n = symbol_table.at(*n_ident);
  EXPECT_EQ(n, with_n);
  auto ret_n = symbol_table.at(*ret_as_n);
  EXPECT_NE(n, ret_n);
}

TYPED_TEST(TestSymbolGenerator, MatchWithReturnUnbound) {
  // Test MATCH (old) WITH old AS n RETURN old
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("old"))), WITH("old", AS("n")), RETURN("old")));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), UnboundVariableError);
}

TYPED_TEST(TestSymbolGenerator, MatchWithWhere) {
  // Test MATCH (old) WITH old AS n WHERE n.prop < 42
  auto prop = this->dba.NameToProperty("prop");
  auto node = NODE("old");
  auto old_ident = IDENT("old");
  auto with_as_n = AS("n");
  auto n_prop = PROPERTY_LOOKUP(this->dba, "n", prop);
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node)), WITH(old_ident, with_as_n), WHERE(LESS(n_prop, LITERAL(42)))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // symbols for pattern, `old` and `n`
  EXPECT_EQ(symbol_table.max_position(), 3);
  auto node_symbol = symbol_table.at(*node->identifier_);
  auto old = symbol_table.at(*old_ident);
  EXPECT_EQ(node_symbol, old);
  auto with_n = symbol_table.at(*with_as_n);
  EXPECT_NE(old, with_n);
  auto n = symbol_table.at(*dynamic_cast<Identifier *>(n_prop->expression_));
  EXPECT_EQ(n, with_n);
}

TYPED_TEST(TestSymbolGenerator, MatchWithWhereUnbound) {
  // Test MATCH (old) WITH COUNT(old) AS c WHERE old.prop < 42
  auto prop = this->dba.NameToProperty("prop");
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("old"))),
                                  WITH(COUNT(IDENT("old"), false), AS("c")),
                                  WHERE(LESS(PROPERTY_LOOKUP(this->dba, "old", prop), LITERAL(42)))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), UnboundVariableError);
}

TYPED_TEST(TestSymbolGenerator, CreateMultiExpand) {
  // Test CREATE (n) -[r :r]-> (m), (n) - [p :p]-> (l)
  const auto *r_type = "r";
  const auto *p_type = "p";
  auto node_n1 = NODE("n");
  auto edge_r = EDGE("r", EdgeAtom::Direction::OUT, {r_type});
  auto node_m = NODE("m");
  auto node_n2 = NODE("n");
  auto edge_p = EDGE("p", EdgeAtom::Direction::OUT, {p_type});
  auto node_l = NODE("l");
  auto query = QUERY(SINGLE_QUERY(CREATE(PATTERN(node_n1, edge_r, node_m), PATTERN(node_n2, edge_p, node_l))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // symbols for pattern * 2, `n`, `r`, `m`, `p`, `l`
  EXPECT_EQ(symbol_table.max_position(), 7);
  auto n1 = symbol_table.at(*node_n1->identifier_);
  auto n2 = symbol_table.at(*node_n2->identifier_);
  EXPECT_EQ(n1, n2);
  EXPECT_EQ(n1.type(), Symbol::Type::VERTEX);
  auto m = symbol_table.at(*node_m->identifier_);
  EXPECT_EQ(m.type(), Symbol::Type::VERTEX);
  EXPECT_NE(m, n1);
  auto l = symbol_table.at(*node_l->identifier_);
  EXPECT_EQ(l.type(), Symbol::Type::VERTEX);
  EXPECT_NE(l, n1);
  EXPECT_NE(l, m);
  auto r = symbol_table.at(*edge_r->identifier_);
  auto p = symbol_table.at(*edge_p->identifier_);
  EXPECT_EQ(r.type(), Symbol::Type::EDGE);
  EXPECT_EQ(p.type(), Symbol::Type::EDGE);
  EXPECT_NE(r, p);
}

TYPED_TEST(TestSymbolGenerator, MatchCreateExpandLabel) {
  // Test MATCH (n) CREATE (m) -[r :r]-> (n:label)
  const auto *r_type = "r";
  const auto *label = "label";
  auto query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                         CREATE(PATTERN(NODE("m"), EDGE("r", EdgeAtom::Direction::OUT, {r_type}), NODE("n", label)))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, CreateExpandProperty) {
  // Test CREATE (n) -[r :r]-> (n {prop: 42})
  const auto *r_type = "r";
  auto n_prop = NODE("n");
  std::get<0>(n_prop->properties_)[this->storage.GetPropertyIx("prop")] = LITERAL(42);
  auto query = QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n"), EDGE("r", EdgeAtom::Direction::OUT, {r_type}), n_prop))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, MatchReturnSum) {
  // Test MATCH (n) RETURN SUM(n.prop) + 42 AS result
  auto prop = this->dba.NameToProperty("prop");
  auto node = NODE("n");
  auto sum = SUM(PROPERTY_LOOKUP(this->dba, "n", prop), false);
  auto as_result = AS("result");
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node)), RETURN(ADD(sum, LITERAL(42)), as_result)));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // 3 symbols for: pattern, 'n', 'sum' and 'result'.
  EXPECT_EQ(symbol_table.max_position(), 4);
  auto node_symbol = symbol_table.at(*node->identifier_);
  auto sum_symbol = symbol_table.at(*sum);
  EXPECT_NE(node_symbol, sum_symbol);
  auto result_symbol = symbol_table.at(*as_result);
  EXPECT_NE(result_symbol, node_symbol);
  EXPECT_NE(result_symbol, sum_symbol);
}

TYPED_TEST(TestSymbolGenerator, NestedAggregation) {
  // Test MATCH (n) RETURN SUM(42 + SUM(n.prop)) AS s
  auto prop = this->dba.NameToProperty("prop");
  auto query = QUERY(
      SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                   RETURN(SUM(ADD(LITERAL(42), SUM(PROPERTY_LOOKUP(this->dba, "n", prop), false)), false), AS("s"))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, WrongAggregationContext) {
  // Test MATCH (n) WITH n.prop AS prop WHERE SUM(prop) < 42
  auto prop = this->dba.NameToProperty("prop");
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                                  WITH(PROPERTY_LOOKUP(this->dba, "n", prop), AS("prop")),
                                  WHERE(LESS(SUM(IDENT("prop"), false), LITERAL(42)))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, MatchPropCreateNodeProp) {
  // Test MATCH (n) CREATE (m {prop: n.prop})
  auto prop = PROPERTY_PAIR(this->dba, "prop");
  auto node_n = NODE("n");
  auto node_m = NODE("m");
  auto n_prop = PROPERTY_LOOKUP(this->dba, "n", prop.second);
  std::get<0>(node_m->properties_)[this->storage.GetPropertyIx(prop.first)] = n_prop;
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node_n)), CREATE(PATTERN(node_m))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // symbols: pattern * 2, `node_n`, `node_m`
  EXPECT_EQ(symbol_table.max_position(), 4);
  auto n = symbol_table.at(*node_n->identifier_);
  EXPECT_EQ(n, symbol_table.at(*dynamic_cast<Identifier *>(n_prop->expression_)));
  auto m = symbol_table.at(*node_m->identifier_);
  EXPECT_NE(n, m);
}

TYPED_TEST(TestSymbolGenerator, CreateNodeEdge) {
  // Test CREATE (n), (n) -[r :r]-> (n)
  const auto *r_type = "r";
  auto node_1 = NODE("n");
  auto node_2 = NODE("n");
  auto edge = EDGE("r", EdgeAtom::Direction::OUT, {r_type});
  auto node_3 = NODE("n");
  auto query = QUERY(SINGLE_QUERY(CREATE(PATTERN(node_1), PATTERN(node_2, edge, node_3))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // symbols: pattern * 2, `n`, `r`
  EXPECT_EQ(symbol_table.max_position(), 4);
  auto n = symbol_table.at(*node_1->identifier_);
  EXPECT_EQ(n, symbol_table.at(*node_2->identifier_));
  EXPECT_EQ(n, symbol_table.at(*node_3->identifier_));
  EXPECT_NE(n, symbol_table.at(*edge->identifier_));
}

TYPED_TEST(TestSymbolGenerator, MatchWithCreate) {
  // Test MATCH (n) WITH n AS m CREATE (m) -[r :r]-> (m)
  const auto *r_type = "r";
  auto node_1 = NODE("n");
  auto node_2 = NODE("m");
  auto edge = EDGE("r", EdgeAtom::Direction::OUT, {r_type});
  auto node_3 = NODE("m");
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node_1)), WITH("n", AS("m")), CREATE(PATTERN(node_2, edge, node_3))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // symbols: pattern * 2, `n`, `m`, `r`
  EXPECT_EQ(symbol_table.max_position(), 5);
  auto n = symbol_table.at(*node_1->identifier_);
  EXPECT_EQ(n.type(), Symbol::Type::VERTEX);
  auto m = symbol_table.at(*node_2->identifier_);
  EXPECT_NE(n, m);
  // Currently we don't infer expression types, so we lost true type of 'm'.
  EXPECT_EQ(m.type(), Symbol::Type::ANY);
  EXPECT_EQ(m, symbol_table.at(*node_3->identifier_));
}

TYPED_TEST(TestSymbolGenerator, SameResultsWith) {
  // Test MATCH (n) WITH n AS m, n AS m
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), WITH("n", AS("m"), "n", AS("m"))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, SameResults) {
  // Test MATCH (n) RETURN n, n
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), RETURN("n", "n")));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, SkipUsingIdentifier) {
  // Test MATCH (old) WITH old AS new SKIP old
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("old"))), WITH("old", AS("new"), SKIP(IDENT("old")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, SkipUsingIdentifierAlias) {
  // Test MATCH (old) WITH old AS new SKIP new
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("old"))), WITH("old", AS("new"), SKIP(IDENT("new")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, LimitUsingIdentifier) {
  // Test MATCH (n) RETURN n AS n LIMIT n
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), RETURN("n", LIMIT(IDENT("n")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, OrderByAggregation) {
  // Test MATCH (old) RETURN old AS new ORDER BY COUNT(1)
  auto query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("old"))), RETURN("old", AS("new"), ORDER_BY(COUNT(LITERAL(1), false)))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, OrderByUnboundVariable) {
  // Test MATCH (old) RETURN COUNT(old) AS new ORDER BY old
  auto query = QUERY(
      SINGLE_QUERY(MATCH(PATTERN(NODE("old"))), RETURN(COUNT(IDENT("old"), false), AS("new"), ORDER_BY(IDENT("old")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), UnboundVariableError);
}

TYPED_TEST(TestSymbolGenerator, AggregationOrderBy) {
  // Test MATCH (old) RETURN COUNT(old) AS new ORDER BY new
  auto node = NODE("old");
  auto ident_old = IDENT("old");
  auto as_new = AS("new");
  auto ident_new = IDENT("new");
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node)), RETURN(COUNT(ident_old, false), as_new, ORDER_BY(ident_new))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // Symbols for pattern, `old`, `count(old)` and `new`
  EXPECT_EQ(symbol_table.max_position(), 4);
  auto old = symbol_table.at(*node->identifier_);
  EXPECT_EQ(old, symbol_table.at(*ident_old));
  auto new_sym = symbol_table.at(*as_new);
  EXPECT_NE(old, new_sym);
  EXPECT_EQ(new_sym, symbol_table.at(*ident_new));
}

TYPED_TEST(TestSymbolGenerator, OrderByOldVariable) {
  // Test MATCH (old) RETURN old AS new ORDER BY old
  auto node = NODE("old");
  auto ident_old = IDENT("old");
  auto as_new = AS("new");
  auto by_old = IDENT("old");
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node)), RETURN(ident_old, as_new, ORDER_BY(by_old))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // Symbols for pattern, `old` and `new`
  EXPECT_EQ(symbol_table.max_position(), 3);
  auto old = symbol_table.at(*node->identifier_);
  EXPECT_EQ(old, symbol_table.at(*ident_old));
  EXPECT_EQ(old, symbol_table.at(*by_old));
  auto new_sym = symbol_table.at(*as_new);
  EXPECT_NE(old, new_sym);
}

TYPED_TEST(TestSymbolGenerator, MergeVariableError) {
  // Test MATCH (n) MERGE (n)
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), MERGE(PATTERN(NODE("n")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), RedeclareVariableError);
}

TYPED_TEST(TestSymbolGenerator, MergeVariableErrorEdge) {
  // Test MATCH (n) -[r]- (m) MERGE (a) -[r :rel]- (b)
  const auto *rel = "rel";
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
                                  MERGE(PATTERN(NODE("a"), EDGE("r", EdgeAtom::Direction::BOTH, {rel}), NODE("b")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), RedeclareVariableError);
}

TYPED_TEST(TestSymbolGenerator, MergeEdgeWithoutType) {
  // Test MERGE (a) -[r]- (b)
  auto query = QUERY(SINGLE_QUERY(MERGE(PATTERN(NODE("a"), EDGE("r"), NODE("b")))));
  // Edge must have a type, since it doesn't we raise.
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, MergeOnMatchOnCreate) {
  // Test MATCH (n) MERGE (n) -[r :rel]- (m) ON MATCH SET n.prop = 42
  //      ON CREATE SET m.prop = 42 RETURN r AS r
  const auto *rel = "rel";
  auto prop = this->dba.NameToProperty("prop");
  auto match_n = NODE("n");
  auto merge_n = NODE("n");
  auto edge_r = EDGE("r", EdgeAtom::Direction::BOTH, {rel});
  auto node_m = NODE("m");
  auto n_prop = PROPERTY_LOOKUP(this->dba, "n", prop);
  auto m_prop = PROPERTY_LOOKUP(this->dba, "m", prop);
  auto ident_r = IDENT("r");
  auto as_r = AS("r");
  auto query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(match_n)),
      MERGE(PATTERN(merge_n, edge_r, node_m), ON_MATCH(SET(n_prop, LITERAL(42))), ON_CREATE(SET(m_prop, LITERAL(42)))),
      RETURN(ident_r, as_r)));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // Symbols for: pattern * 2, `n`, `r`, `m` and `AS r`.
  EXPECT_EQ(symbol_table.max_position(), 6);
  auto n = symbol_table.at(*match_n->identifier_);
  EXPECT_EQ(n, symbol_table.at(*merge_n->identifier_));
  EXPECT_EQ(n, symbol_table.at(*dynamic_cast<Identifier *>(n_prop->expression_)));
  auto r = symbol_table.at(*edge_r->identifier_);
  EXPECT_NE(r, n);
  EXPECT_EQ(r, symbol_table.at(*ident_r));
  EXPECT_NE(r, symbol_table.at(*as_r));
  auto m = symbol_table.at(*node_m->identifier_);
  EXPECT_NE(m, n);
  EXPECT_NE(m, r);
  EXPECT_NE(m, symbol_table.at(*as_r));
  EXPECT_EQ(m, symbol_table.at(*dynamic_cast<Identifier *>(m_prop->expression_)));
}

TYPED_TEST(TestSymbolGenerator, WithUnwindRedeclareReturn) {
  // Test WITH [1, 2] AS list UNWIND list AS list RETURN list
  auto query = QUERY(
      SINGLE_QUERY(WITH(LIST(LITERAL(1), LITERAL(2)), AS("list")), UNWIND(IDENT("list"), AS("list")), RETURN("list")));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), RedeclareVariableError);
}

TYPED_TEST(TestSymbolGenerator, WithUnwindReturn) {
  // WITH [1, 2] AS list UNWIND list AS elem RETURN list AS list, elem AS elem
  auto with_as_list = AS("list");
  auto unwind = UNWIND(IDENT("list"), AS("elem"));
  auto ret_list = IDENT("list");
  auto ret_as_list = AS("list");
  auto ret_elem = IDENT("elem");
  auto ret_as_elem = AS("elem");
  auto query = QUERY(SINGLE_QUERY(
      WITH(LIST(LITERAL(1), LITERAL(2)), with_as_list), unwind, RETURN(ret_list, ret_as_list, ret_elem, ret_as_elem)));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // Symbols for: `list`, `elem`, `AS list`, `AS elem`
  EXPECT_EQ(symbol_table.max_position(), 4);
  const auto &list = symbol_table.at(*with_as_list);
  EXPECT_EQ(list, symbol_table.at(*dynamic_cast<Identifier *>(unwind->named_expression_->expression_)));
  const auto &elem = symbol_table.at(*unwind->named_expression_);
  EXPECT_NE(list, elem);
  EXPECT_EQ(list, symbol_table.at(*ret_list));
  EXPECT_NE(list, symbol_table.at(*ret_as_list));
  EXPECT_EQ(elem, symbol_table.at(*ret_elem));
  EXPECT_NE(elem, symbol_table.at(*ret_as_elem));
}

TYPED_TEST(TestSymbolGenerator, MatchCrossReferenceVariable) {
  // MATCH (n {prop: m.prop}), (m {prop: n.prop}) RETURN n
  auto prop = PROPERTY_PAIR(this->dba, "prop");
  auto node_n = NODE("n");
  auto m_prop = PROPERTY_LOOKUP(this->dba, "m", prop.second);
  std::get<0>(node_n->properties_)[this->storage.GetPropertyIx(prop.first)] = m_prop;
  auto node_m = NODE("m");
  auto n_prop = PROPERTY_LOOKUP(this->dba, "n", prop.second);
  std::get<0>(node_m->properties_)[this->storage.GetPropertyIx(prop.first)] = n_prop;
  auto ident_n = IDENT("n");
  auto as_n = AS("n");
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node_n), PATTERN(node_m)), RETURN(ident_n, as_n)));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // Symbols for pattern * 2, `n`, `m` and `AS n`
  EXPECT_EQ(symbol_table.max_position(), 5);
  auto n = symbol_table.at(*node_n->identifier_);
  EXPECT_EQ(n, symbol_table.at(*dynamic_cast<Identifier *>(n_prop->expression_)));
  EXPECT_EQ(n, symbol_table.at(*ident_n));
  EXPECT_NE(n, symbol_table.at(*as_n));
  auto m = symbol_table.at(*node_m->identifier_);
  EXPECT_EQ(m, symbol_table.at(*dynamic_cast<Identifier *>(m_prop->expression_)));
  EXPECT_NE(n, m);
  EXPECT_NE(m, symbol_table.at(*as_n));
}

TYPED_TEST(TestSymbolGenerator, MatchWithAsteriskReturnAsterisk) {
  // MATCH (n) -[e]- (m) WITH * RETURN *, n.prop
  auto prop = this->dba.NameToProperty("prop");
  auto n_prop = PROPERTY_LOOKUP(this->dba, "n", prop);
  auto ret = RETURN(n_prop, AS("n.prop"));
  ret->body_.all_identifiers = true;
  auto node_n = NODE("n");
  auto edge = EDGE("e");
  auto node_m = NODE("m");
  auto with = this->storage.template Create<With>();
  with->body_.all_identifiers = true;
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node_n, edge, node_m)), with, ret));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // Symbols for pattern, `n`, `e`, `m`, `AS n.prop`.
  EXPECT_EQ(symbol_table.max_position(), 5);
  auto n = symbol_table.at(*node_n->identifier_);
  EXPECT_EQ(n, symbol_table.at(*dynamic_cast<Identifier *>(n_prop->expression_)));
}

TYPED_TEST(TestSymbolGenerator, MatchReturnAsteriskSameResult) {
  // MATCH (n) RETURN *, n
  auto ret = RETURN("n");
  ret->body_.all_identifiers = true;
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), ret));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, MatchReturnAsteriskNoUserVariables) {
  // MATCH () RETURN *
  auto ret = this->storage.template Create<Return>();
  ret->body_.all_identifiers = true;
  auto ident_n = this->storage.template Create<Identifier>("anon", false);
  auto node = this->storage.template Create<NodeAtom>(ident_n);
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node)), ret));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, MatchReturnAsteriskNoUserVariablesInsideSubquery) {
  // MATCH (n) CALL { RETURN * } RETURN n
  auto subquery_ret = this->storage.template Create<Return>();
  subquery_ret->body_.all_identifiers = true;

  auto subquery = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), subquery_ret));
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), CALL_SUBQUERY(subquery), RETURN("n")));
  EXPECT_THROW(MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, WithAsteriskAllowedAtTopLevel) {
  // WITH * RETURN 42
  auto with = this->storage.template Create<With>();
  with->body_.all_identifiers = true;

  auto query = QUERY(SINGLE_QUERY(with, RETURN(LITERAL(42), AS("res"))));
  EXPECT_NO_THROW(memgraph::query::MakeSymbolTable(query));
}

TYPED_TEST(TestSymbolGenerator, MatchMergeExpandLabel) {
  // Test MATCH (n) MERGE (m) -[r :r]-> (n:label)
  const auto *r_type = "r";
  const auto *label = "label";
  auto query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                         MERGE(PATTERN(NODE("m"), EDGE("r", EdgeAtom::Direction::OUT, {r_type}), NODE("n", label)))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, MatchEdgeWithIdentifierInProperty) {
  // Test MATCH (n) -[r {prop: n.prop}]- (m) RETURN r
  auto prop = PROPERTY_PAIR(this->dba, "prop");
  auto edge = EDGE("r");
  auto n_prop = PROPERTY_LOOKUP(this->dba, "n", prop.second);
  std::get<0>(edge->properties_)[this->storage.GetPropertyIx(prop.first)] = n_prop;
  auto node_n = NODE("n");
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node_n, edge, NODE("m"))), RETURN("r")));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // Symbols for pattern, `n`, `r`, `m` and implicit in RETURN `r AS r`
  EXPECT_EQ(symbol_table.max_position(), 5);
  auto n = symbol_table.at(*node_n->identifier_);
  EXPECT_EQ(n, symbol_table.at(*dynamic_cast<Identifier *>(n_prop->expression_)));
}

TYPED_TEST(TestSymbolGenerator, MatchVariablePathUsingIdentifier) {
  // Test MATCH (n) -[r *..l.prop]- (m), (l) RETURN r
  auto prop = this->dba.NameToProperty("prop");
  auto edge = EDGE_VARIABLE("r");
  auto l_prop = PROPERTY_LOOKUP(this->dba, "l", prop);
  edge->upper_bound_ = l_prop;
  auto node_l = NODE("l");
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m")), PATTERN(node_l)), RETURN("r")));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // Symbols for pattern * 2, `n`, `r`, inner_node, inner_edge, `m`, `l` and
  // implicit in RETURN `r AS r`
  EXPECT_EQ(symbol_table.max_position(), 9);
  auto l = symbol_table.at(*node_l->identifier_);
  EXPECT_EQ(l, symbol_table.at(*dynamic_cast<Identifier *>(l_prop->expression_)));
  auto r = symbol_table.at(*edge->identifier_);
  EXPECT_EQ(r.type(), Symbol::Type::EDGE_LIST);
}

TYPED_TEST(TestSymbolGenerator, MatchVariablePathUsingUnboundIdentifier) {
  // Test MATCH (n) -[r *..l.prop]- (m) MATCH (l) RETURN r
  auto prop = this->dba.NameToProperty("prop");
  auto edge = EDGE_VARIABLE("r");
  auto l_prop = PROPERTY_LOOKUP(this->dba, "l", prop);
  edge->upper_bound_ = l_prop;
  auto node_l = NODE("l");
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m"))), MATCH(PATTERN(node_l)), RETURN("r")));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, CreateVariablePath) {
  // Test CREATE (n) -[r *]-> (m) raises a SemanticException, since variable
  // paths cannot be created.
  auto edge = EDGE_VARIABLE("r", EdgeAtom::Type::DEPTH_FIRST, EdgeAtom::Direction::OUT);
  auto query = QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n"), edge, NODE("m")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, MergeVariablePath) {
  // Test MERGE (n) -[r *]-> (m) raises a SemanticException, since variable
  // paths cannot be created.
  auto edge = EDGE_VARIABLE("r", EdgeAtom::Type::DEPTH_FIRST, EdgeAtom::Direction::OUT);
  auto query = QUERY(SINGLE_QUERY(MERGE(PATTERN(NODE("n"), edge, NODE("m")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, RedeclareVariablePath) {
  // Test MATCH (n) -[n*]-> (m) RETURN n raises RedeclareVariableError.
  // This is just a temporary solution, before we add the support for using
  // variable paths with already declared symbols. In the future, this test
  // should be changed to check for type errors.
  auto edge = EDGE_VARIABLE("n", EdgeAtom::Type::DEPTH_FIRST, EdgeAtom::Direction::OUT);
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m"))), RETURN("n")));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), RedeclareVariableError);
}

TYPED_TEST(TestSymbolGenerator, VariablePathSameIdentifier) {
  // Test MATCH (n) -[r *r.prop..]-> (m) RETURN r raises UnboundVariableError.
  // `r` cannot be used inside the range expression, since it is bound by the
  // variable expansion itself.
  auto prop = this->dba.NameToProperty("prop");
  auto edge = EDGE_VARIABLE("r", EdgeAtom::Type::DEPTH_FIRST, EdgeAtom::Direction::OUT);
  edge->lower_bound_ = PROPERTY_LOOKUP(this->dba, "r", prop);
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m"))), RETURN("r")));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), UnboundVariableError);
}

TYPED_TEST(TestSymbolGenerator, MatchPropertySameIdentifier) {
  // Test MATCH (n {prop: n.prop}) RETURN n
  // Using `n.prop` needs to work, because filters are run after the value for
  // matched symbol is obtained.
  auto prop = PROPERTY_PAIR(this->dba, "prop");
  auto node_n = NODE("n");
  auto n_prop = PROPERTY_LOOKUP(this->dba, "n", prop.second);
  std::get<0>(node_n->properties_)[this->storage.GetPropertyIx(prop.first)] = n_prop;
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node_n)), RETURN("n")));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  auto n = symbol_table.at(*node_n->identifier_);
  EXPECT_EQ(n, symbol_table.at(*dynamic_cast<Identifier *>(n_prop->expression_)));
}

TYPED_TEST(TestSymbolGenerator, WithReturnAll) {
  // Test WITH 42 AS x RETURN all(x IN [x] WHERE x = 2) AS x, x AS y
  auto *with_as_x = AS("x");
  auto *list_x = IDENT("x");
  auto *where_x = IDENT("x");
  auto *all = ALL("x", LIST(list_x), WHERE(EQ(where_x, LITERAL(2))));
  auto *ret_as_x = AS("x");
  auto *ret_x = IDENT("x");
  auto query = QUERY(SINGLE_QUERY(WITH(LITERAL(42), with_as_x), RETURN(all, ret_as_x, ret_x, AS("y"))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // Symbols for `WITH .. AS x`, `ALL(x ...)`, `ALL(...) AS x` and `AS y`.
  EXPECT_EQ(symbol_table.max_position(), 4);
  // Check `WITH .. AS x` is the same as `[x]` and `RETURN ... x AS y`
  EXPECT_EQ(symbol_table.at(*with_as_x), symbol_table.at(*list_x));
  EXPECT_EQ(symbol_table.at(*with_as_x), symbol_table.at(*ret_x));
  EXPECT_NE(symbol_table.at(*with_as_x), symbol_table.at(*all->identifier_));
  EXPECT_NE(symbol_table.at(*with_as_x), symbol_table.at(*ret_as_x));
  // Check `ALL(x ...)` is only equal to `WHERE x = 2`
  EXPECT_EQ(symbol_table.at(*all->identifier_), symbol_table.at(*where_x));
  EXPECT_NE(symbol_table.at(*all->identifier_), symbol_table.at(*ret_as_x));
}

TYPED_TEST(TestSymbolGenerator, WithReturnSingle) {
  // Test WITH 42 AS x RETURN single(x IN [x] WHERE x = 2) AS x, x AS y
  auto *with_as_x = AS("x");
  auto *list_x = IDENT("x");
  auto *where_x = IDENT("x");
  auto *single = SINGLE("x", LIST(list_x), WHERE(EQ(where_x, LITERAL(2))));
  auto *ret_as_x = AS("x");
  auto *ret_x = IDENT("x");
  auto query = QUERY(SINGLE_QUERY(WITH(LITERAL(42), with_as_x), RETURN(single, ret_as_x, ret_x, AS("y"))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // Symbols for `WITH .. AS x`, `SINGLE(x ...)`, `SINGLE(...) AS x` and `AS y`.
  EXPECT_EQ(symbol_table.max_position(), 4);
  // Check `WITH .. AS x` is the same as `[x]` and `RETURN ... x AS y`
  EXPECT_EQ(symbol_table.at(*with_as_x), symbol_table.at(*list_x));
  EXPECT_EQ(symbol_table.at(*with_as_x), symbol_table.at(*ret_x));
  EXPECT_NE(symbol_table.at(*with_as_x), symbol_table.at(*single->identifier_));
  EXPECT_NE(symbol_table.at(*with_as_x), symbol_table.at(*ret_as_x));
  // Check `SINGLE(x ...)` is only equal to `WHERE x = 2`
  EXPECT_EQ(symbol_table.at(*single->identifier_), symbol_table.at(*where_x));
  EXPECT_NE(symbol_table.at(*single->identifier_), symbol_table.at(*ret_as_x));
}

TYPED_TEST(TestSymbolGenerator, WithReturnReduce) {
  // Test WITH 42 AS x RETURN reduce(y = 0, x IN [x] y + x) AS x, x AS y
  auto *with_as_x = AS("x");
  auto *list_x = IDENT("x");
  auto *expr_x = IDENT("x");
  auto *expr_y = IDENT("y");
  auto *reduce = REDUCE("y", LITERAL(0), "x", LIST(list_x), ADD(expr_y, expr_x));
  auto *ret_as_x = AS("x");
  auto *ret_x = IDENT("x");
  auto *ret_as_y = AS("y");
  auto query = QUERY(SINGLE_QUERY(WITH(LITERAL(42), with_as_x), RETURN(reduce, ret_as_x, ret_x, ret_as_y)));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // Symbols for `WITH .. AS x`, `REDUCE(y, x ...)`, `REDUCE(...) AS x` and `AS
  // y`.
  EXPECT_EQ(symbol_table.max_position(), 5);
  // Check `WITH .. AS x` is the same as `[x]` and `RETURN ... x AS y`
  EXPECT_EQ(symbol_table.at(*with_as_x), symbol_table.at(*list_x));
  EXPECT_EQ(symbol_table.at(*with_as_x), symbol_table.at(*ret_x));
  EXPECT_NE(symbol_table.at(*with_as_x), symbol_table.at(*reduce->identifier_));
  EXPECT_NE(symbol_table.at(*with_as_x), symbol_table.at(*ret_as_x));
  // Check `REDUCE(y, x ...)` is only equal to `y + x`
  EXPECT_EQ(symbol_table.at(*reduce->identifier_), symbol_table.at(*expr_x));
  EXPECT_NE(symbol_table.at(*reduce->identifier_), symbol_table.at(*ret_as_x));
  EXPECT_EQ(symbol_table.at(*reduce->accumulator_), symbol_table.at(*expr_y));
  EXPECT_NE(symbol_table.at(*reduce->accumulator_), symbol_table.at(*ret_as_y));
}

TYPED_TEST(TestSymbolGenerator, WithReturnExtract) {
  // Test WITH [1, 2, 3] AS x RETURN extract(x IN x | x + 1) AS x, x AS y
  auto *with_as_x = AS("x");
  auto *list_x = IDENT("x");
  auto *expr_x = IDENT("x");
  auto *extract = EXTRACT("x", LIST(list_x), ADD(expr_x, LITERAL(1)));
  auto *ret_as_x = AS("x");
  auto *ret_x = IDENT("x");
  auto *ret_as_y = AS("y");
  auto query = QUERY(SINGLE_QUERY(WITH(LIST(LITERAL(1), LITERAL(2), LITERAL(3)), with_as_x),
                                  RETURN(extract, ret_as_x, ret_x, ret_as_y)));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // Symbols for `WITH .. AS x`, `EXTRACT(x ...)`, `EXTRACT(...) AS x` and
  // `AS y`.
  EXPECT_EQ(symbol_table.max_position(), 4);
  // Check `WITH .. AS x` is the same as `... IN x` and `RETURN ... x AS y`
  EXPECT_EQ(symbol_table.at(*with_as_x), symbol_table.at(*list_x));
  EXPECT_EQ(symbol_table.at(*with_as_x), symbol_table.at(*ret_x));
  EXPECT_NE(symbol_table.at(*with_as_x), symbol_table.at(*extract->identifier_));
  EXPECT_NE(symbol_table.at(*with_as_x), symbol_table.at(*ret_as_x));
  // Check `EXTRACT(x ...)` is only equal to `x + 1`
  EXPECT_EQ(symbol_table.at(*extract->identifier_), symbol_table.at(*expr_x));
  EXPECT_NE(symbol_table.at(*extract->identifier_), symbol_table.at(*ret_as_x));
}

TYPED_TEST(TestSymbolGenerator, MatchBfsReturn) {
  // Test MATCH (n) -[r *bfs..n.prop] (r, n | r.prop)]-> (m) RETURN r AS r
  auto prop = this->dba.NameToProperty("prop");
  auto *node_n = NODE("n");
  auto *r_prop = PROPERTY_LOOKUP(this->dba, "r", prop);
  auto *n_prop = PROPERTY_LOOKUP(this->dba, "n", prop);
  auto *bfs = this->storage.template Create<EdgeAtom>(
      IDENT("r"), EdgeAtom::Type::BREADTH_FIRST, EdgeAtom::Direction::OUT, std::vector<QueryEdgeType>{});
  bfs->filter_lambda_.inner_edge = IDENT("r");
  bfs->filter_lambda_.inner_node = IDENT("n");
  bfs->filter_lambda_.expression = r_prop;
  bfs->upper_bound_ = n_prop;
  auto *ret_r = IDENT("r");
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node_n, bfs, NODE("m"))), RETURN(ret_r, AS("r"))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // Symbols for pattern, `n`, `[r]`, `r|`, `n|`, `m` and `AS r`.
  EXPECT_EQ(symbol_table.max_position(), 7);
  EXPECT_EQ(symbol_table.at(*ret_r), symbol_table.at(*bfs->identifier_));
  EXPECT_NE(symbol_table.at(*ret_r), symbol_table.at(*bfs->filter_lambda_.inner_edge));
  EXPECT_TRUE(symbol_table.at(*bfs->filter_lambda_.inner_edge).user_declared());
  EXPECT_EQ(symbol_table.at(*bfs->filter_lambda_.inner_edge),
            symbol_table.at(*dynamic_cast<Identifier *>(r_prop->expression_)));
  EXPECT_NE(symbol_table.at(*node_n->identifier_), symbol_table.at(*bfs->filter_lambda_.inner_node));
  EXPECT_TRUE(symbol_table.at(*bfs->filter_lambda_.inner_node).user_declared());
  EXPECT_EQ(symbol_table.at(*node_n->identifier_), symbol_table.at(*dynamic_cast<Identifier *>(n_prop->expression_)));
}

TYPED_TEST(TestSymbolGenerator, MatchBfsUsesEdgeSymbolError) {
  // Test MATCH (n) -[r *bfs..10 (e, n | r)]-> (m) RETURN r
  auto *bfs =
      this->storage.template Create<EdgeAtom>(IDENT("r"), EdgeAtom::Type::BREADTH_FIRST, EdgeAtom::Direction::OUT);
  bfs->filter_lambda_.inner_edge = IDENT("e");
  bfs->filter_lambda_.inner_node = IDENT("n");
  bfs->filter_lambda_.expression = IDENT("r");
  bfs->upper_bound_ = LITERAL(10);
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), bfs, NODE("m"))), RETURN("r")));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), UnboundVariableError);
}

TYPED_TEST(TestSymbolGenerator, MatchBfsUsesPreviousOuterSymbol) {
  // Test MATCH (a) -[r *bfs..10 (e, n | a)]-> (m) RETURN r
  auto *node_a = NODE("a");
  auto *bfs =
      this->storage.template Create<EdgeAtom>(IDENT("r"), EdgeAtom::Type::BREADTH_FIRST, EdgeAtom::Direction::OUT);
  bfs->filter_lambda_.inner_edge = IDENT("e");
  bfs->filter_lambda_.inner_node = IDENT("n");
  bfs->filter_lambda_.expression = IDENT("a");
  bfs->upper_bound_ = LITERAL(10);
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node_a, bfs, NODE("m"))), RETURN("r")));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  EXPECT_EQ(symbol_table.at(*node_a->identifier_),
            symbol_table.at(*dynamic_cast<Identifier *>(bfs->filter_lambda_.expression)));
}

TYPED_TEST(TestSymbolGenerator, MatchBfsUsesLaterSymbolError) {
  // Test MATCH (n) -[r *bfs..10 (e, n | m)]-> (m) RETURN r
  auto *bfs =
      this->storage.template Create<EdgeAtom>(IDENT("r"), EdgeAtom::Type::BREADTH_FIRST, EdgeAtom::Direction::OUT);
  bfs->filter_lambda_.inner_edge = IDENT("e");
  bfs->filter_lambda_.inner_node = IDENT("n");
  bfs->filter_lambda_.expression = IDENT("m");
  bfs->upper_bound_ = LITERAL(10);
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), bfs, NODE("m"))), RETURN("r")));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), UnboundVariableError);
}

TYPED_TEST(TestSymbolGenerator, MatchVariableLambdaSymbols) {
  // MATCH ()-[*]-() RETURN 42 AS res
  auto ident_n = this->storage.template Create<Identifier>("anon_n", false);
  auto node = this->storage.template Create<NodeAtom>(ident_n);
  auto edge = this->storage.template Create<EdgeAtom>(this->storage.template Create<Identifier>("anon_r", false),
                                                      EdgeAtom::Type::DEPTH_FIRST,
                                                      EdgeAtom::Direction::BOTH);
  edge->filter_lambda_.inner_edge = this->storage.template Create<Identifier>("anon_inner_e", false);
  edge->filter_lambda_.inner_node = this->storage.template Create<Identifier>("anon_inner_n", false);
  auto end_node = this->storage.template Create<NodeAtom>(this->storage.template Create<Identifier>("anon_end", false));
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node, edge, end_node)), RETURN(LITERAL(42), AS("res"))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // Symbols for `anon_n`, `anon_r`, `anon_inner_e`, `anon_inner_n`, `anon_end`
  // `AS res` and the auto-generated path name symbol.
  EXPECT_EQ(symbol_table.max_position(), 7);
  // All symbols except `AS res` are anonymously generated.
  for (const auto &symbol : symbol_table.table()) {
    if (symbol.name() == "res") {
      EXPECT_TRUE(symbol.user_declared());
    } else {
      EXPECT_FALSE(symbol.user_declared());
    }
  }
}

TYPED_TEST(TestSymbolGenerator, MatchWShortestReturn) {
  // Test MATCH (n) -[r *wShortest (r, n | r.weight) (r, n | r.filter)]-> (m)
  // RETURN r AS r
  auto weight = this->dba.NameToProperty("weight");
  auto filter = this->dba.NameToProperty("filter");
  auto *node_n = NODE("n");
  auto *r_weight = PROPERTY_LOOKUP(this->dba, "r", weight);
  auto *r_filter = PROPERTY_LOOKUP(this->dba, "r", filter);
  auto *shortest = this->storage.template Create<EdgeAtom>(
      IDENT("r"), EdgeAtom::Type::WEIGHTED_SHORTEST_PATH, EdgeAtom::Direction::OUT, std::vector<QueryEdgeType>{});
  {
    shortest->weight_lambda_.inner_edge = IDENT("r");
    shortest->weight_lambda_.inner_node = IDENT("n");
    shortest->weight_lambda_.expression = r_weight;
    shortest->total_weight_ = IDENT("total_weight");
  }
  {
    shortest->filter_lambda_.inner_edge = IDENT("r");
    shortest->filter_lambda_.inner_node = IDENT("n");
    shortest->filter_lambda_.expression = r_filter;
  }
  auto *ret_r = IDENT("r");
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node_n, shortest, NODE("m"))), RETURN(ret_r, AS("r"))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  // Symbols for pattern, `n`, `[r]`, `total_weight`, (`r|`, `n|`)x2, `m` and
  // `AS r`.
  EXPECT_EQ(symbol_table.max_position(), 10);
  EXPECT_EQ(symbol_table.at(*ret_r), symbol_table.at(*shortest->identifier_));
  EXPECT_NE(symbol_table.at(*ret_r), symbol_table.at(*shortest->weight_lambda_.inner_edge));
  EXPECT_NE(symbol_table.at(*ret_r), symbol_table.at(*shortest->filter_lambda_.inner_edge));
  EXPECT_TRUE(symbol_table.at(*shortest->filter_lambda_.inner_edge).user_declared());
  EXPECT_EQ(symbol_table.at(*shortest->weight_lambda_.inner_edge),
            symbol_table.at(*dynamic_cast<Identifier *>(r_weight->expression_)));
  EXPECT_NE(symbol_table.at(*shortest->weight_lambda_.inner_edge),
            symbol_table.at(*shortest->filter_lambda_.inner_edge));
  EXPECT_NE(symbol_table.at(*shortest->weight_lambda_.inner_node),
            symbol_table.at(*shortest->filter_lambda_.inner_node));
  EXPECT_EQ(symbol_table.at(*shortest->filter_lambda_.inner_edge),
            symbol_table.at(*dynamic_cast<Identifier *>(r_filter->expression_)));
  EXPECT_TRUE(symbol_table.at(*shortest->filter_lambda_.inner_node).user_declared());
}

TYPED_TEST(TestSymbolGenerator, MatchUnionSymbols) {
  // RETURN 5 as X UNION RETURN 6 AS x
  auto query = QUERY(SINGLE_QUERY(RETURN(LITERAL(5), AS("X"))), UNION(SINGLE_QUERY(RETURN(LITERAL(6), AS("X")))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  EXPECT_EQ(symbol_table.max_position(), 3);
}

TYPED_TEST(TestSymbolGenerator, MatchUnionMultipleSymbols) {
  // RETURN 5 as X, 6 AS Y UNION RETURN 5 AS Y, 6 AS x
  auto query = QUERY(SINGLE_QUERY(RETURN(LITERAL(5), AS("X"), LITERAL(6), AS("Y"))),
                     UNION(SINGLE_QUERY(RETURN(LITERAL(5), AS("Y"), LITERAL(6), AS("X")))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  EXPECT_EQ(symbol_table.max_position(), 6);
}

TYPED_TEST(TestSymbolGenerator, MatchUnionAllSymbols) {
  // RETURN 5 as X UNION ALL RETURN 6 AS x
  auto query = QUERY(SINGLE_QUERY(RETURN(LITERAL(5), AS("X"))), UNION_ALL(SINGLE_QUERY(RETURN(LITERAL(6), AS("X")))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  EXPECT_EQ(symbol_table.max_position(), 3);
}

TYPED_TEST(TestSymbolGenerator, MatchUnionAllMultipleSymbols) {
  // RETURN 5 as X, 6 AS Y UNION ALL RETURN 5 AS Y, 6 AS x
  auto query = QUERY(SINGLE_QUERY(RETURN(LITERAL(5), AS("X"), LITERAL(6), AS("Y"))),
                     UNION_ALL(SINGLE_QUERY(RETURN(LITERAL(5), AS("Y"), LITERAL(6), AS("X")))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  EXPECT_EQ(symbol_table.max_position(), 6);
}

TYPED_TEST(TestSymbolGenerator, MatchUnionReturnAllSymbols) {
  // WITH 1 as X, 2 AS Y RETURN * UNION RETURN 3 AS X, 4 AS Y
  auto ret = this->storage.template Create<Return>();
  ret->body_.all_identifiers = true;
  auto query = QUERY(SINGLE_QUERY(WITH(LITERAL(1), AS("X"), LITERAL(2), AS("Y")), ret),
                     UNION(SINGLE_QUERY(RETURN(LITERAL(3), AS("X"), LITERAL(4), AS("Y")))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  EXPECT_EQ(symbol_table.max_position(), 6);
}

TYPED_TEST(TestSymbolGenerator, MatchUnionReturnSymbols) {
  // WITH 1 as X, 2 AS Y RETURN Y, X UNION RETURN 3 AS X, 4 AS Y
  auto query = QUERY(SINGLE_QUERY(WITH(LITERAL(1), AS("X"), LITERAL(2), AS("Y")), RETURN("Y", "X")),
                     UNION(SINGLE_QUERY(RETURN(LITERAL(3), AS("X"), LITERAL(4), AS("Y")))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  EXPECT_EQ(symbol_table.max_position(), 8);
}

TYPED_TEST(TestSymbolGenerator, MatchUnionParameterNameThrowSemanticException) {
  // WITH 1 as X, 2 AS Y RETURN * UNION RETURN 3 AS Z, 4 AS Y
  auto ret = this->storage.template Create<Return>();
  ret->body_.all_identifiers = true;
  auto query = QUERY(SINGLE_QUERY(WITH(LITERAL(1), AS("X"), LITERAL(2), AS("Y")), ret),
                     UNION(SINGLE_QUERY(RETURN(LITERAL(3), AS("Z"), LITERAL(4), AS("Y")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, MatchUnionParameterNumberThrowSemanticException) {
  // WITH 1 as X, 2 AS Y RETURN * UNION RETURN 4 AS Y
  auto ret = this->storage.template Create<Return>();
  ret->body_.all_identifiers = true;
  auto query = QUERY(SINGLE_QUERY(WITH(LITERAL(1), AS("X"), LITERAL(2), AS("Y")), ret),
                     UNION(SINGLE_QUERY(RETURN(LITERAL(4), AS("Y")))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, MatchUnion) {
  // WITH 5 AS X, 3 AS Y RETURN * UNION WITH 9 AS Y, 4 AS X RETURN Y, X
  auto ret = this->storage.template Create<Return>();
  ret->body_.all_identifiers = true;
  auto query = QUERY(SINGLE_QUERY(WITH(LITERAL(5), AS("X"), LITERAL(3), AS("Y")), ret),
                     UNION(SINGLE_QUERY(WITH(LITERAL(9), AS("Y"), LITERAL(4), AS("X")), RETURN("Y", "X"))));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  EXPECT_EQ(symbol_table.max_position(), 8);
}

TYPED_TEST(TestSymbolGenerator, CallProcedureYield) {
  // WITH 1 AS x CALL proc(x) YIELD x AS y RETURN x, y
  auto call = this->storage.template Create<CallProcedure>();
  call->procedure_name_ = "proc";
  auto *arg_x = IDENT("x");
  call->arguments_.push_back(arg_x);
  call->result_fields_.emplace_back("x");
  call->result_identifiers_.push_back(IDENT("y"));
  auto *as_x = AS("x");
  auto *ret = RETURN("x", "y");
  auto query = QUERY(SINGLE_QUERY(WITH(LITERAL(1), as_x), call, ret));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  EXPECT_EQ(symbol_table.max_position(), 4);
  const auto &sym_x = symbol_table.at(*as_x);
  const auto &sym_y = symbol_table.at(*call->result_identifiers_.back());
  EXPECT_EQ(symbol_table.at(*arg_x), sym_x);
  auto *ret_x = dynamic_cast<Identifier *>(ret->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(ret_x);
  auto *ret_y = dynamic_cast<Identifier *>(ret->body_.named_expressions[1]->expression_);
  ASSERT_TRUE(ret_y);
  EXPECT_EQ(symbol_table.at(*ret_x), sym_x);
  EXPECT_EQ(symbol_table.at(*ret_y), sym_y);
  EXPECT_NE(symbol_table.at(*ret->body_.named_expressions[0]), sym_x);
  EXPECT_NE(symbol_table.at(*ret->body_.named_expressions[1]), sym_y);
}

TYPED_TEST(TestSymbolGenerator, CallProcedureShadowingYield) {
  // WITH 1 AS x CALL proc() YIELD x RETURN 42 AS res
  auto call = this->storage.template Create<CallProcedure>();
  call->procedure_name_ = "proc";
  call->result_fields_.emplace_back("x");
  call->result_identifiers_.push_back(IDENT("x"));
  auto query = QUERY(SINGLE_QUERY(WITH(LITERAL(1), AS("x")), call, RETURN(LITERAL(42), AS("res"))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, CallProcedureShadowingYieldAlias) {
  // WITH 1 AS x CALL proc() YIELD y AS x RETURN 42 AS res
  auto call = this->storage.template Create<CallProcedure>();
  call->procedure_name_ = "proc";
  call->result_fields_.emplace_back("y");
  call->result_identifiers_.push_back(IDENT("x"));
  auto query = QUERY(SINGLE_QUERY(WITH(LITERAL(1), AS("x")), call, RETURN(LITERAL(42), AS("res"))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, CallProcedureUnboundArgument) {
  // CALL proc(unbound)
  auto call = this->storage.template Create<CallProcedure>();
  call->procedure_name_ = "proc";
  call->arguments_.push_back(IDENT("unbound"));
  auto query = QUERY(SINGLE_QUERY(call));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, CallWithoutFieldsReturnAsterisk) {
  // CALL proc() RETURN *
  auto call = this->storage.template Create<CallProcedure>();
  call->procedure_name_ = "proc";
  auto ret = this->storage.template Create<Return>();
  ret->body_.all_identifiers = true;
  auto query = QUERY(SINGLE_QUERY(call, ret));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
}

TEST(TestSymbolTable, CreateAnonymousSymbols) {
  SymbolTable symbol_table;
  auto anon1 = symbol_table.CreateAnonymousSymbol();
  ASSERT_EQ(anon1.name(), "anon1");
  auto anon2 = symbol_table.CreateAnonymousSymbol();
  ASSERT_EQ(anon2.name(), "anon2");
}

TEST(TestSymbolTable, CreateAnonymousSymbolWithExistingUserSymbolCalledAnon) {
  SymbolTable symbol_table;
  symbol_table.CreateSymbol("anon1", false);
  auto anon2 = symbol_table.CreateAnonymousSymbol();
  ASSERT_EQ(anon2.name(), "anon2");
}

TYPED_TEST(TestSymbolGenerator, PredefinedIdentifiers) {
  auto *first_op = IDENT("first_op", false);
  auto *second_op = IDENT("second_op", false);
  // RETURN first_op + second_op AS result
  auto query = QUERY(SINGLE_QUERY(RETURN(ADD(first_op, second_op), AS("result"))));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query, {first_op}), SemanticException);
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query, {second_op}), SemanticException);
  auto symbol_table = memgraph::query::MakeSymbolTable(query, {first_op, second_op});
  ASSERT_EQ(symbol_table.max_position(), 3);

  // predefined identifier can only be used in one scope
  // RETURN first_op + second_op AS result UNION RETURN second_op + first_op AS result
  query = QUERY(SINGLE_QUERY(RETURN(ADD(first_op, second_op), AS("result"))),
                UNION(SINGLE_QUERY(RETURN(ADD(second_op, first_op), AS("result")))));
  ASSERT_THROW(memgraph::query::MakeSymbolTable(query, {first_op, second_op}), SemanticException);

  // predefined identifier can be introduced in any of the scope
  // different predefined identifiers can be introduced in different scopes
  // RETURN first_op AS result UNION RETURN second_op AS result
  query = QUERY(SINGLE_QUERY(RETURN(first_op, AS("result"))), UNION(SINGLE_QUERY(RETURN(second_op, AS("result")))));
  ASSERT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
  symbol_table = memgraph::query::MakeSymbolTable(query, {first_op, second_op});
  ASSERT_EQ(symbol_table.max_position(), 5);

  // WITH statement resets the scope, but the predefined identifier is okay
  // because it's the first introduction of it in the query
  // WITH 1 as one RETURN first_op AS first
  query = QUERY(SINGLE_QUERY(WITH(LITERAL(1), AS("one")), RETURN(first_op, AS("first"))));
  ASSERT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
  symbol_table = memgraph::query::MakeSymbolTable(query, {first_op});
  ASSERT_EQ(symbol_table.max_position(), 3);

  // In the first scope, first_op represents identifier created by match,
  // in the second it represent the predefined identifier
  // MATCH(first_op) WITH first_op as n RETURN first_op, n
  query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("first_op"))), WITH("first_op", AS("n")), RETURN("first_op", "n")));
  ASSERT_THROW(memgraph::query::MakeSymbolTable(query), SemanticException);
  symbol_table = memgraph::query::MakeSymbolTable(query, {first_op});
  ASSERT_EQ(symbol_table.max_position(), 6);

  // You cannot redaclare the predefined identifier in the same scope
  // UNWIND first_op as u CREATE(first_op {prop: u})
  auto unwind = UNWIND(first_op, AS("u"));
  auto node = NODE("first_op");
  std::get<0>(node->properties_)[this->storage.GetPropertyIx("prop")] =
      dynamic_cast<Identifier *>(unwind->named_expression_->expression_);
  query = QUERY(SINGLE_QUERY(unwind, CREATE(PATTERN(node))));
  ASSERT_THROW(memgraph::query::MakeSymbolTable(query, {first_op}), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, Foreach) {
  auto *i = NEXPR("i", IDENT("i"));
  auto query = QUERY(SINGLE_QUERY(FOREACH(i, {CREATE(PATTERN(NODE("n")))}), RETURN("n")));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), UnboundVariableError);

  query = QUERY(SINGLE_QUERY(FOREACH(i, {CREATE(PATTERN(NODE("n")))}), FOREACH(i, {CREATE(PATTERN(NODE("v")))})));
  auto symbol_table = memgraph::query::MakeSymbolTable(query);
  ASSERT_EQ(symbol_table.max_position(), 6);

  query = QUERY(SINGLE_QUERY(FOREACH(i, {FOREACH(i, {CREATE(PATTERN(NODE("i")))})})));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), RedeclareVariableError);

  query = QUERY(SINGLE_QUERY(FOREACH(i, {FOREACH(i, {CREATE(PATTERN(NODE("v")))})})));
  symbol_table = memgraph::query::MakeSymbolTable(query);
  ASSERT_EQ(symbol_table.max_position(), 4);

  query = QUERY(SINGLE_QUERY(FOREACH(i, {CREATE(PATTERN(NODE("n")))}), RETURN("i")));
  EXPECT_THROW(memgraph::query::MakeSymbolTable(query), UnboundVariableError);
}

TYPED_TEST(TestSymbolGenerator, Exists) {
  {
    auto query =
        QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                           WHERE(EXISTS(PATTERN(NODE("n"), EDGE("", EdgeAtom::Direction::BOTH, {}, false), NODE("m")))),
                           RETURN("n")));
    EXPECT_THROW(MakeSymbolTable(query), SemanticException);
  }

  {
    auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                                    WHERE(EXISTS(PATTERN(NODE("n"), EDGE("r"), NODE("", std::nullopt, false)))),
                                    RETURN("n")));
    EXPECT_THROW(MakeSymbolTable(query), SemanticException);
  }

  {
    auto query = QUERY(
        SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), WHERE(EXISTS(PATTERN(NODE("n"), EDGE("r"), NODE("m")))), RETURN("n")));
    EXPECT_THROW(MakeSymbolTable(query), SemanticException);
  }

  {
    // Symbols for match pattern, node symbol, exists pattern, exists edge, exists second node, named expression in
    // return
    auto query = QUERY(SINGLE_QUERY(
        MATCH(PATTERN(NODE("n"))),
        WHERE(EXISTS(
            PATTERN(NODE("n"), EDGE("edge", EdgeAtom::Direction::BOTH, {}, false), NODE("node", std::nullopt, false)))),
        RETURN("n")));
    auto symbol_table = MakeSymbolTable(query);
    ASSERT_EQ(symbol_table.max_position(), 7);

    memgraph::query::plan::UsedSymbolsCollector collector(symbol_table);
    auto *match = dynamic_cast<Match *>(query->single_query_->clauses_[0]);
    auto *expression = dynamic_cast<Expression *>(match->where_->expression_);

    expression->Accept(collector);

    ASSERT_EQ(collector.symbols_.size(), 1);

    auto symbol = *collector.symbols_.begin();
    ASSERT_EQ(symbol.name(), "n");
  }

  {
    auto subquery = QUERY(SINGLE_QUERY(
        MATCH(
            PATTERN(NODE("n"), EDGE("edge", EdgeAtom::Direction::BOTH, {}, false), NODE("node", std::nullopt, false))),
        RETURN("n")));
    auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), WHERE(EXISTS_SUBQUERY(subquery)), RETURN("n")));
    auto symbol_table = MakeSymbolTable(query);

    // match pattern, node, edge, second node, return param, node, return param, exists
    ASSERT_EQ(symbol_table.max_position(), 8);

    memgraph::query::plan::UsedSymbolsCollector collector(symbol_table);
    auto *match = dynamic_cast<Match *>(query->single_query_->clauses_[0]);
    auto *expression = dynamic_cast<Expression *>(match->where_->expression_);

    expression->Accept(collector);

    ASSERT_EQ(collector.symbols_.size(), 1);

    auto symbol = *collector.symbols_.begin();
    ASSERT_EQ(symbol.name(), "n");
  }
}

// The gate ladder: EXISTS is allowed only in the positions the planner has a splice point for, and the checks run in a
// fixed order - so a refusal can change identity when an earlier rung moves. Every position gets a case, allowed or
// refused, and the refused ones assert the message.
TYPED_TEST(TestSymbolGenerator, ExistsAllowedPositions) {
  auto exists_subquery = [this] {
    return EXISTS_SUBQUERY(QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))))));
  };

  // MATCH (n) WHERE EXISTS { ... } RETURN n
  MakeSymbolTable(QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), WHERE(exists_subquery()), RETURN("n"))));

  // MATCH (n) RETURN EXISTS { ... } AS h
  MakeSymbolTable(QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), RETURN(exists_subquery(), AS("h")))));

  // MATCH (n) WITH n, EXISTS { ... } AS h RETURN h
  MakeSymbolTable(QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"))), WITH(NEXPR("n", IDENT("n")), NEXPR("h", exists_subquery())), RETURN("h"))));

  // MATCH (n) WITH n WHERE EXISTS { ... } RETURN n  (issue #3385)
  MakeSymbolTable(QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), WITH("n"), WHERE(exists_subquery()), RETURN("n"))));

  // MATCH (n) WITH n ORDER BY EXISTS { ... } RETURN n
  MakeSymbolTable(QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), WITH("n", ORDER_BY(exists_subquery())), RETURN("n"))));

  // MATCH (n) RETURN n ORDER BY EXISTS { ... }
  MakeSymbolTable(QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), RETURN("n", ORDER_BY(exists_subquery())))));

  // MATCH (n) RETURN collect(EXISTS { ... }) AS c - inside an aggregate argument.
  MakeSymbolTable(
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), RETURN(COLLECT_LIST(exists_subquery(), false), AS("c")))));

  // The pattern form takes the same positions.
  MakeSymbolTable(QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"))),
      RETURN(EXISTS(PATTERN(NODE("n"), EDGE("r", EdgeAtom::Direction::OUT, {}, false), NODE("m", std::nullopt, false))),
             AS("h")))));
}

TYPED_TEST(TestSymbolGenerator, ExistsRefusedPositions) {
  auto prop = this->dba.NameToProperty("prop");
  auto exists_subquery = [this] {
    return EXISTS_SUBQUERY(QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))))));
  };
  auto expect_message = [](auto *query, std::string_view message) {
    try {
      MakeSymbolTable(query);
      FAIL() << "expected the query to be refused";
    } catch (const memgraph::utils::NotYetImplemented &e) {
      EXPECT_EQ(std::string_view{e.what()}, message);
    }
  };

  const std::string_view generic = "Not yet implemented: Exists is not supported in this position yet!";

  // SET n.prop = EXISTS { ... }. The dedicated in_set_property gate is gone: default-deny covers it, and the message
  // identity changes to the generic one.
  expect_message(
      QUERY(SINGLE_QUERY(
          MATCH(PATTERN(NODE("n"))), SET(PROPERTY_LOOKUP(this->dba, "n", prop), exists_subquery()), RETURN("n"))),
      generic);

  // FOREACH (i IN [1] | SET n.prop = EXISTS { ... })
  expect_message(QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                                    FOREACH(NEXPR("i", LIST(LITERAL(1))),
                                            {SET(PROPERTY_LOOKUP(this->dba, "n", prop), exists_subquery())}))),
                 generic);

  // UNWIND [EXISTS { ... }] AS h - PR 4's territory, no splice point here yet.
  expect_message(
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), UNWIND(NEXPR("h", LIST(exists_subquery()))), RETURN("h"))),
      generic);

  // RETURN n SKIP EXISTS { ... } - SKIP shares in_return with the projection but has no splice point of its own.
  expect_message(QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), RETURN("n", SKIP(exists_subquery())))), generic);

  // reduce(...) keeps its own message, and it now fires from a RETURN too, where !in_where used to answer first.
  expect_message(
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                         RETURN(REDUCE("acc", LITERAL(false), "x", LIST(LITERAL(1)), exists_subquery()), AS("h")))),
      "Not yet implemented: Exists cannot be used within REDUCE!");

  // CASE is PR 4. Its message is unchanged in a WHERE and now also fires in a RETURN.
  expect_message(QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                                    WHERE(this->storage.template Create<memgraph::query::IfOperator>(
                                        exists_subquery(), LITERAL(true), LITERAL(false))),
                                    RETURN("n"))),
                 "Not yet implemented: IF operator cannot be used with exists, but only during matching!");
  expect_message(QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                                    RETURN(this->storage.template Create<memgraph::query::IfOperator>(
                                               exists_subquery(), LITERAL(true), LITERAL(false)),
                                           AS("h")))),
                 "Not yet implemented: IF operator cannot be used with exists, but only during matching!");
}

TYPED_TEST(TestSymbolGenerator, Subqueries) {
  // MATCH (n) CALL { MATCH (n) RETURN n } RETURN n
  // Yields exception because n in subquery is referenced in outer scope
  auto subquery = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), RETURN("n")));
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), CALL_SUBQUERY(subquery), RETURN("n")));
  EXPECT_THROW(MakeSymbolTable(query), SemanticException);

  // WITH 1 AS x CALL { RETURN 2 AS x } RETURN x
  // Yields exception because aliased return in subquery conflicts with outer scope variable
  subquery = QUERY(SINGLE_QUERY(RETURN(LITERAL(2), AS("x"))));
  query = QUERY(SINGLE_QUERY(WITH(LITERAL(1), AS("x")), CALL_SUBQUERY(subquery), RETURN("x")));
  EXPECT_THROW(MakeSymbolTable(query), SemanticException);

  // MATCH (n) CALL { MATCH (m) RETURN m.prop } RETURN n
  // Yields exception because m.prop must be aliased before returning
  subquery = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("m"))), RETURN("m.prop")));
  query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), CALL_SUBQUERY(subquery), RETURN("n")));
  EXPECT_THROW(MakeSymbolTable(query), SemanticException);

  // MATCH (n) CALL { MATCH (m) RETURN m, m.prop } RETURN n
  // Yields exception because m.prop must be aliased before returning
  subquery = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("m"))), RETURN("m", "m.prop")));
  query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), CALL_SUBQUERY(subquery), RETURN("n")));
  EXPECT_THROW(MakeSymbolTable(query), SemanticException);

  // MATCH (n) CALL { MATCH (m) RETURN m.prop, m } RETURN n
  // Yields exception because m.prop must be aliased before returning
  subquery = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("m"))), RETURN("m.prop", "m")));
  query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), CALL_SUBQUERY(subquery), RETURN("n")));
  EXPECT_THROW(MakeSymbolTable(query), SemanticException);

  // MATCH (n) CALL { MATCH (m) RETURN m } RETURN n, m
  subquery = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("m"))), RETURN("m")));
  query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), CALL_SUBQUERY(subquery), RETURN("n", "m")));
  auto symbol_table = MakeSymbolTable(query);
  ASSERT_EQ(symbol_table.max_position(), 7);

  // MATCH (n) CALL { MATCH (m) RETURN m UNION MATCH (m) RETURN m } RETURN n, m
  subquery = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("m"))), RETURN("m")),
                   UNION(SINGLE_QUERY(MATCH(PATTERN(NODE("m"))), RETURN("m"))));
  query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), CALL_SUBQUERY(subquery), RETURN("n", "m")));
  symbol_table = MakeSymbolTable(query);
  ASSERT_EQ(symbol_table.max_position(), 11);

  // MATCH (n) CALL { MATCH (s) RETURN s } RETURN n UNION MATCH (n) CALL { MATCH (s) RETURN s } RETURN n
  subquery = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("s"))), RETURN("s")));
  query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), CALL_SUBQUERY(subquery), RETURN("n")),
                UNION(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), CALL_SUBQUERY(subquery), RETURN("n"))));
  symbol_table = MakeSymbolTable(query);
  ASSERT_EQ(symbol_table.max_position(), 13);
}

TYPED_TEST(TestSymbolGenerator, PropertyCachingSingleLookup) {
  // WITH {icode: 0000} AS item
  // RETURN {icode: item.icode} AS new_map;

  auto prop1_key = this->storage.GetPropertyIx("icode");
  auto prop1_val = PROPERTY_LOOKUP(this->dba, "item", this->dba.NameToProperty("icode"));

  auto has_properties = MAP({prop1_key, LITERAL(0000)});
  auto new_map = MAP({prop1_key, prop1_val});
  auto query = QUERY(SINGLE_QUERY(WITH(has_properties, AS("item")), RETURN(new_map, AS("new_map"))));

  memgraph::query::MakeSymbolTable(query);

  auto prop1_eval_mode = dynamic_cast<PropertyLookup *>(new_map->elements_[prop1_key])->evaluation_mode_;

  ASSERT_TRUE(prop1_eval_mode == PropertyLookup::EvaluationMode::GET_OWN_PROPERTY);
}

TYPED_TEST(TestSymbolGenerator, PropertyCachingTwoSingleLookups) {
  // WITH {icode: 0000} AS item1, {icode: 1111} AS item2
  // RETURN {icode1: item1.icode, icode2: item2.icode} AS new_map;

  auto in_prop1_key = this->storage.GetPropertyIx("icode");
  auto out_prop1_key = this->storage.GetPropertyIx("icode1");
  auto out_prop1_val = PROPERTY_LOOKUP(this->dba, "item1", this->dba.NameToProperty("icode"));
  auto out_prop2_key = this->storage.GetPropertyIx("icode2");
  auto out_prop2_val = PROPERTY_LOOKUP(this->dba, "item2", this->dba.NameToProperty("icode"));

  auto has_properties1 = MAP({in_prop1_key, LITERAL(0000)});
  auto has_properties2 = MAP({in_prop1_key, LITERAL(1111)});
  auto new_map = MAP({out_prop1_key, out_prop1_val}, {out_prop2_key, out_prop2_val});
  auto query = QUERY(
      SINGLE_QUERY(WITH(has_properties1, AS("item1"), has_properties2, AS("item2")), RETURN(new_map, AS("new_map"))));

  memgraph::query::MakeSymbolTable(query);

  auto prop1_eval_mode = dynamic_cast<PropertyLookup *>(new_map->elements_[out_prop1_key])->evaluation_mode_;
  auto prop2_eval_mode = dynamic_cast<PropertyLookup *>(new_map->elements_[out_prop2_key])->evaluation_mode_;

  ASSERT_TRUE(prop1_eval_mode == PropertyLookup::EvaluationMode::GET_OWN_PROPERTY);
  ASSERT_TRUE(prop2_eval_mode == PropertyLookup::EvaluationMode::GET_OWN_PROPERTY);
}

TYPED_TEST(TestSymbolGenerator, PropertyCachingMultipleLookup) {
  // WITH {icode: 0000, price: 10} AS item
  // RETURN {icode: item.icode, price: item.price} AS new_map;

  auto prop1_key = this->storage.GetPropertyIx("icode");
  auto prop1_val = PROPERTY_LOOKUP(this->dba, "item", this->dba.NameToProperty("icode"));
  auto prop2_key = this->storage.GetPropertyIx("price");
  auto prop2_val = PROPERTY_LOOKUP(this->dba, "item", this->dba.NameToProperty("price"));

  auto has_properties = MAP({prop1_key, LITERAL(0000)}, {prop2_key, LITERAL(10)});
  auto new_map = MAP({prop1_key, prop1_val}, {prop2_key, prop2_val});
  auto query = QUERY(SINGLE_QUERY(WITH(has_properties, AS("item")), RETURN(new_map, AS("new_map"))));

  memgraph::query::MakeSymbolTable(query);

  auto prop1_eval_mode = dynamic_cast<PropertyLookup *>(new_map->elements_[prop1_key])->evaluation_mode_;
  auto prop2_eval_mode = dynamic_cast<PropertyLookup *>(new_map->elements_[prop2_key])->evaluation_mode_;

  ASSERT_TRUE(prop1_eval_mode == PropertyLookup::EvaluationMode::GET_ALL_PROPERTIES);
  ASSERT_TRUE(prop2_eval_mode == PropertyLookup::EvaluationMode::GET_ALL_PROPERTIES);
}

TYPED_TEST(TestSymbolGenerator, PropertyCachingTwoMultipleLookups) {
  // WITH {icode: 0000, price: 10} AS item1, {icode: 1111, price: 16} AS item2
  // RETURN {icode1: item1.icode, price1: item1.price, icode2: item2.icode, price2: item2.price} AS new_map;

  auto in_prop1_key = this->storage.GetPropertyIx("icode");
  auto in_prop2_key = this->storage.GetPropertyIx("price");

  auto out_prop1_key = this->storage.GetPropertyIx("icode1");
  auto out_prop1_val = PROPERTY_LOOKUP(this->dba, "item1", this->dba.NameToProperty("icode"));
  auto out_prop2_key = this->storage.GetPropertyIx("price1");
  auto out_prop2_val = PROPERTY_LOOKUP(this->dba, "item1", this->dba.NameToProperty("price"));
  auto out_prop3_key = this->storage.GetPropertyIx("icode2");
  auto out_prop3_val = PROPERTY_LOOKUP(this->dba, "item2", this->dba.NameToProperty("icode"));
  auto out_prop4_key = this->storage.GetPropertyIx("price2");
  auto out_prop4_val = PROPERTY_LOOKUP(this->dba, "item2", this->dba.NameToProperty("price"));

  auto has_properties1 = MAP({in_prop1_key, LITERAL(0000)}, {in_prop2_key, LITERAL(10)});
  auto has_properties2 = MAP({in_prop1_key, LITERAL(1111)}, {in_prop2_key, LITERAL(16)});
  auto new_map = MAP({out_prop1_key, out_prop1_val},
                     {out_prop2_key, out_prop2_val},
                     {out_prop3_key, out_prop3_val},
                     {out_prop4_key, out_prop4_val});
  auto query = QUERY(
      SINGLE_QUERY(WITH(has_properties1, AS("item1"), has_properties2, AS("item2")), RETURN(new_map, AS("new_map"))));

  memgraph::query::MakeSymbolTable(query);

  auto prop1_eval_mode = dynamic_cast<PropertyLookup *>(new_map->elements_[out_prop1_key])->evaluation_mode_;
  auto prop2_eval_mode = dynamic_cast<PropertyLookup *>(new_map->elements_[out_prop2_key])->evaluation_mode_;
  auto prop3_eval_mode = dynamic_cast<PropertyLookup *>(new_map->elements_[out_prop3_key])->evaluation_mode_;
  auto prop4_eval_mode = dynamic_cast<PropertyLookup *>(new_map->elements_[out_prop4_key])->evaluation_mode_;

  ASSERT_TRUE(prop1_eval_mode == PropertyLookup::EvaluationMode::GET_ALL_PROPERTIES);
  ASSERT_TRUE(prop2_eval_mode == PropertyLookup::EvaluationMode::GET_ALL_PROPERTIES);
  ASSERT_TRUE(prop3_eval_mode == PropertyLookup::EvaluationMode::GET_ALL_PROPERTIES);
  ASSERT_TRUE(prop4_eval_mode == PropertyLookup::EvaluationMode::GET_ALL_PROPERTIES);
}

TYPED_TEST(TestSymbolGenerator, PropertyCachingMixedLookups1) {
  // WITH {icode: 0000, price: 10} AS item1, {icode: 1111, price: 16} AS item2
  // RETURN {icode1: item1.icode, price1: item1.price, icode2: item2.icode} AS new_map;

  auto in_prop1_key = this->storage.GetPropertyIx("icode");
  auto in_prop2_key = this->storage.GetPropertyIx("price");

  auto out_prop1_key = this->storage.GetPropertyIx("icode1");
  auto out_prop1_val = PROPERTY_LOOKUP(this->dba, "item1", this->dba.NameToProperty("icode"));
  auto out_prop2_key = this->storage.GetPropertyIx("price1");
  auto out_prop2_val = PROPERTY_LOOKUP(this->dba, "item1", this->dba.NameToProperty("price"));
  auto out_prop3_key = this->storage.GetPropertyIx("icode2");
  auto out_prop3_val = PROPERTY_LOOKUP(this->dba, "item2", this->dba.NameToProperty("icode"));

  auto has_properties1 = MAP({in_prop1_key, LITERAL(0000)}, {in_prop2_key, LITERAL(10)});
  auto has_properties2 = MAP({in_prop1_key, LITERAL(1111)}, {in_prop2_key, LITERAL(16)});
  auto new_map = MAP({out_prop1_key, out_prop1_val}, {out_prop2_key, out_prop2_val}, {out_prop3_key, out_prop3_val});
  auto query = QUERY(
      SINGLE_QUERY(WITH(has_properties1, AS("item1"), has_properties2, AS("item2")), RETURN(new_map, AS("new_map"))));

  memgraph::query::MakeSymbolTable(query);

  auto prop1_eval_mode = dynamic_cast<PropertyLookup *>(new_map->elements_[out_prop1_key])->evaluation_mode_;
  auto prop2_eval_mode = dynamic_cast<PropertyLookup *>(new_map->elements_[out_prop2_key])->evaluation_mode_;
  auto prop3_eval_mode = dynamic_cast<PropertyLookup *>(new_map->elements_[out_prop3_key])->evaluation_mode_;

  ASSERT_TRUE(prop1_eval_mode == PropertyLookup::EvaluationMode::GET_ALL_PROPERTIES);
  ASSERT_TRUE(prop2_eval_mode == PropertyLookup::EvaluationMode::GET_ALL_PROPERTIES);
  ASSERT_TRUE(prop3_eval_mode == PropertyLookup::EvaluationMode::GET_OWN_PROPERTY);
}

TYPED_TEST(TestSymbolGenerator, PropertyCachingMixedLookups2) {
  // WITH {icode: 0000, price: 10} AS item1, {icode: 1111, price: 16} AS item2
  // RETURN {icode1: item1.icode, icode2: item2.icode, price2: item2.price} AS new_map;

  auto in_prop1_key = this->storage.GetPropertyIx("icode");
  auto in_prop2_key = this->storage.GetPropertyIx("price");

  auto out_prop1_key = this->storage.GetPropertyIx("icode1");
  auto out_prop1_val = PROPERTY_LOOKUP(this->dba, "item1", this->dba.NameToProperty("icode"));
  auto out_prop3_key = this->storage.GetPropertyIx("icode2");
  auto out_prop3_val = PROPERTY_LOOKUP(this->dba, "item2", this->dba.NameToProperty("icode"));
  auto out_prop4_key = this->storage.GetPropertyIx("price2");
  auto out_prop4_val = PROPERTY_LOOKUP(this->dba, "item2", this->dba.NameToProperty("price"));

  auto has_properties1 = MAP({in_prop1_key, LITERAL(0000)}, {in_prop2_key, LITERAL(10)});
  auto has_properties2 = MAP({in_prop1_key, LITERAL(1111)}, {in_prop2_key, LITERAL(16)});
  auto new_map = MAP({out_prop1_key, out_prop1_val}, {out_prop3_key, out_prop3_val}, {out_prop4_key, out_prop4_val});
  auto query = QUERY(
      SINGLE_QUERY(WITH(has_properties1, AS("item1"), has_properties2, AS("item2")), RETURN(new_map, AS("new_map"))));

  memgraph::query::MakeSymbolTable(query);

  auto prop1_eval_mode = dynamic_cast<PropertyLookup *>(new_map->elements_[out_prop1_key])->evaluation_mode_;
  auto prop3_eval_mode = dynamic_cast<PropertyLookup *>(new_map->elements_[out_prop3_key])->evaluation_mode_;
  auto prop4_eval_mode = dynamic_cast<PropertyLookup *>(new_map->elements_[out_prop4_key])->evaluation_mode_;

  ASSERT_TRUE(prop1_eval_mode == PropertyLookup::EvaluationMode::GET_OWN_PROPERTY);
  ASSERT_TRUE(prop3_eval_mode == PropertyLookup::EvaluationMode::GET_ALL_PROPERTIES);
  ASSERT_TRUE(prop4_eval_mode == PropertyLookup::EvaluationMode::GET_ALL_PROPERTIES);
}

TYPED_TEST(TestSymbolGenerator, PatternComprehensionInReturn) {
  auto prop = this->dba.NameToProperty("prop");

  // MATCH (n) RETURN [(n)-[edge]->(m) | m.prop] AS alias
  auto query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"))),
      RETURN(NEXPR(
          "alias",
          PATTERN_COMPREHENSION(
              nullptr,
              PATTERN(NODE("n"), EDGE("edge", EdgeAtom::Direction::BOTH, {}, false), NODE("m", std::nullopt, false)),
              nullptr,
              PROPERTY_LOOKUP(this->dba, "m", prop))))));

  auto symbol_table = MakeSymbolTable(query);
  ASSERT_EQ(symbol_table.max_position(), 7);

  memgraph::query::plan::UsedSymbolsCollector collector(symbol_table);
  auto *ret = dynamic_cast<Return *>(query->single_query_->clauses_[1]);
  auto *pc = dynamic_cast<PatternComprehension *>(ret->body_.named_expressions[0]->expression_);

  pc->Accept(collector);

  // n, edge, m, Path
  ASSERT_EQ(collector.symbols_.size(), 1);
}

TYPED_TEST(TestSymbolGenerator, PatternComprehensionInWith) {
  auto prop = this->dba.NameToProperty("prop");

  // MATCH (n) WITH [(n)-[edge]->(m) | m.prop] AS alias RETURN alias
  auto query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"))),
      WITH(NEXPR(
          "alias",
          PATTERN_COMPREHENSION(
              nullptr,
              PATTERN(NODE("n"), EDGE("edge", EdgeAtom::Direction::BOTH, {}, false), NODE("m", std::nullopt, false)),
              nullptr,
              PROPERTY_LOOKUP(this->dba, "m", prop)))),
      RETURN("alias")));

  auto symbol_table = MakeSymbolTable(query);
  ASSERT_EQ(symbol_table.max_position(), 8);

  memgraph::query::plan::UsedSymbolsCollector collector(symbol_table);
  auto *with = dynamic_cast<With *>(query->single_query_->clauses_[1]);
  auto *pc = dynamic_cast<PatternComprehension *>(with->body_.named_expressions[0]->expression_);

  pc->Accept(collector);

  // n, edge, m, Path
  ASSERT_EQ(collector.symbols_.size(), 1);
}

TYPED_TEST(TestSymbolGenerator, ListComprehensionInReturn) {
  // RETURN [x in [1, 2, 3] | x + 1] AS added_numbers
  auto *ident = IDENT("x");
  auto query = QUERY(SINGLE_QUERY(RETURN(
      NEXPR("added_numbers",
            LIST_COMPREHENSION(ident, LIST(LITERAL(1), LITERAL(2), LITERAL(3)), nullptr, ADD(ident, LITERAL(1)))))));

  auto symbol_table = MakeSymbolTable(query);
  ASSERT_EQ(symbol_table.max_position(), 2);

  memgraph::query::plan::UsedSymbolsCollector collector(symbol_table);
  auto *ret = dynamic_cast<Return *>(query->single_query_->clauses_[0]);
  auto *lc = dynamic_cast<ListComprehension *>(ret->body_.named_expressions[0]->expression_);

  lc->Accept(collector);

  // 0, we erase the x symbol as it is only mentioned in the list comprehension
  ASSERT_EQ(collector.symbols_.size(), 0);
}

TYPED_TEST(TestSymbolGenerator, ListComprehensionInWith) {
  // WITH [x in [1, 2, 3] WHERE x = 2 | x + 1] AS added_numbers RETURN added_numbers
  auto *ident = IDENT("x");
  auto query = QUERY(SINGLE_QUERY(
      WITH(NEXPR(
          "added_numbers",
          LIST_COMPREHENSION(
              ident, LIST(LITERAL(1), LITERAL(2), LITERAL(3)), WHERE(EQ(ident, LITERAL(2))), ADD(ident, LITERAL(1))))),
      RETURN("added_numbers")));

  auto symbol_table = MakeSymbolTable(query);
  ASSERT_EQ(symbol_table.max_position(), 3);

  memgraph::query::plan::UsedSymbolsCollector collector(symbol_table);
  auto *with = dynamic_cast<With *>(query->single_query_->clauses_[0]);
  auto *lc = dynamic_cast<ListComprehension *>(with->body_.named_expressions[0]->expression_);

  lc->Accept(collector);

  // 0, we erase the x symbol as it is only mentioned in the list comprehension
  ASSERT_EQ(collector.symbols_.size(), 0);
}

// Shadowing of un-imported outer variables inside a `CALL {}` subquery. Such a name is out of scope in the subquery,
// so a pattern occurrence of it declares a fresh variable rather than referencing the outer one. Referencing it would
// resolve to the outer symbol and make the branch write through its frame slot, which the subquery shares with its
// caller. `SUBQUERY_COMPREHENSION(name)` builds `[(<name>)-->() | 1]`.
//
// Note the subquery RETURNs below list two NamedExpressions rather than a bare name plus one: `RETURN("t", NEXPR(...))`
// selects the `RETURN(expr, AS(name))` overload, which would overwrite the comprehension with an identifier.
#define SUBQUERY_COMPREHENSION(name)                                                                                   \
  PATTERN_COMPREHENSION(                                                                                               \
      nullptr,                                                                                                         \
      PATTERN(                                                                                                         \
          NODE(name), EDGE("anon_edge", EdgeAtom::Direction::OUT, {}, false), NODE("anon_node", std::nullopt, false)), \
      nullptr,                                                                                                         \
      LITERAL(1))

namespace {

const Identifier &ComprehensionStartNode(PatternComprehension *pc) {
  auto *node = dynamic_cast<NodeAtom *>(pc->pattern_->atoms_[0]);
  MG_ASSERT(node, "expected the comprehension's first atom to be a node");
  return *node->identifier_;
}

// A subquery's RETURN rejects an unaliased expression, and the NEXPR helper does not set the flag.
NamedExpression *Aliased(NamedExpression *named_expr) {
  named_expr->is_aliased_ = true;
  return named_expr;
}

}  // namespace

TYPED_TEST(TestSymbolGenerator, PatternComprehensionInSubqueryShadowsUnimportedOuterVariable) {
  // MATCH (p) CALL { MATCH (t) RETURN t, [(p)-->() | 1] AS k } RETURN p, k
  auto *outer_p = NODE("p");
  auto *comprehension = SUBQUERY_COMPREHENSION("p");
  auto *subquery = SINGLE_QUERY(MATCH(PATTERN(NODE("t"))),
                                RETURN(Aliased(NEXPR("t", IDENT("t"))), Aliased(NEXPR("k", comprehension))));
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(outer_p)), CALL_SUBQUERY(subquery), RETURN("p", "k")));

  auto symbol_table = MakeSymbolTable(query);

  const auto outer_symbol = symbol_table.at(*outer_p->identifier_);
  const auto inner_symbol = symbol_table.at(ComprehensionStartNode(comprehension));
  EXPECT_NE(outer_symbol, inner_symbol) << "the un-imported name must be declared afresh, not bound to the outer `p`";
  EXPECT_EQ(outer_symbol.name(), inner_symbol.name());
}

TYPED_TEST(TestSymbolGenerator, PatternComprehensionInSubqueryCorrelatesExplicitlyImportedVariable) {
  // MATCH (p) CALL (p) { MATCH (t) RETURN t, [(p)-->() | 1] AS k } RETURN p, k
  auto *outer_p = NODE("p");
  auto *comprehension = SUBQUERY_COMPREHENSION("p");
  auto *subquery = SINGLE_QUERY(MATCH(PATTERN(NODE("t"))),
                                RETURN(Aliased(NEXPR("t", IDENT("t"))), Aliased(NEXPR("k", comprehension))));
  auto *call_sub = CALL_SUBQUERY(subquery);
  call_sub->has_variable_scope_ = true;
  call_sub->scoped_variables_.push_back(NEXPR("p", IDENT("p")));
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(outer_p)), call_sub, RETURN("p", "k")));

  auto symbol_table = MakeSymbolTable(query);

  EXPECT_EQ(symbol_table.at(*outer_p->identifier_), symbol_table.at(ComprehensionStartNode(comprehension)))
      << "an explicitly imported name is in scope, so it must still correlate";
}

TYPED_TEST(TestSymbolGenerator, PatternComprehensionInSubqueryCorrelatesWithCallStar) {
  // MATCH (p) CALL (*) { MATCH (t) RETURN t, [(p)-->() | 1] AS k } RETURN p, k
  auto *outer_p = NODE("p");
  auto *comprehension = SUBQUERY_COMPREHENSION("p");
  auto *subquery = SINGLE_QUERY(MATCH(PATTERN(NODE("t"))),
                                RETURN(Aliased(NEXPR("t", IDENT("t"))), Aliased(NEXPR("k", comprehension))));
  auto *call_sub = CALL_SUBQUERY(subquery);
  call_sub->has_variable_scope_ = true;
  call_sub->all_variables_scoped_ = true;
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(outer_p)), call_sub, RETURN("p", "k")));

  auto symbol_table = MakeSymbolTable(query);

  EXPECT_EQ(symbol_table.at(*outer_p->identifier_), symbol_table.at(ComprehensionStartNode(comprehension)))
      << "CALL (*) imports every user-declared outer variable";
}

TYPED_TEST(TestSymbolGenerator, PatternComprehensionInNestedSubqueryReshadowsImportedVariable) {
  // MATCH (p) CALL (p) { CALL { MATCH (t) RETURN t, [(p)-->() | 1] AS k } RETURN t, k } RETURN p, k
  // The inner `CALL {}` imports nothing, so `p` is out of scope again even though the outer subquery imported it.
  auto *outer_p = NODE("p");
  auto *comprehension = SUBQUERY_COMPREHENSION("p");
  auto *inner_subquery = SINGLE_QUERY(MATCH(PATTERN(NODE("t"))),
                                      RETURN(Aliased(NEXPR("t", IDENT("t"))), Aliased(NEXPR("k", comprehension))));
  auto *outer_subquery = SINGLE_QUERY(CALL_SUBQUERY(inner_subquery), RETURN("t", "k"));
  auto *call_sub = CALL_SUBQUERY(outer_subquery);
  call_sub->has_variable_scope_ = true;
  call_sub->scoped_variables_.push_back(NEXPR("p", IDENT("p")));
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(outer_p)), call_sub, RETURN("p", "k")));

  auto symbol_table = MakeSymbolTable(query);

  EXPECT_NE(symbol_table.at(*outer_p->identifier_), symbol_table.at(ComprehensionStartNode(comprehension)))
      << "the innermost CALL {} imported nothing, so `p` is out of scope there too";
}

TYPED_TEST(TestSymbolGenerator, PatternComprehensionInSubqueryCorrelatesItsOwnScopeVariable) {
  // MATCH (p) CALL { MATCH (t) RETURN t, [(t)-->() | 1] AS k } RETURN t, k
  // `t` is declared inside the subquery, so it is in scope there and must correlate, not be shadowed.
  auto *inner_t = NODE("t");
  auto *comprehension = SUBQUERY_COMPREHENSION("t");
  auto *subquery = SINGLE_QUERY(MATCH(PATTERN(inner_t)),
                                RETURN(Aliased(NEXPR("t", IDENT("t"))), Aliased(NEXPR("k", comprehension))));
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("p"))), CALL_SUBQUERY(subquery), RETURN("t", "k")));

  auto symbol_table = MakeSymbolTable(query);

  EXPECT_EQ(symbol_table.at(*inner_t->identifier_), symbol_table.at(ComprehensionStartNode(comprehension)))
      << "a name declared inside the subquery is in scope there, so shadowing must not fire";
}

TYPED_TEST(TestSymbolGenerator, PatternComprehensionAtTopLevelDeclaresUndeclaredNameAfresh) {
  // MATCH (t) RETURN [(zz)-->() | 1] AS k -- no CALL {} anywhere, so the rule must not fire differently here
  auto *comprehension = SUBQUERY_COMPREHENSION("zz");
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("t"))), RETURN(NEXPR("k", comprehension))));

  auto symbol_table = MakeSymbolTable(query);

  EXPECT_EQ(symbol_table.at(ComprehensionStartNode(comprehension)).name(), "zz");
}

// Moved here from `subqueries.feature` because that suite also runs with `USING PARALLEL EXECUTION`, and this shape
// (an aggregation inside a `CALL {}`) hits a pre-existing parallel-executor bug that nulls every outer variable --
// unrelated to what the scenario is checking. Asserting the symbol identity pins the rule directly instead.
TYPED_TEST(TestSymbolGenerator, PatternComprehensionInAggregatingSubqueryWhereShadowsUnimportedOuterVariable) {
  // MATCH (p) CALL { MATCH (t) WITH t, count(*) AS c WHERE size([(p)-->() | 1]) > 0 RETURN t } RETURN p
  auto *outer_p = NODE("p");
  auto *comprehension = SUBQUERY_COMPREHENSION("p");
  auto *subquery = SINGLE_QUERY(MATCH(PATTERN(NODE("t"))),
                                WITH(IDENT("t"), AS("t"), COUNT(LITERAL(1), false), AS("c")),
                                WHERE(GREATER(comprehension, LITERAL(0))),
                                RETURN(Aliased(NEXPR("t", IDENT("t")))));
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(outer_p)), CALL_SUBQUERY(subquery), RETURN("p")));

  auto symbol_table = MakeSymbolTable(query);

  const auto outer_symbol = symbol_table.at(*outer_p->identifier_);
  const auto inner_symbol = symbol_table.at(ComprehensionStartNode(comprehension));
  EXPECT_NE(outer_symbol, inner_symbol) << "an aggregating WHERE must not correlate the un-imported outer `p` either";
  EXPECT_EQ(outer_symbol.name(), inner_symbol.name());
}

TYPED_TEST(TestSymbolGenerator, PlainMatchInSubqueryShadowsUnimportedOuterVariable) {
  // MATCH (a) CALL { MATCH (a)-[:R]->(x) RETURN x } RETURN a, x
  // The subquery's own MATCH pattern is the same case as the comprehension: `a` is out of scope, so the pattern
  // declares it afresh. Binding it to the outer symbol made the branch's scan overwrite the caller's frame slot,
  // which dropped every outer row but the last.
  auto *outer_a = NODE("a");
  auto *inner_a = NODE("a");
  auto *subquery = SINGLE_QUERY(
      MATCH(PATTERN(inner_a, EDGE("anon_edge", EdgeAtom::Direction::OUT, {}, false), NODE("x"))), RETURN("x"));
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(outer_a)), CALL_SUBQUERY(subquery), RETURN("a", "x")));

  auto symbol_table = MakeSymbolTable(query);

  EXPECT_NE(symbol_table.at(*outer_a->identifier_), symbol_table.at(*inner_a->identifier_))
      << "a subquery MATCH pattern must declare the un-imported name afresh, leaving the outer `a` intact";
}

TYPED_TEST(TestSymbolGenerator, CreateInSubqueryMayRedeclareUnimportedOuterVariable) {
  // MATCH (p) CALL { CREATE (p) RETURN p AS q } RETURN q
  // `p` is out of scope in the subquery, so the CREATE declares a fresh node instead of raising a redeclaration
  // error, and the outer `p` keeps its own frame slot.
  auto *outer_p = NODE("p");
  auto *created_p = NODE("p");
  auto *subquery = SINGLE_QUERY(CREATE(PATTERN(created_p)), RETURN(IDENT("p"), AS("q")));
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(outer_p)), CALL_SUBQUERY(subquery), RETURN("q")));

  auto symbol_table = MakeSymbolTable(query);

  EXPECT_NE(symbol_table.at(*outer_p->identifier_), symbol_table.at(*created_p->identifier_))
      << "the created node must be a fresh symbol, so the outer `p` survives the subquery";
}

TYPED_TEST(TestSymbolGenerator, SubqueryReturningShadowingNameStillCollidesWithOuterScope) {
  // MATCH (p) CALL { CREATE (p) RETURN p } RETURN p
  // Returning the shadowing name under its own name would collide with the caller's `p`; still rejected.
  auto *subquery = SINGLE_QUERY(CREATE(PATTERN(NODE("p"))), RETURN("p"));
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("p"))), CALL_SUBQUERY(subquery), RETURN("p")));

  EXPECT_THROW(MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, ExistsPatternInSubqueryRejectsUnimportedOuterVariable) {
  // MATCH (p) CALL { MATCH (t) WHERE exists((p)-->()) RETURN t } RETURN t
  // A pattern expression may not introduce variables, so the out-of-scope `p` is an unbound reference.
  auto *exists = EXISTS(PATTERN(
      NODE("p"), EDGE("anon_edge", EdgeAtom::Direction::OUT, {}, false), NODE("anon_node", std::nullopt, false)));
  auto *subquery = SINGLE_QUERY(MATCH(PATTERN(NODE("t"))), WHERE(exists), RETURN("t"));
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("p"))), CALL_SUBQUERY(subquery), RETURN("t")));

  EXPECT_THROW(MakeSymbolTable(query), SemanticException);
}

#undef SUBQUERY_COMPREHENSION

// A pattern comprehension may not reference a symbol the same CREATE clause declares. The operator that binds the
// symbol is the one that reads the comprehension's result, so the frame slot is unwritten no matter where the
// RollUpApply is spliced. A comprehension over a symbol bound by an *earlier* clause is fine and must keep working -
// that is the whole point of correlating them.
namespace {

// NODE() has no properties overload, so set the map directly. The variant's first alternative is the map, which is
// what a default-constructed NodeAtom holds.
NodeAtom *WithProperty(AstStorage &storage, NodeAtom *node, const std::string &name, Expression *value) {
  std::get<std::unordered_map<PropertyIx, Expression *>>(node->properties_)[storage.GetPropertyIx(name)] = value;
  return node;
}

}  // namespace

// `[(<name>)-->() | 1]`
#define SELF_COMPREHENSION(name)                                                                                       \
  PATTERN_COMPREHENSION(                                                                                               \
      nullptr,                                                                                                         \
      PATTERN(                                                                                                         \
          NODE(name), EDGE("anon_edge", EdgeAtom::Direction::OUT, {}, false), NODE("anon_node", std::nullopt, false)), \
      nullptr,                                                                                                         \
      LITERAL(1))

TYPED_TEST(TestSymbolGenerator, PatternComprehensionOverNodeCreatedBySameClauseIsRejected) {
  // CREATE (q:L {c: [(q)-->() | 1]})
  auto *created = WithProperty(this->storage, NODE("q", "L"), "c", SELF_COMPREHENSION("q"));
  auto *query = QUERY(SINGLE_QUERY(CREATE(PATTERN(created))));

  EXPECT_THROW(MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, PatternComprehensionOverNodeCreatedBySameClauseInForeachIsRejected) {
  // FOREACH (i IN [1] | CREATE (q:L {c: [(q)-->() | 1]}))
  // The FOREACH-wrapped form is the one that regressed: before the fix the merge-base planned it with an uncorrelated
  // scan and completed with a wrong count, and afterwards it failed with an internal error.
  auto *created = WithProperty(this->storage, NODE("q", "L"), "c", SELF_COMPREHENSION("q"));
  auto *query = QUERY(SINGLE_QUERY(FOREACH(NEXPR("i", LIST(LITERAL(1))), {CREATE(PATTERN(created))})));

  EXPECT_THROW(MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, PatternComprehensionOverNodeCreatedByEarlierPatternInSameClauseIsRejected) {
  // CREATE (a:L), (q:L {c: [(a)-->() | 1]})
  auto *created = WithProperty(this->storage, NODE("q", "L"), "c", SELF_COMPREHENSION("a"));
  auto *query = QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("a", "L")), PATTERN(created))));

  EXPECT_THROW(MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, PatternComprehensionReferencingCreatedNodeOnlyInItsFilterIsRejected) {
  // CREATE (q:L {c: [(z)-->() WHERE z = q | 1]})
  // The reference is outside the comprehension's pattern, so it resolves through a different branch of
  // Visit(Identifier) than the cases above.
  auto *comprehension = PATTERN_COMPREHENSION(
      nullptr,
      PATTERN(
          NODE("z"), EDGE("anon_edge", EdgeAtom::Direction::OUT, {}, false), NODE("anon_node", std::nullopt, false)),
      WHERE(EQ(IDENT("z"), IDENT("q"))),
      LITERAL(1));
  auto *created = WithProperty(this->storage, NODE("q", "L"), "c", comprehension);
  auto *query = QUERY(SINGLE_QUERY(CREATE(PATTERN(created))));

  EXPECT_THROW(MakeSymbolTable(query), SemanticException);
}

TYPED_TEST(TestSymbolGenerator, PatternComprehensionOverNodeCreatedByAnEarlierCreateClauseIsAccepted) {
  // CREATE (a:L) CREATE (q:L {c: [(a)-->() | 1]})
  // A separate clause binds `a`, so the comprehension is drained after it and correlates normally.
  auto *created = WithProperty(this->storage, NODE("q", "L"), "c", SELF_COMPREHENSION("a"));
  auto *query = QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("a", "L"))), CREATE(PATTERN(created))));

  EXPECT_NO_THROW(MakeSymbolTable(query));
}

TYPED_TEST(TestSymbolGenerator, PatternComprehensionOverMatchedNodeInsideCreateIsAccepted) {
  // MATCH (p) CREATE (q:L {c: [(p)-->() | 1]})
  auto *created = WithProperty(this->storage, NODE("q", "L"), "c", SELF_COMPREHENSION("p"));
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("p"))), CREATE(PATTERN(created))));

  EXPECT_NO_THROW(MakeSymbolTable(query));
}

TYPED_TEST(TestSymbolGenerator, PatternComprehensionOverItsOwnNodesInsideCreateIsAccepted) {
  // CREATE (q:L {c: [(z)-->() | 1]})
  // Nothing the CREATE binds is referenced, so the comprehension stands alone.
  auto *created = WithProperty(this->storage, NODE("q", "L"), "c", SELF_COMPREHENSION("z"));
  auto *query = QUERY(SINGLE_QUERY(CREATE(PATTERN(created))));

  EXPECT_NO_THROW(MakeSymbolTable(query));
}

#undef SELF_COMPREHENSION
