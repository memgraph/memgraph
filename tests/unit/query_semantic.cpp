#include <memory>

#include "gtest/gtest.h"

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/interpret/interpret.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"

using namespace query;

namespace {

// Build a simple AST which describes:
// MATCH (node_atom_1) RETURN node_atom_1 AS node_atom_1
Query *MatchNodeReturn(AstTreeStorage &storage) {
  auto node_atom = storage.Create<NodeAtom>();
  node_atom->identifier_ = storage.Create<Identifier>("node_atom_1");
  auto pattern = storage.Create<Pattern>();
  pattern->atoms_.emplace_back(node_atom);
  auto match = storage.Create<Match>();
  match->patterns_.emplace_back(pattern);
  auto query = storage.query();
  query->clauses_.emplace_back(match);

  auto named_expr = storage.Create<NamedExpression>();
  named_expr->name_ = "node_atom_1";
  named_expr->expression_ = storage.Create<Identifier>("node_atom_1");
  auto ret = storage.Create<Return>();
  ret->named_expressions_.emplace_back(named_expr);
  query->clauses_.emplace_back(ret);
  return query;
}

// AST using variable in return bound by naming the previous return expression.
// This is treated as an unbound variable.
// MATCH (node_atom_1) RETURN node_atom_1 AS n, n AS n
Query *MatchUnboundMultiReturn(AstTreeStorage &storage) {
  auto node_atom = storage.Create<NodeAtom>();
  node_atom->identifier_ = storage.Create<Identifier>("node_atom_1");
  auto pattern = storage.Create<Pattern>();
  pattern->atoms_.emplace_back(node_atom);
  auto match = storage.Create<Match>();
  match->patterns_.emplace_back(pattern);
  auto query = storage.query();
  query->clauses_.emplace_back(match);

  auto named_expr_1 = storage.Create<NamedExpression>();
  named_expr_1->name_ = "n";
  named_expr_1->expression_ = storage.Create<Identifier>("node_atom_1");
  auto named_expr_2 = storage.Create<NamedExpression>();
  named_expr_2->name_ = "n";
  named_expr_2->expression_ = storage.Create<Identifier>("n");
  auto ret = storage.Create<Return>();
  ret->named_expressions_.emplace_back(named_expr_1);
  ret->named_expressions_.emplace_back(named_expr_2);
  query->clauses_.emplace_back(ret);
  return query;
}

// AST with unbound variable in return: MATCH (n) RETURN x AS x
Query *MatchNodeUnboundReturn(AstTreeStorage &storage) {
  auto node_atom = storage.Create<NodeAtom>();
  node_atom->identifier_ = storage.Create<Identifier>("n");
  auto pattern = storage.Create<Pattern>();
  pattern->atoms_.emplace_back(node_atom);
  auto match = storage.Create<Match>();
  match->patterns_.emplace_back(pattern);
  auto query = storage.query();
  query->clauses_.emplace_back(match);

  auto named_expr = storage.Create<NamedExpression>();
  named_expr->name_ = "x";
  named_expr->expression_ = storage.Create<Identifier>("x");
  auto ret = storage.Create<Return>();
  ret->named_expressions_.emplace_back(named_expr);
  query->clauses_.emplace_back(ret);
  return query;
}

TEST(TestSymbolGenerator, MatchNodeReturn) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  auto query_ast = MatchNodeReturn(storage);
  SymbolGenerator symbol_generator(symbol_table);
  query_ast->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.max_position(), 2);
  auto match = dynamic_cast<Match*>(query_ast->clauses_[0]);
  auto pattern = match->patterns_[0];
  auto node_atom = dynamic_cast<NodeAtom*>(pattern->atoms_[0]);
  auto node_sym = symbol_table[*node_atom->identifier_];
  EXPECT_EQ(node_sym.name_, "node_atom_1");
  auto ret = dynamic_cast<Return*>(query_ast->clauses_[1]);
  auto named_expr = ret->named_expressions_[0];
  auto column_sym = symbol_table[*named_expr];
  EXPECT_EQ(node_sym.name_, column_sym.name_);
  EXPECT_NE(node_sym, column_sym);
  auto ret_sym = symbol_table[*named_expr->expression_];
  EXPECT_EQ(node_sym, ret_sym);
}

TEST(TestSymbolGenerator, MatchUnboundMultiReturn) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  auto query_ast = MatchUnboundMultiReturn(storage);
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, MatchNodeUnboundReturn) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  auto query_ast = MatchNodeUnboundReturn(storage);
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), SemanticException);
}
}
