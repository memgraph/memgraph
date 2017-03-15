#include <memory>

#include "gtest/gtest.h"

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/interpret/interpret.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"

using namespace query;

// Build a simple AST which describes:
// MATCH (node_atom_1) RETURN node_atom_1 AS node_atom_1
static std::unique_ptr<Query> MatchNodeReturn() {
  int uid = 0;
  auto node_atom = std::make_shared<NodeAtom>(uid++);
  node_atom->identifier_ = std::make_shared<Identifier>(uid++, "node_atom_1");
  auto pattern = std::make_shared<Pattern>(uid++);
  pattern->atoms_.emplace_back(node_atom);
  auto match = std::make_shared<Match>(uid++);
  match->patterns_.emplace_back(pattern);
  auto query = std::make_unique<Query>(uid++);
  query->clauses_.emplace_back(match);

  auto named_expr = std::make_shared<NamedExpression>(uid++);
  named_expr->name_ = "node_atom_1";
  named_expr->expression_ = std::make_shared<Identifier>(uid++, "node_atom_1");
  auto ret = std::make_shared<Return>(uid++);
  ret->named_expressions_.emplace_back(named_expr);
  query->clauses_.emplace_back(ret);
  return query;
}

// AST using variable in return bound by naming the previous return expression.
// This is treated as an unbound variable.
// MATCH (node_atom_1) RETURN node_atom_1 AS n, n AS n
static std::unique_ptr<Query> MatchUnboundMultiReturn() {
  int uid = 0;
  auto node_atom = std::make_shared<NodeAtom>(uid++);
  node_atom->identifier_ = std::make_shared<Identifier>(uid++, "node_atom_1");
  auto pattern = std::make_shared<Pattern>(uid++);
  pattern->atoms_.emplace_back(node_atom);
  auto match = std::make_shared<Match>(uid++);
  match->patterns_.emplace_back(pattern);
  auto query = std::make_unique<Query>(uid++);
  query->clauses_.emplace_back(match);

  auto named_expr_1 = std::make_shared<NamedExpression>(uid++);
  named_expr_1->name_ = "n";
  named_expr_1->expression_ = std::make_shared<Identifier>(uid++, "node_atom_1");
  auto named_expr_2 = std::make_shared<NamedExpression>(uid++);
  named_expr_2->name_ = "n";
  named_expr_2->expression_ = std::make_shared<Identifier>(uid++, "n");
  auto ret = std::make_shared<Return>(uid++);
  ret->named_expressions_.emplace_back(named_expr_1);
  ret->named_expressions_.emplace_back(named_expr_2);
  query->clauses_.emplace_back(ret);
  return query;
}

// AST with unbound variable in return: MATCH (n) RETURN x AS x
static std::unique_ptr<Query> MatchNodeUnboundReturn() {
  int uid = 0;
  auto node_atom = std::make_shared<NodeAtom>(uid++);
  node_atom->identifier_ = std::make_shared<Identifier>(uid++, "n");
  auto pattern = std::make_shared<Pattern>(uid++);
  pattern->atoms_.emplace_back(node_atom);
  auto match = std::make_shared<Match>(uid++);
  match->patterns_.emplace_back(pattern);
  auto query = std::make_unique<Query>(uid++);
  query->clauses_.emplace_back(match);

  auto named_expr = std::make_shared<NamedExpression>(uid++);
  named_expr->name_ = "x";
  named_expr->expression_ = std::make_shared<Identifier>(uid++, "x");
  auto ret = std::make_shared<Return>(uid++);
  ret->named_expressions_.emplace_back(named_expr);
  query->clauses_.emplace_back(ret);
  return query;
}

TEST(TestSymbolGenerator, MatchNodeReturn) {
  SymbolTable symbol_table;
  auto query_ast = MatchNodeReturn();
  SymbolGenerator symbol_generator(symbol_table);
  query_ast->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.max_position(), 2);
  auto match = std::dynamic_pointer_cast<Match>(query_ast->clauses_[0]);
  auto pattern = match->patterns_[0];
  auto node_atom = std::dynamic_pointer_cast<NodeAtom>(pattern->atoms_[0]);
  auto node_sym = symbol_table[*node_atom->identifier_];
  EXPECT_EQ(node_sym.name_, "node_atom_1");
  auto ret = std::dynamic_pointer_cast<Return>(query_ast->clauses_[1]);
  auto named_expr = ret->named_expressions_[0];
  auto column_sym = symbol_table[*named_expr];
  EXPECT_EQ(node_sym.name_, column_sym.name_);
  EXPECT_NE(node_sym, column_sym);
  auto ret_sym = symbol_table[*named_expr->expression_];
  EXPECT_EQ(node_sym, ret_sym);
}

TEST(TestSymbolGenerator, MatchUnboundMultiReturn) {
  SymbolTable symbol_table;
  auto query_ast = MatchUnboundMultiReturn();
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, MatchNodeUnboundReturn) {
  SymbolTable symbol_table;
  auto query_ast = MatchNodeUnboundReturn();
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), SemanticException);
}
