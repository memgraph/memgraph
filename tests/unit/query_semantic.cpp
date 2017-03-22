#include <memory>

#include "gtest/gtest.h"

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/interpret/interpret.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"

using namespace query;

namespace {

// Returns a `(name1) -[name2]- (name3) ...` pattern.
auto GetPattern(AstTreeStorage &storage, std::vector<std::string> names) {
  bool is_node{true};
  auto pattern = storage.Create<Pattern>();
  for (auto &name : names) {
    PatternAtom *atom;
    auto identifier = storage.Create<Identifier>(name);
    if (is_node) {
      atom = storage.Create<NodeAtom>(identifier);
    } else {
      atom = storage.Create<EdgeAtom>(identifier);
    }
    pattern->atoms_.emplace_back(atom);
    is_node = !is_node;
  }
  return pattern;
}

// Returns a `MATCH (node)` clause.
auto GetMatchNode(AstTreeStorage &storage, const std::string &node_name) {
  auto match = storage.Create<Match>();
  match->patterns_.emplace_back(GetPattern(storage, {node_name}));
  return match;
}

// Build a simple AST which describes:
// MATCH (node_atom_1) RETURN node_atom_1 AS node_atom_1
Query *MatchNodeReturn(AstTreeStorage &storage) {
  auto match = GetMatchNode(storage, "node_atom_1");
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
  auto match = GetMatchNode(storage, "node_atom_1");
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
  auto match = GetMatchNode(storage, "n");
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

// AST with match pattern referencing an edge multiple times:
// MATCH (n) -[r]-> (n) -[r]-> (n) RETURN r AS r
// This usually throws a redeclaration error, but we support it.
Query *MatchSameEdge(AstTreeStorage &storage) {
  auto match = storage.Create<Match>();
  match->patterns_.emplace_back(GetPattern(storage, {"n", "r", "n", "r", "n"}));
  auto query = storage.query();
  query->clauses_.emplace_back(match);

  auto named_expr = storage.Create<NamedExpression>();
  named_expr->name_ = "r";
  named_expr->expression_ = storage.Create<Identifier>("r");
  auto ret = storage.Create<Return>();
  ret->named_expressions_.emplace_back(named_expr);
  query->clauses_.emplace_back(ret);
  return query;
}

std::string prop_name = "prop";

// AST with unbound variable in create: CREATE ({prop: x})
Query *CreatePropertyUnbound(AstTreeStorage &storage) {
  auto prop_expr = storage.Create<Identifier>("x");
  auto node_atom = storage.Create<NodeAtom>();
  node_atom->identifier_ = storage.Create<Identifier>("anon");
  node_atom->properties_[&prop_name] = prop_expr;
  auto pattern = storage.Create<Pattern>();
  pattern->atoms_.emplace_back(node_atom);
  auto create = storage.Create<Create>();
  create->patterns_.emplace_back(pattern);
  auto query = storage.query();
  query->clauses_.emplace_back(create);
  return query;
}

// Simple AST returning a created node: CREATE (n) RETURN n
Query *CreateNodeReturn(AstTreeStorage &storage) {
  auto node_atom = storage.Create<NodeAtom>();
  node_atom->identifier_ = storage.Create<Identifier>("n");
  auto pattern = storage.Create<Pattern>();
  pattern->atoms_.emplace_back(node_atom);
  auto create = storage.Create<Create>();
  create->patterns_.emplace_back(pattern);
  auto query = storage.query();
  query->clauses_.emplace_back(create);

  auto named_expr = storage.Create<NamedExpression>();
  named_expr->name_ = "n";
  named_expr->expression_ = storage.Create<Identifier>("n");
  auto ret = storage.Create<Return>();
  ret->named_expressions_.emplace_back(named_expr);
  query->clauses_.emplace_back(ret);
  return query;
}

// AST with redeclaring a variable when creating nodes: CREATE (n), (n)
Query *CreateRedeclareNode(AstTreeStorage &storage) {
  auto create = storage.Create<Create>();
  for (int patterns = 0; patterns < 2; ++patterns) {
    auto pattern = storage.Create<Pattern>();
    auto node_atom = storage.Create<NodeAtom>();
    node_atom->identifier_ = storage.Create<Identifier>("n");
    pattern->atoms_.emplace_back(node_atom);
    create->patterns_.emplace_back(pattern);
  }
  auto query = storage.query();
  query->clauses_.emplace_back(create);
  return query;
}

// AST with redeclaring a variable when creating nodes with multiple creates:
// CREATE (n) CREATE (n)
Query *MultiCreateRedeclareNode(AstTreeStorage &storage) {
  auto query = storage.query();

  for (int creates = 0; creates < 2; ++creates) {
    auto pattern = storage.Create<Pattern>();
    auto node_atom = storage.Create<NodeAtom>();
    node_atom->identifier_ = storage.Create<Identifier>("n");
    pattern->atoms_.emplace_back(node_atom);
    auto create = storage.Create<Create>();
    create->patterns_.emplace_back(pattern);
    query->clauses_.emplace_back(create);
  }
  return query;
}

// AST with redeclaring a match node variable in create: MATCH (n) CREATE (n)
Query *MatchCreateRedeclareNode(AstTreeStorage &storage) {
  auto match = GetMatchNode(storage, "n");
  auto query = storage.query();
  query->clauses_.emplace_back(match);

  auto node_atom_2 = storage.Create<NodeAtom>();
  node_atom_2->identifier_ = storage.Create<Identifier>("n");
  auto pattern_2 = storage.Create<Pattern>();
  pattern_2->atoms_.emplace_back(node_atom_2);
  auto create = storage.Create<Create>();
  create->patterns_.emplace_back(pattern_2);
  query->clauses_.emplace_back(create);
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
  EXPECT_THROW(query_ast->Accept(symbol_generator), UnboundVariableError);
}

TEST(TestSymbolGenerator, MatchNodeUnboundReturn) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  auto query_ast = MatchNodeUnboundReturn(storage);
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), UnboundVariableError);
}

TEST(TestSymbolGenerator, MatchSameEdge) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  auto query_ast = MatchSameEdge(storage);
  SymbolGenerator symbol_generator(symbol_table);
  query_ast->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.max_position(), 3);
  auto match = dynamic_cast<Match*>(query_ast->clauses_[0]);
  auto pattern = match->patterns_[0];
  std::vector<Symbol> node_symbols;
  std::vector<Symbol> edge_symbols;
  bool is_node{true};
  for (auto &atom : pattern->atoms_) {
    auto symbol = symbol_table[*atom->identifier_];
    if (is_node) {
      node_symbols.emplace_back(symbol);
    } else {
      edge_symbols.emplace_back(symbol);
    }
    is_node = !is_node;
  }
  auto &node_symbol = node_symbols.front();
  for (auto &symbol : node_symbols) {
    EXPECT_EQ(node_symbol, symbol);
  }
  auto &edge_symbol = edge_symbols.front();
  for (auto &symbol : edge_symbols) {
    EXPECT_EQ(edge_symbol, symbol);
  }
  auto ret = dynamic_cast<Return*>(query_ast->clauses_[1]);
  auto named_expr = ret->named_expressions_[0];
  auto ret_symbol = symbol_table[*named_expr->expression_];
  EXPECT_EQ(edge_symbol, ret_symbol);
}

TEST(TestSymbolGenerator, CreatePropertyUnbound) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  auto query_ast = CreatePropertyUnbound(storage);
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), UnboundVariableError);
}

TEST(TestSymbolGenerator, CreateNodeReturn) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  auto query_ast = CreateNodeReturn(storage);
  SymbolGenerator symbol_generator(symbol_table);
  query_ast->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.max_position(), 2);
  auto create = dynamic_cast<Create*>(query_ast->clauses_[0]);
  auto pattern = create->patterns_[0];
  auto node_atom = dynamic_cast<NodeAtom*>(pattern->atoms_[0]);
  auto node_sym = symbol_table[*node_atom->identifier_];
  EXPECT_EQ(node_sym.name_, "n");
  auto ret = dynamic_cast<Return*>(query_ast->clauses_[1]);
  auto named_expr = ret->named_expressions_[0];
  auto column_sym = symbol_table[*named_expr];
  EXPECT_EQ(node_sym.name_, column_sym.name_);
  EXPECT_NE(node_sym, column_sym);
  auto ret_sym = symbol_table[*named_expr->expression_];
  EXPECT_EQ(node_sym, ret_sym);
}

TEST(TestSymbolGenerator, CreateRedeclareNode) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  auto query_ast = CreateRedeclareNode(storage);
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), RedeclareVariableError);
}

TEST(TestSymbolGenerator, MultiCreateRedeclareNode) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  auto query_ast = MultiCreateRedeclareNode(storage);
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), RedeclareVariableError);
}

TEST(TestSymbolGenerator, MatchCreateRedeclareNode) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  auto query_ast = MatchCreateRedeclareNode(storage);
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), RedeclareVariableError);
}

TEST(TestSymbolGenerator, MatchCreateRedeclareEdge) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  // AST with redeclaring a match edge variable in create:
  // MATCH (n) -[r]- (m) CREATE (n) -[r] -> (l)
  auto match = storage.Create<Match>();
  match->patterns_.emplace_back(GetPattern(storage, {"n", "r", "m"}));
  auto query = storage.query();
  query->clauses_.emplace_back(match);

  auto create = storage.Create<Create>();
  auto pattern = GetPattern(storage, {"n", "r", "l"});
  auto edge_atom = dynamic_cast<EdgeAtom*>(pattern->atoms_[1]);
  edge_atom->direction_ = EdgeAtom::Direction::RIGHT;
  std::string relationship("relationship");
  edge_atom->edge_types_.emplace_back(&relationship);
  create->patterns_.emplace_back(pattern);
  query->clauses_.emplace_back(create);
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), RedeclareVariableError);
}

TEST(TestSymbolGenerator, MatchTypeMismatch) {
  AstTreeStorage storage;
  // Using an edge variable as a node causes a type mismatch.
  // MATCH (n) -[r]-> (r)
  auto match = storage.Create<Match>();
  match->patterns_.emplace_back(GetPattern(storage, {"n", "r", "r"}));
  auto query = storage.query();
  query->clauses_.emplace_back(match);
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), TypeMismatchError);
}

TEST(TestSymbolGenerator, MatchCreateTypeMismatch) {
  AstTreeStorage storage;
  // Using an edge variable as a node causes a type mismatch.
  // MATCH (n1) -[r1]- (n2) CREATE (r1) -[r2]-> (n2)
  auto match = storage.Create<Match>();
  match->patterns_.emplace_back(GetPattern(storage, {"n1", "r1", "n2"}));
  auto query = storage.query();
  query->clauses_.emplace_back(match);
  auto create = storage.Create<Create>();
  auto pattern = GetPattern(storage, {"r1", "r2", "n2"});
  create->patterns_.emplace_back(pattern);
  auto edge_atom = dynamic_cast<EdgeAtom*>(pattern->atoms_[1]);
  edge_atom->direction_ = EdgeAtom::Direction::RIGHT;
  query->clauses_.emplace_back(create);
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), TypeMismatchError);
}

TEST(TestSymbolGenerator, CreateMultipleEdgeType) {
  AstTreeStorage storage;
  // Multiple edge relationship are not allowed when creating edges.
  // CREATE (n) -[r :rel1 | :rel2]-> (m)
  auto pattern = GetPattern(storage, {"n", "r", "m"});
  auto edge_atom = dynamic_cast<EdgeAtom*>(pattern->atoms_[1]);
  edge_atom->direction_ = EdgeAtom::Direction::RIGHT;
  std::string rel1("rel1");
  edge_atom->edge_types_.emplace_back(&rel1);
  std::string rel2("rel2");
  edge_atom->edge_types_.emplace_back(&rel2);
  auto create = storage.Create<Create>();
  create->patterns_.emplace_back(pattern);
  auto query = storage.query();
  query->clauses_.emplace_back(create);
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, CreateBidirectionalEdge) {
  AstTreeStorage storage;
  // Bidirectional relationships are not allowed when creating edges.
  // CREATE (n) -[r :rel1]- (m)
  auto pattern = GetPattern(storage, {"n", "r", "m"});
  auto edge_atom = dynamic_cast<EdgeAtom*>(pattern->atoms_[1]);
  edge_atom->direction_ = EdgeAtom::Direction::BOTH;
  std::string rel1("rel1");
  edge_atom->edge_types_.emplace_back(&rel1);
  std::string rel2("rel2");
  edge_atom->edge_types_.emplace_back(&rel2);
  auto create = storage.Create<Create>();
  create->patterns_.emplace_back(pattern);
  auto query = storage.query();
  query->clauses_.emplace_back(create);
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

}
