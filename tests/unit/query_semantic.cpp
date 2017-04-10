#include <memory>

#include "gtest/gtest.h"

#include "dbms/dbms.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/interpret/interpret.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"

#include "query_common.hpp"

using namespace query;

namespace {

TEST(TestSymbolGenerator, MatchNodeReturn) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  // MATCH (node_atom_1) RETURN node_atom_1 AS node_atom_1
  auto query_ast = QUERY(MATCH(PATTERN(NODE("node_atom_1"))),
                         RETURN(IDENT("node_atom_1"), AS("node_atom_1")));
  SymbolGenerator symbol_generator(symbol_table);
  query_ast->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.max_position(), 2);
  auto match = dynamic_cast<Match *>(query_ast->clauses_[0]);
  auto pattern = match->patterns_[0];
  auto node_atom = dynamic_cast<NodeAtom *>(pattern->atoms_[0]);
  auto node_sym = symbol_table[*node_atom->identifier_];
  EXPECT_EQ(node_sym.name_, "node_atom_1");
  EXPECT_EQ(node_sym.type_, Symbol::Type::Vertex);
  auto ret = dynamic_cast<Return *>(query_ast->clauses_[1]);
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
  // AST using variable in return bound by naming the previous return
  // expression. This is treated as an unbound variable.
  // MATCH (node_atom_1) RETURN node_atom_1 AS n, n AS n
  auto query_ast =
      QUERY(MATCH(PATTERN(NODE("node_atom_1"))),
            RETURN(IDENT("node_atom_1"), AS("n"), IDENT("n"), AS("n")));
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), UnboundVariableError);
}

TEST(TestSymbolGenerator, MatchNodeUnboundReturn) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  // AST with unbound variable in return: MATCH (n) RETURN x AS x
  auto query_ast =
      QUERY(MATCH(PATTERN(NODE("n"))), RETURN(IDENT("x"), AS("x")));
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), UnboundVariableError);
}

TEST(TestSymbolGenerator, MatchSameEdge) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  // AST with match pattern referencing an edge multiple times:
  // MATCH (n) -[r]- (n) -[r]- (n) RETURN r AS r
  // This usually throws a redeclaration error, but we support it.
  auto query_ast = QUERY(
      MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("n"), EDGE("r"), NODE("n"))),
      RETURN(IDENT("r"), AS("r")));
  SymbolGenerator symbol_generator(symbol_table);
  query_ast->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.max_position(), 3);
  auto match = dynamic_cast<Match *>(query_ast->clauses_[0]);
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
  EXPECT_EQ(node_symbol.type_, Symbol::Type::Vertex);
  for (auto &symbol : node_symbols) {
    EXPECT_EQ(node_symbol, symbol);
  }
  auto &edge_symbol = edge_symbols.front();
  EXPECT_EQ(edge_symbol.type_, Symbol::Type::Edge);
  for (auto &symbol : edge_symbols) {
    EXPECT_EQ(edge_symbol, symbol);
  }
  auto ret = dynamic_cast<Return *>(query_ast->clauses_[1]);
  auto named_expr = ret->named_expressions_[0];
  auto ret_symbol = symbol_table[*named_expr->expression_];
  EXPECT_EQ(edge_symbol, ret_symbol);
}

TEST(TestSymbolGenerator, CreatePropertyUnbound) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  // AST with unbound variable in create: CREATE ({prop: x})
  auto node = NODE("anon");
  std::string prop_name = "prop";
  node->properties_[&prop_name] = IDENT("x");
  auto query_ast = QUERY(CREATE(PATTERN(node)));
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), UnboundVariableError);
}

TEST(TestSymbolGenerator, CreateNodeReturn) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  // Simple AST returning a created node: CREATE (n) RETURN n
  auto query_ast =
      QUERY(CREATE(PATTERN(NODE("n"))), RETURN(IDENT("n"), AS("n")));
  SymbolGenerator symbol_generator(symbol_table);
  query_ast->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.max_position(), 2);
  auto create = dynamic_cast<Create *>(query_ast->clauses_[0]);
  auto pattern = create->patterns_[0];
  auto node_atom = dynamic_cast<NodeAtom *>(pattern->atoms_[0]);
  auto node_sym = symbol_table[*node_atom->identifier_];
  EXPECT_EQ(node_sym.name_, "n");
  EXPECT_EQ(node_sym.type_, Symbol::Type::Vertex);
  auto ret = dynamic_cast<Return *>(query_ast->clauses_[1]);
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
  // AST with redeclaring a variable when creating nodes: CREATE (n), (n)
  auto query_ast = QUERY(CREATE(PATTERN(NODE("n")), PATTERN(NODE("n"))));
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), RedeclareVariableError);
}

TEST(TestSymbolGenerator, MultiCreateRedeclareNode) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  // AST with redeclaring a variable when creating nodes with multiple creates:
  // CREATE (n) CREATE (n)
  auto query_ast =
      QUERY(CREATE(PATTERN(NODE("n"))), CREATE(PATTERN(NODE("n"))));
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), RedeclareVariableError);
}

TEST(TestSymbolGenerator, MatchCreateRedeclareNode) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  // AST with redeclaring a match node variable in create: MATCH (n) CREATE (n)
  auto query_ast = QUERY(MATCH(PATTERN(NODE("n"))), CREATE(PATTERN(NODE("n"))));
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), RedeclareVariableError);
}

TEST(TestSymbolGenerator, MatchCreateRedeclareEdge) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  // AST with redeclaring a match edge variable in create:
  // MATCH (n) -[r]- (m) CREATE (n) -[r :relationship]-> (l)
  std::string relationship("relationship");
  auto query = QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
                     CREATE(PATTERN(NODE("n"), EDGE("r", &relationship,
                                                    EdgeAtom::Direction::RIGHT),
                                    NODE("l"))));
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), RedeclareVariableError);
}

TEST(TestSymbolGenerator, MatchTypeMismatch) {
  AstTreeStorage storage;
  // Using an edge variable as a node causes a type mismatch.
  // MATCH (n) -[r]-> (r)
  auto query = QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("r"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), TypeMismatchError);
}

TEST(TestSymbolGenerator, MatchCreateTypeMismatch) {
  AstTreeStorage storage;
  // Using an edge variable as a node causes a type mismatch.
  // MATCH (n1) -[r1]- (n2) CREATE (r1) -[r2]-> (n2)
  auto query =
      QUERY(MATCH(PATTERN(NODE("n1"), EDGE("r1"), NODE("n2"))),
            CREATE(PATTERN(NODE("r1"), EDGE("r2", EdgeAtom::Direction::RIGHT),
                           NODE("n2"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), TypeMismatchError);
}

TEST(TestSymbolGenerator, CreateMultipleEdgeType) {
  AstTreeStorage storage;
  // Multiple edge relationship are not allowed when creating edges.
  // CREATE (n) -[r :rel1 | :rel2]-> (m)
  std::string rel1("rel1");
  std::string rel2("rel2");
  auto edge = EDGE("r", &rel1, EdgeAtom::Direction::RIGHT);
  edge->edge_types_.emplace_back(&rel2);
  auto query = QUERY(CREATE(PATTERN(NODE("n"), edge, NODE("m"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, CreateBidirectionalEdge) {
  AstTreeStorage storage;
  // Bidirectional relationships are not allowed when creating edges.
  // CREATE (n) -[r :rel1]- (m)
  std::string rel1("rel1");
  auto query = QUERY(CREATE(PATTERN(NODE("n"), EDGE("r", &rel1), NODE("m"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, MatchWhereUnbound) {
  // Test MATCH (n) WHERE missing < 42 RETURN n AS n
  AstTreeStorage storage;
  std::string property("property");
  auto query = QUERY(MATCH(PATTERN(NODE("n"))),
                     WHERE(LESS(IDENT("missing"), LITERAL(42))),
                     RETURN(IDENT("n"), AS("n")));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), UnboundVariableError);
}

TEST(TestSymbolGenerator, CreateDelete) {
  // Test CREATE (n) DELETE n
  AstTreeStorage storage;
  auto node = NODE("n");
  auto ident = IDENT("n");
  auto query = QUERY(CREATE(PATTERN(node)), DELETE(ident));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.max_position(), 1);
  auto node_symbol = symbol_table.at(*node->identifier_);
  auto ident_symbol = symbol_table.at(*ident);
  EXPECT_EQ(node_symbol.type_, Symbol::Type::Vertex);
  EXPECT_EQ(node_symbol, ident_symbol);
}

TEST(TestSymbolGenerator, CreateDeleteUnbound) {
  // Test CREATE (n) DELETE missing
  AstTreeStorage storage;
  auto query = QUERY(CREATE(PATTERN(NODE("n"))), DELETE(IDENT("missing")));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), UnboundVariableError);
}

TEST(TestSymbolGenerator, MatchWithReturn) {
  // Test MATCH (old) WITH old AS n RETURN n AS n
  AstTreeStorage storage;
  auto node = NODE("old");
  auto old_ident = IDENT("old");
  auto with_as_n = AS("n");
  auto n_ident = IDENT("n");
  auto ret_as_n = AS("n");
  auto query =
      QUERY(MATCH(PATTERN(node)), WITH(old_ident, with_as_n), RETURN(n_ident, ret_as_n));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.max_position(), 3);
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

TEST(TestSymbolGenerator, MatchWithReturnUnbound) {
  // Test MATCH (old) WITH old AS n RETURN old AS old
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("old"))), WITH(IDENT("old"), AS("n")),
                     RETURN(IDENT("old"), AS("old")));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), UnboundVariableError);
}

TEST(TestSymbolGenerator, MatchWithWhere) {
  // Test MATCH (old) WITH old AS n WHERE n.prop < 42
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto node = NODE("old");
  auto old_ident = IDENT("old");
  auto with_as_n = AS("n");
  auto n_prop = PROPERTY_LOOKUP("n", prop);
  auto query = QUERY(MATCH(PATTERN(node)), WITH(old_ident, with_as_n),
                     WHERE(LESS(n_prop, LITERAL(42))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.max_position(), 2);
  auto node_symbol = symbol_table.at(*node->identifier_);
  auto old = symbol_table.at(*old_ident);
  EXPECT_EQ(node_symbol, old);
  auto with_n = symbol_table.at(*with_as_n);
  EXPECT_NE(old, with_n);
  auto n = symbol_table.at(*n_prop->expression_);
  EXPECT_EQ(n, with_n);
}

TEST(TestSymbolGenerator, MatchWithWhereUnbound) {
  // Test MATCH (old) WITH old AS n WHERE old.prop < 42
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("old"))), WITH(IDENT("old"), AS("n")),
                     WHERE(LESS(PROPERTY_LOOKUP("old", prop), LITERAL(42))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), UnboundVariableError);
}

TEST(TestSymbolGenerator, CreateMultiExpand) {
  // Test CREATE (n) -[r :r]-> (m), (n) - [p :p]-> (l)
  Dbms dbms;
  auto dba = dbms.active();
  auto r_type = dba->edge_type("r");
  auto p_type = dba->edge_type("p");
  AstTreeStorage storage;
  auto node_n1 = NODE("n");
  auto edge_r = EDGE("r", r_type, EdgeAtom::Direction::RIGHT);
  auto node_m = NODE("m");
  auto node_n2 = NODE("n");
  auto edge_p = EDGE("p", p_type, EdgeAtom::Direction::RIGHT);
  auto node_l = NODE("l");
  auto query = QUERY(CREATE(PATTERN(node_n1, edge_r, node_m),
                            PATTERN(node_n2, edge_p, node_l)));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  auto n1 = symbol_table.at(*node_n1->identifier_);
  auto n2 = symbol_table.at(*node_n2->identifier_);
  EXPECT_EQ(n1, n2);
  EXPECT_EQ(n1.type_, Symbol::Type::Vertex);
  auto m = symbol_table.at(*node_m->identifier_);
  EXPECT_EQ(m.type_, Symbol::Type::Vertex);
  EXPECT_NE(m, n1);
  auto l = symbol_table.at(*node_l->identifier_);
  EXPECT_EQ(l.type_, Symbol::Type::Vertex);
  EXPECT_NE(l, n1);
  EXPECT_NE(l, m);
  auto r = symbol_table.at(*edge_r->identifier_);
  auto p = symbol_table.at(*edge_p->identifier_);
  EXPECT_EQ(r.type_, Symbol::Type::Edge);
  EXPECT_EQ(p.type_, Symbol::Type::Edge);
  EXPECT_NE(r, p);
}

}
