#include <memory>

#include "gtest/gtest.h"

#include "database/dbms.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/frontend/semantic/symbol_table.hpp"

#include "query_common.hpp"

using namespace query;

namespace {

TEST(TestSymbolGenerator, MatchNodeReturn) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  // MATCH (node_atom_1) RETURN node_atom_1
  auto query_ast =
      QUERY(MATCH(PATTERN(NODE("node_atom_1"))), RETURN("node_atom_1"));
  SymbolGenerator symbol_generator(symbol_table);
  query_ast->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.max_position(), 2);
  auto match = dynamic_cast<Match *>(query_ast->clauses_[0]);
  auto pattern = match->patterns_[0];
  auto node_atom = dynamic_cast<NodeAtom *>(pattern->atoms_[0]);
  auto node_sym = symbol_table[*node_atom->identifier_];
  EXPECT_EQ(node_sym.name(), "node_atom_1");
  EXPECT_EQ(node_sym.type(), Symbol::Type::Vertex);
  auto ret = dynamic_cast<Return *>(query_ast->clauses_[1]);
  auto named_expr = ret->body_.named_expressions[0];
  auto column_sym = symbol_table[*named_expr];
  EXPECT_EQ(node_sym.name(), column_sym.name());
  EXPECT_NE(node_sym, column_sym);
  auto ret_sym = symbol_table[*named_expr->expression_];
  EXPECT_EQ(node_sym, ret_sym);
}

TEST(TestSymbolGenerator, MatchUnboundMultiReturn) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  // AST using variable in return bound by naming the previous return
  // expression. This is treated as an unbound variable.
  // MATCH (node_atom_1) RETURN node_atom_1 AS n, n
  auto query_ast = QUERY(MATCH(PATTERN(NODE("node_atom_1"))),
                         RETURN("node_atom_1", AS("n"), "n"));
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), UnboundVariableError);
}

TEST(TestSymbolGenerator, MatchNodeUnboundReturn) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  // AST with unbound variable in return: MATCH (n) RETURN x
  auto query_ast = QUERY(MATCH(PATTERN(NODE("n"))), RETURN("x"));
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), UnboundVariableError);
}

TEST(TestSymbolGenerator, MatchSameEdge) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  // AST with match pattern referencing an edge multiple times:
  // MATCH (n) -[r]- (n) -[r]- (n) RETURN r
  // This usually throws a redeclaration error, but we support it.
  auto query_ast = QUERY(
      MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("n"), EDGE("r"), NODE("n"))),
      RETURN("r"));
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
  EXPECT_EQ(node_symbol.type(), Symbol::Type::Vertex);
  for (auto &symbol : node_symbols) {
    EXPECT_EQ(node_symbol, symbol);
  }
  auto &edge_symbol = edge_symbols.front();
  EXPECT_EQ(edge_symbol.type(), Symbol::Type::Edge);
  for (auto &symbol : edge_symbols) {
    EXPECT_EQ(edge_symbol, symbol);
  }
  auto ret = dynamic_cast<Return *>(query_ast->clauses_[1]);
  auto named_expr = ret->body_.named_expressions[0];
  auto ret_symbol = symbol_table[*named_expr->expression_];
  EXPECT_EQ(edge_symbol, ret_symbol);
}

TEST(TestSymbolGenerator, CreatePropertyUnbound) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  // AST with unbound variable in create: CREATE ({prop: x})
  auto node = NODE("anon");
  Dbms dbms;
  auto dba = dbms.active();
  node->properties_[PROPERTY_PAIR("prop")] = IDENT("x");
  auto query_ast = QUERY(CREATE(PATTERN(node)));
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query_ast->Accept(symbol_generator), UnboundVariableError);
}

TEST(TestSymbolGenerator, CreateNodeReturn) {
  SymbolTable symbol_table;
  AstTreeStorage storage;
  // Simple AST returning a created node: CREATE (n) RETURN n
  auto query_ast = QUERY(CREATE(PATTERN(NODE("n"))), RETURN("n"));
  SymbolGenerator symbol_generator(symbol_table);
  query_ast->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.max_position(), 2);
  auto create = dynamic_cast<Create *>(query_ast->clauses_[0]);
  auto pattern = create->patterns_[0];
  auto node_atom = dynamic_cast<NodeAtom *>(pattern->atoms_[0]);
  auto node_sym = symbol_table[*node_atom->identifier_];
  EXPECT_EQ(node_sym.name(), "n");
  EXPECT_EQ(node_sym.type(), Symbol::Type::Vertex);
  auto ret = dynamic_cast<Return *>(query_ast->clauses_[1]);
  auto named_expr = ret->body_.named_expressions[0];
  auto column_sym = symbol_table[*named_expr];
  EXPECT_EQ(node_sym.name(), column_sym.name());
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
  Dbms dbms;
  auto dba = dbms.active();
  auto relationship = dba->edge_type("relationship");
  auto query = QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
                     CREATE(PATTERN(NODE("n"), EDGE("r", relationship,
                                                    EdgeAtom::Direction::OUT),
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
            CREATE(PATTERN(NODE("r1"), EDGE("r2", EdgeAtom::Direction::OUT),
                           NODE("n2"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), TypeMismatchError);
}

TEST(TestSymbolGenerator, CreateMultipleEdgeType) {
  AstTreeStorage storage;
  // Multiple edge relationship are not allowed when creating edges.
  // CREATE (n) -[r :rel1 | :rel2]-> (m)
  Dbms dbms;
  auto dba = dbms.active();
  auto rel1 = dba->edge_type("rel1");
  auto rel2 = dba->edge_type("rel2");
  auto edge = EDGE("r", rel1, EdgeAtom::Direction::OUT);
  edge->edge_types_.emplace_back(rel2);
  auto query = QUERY(CREATE(PATTERN(NODE("n"), edge, NODE("m"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, CreateBidirectionalEdge) {
  AstTreeStorage storage;
  // Bidirectional relationships are not allowed when creating edges.
  // CREATE (n) -[r :rel1]- (m)
  Dbms dbms;
  auto dba = dbms.active();
  auto rel1 = dba->edge_type("rel1");
  auto query = QUERY(CREATE(PATTERN(NODE("n"), EDGE("r", rel1), NODE("m"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, MatchWhereUnbound) {
  // Test MATCH (n) WHERE missing < 42 RETURN n
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("n"))),
                     WHERE(LESS(IDENT("missing"), LITERAL(42))), RETURN("n"));
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
  EXPECT_EQ(node_symbol.type(), Symbol::Type::Vertex);
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
  auto query = QUERY(MATCH(PATTERN(node)), WITH(old_ident, with_as_n),
                     RETURN(n_ident, ret_as_n));
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
  // Test MATCH (old) WITH old AS n RETURN old
  AstTreeStorage storage;
  auto query =
      QUERY(MATCH(PATTERN(NODE("old"))), WITH("old", AS("n")), RETURN("old"));
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
  // Test MATCH (old) WITH COUNT(old) AS c WHERE old.prop < 42
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto query =
      QUERY(MATCH(PATTERN(NODE("old"))), WITH(COUNT(IDENT("old")), AS("c")),
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
  auto edge_r = EDGE("r", r_type, EdgeAtom::Direction::OUT);
  auto node_m = NODE("m");
  auto node_n2 = NODE("n");
  auto edge_p = EDGE("p", p_type, EdgeAtom::Direction::OUT);
  auto node_l = NODE("l");
  auto query = QUERY(CREATE(PATTERN(node_n1, edge_r, node_m),
                            PATTERN(node_n2, edge_p, node_l)));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.max_position(), 5);
  auto n1 = symbol_table.at(*node_n1->identifier_);
  auto n2 = symbol_table.at(*node_n2->identifier_);
  EXPECT_EQ(n1, n2);
  EXPECT_EQ(n1.type(), Symbol::Type::Vertex);
  auto m = symbol_table.at(*node_m->identifier_);
  EXPECT_EQ(m.type(), Symbol::Type::Vertex);
  EXPECT_NE(m, n1);
  auto l = symbol_table.at(*node_l->identifier_);
  EXPECT_EQ(l.type(), Symbol::Type::Vertex);
  EXPECT_NE(l, n1);
  EXPECT_NE(l, m);
  auto r = symbol_table.at(*edge_r->identifier_);
  auto p = symbol_table.at(*edge_p->identifier_);
  EXPECT_EQ(r.type(), Symbol::Type::Edge);
  EXPECT_EQ(p.type(), Symbol::Type::Edge);
  EXPECT_NE(r, p);
}

TEST(TestSymbolGenerator, MatchCreateExpandLabel) {
  // Test MATCH (n) CREATE (m) -[r :r]-> (n:label)
  Dbms dbms;
  auto dba = dbms.active();
  auto r_type = dba->edge_type("r");
  auto label = dba->label("label");
  AstTreeStorage storage;
  auto query = QUERY(
      MATCH(PATTERN(NODE("n"))),
      CREATE(PATTERN(NODE("m"), EDGE("r", r_type, EdgeAtom::Direction::OUT),
                     NODE("n", label))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, CreateExpandProperty) {
  // Test CREATE (n) -[r :r]-> (n {prop: 42})
  Dbms dbms;
  auto dba = dbms.active();
  auto r_type = dba->edge_type("r");
  AstTreeStorage storage;
  auto n_prop = NODE("n");
  n_prop->properties_[PROPERTY_PAIR("prop")] = LITERAL(42);
  auto query = QUERY(CREATE(
      PATTERN(NODE("n"), EDGE("r", r_type, EdgeAtom::Direction::OUT), n_prop)));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, MatchReturnSum) {
  // Test MATCH (n) RETURN SUM(n.prop) + 42 AS result
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto node = NODE("n");
  auto sum = SUM(PROPERTY_LOOKUP("n", prop));
  auto as_result = AS("result");
  auto query =
      QUERY(MATCH(PATTERN(node)), RETURN(ADD(sum, LITERAL(42)), as_result));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  // 3 symbols for: 'n', 'sum' and 'result'.
  EXPECT_EQ(symbol_table.max_position(), 3);
  auto node_symbol = symbol_table.at(*node->identifier_);
  auto sum_symbol = symbol_table.at(*sum);
  EXPECT_NE(node_symbol, sum_symbol);
  auto result_symbol = symbol_table.at(*as_result);
  EXPECT_NE(result_symbol, node_symbol);
  EXPECT_NE(result_symbol, sum_symbol);
}

TEST(TestSymbolGenerator, NestedAggregation) {
  // Test MATCH (n) RETURN SUM(42 + SUM(n.prop)) AS s
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto query = QUERY(
      MATCH(PATTERN(NODE("n"))),
      RETURN(SUM(ADD(LITERAL(42), SUM(PROPERTY_LOOKUP("n", prop)))), AS("s")));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, WrongAggregationContext) {
  // Test MATCH (n) WITH n.prop AS prop WHERE SUM(prop) < 42
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("n"))),
                     WITH(PROPERTY_LOOKUP("n", prop), AS("prop")),
                     WHERE(LESS(SUM(IDENT("prop")), LITERAL(42))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, MatchPropCreateNodeProp) {
  // Test MATCH (n) CREATE (m {prop: n.prop})
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = PROPERTY_PAIR("prop");
  AstTreeStorage storage;
  auto node_n = NODE("n");
  auto node_m = NODE("m");
  auto n_prop = PROPERTY_LOOKUP("n", prop.second);
  node_m->properties_[prop] = n_prop;
  auto query = QUERY(MATCH(PATTERN(node_n)), CREATE(PATTERN(node_m)));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.max_position(), 2);
  auto n = symbol_table.at(*node_n->identifier_);
  EXPECT_EQ(n, symbol_table.at(*n_prop->expression_));
  auto m = symbol_table.at(*node_m->identifier_);
  EXPECT_NE(n, m);
}

TEST(TestSymbolGenerator, CreateNodeEdge) {
  // Test CREATE (n), (n) -[r :r]-> (n)
  Dbms dbms;
  auto dba = dbms.active();
  auto r_type = dba->edge_type("r");
  AstTreeStorage storage;
  auto node_1 = NODE("n");
  auto node_2 = NODE("n");
  auto edge = EDGE("r", r_type, EdgeAtom::Direction::OUT);
  auto node_3 = NODE("n");
  auto query = QUERY(CREATE(PATTERN(node_1), PATTERN(node_2, edge, node_3)));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.max_position(), 2);
  auto n = symbol_table.at(*node_1->identifier_);
  EXPECT_EQ(n, symbol_table.at(*node_2->identifier_));
  EXPECT_EQ(n, symbol_table.at(*node_3->identifier_));
  EXPECT_NE(n, symbol_table.at(*edge->identifier_));
}

TEST(TestSymbolGenerator, MatchWithCreate) {
  // Test MATCH (n) WITH n AS m CREATE (m) -[r :r]-> (m)
  Dbms dbms;
  auto dba = dbms.active();
  auto r_type = dba->edge_type("r");
  AstTreeStorage storage;
  auto node_1 = NODE("n");
  auto node_2 = NODE("m");
  auto edge = EDGE("r", r_type, EdgeAtom::Direction::OUT);
  auto node_3 = NODE("m");
  auto query = QUERY(MATCH(PATTERN(node_1)), WITH("n", AS("m")),
                     CREATE(PATTERN(node_2, edge, node_3)));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.max_position(), 3);
  auto n = symbol_table.at(*node_1->identifier_);
  EXPECT_EQ(n.type(), Symbol::Type::Vertex);
  auto m = symbol_table.at(*node_2->identifier_);
  EXPECT_NE(n, m);
  // Currently we don't infer expression types, so we lost true type of 'm'.
  EXPECT_EQ(m.type(), Symbol::Type::Any);
  EXPECT_EQ(m, symbol_table.at(*node_3->identifier_));
}

TEST(TestSymbolGenerator, SameResults) {
  // Test MATCH (n) WITH n AS m, n AS m
  {
    AstTreeStorage storage;
    auto query =
        QUERY(MATCH(PATTERN(NODE("n"))), WITH("n", AS("m"), "n", AS("m")));
    SymbolTable symbol_table;
    SymbolGenerator symbol_generator(symbol_table);
    EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
  }
  // Test MATCH (n) RETURN n, n
  {
    AstTreeStorage storage;
    auto query = QUERY(MATCH(PATTERN(NODE("n"))), RETURN("n", "n"));
    SymbolTable symbol_table;
    SymbolGenerator symbol_generator(symbol_table);
    EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
  }
}

TEST(TestSymbolGenerator, SkipUsingIdentifier) {
  // Test MATCH (old) WITH old AS new SKIP old
  {
    AstTreeStorage storage;
    auto query = QUERY(MATCH(PATTERN(NODE("old"))),
                       WITH("old", AS("new"), SKIP(IDENT("old"))));
    SymbolTable symbol_table;
    SymbolGenerator symbol_generator(symbol_table);
    EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
  }
  // Test MATCH (old) WITH old AS new SKIP new
  {
    AstTreeStorage storage;
    auto query = QUERY(MATCH(PATTERN(NODE("old"))),
                       WITH("old", AS("new"), SKIP(IDENT("new"))));
    SymbolTable symbol_table;
    SymbolGenerator symbol_generator(symbol_table);
    EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
  }
}

TEST(TestSymbolGenerator, LimitUsingIdentifier) {
  // Test MATCH (n) RETURN n AS n LIMIT n
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("n"))), RETURN("n", LIMIT(IDENT("n"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, OrderByAggregation) {
  // Test MATCH (old) RETURN old AS new ORDER BY COUNT(1)
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("old"))),
                     RETURN("old", AS("new"), ORDER_BY(COUNT(LITERAL(1)))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, OrderByUnboundVariable) {
  // Test MATCH (old) RETURN COUNT(old) AS new ORDER BY old
  AstTreeStorage storage;
  auto query =
      QUERY(MATCH(PATTERN(NODE("old"))),
            RETURN(COUNT(IDENT("old")), AS("new"), ORDER_BY(IDENT("old"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), UnboundVariableError);
}

TEST(TestSymbolGenerator, AggregationOrderBy) {
  // Test MATCH (old) RETURN COUNT(old) AS new ORDER BY new
  AstTreeStorage storage;
  auto node = NODE("old");
  auto ident_old = IDENT("old");
  auto as_new = AS("new");
  auto ident_new = IDENT("new");
  auto query = QUERY(MATCH(PATTERN(node)),
                     RETURN(COUNT(ident_old), as_new, ORDER_BY(ident_new)));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  // Symbols for `old`, `count(old)` and `new`
  EXPECT_EQ(symbol_table.max_position(), 3);
  auto old = symbol_table.at(*node->identifier_);
  EXPECT_EQ(old, symbol_table.at(*ident_old));
  auto new_sym = symbol_table.at(*as_new);
  EXPECT_NE(old, new_sym);
  EXPECT_EQ(new_sym, symbol_table.at(*ident_new));
}

TEST(TestSymbolGenerator, OrderByOldVariable) {
  // Test MATCH (old) RETURN old AS new ORDER BY old
  AstTreeStorage storage;
  auto node = NODE("old");
  auto ident_old = IDENT("old");
  auto as_new = AS("new");
  auto by_old = IDENT("old");
  auto query =
      QUERY(MATCH(PATTERN(node)), RETURN(ident_old, as_new, ORDER_BY(by_old)));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  // Symbols for `old` and `new`
  EXPECT_EQ(symbol_table.max_position(), 2);
  auto old = symbol_table.at(*node->identifier_);
  EXPECT_EQ(old, symbol_table.at(*ident_old));
  EXPECT_EQ(old, symbol_table.at(*by_old));
  auto new_sym = symbol_table.at(*as_new);
  EXPECT_NE(old, new_sym);
}

TEST(TestSymbolGenerator, MergeVariableError) {
  // Test MATCH (n) MERGE (n)
  {
    AstTreeStorage storage;
    auto query = QUERY(MATCH(PATTERN(NODE("n"))), MERGE(PATTERN(NODE("n"))));
    SymbolTable symbol_table;
    SymbolGenerator symbol_generator(symbol_table);
    EXPECT_THROW(query->Accept(symbol_generator), RedeclareVariableError);
  }
  // Test MATCH (n) -[r]- (m) MERGE (a) -[r :rel]- (b)
  {
    Dbms dbms;
    auto dba = dbms.active();
    auto rel = dba->edge_type("rel");
    AstTreeStorage storage;
    auto query = QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
                       MERGE(PATTERN(NODE("a"), EDGE("r", rel), NODE("b"))));
    SymbolTable symbol_table;
    SymbolGenerator symbol_generator(symbol_table);
    EXPECT_THROW(query->Accept(symbol_generator), RedeclareVariableError);
  }
}

TEST(TestSymbolGenerator, MergeEdgeWithoutType) {
  // Test MERGE (a) -[r]- (b)
  AstTreeStorage storage;
  auto query = QUERY(MERGE(PATTERN(NODE("a"), EDGE("r"), NODE("b"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  // Edge must have a type, since it doesn't we raise.
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, MergeOnMatchOnCreate) {
  // Test MATCH (n) MERGE (n) -[r :rel]- (m) ON MATCH SET n.prop = 42
  //      ON CREATE SET m.prop = 42 RETURN r AS r
  Dbms dbms;
  auto dba = dbms.active();
  auto rel = dba->edge_type("rel");
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto match_n = NODE("n");
  auto merge_n = NODE("n");
  auto edge_r = EDGE("r", rel);
  auto node_m = NODE("m");
  auto n_prop = PROPERTY_LOOKUP("n", prop);
  auto m_prop = PROPERTY_LOOKUP("m", prop);
  auto ident_r = IDENT("r");
  auto as_r = AS("r");
  auto query =
      QUERY(MATCH(PATTERN(match_n)), MERGE(PATTERN(merge_n, edge_r, node_m),
                                           ON_MATCH(SET(n_prop, LITERAL(42))),
                                           ON_CREATE(SET(m_prop, LITERAL(42)))),
            RETURN(ident_r, as_r));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  // Symbols for: `n`, `r`, `m` and `AS r`.
  EXPECT_EQ(symbol_table.max_position(), 4);
  auto n = symbol_table.at(*match_n->identifier_);
  EXPECT_EQ(n, symbol_table.at(*merge_n->identifier_));
  EXPECT_EQ(n, symbol_table.at(*n_prop->expression_));
  auto r = symbol_table.at(*edge_r->identifier_);
  EXPECT_NE(r, n);
  EXPECT_EQ(r, symbol_table.at(*ident_r));
  EXPECT_NE(r, symbol_table.at(*as_r));
  auto m = symbol_table.at(*node_m->identifier_);
  EXPECT_NE(m, n);
  EXPECT_NE(m, r);
  EXPECT_NE(m, symbol_table.at(*as_r));
  EXPECT_EQ(m, symbol_table.at(*m_prop->expression_));
}

TEST(TestSymbolGenerator, WithUnwindRedeclareReturn) {
  // Test WITH [1, 2] AS list UNWIND list AS list RETURN list
  AstTreeStorage storage;
  auto query = QUERY(WITH(LIST(LITERAL(1), LITERAL(2)), AS("list")),
                     UNWIND(IDENT("list"), AS("list")), RETURN("list"));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), RedeclareVariableError);
}

TEST(TestSymbolGenerator, WithUnwindReturn) {
  // WITH [1, 2] AS list UNWIND list AS elem RETURN list AS list, elem AS elem
  AstTreeStorage storage;
  auto with_as_list = AS("list");
  auto unwind = UNWIND(IDENT("list"), AS("elem"));
  auto ret_list = IDENT("list");
  auto ret_as_list = AS("list");
  auto ret_elem = IDENT("elem");
  auto ret_as_elem = AS("elem");
  auto query = QUERY(WITH(LIST(LITERAL(1), LITERAL(2)), with_as_list), unwind,
                     RETURN(ret_list, ret_as_list, ret_elem, ret_as_elem));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  // Symbols for: `list`, `elem`, `AS list`, `AS elem`
  EXPECT_EQ(symbol_table.max_position(), 4);
  const auto &list = symbol_table.at(*with_as_list);
  EXPECT_EQ(list, symbol_table.at(*unwind->named_expression_->expression_));
  const auto &elem = symbol_table.at(*unwind->named_expression_);
  EXPECT_NE(list, elem);
  EXPECT_EQ(list, symbol_table.at(*ret_list));
  EXPECT_NE(list, symbol_table.at(*ret_as_list));
  EXPECT_EQ(elem, symbol_table.at(*ret_elem));
  EXPECT_NE(elem, symbol_table.at(*ret_as_elem));
}

TEST(TestSymbolGenerator, MatchCrossReferenceVariable) {
  // MATCH (n {prop: m.prop}), (m {prop: n.prop}) RETURN n
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = PROPERTY_PAIR("prop");
  AstTreeStorage storage;
  auto node_n = NODE("n");
  auto m_prop = PROPERTY_LOOKUP("m", prop.second);
  node_n->properties_[prop] = m_prop;
  auto node_m = NODE("m");
  auto n_prop = PROPERTY_LOOKUP("n", prop.second);
  node_m->properties_[prop] = n_prop;
  auto ident_n = IDENT("n");
  auto as_n = AS("n");
  auto query =
      QUERY(MATCH(PATTERN(node_n), PATTERN(node_m)), RETURN(ident_n, as_n));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  // Symbols for `n`, `m` and `AS n`
  EXPECT_EQ(symbol_table.max_position(), 3);
  auto n = symbol_table.at(*node_n->identifier_);
  EXPECT_EQ(n, symbol_table.at(*n_prop->expression_));
  EXPECT_EQ(n, symbol_table.at(*ident_n));
  EXPECT_NE(n, symbol_table.at(*as_n));
  auto m = symbol_table.at(*node_m->identifier_);
  EXPECT_EQ(m, symbol_table.at(*m_prop->expression_));
  EXPECT_NE(n, m);
  EXPECT_NE(m, symbol_table.at(*as_n));
}

TEST(TestSymbolGenerator, MatchWithAsteriskReturnAsterisk) {
  // MATCH (n) -[e]- (m) WITH * RETURN *, n.prop
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto n_prop = PROPERTY_LOOKUP("n", prop);
  auto ret = RETURN(n_prop, AS("n.prop"));
  ret->body_.all_identifiers = true;
  auto node_n = NODE("n");
  auto edge = EDGE("e");
  auto node_m = NODE("m");
  auto with = storage.Create<With>();
  with->body_.all_identifiers = true;
  auto query = QUERY(MATCH(PATTERN(node_n, edge, node_m)), with, ret);
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  // Symbols for `n`, `e`, `m`, `AS n.prop`.
  EXPECT_EQ(symbol_table.max_position(), 4);
  auto n = symbol_table.at(*node_n->identifier_);
  EXPECT_EQ(n, symbol_table.at(*n_prop->expression_));
}

TEST(TestSymbolGenerator, MatchReturnAsteriskSameResult) {
  // MATCH (n) RETURN *, n
  AstTreeStorage storage;
  auto ret = RETURN("n");
  ret->body_.all_identifiers = true;
  auto query = QUERY(MATCH(PATTERN(NODE("n"))), ret);
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, MatchReturnAsteriskNoUserVariables) {
  // MATCH () RETURN *
  AstTreeStorage storage;
  auto ret = storage.Create<Return>();
  ret->body_.all_identifiers = true;
  auto ident_n = storage.Create<Identifier>("anon", false);
  auto node = storage.Create<NodeAtom>(ident_n);
  auto query = QUERY(MATCH(PATTERN(node)), ret);
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, MatchMergeExpandLabel) {
  // Test MATCH (n) MERGE (m) -[r :r]-> (n:label)
  Dbms dbms;
  auto dba = dbms.active();
  auto r_type = dba->edge_type("r");
  auto label = dba->label("label");
  AstTreeStorage storage;
  auto query = QUERY(
      MATCH(PATTERN(NODE("n"))),
      MERGE(PATTERN(NODE("m"), EDGE("r", r_type, EdgeAtom::Direction::OUT),
                    NODE("n", label))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, MatchEdgeWithIdentifierInProperty) {
  // Test MATCH (n) -[r {prop: n.prop}]- (m) RETURN r
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = PROPERTY_PAIR("prop");
  AstTreeStorage storage;
  auto edge = EDGE("r");
  auto n_prop = PROPERTY_LOOKUP("n", prop.second);
  edge->properties_[prop] = n_prop;
  auto node_n = NODE("n");
  auto query = QUERY(MATCH(PATTERN(node_n, edge, NODE("m"))), RETURN("r"));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  // Symbols for `n`, `r`, `m` and implicit in RETURN `r AS r`
  EXPECT_EQ(symbol_table.max_position(), 4);
  auto n = symbol_table.at(*node_n->identifier_);
  EXPECT_EQ(n, symbol_table.at(*n_prop->expression_));
}

TEST(TestSymbolGenerator, MatchVariablePathUsingIdentifier) {
  // Test MATCH (n) -[r *..l.prop]- (m), (l) RETURN r
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto edge = EDGE("r");
  edge->has_range_ = true;
  auto l_prop = PROPERTY_LOOKUP("l", prop);
  edge->upper_bound_ = l_prop;
  auto node_l = NODE("l");
  auto query = QUERY(
      MATCH(PATTERN(NODE("n"), edge, NODE("m")), PATTERN(node_l)), RETURN("r"));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  // Symbols for `n`, `r`, `m`, `l` and implicit in RETURN `r AS r`
  EXPECT_EQ(symbol_table.max_position(), 5);
  auto l = symbol_table.at(*node_l->identifier_);
  EXPECT_EQ(l, symbol_table.at(*l_prop->expression_));
  auto r = symbol_table.at(*edge->identifier_);
  EXPECT_EQ(r.type(), Symbol::Type::EdgeList);
}

TEST(TestSymbolGenerator, MatchVariablePathUsingUnboundIdentifier) {
  // Test MATCH (n) -[r *..l.prop]- (m) MATCH (l) RETURN r
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto edge = EDGE("r");
  edge->has_range_ = true;
  auto l_prop = PROPERTY_LOOKUP("l", prop);
  edge->upper_bound_ = l_prop;
  auto node_l = NODE("l");
  auto query = QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m"))),
                     MATCH(PATTERN(node_l)), RETURN("r"));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, CreateVariablePath) {
  // Test CREATE (n) -[r *]-> (m) raises a SemanticException, since variable
  // paths cannot be created.
  AstTreeStorage storage;
  auto edge = EDGE("r", EdgeAtom::Direction::OUT);
  edge->has_range_ = true;
  auto query = QUERY(CREATE(PATTERN(NODE("n"), edge, NODE("m"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, MergeVariablePath) {
  // Test MERGE (n) -[r *]-> (m) raises a SemanticException, since variable
  // paths cannot be created.
  AstTreeStorage storage;
  auto edge = EDGE("r", EdgeAtom::Direction::OUT);
  edge->has_range_ = true;
  auto query = QUERY(MERGE(PATTERN(NODE("n"), edge, NODE("m"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), SemanticException);
}

TEST(TestSymbolGenerator, RedeclareVariablePath) {
  // Test MATCH (n) -[n*]-> (m) RETURN n raises RedeclareVariableError.
  // This is just a temporary solution, before we add the support for using
  // variable paths with already declared symbols. In the future, this test
  // should be changed to check for type errors.
  AstTreeStorage storage;
  auto edge = EDGE("n", EdgeAtom::Direction::OUT);
  edge->has_range_ = true;
  auto query = QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m"))), RETURN("n"));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), RedeclareVariableError);
}

TEST(TestSymbolGenerator, VariablePathSameIdentifier) {
  // Test MATCH (n) -[r *r.prop..]-> (m) RETURN r raises UnboundVariableError.
  // `r` cannot be used inside the range expression, since it is bound by the
  // variable expansion itself.
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto edge = EDGE("r", EdgeAtom::Direction::OUT);
  edge->has_range_ = true;
  edge->lower_bound_ = PROPERTY_LOOKUP("r", prop);
  auto query = QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m"))), RETURN("r"));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), UnboundVariableError);
}

TEST(TestSymbolGenerator, MatchPropertySameIdentifier) {
  // Test MATCH (n {prop: n.prop}) RETURN n
  // Using `n.prop` needs to work, because filters are run after the value for
  // matched symbol is obtained.
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = PROPERTY_PAIR("prop");
  AstTreeStorage storage;
  auto node_n = NODE("n");
  auto n_prop = PROPERTY_LOOKUP("n", prop.second);
  node_n->properties_[prop] = n_prop;
  auto query = QUERY(MATCH(PATTERN(node_n)), RETURN("n"));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  auto n = symbol_table.at(*node_n->identifier_);
  EXPECT_EQ(n, symbol_table.at(*n_prop->expression_));
}

TEST(TestSymbolGenerator, WithReturnAll) {
  // Test WITH 42 AS x RETURN all(x IN [x] WHERE x = 2) AS x, x AS y
  AstTreeStorage storage;
  auto *with_as_x = AS("x");
  auto *list_x = IDENT("x");
  auto *where_x = IDENT("x");
  auto *all = ALL("x", LIST(list_x), WHERE(EQ(where_x, LITERAL(2))));
  auto *ret_as_x = AS("x");
  auto *ret_x = IDENT("x");
  auto query = QUERY(WITH(LITERAL(42), with_as_x),
                     RETURN(all, ret_as_x, ret_x, AS("y")));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
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

TEST(TestSymbolGenerator, MatchBfsReturn) {
  // Test MATCH (n) -bfs[r](r, n | r.prop, n.prop)-> (m) RETURN r AS r
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto *node_n = NODE("n");
  auto *r_prop = PROPERTY_LOOKUP("r", prop);
  auto *n_prop = PROPERTY_LOOKUP("n", prop);
  auto *bfs =
      storage.Create<BreadthFirstAtom>(IDENT("r"), EdgeAtom::Direction::OUT,
                                       IDENT("r"), IDENT("n"), r_prop, n_prop);
  auto *ret_r = IDENT("r");
  auto *query =
      QUERY(MATCH(PATTERN(node_n, bfs, NODE("m"))), RETURN(ret_r, AS("r")));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  // Symbols for `n`, `[r]`, `r|`, `n|`, `m` and `AS r`.
  EXPECT_EQ(symbol_table.max_position(), 6);
  EXPECT_EQ(symbol_table.at(*ret_r), symbol_table.at(*bfs->identifier_));
  EXPECT_NE(symbol_table.at(*ret_r),
            symbol_table.at(*bfs->traversed_edge_identifier_));
  EXPECT_EQ(symbol_table.at(*bfs->traversed_edge_identifier_),
            symbol_table.at(*r_prop->expression_));
  EXPECT_NE(symbol_table.at(*node_n->identifier_),
            symbol_table.at(*bfs->next_node_identifier_));
  EXPECT_EQ(symbol_table.at(*node_n->identifier_),
            symbol_table.at(*n_prop->expression_));
}

TEST(TestSymbolGenerator, MatchBfsUsesEdgeSymbolError) {
  // Test MATCH (n) -bfs[r](e, n | r, 10)-> (m) RETURN r
  AstTreeStorage storage;
  auto *bfs = storage.Create<BreadthFirstAtom>(
      IDENT("r"), EdgeAtom::Direction::OUT, IDENT("e"), IDENT("n"), IDENT("r"),
      LITERAL(10));
  auto *query = QUERY(MATCH(PATTERN(NODE("n"), bfs, NODE("m"))), RETURN("r"));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), UnboundVariableError);
}

TEST(TestSymbolGenerator, MatchBfsUsesPreviousOuterSymbol) {
  // Test MATCH (a) -bfs[r](e, n | a, 10)-> (m) RETURN r
  AstTreeStorage storage;
  auto *node_a = NODE("a");
  auto *bfs = storage.Create<BreadthFirstAtom>(
      IDENT("r"), EdgeAtom::Direction::OUT, IDENT("e"), IDENT("n"), IDENT("a"),
      LITERAL(10));
  auto *query = QUERY(MATCH(PATTERN(node_a, bfs, NODE("m"))), RETURN("r"));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  EXPECT_EQ(symbol_table.at(*node_a->identifier_),
            symbol_table.at(*bfs->filter_expression_));
}

TEST(TestSymbolGenerator, MatchBfsUsesLaterSymbolError) {
  // Test MATCH (n) -bfs[r](e, n | m, 10)-> (m) RETURN r
  AstTreeStorage storage;
  auto *bfs = storage.Create<BreadthFirstAtom>(
      IDENT("r"), EdgeAtom::Direction::OUT, IDENT("e"), IDENT("n"), IDENT("m"),
      LITERAL(10));
  auto *query = QUERY(MATCH(PATTERN(NODE("n"), bfs, NODE("m"))), RETURN("r"));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  EXPECT_THROW(query->Accept(symbol_generator), UnboundVariableError);
}

}  // namespace
