#include <list>
#include <typeinfo>

#include "gtest/gtest.h"

#include "dbms/dbms.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/logical/operator.hpp"
#include "query/frontend/logical/planner.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"

#include "query_common.hpp"

using namespace query::plan;
using query::AstTreeStorage;
using query::SymbolTable;
using query::SymbolGenerator;
using Direction = query::EdgeAtom::Direction;

namespace {

class PlanChecker : public LogicalOperatorVisitor {
 public:
  using LogicalOperatorVisitor::Visit;
  using LogicalOperatorVisitor::PostVisit;

  PlanChecker(const std::list<size_t> &types) : types_(types) {}

  void Visit(CreateNode &op) override { AssertType(op); }
  void Visit(CreateExpand &op) override { AssertType(op); }
  void Visit(Delete &op) override { AssertType(op); }
  void Visit(ScanAll &op) override { AssertType(op); }
  void Visit(Expand &op) override { AssertType(op); }
  void Visit(NodeFilter &op) override { AssertType(op); }
  void Visit(EdgeFilter &op) override { AssertType(op); }
  void Visit(Filter &op) override { AssertType(op); }
  void Visit(Produce &op) override { AssertType(op); }
  void Visit(SetProperty &op) override { AssertType(op); }
  void Visit(SetProperties &op) override { AssertType(op); }
  void Visit(SetLabels &op) override { AssertType(op); }
  void Visit(RemoveProperty &op) override { AssertType(op); }
  void Visit(RemoveLabels &op) override { AssertType(op); }
  void Visit(ExpandUniquenessFilter<VertexAccessor> &op) override {
    AssertType(op);
  }
  void Visit(ExpandUniquenessFilter<EdgeAccessor> &op) override {
    AssertType(op);
  }

 std::list<size_t> types_;

 private:
  void AssertType(const LogicalOperator &op) {
    ASSERT_FALSE(types_.empty());
    ASSERT_EQ(types_.back(), typeid(op).hash_code());
    types_.pop_back();
  }
};

template <class... TOps>
auto CheckPlan(query::Query &query) {
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query.Accept(symbol_generator);
  auto plan = MakeLogicalPlan(query, symbol_table);
  std::list<size_t> type_hashes{typeid(TOps).hash_code()...};
  PlanChecker plan_checker(type_hashes);
  plan->Accept(plan_checker);
  EXPECT_TRUE(plan_checker.types_.empty());
}

TEST(TestLogicalPlanner, MatchNodeReturn) {
  // Test MATCH (n) RETURN n AS n
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("n"))), RETURN(IDENT("n"), AS("n")));
  CheckPlan<ScanAll, Produce>(*query);
}

TEST(TestLogicalPlanner, CreateNodeReturn) {
  // Test CREATE (n) RETURN n AS n
  AstTreeStorage storage;
  auto query = QUERY(CREATE(PATTERN(NODE("n"))), RETURN(IDENT("n"), AS("n")));
  CheckPlan<CreateNode, Produce>(*query);
}

TEST(TestLogicalPlanner, CreateExpand) {
  // Test CREATE (n) -[r :rel1]-> (m)
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto relationship = dba->edge_type("relationship");
  auto query = QUERY(CREATE(PATTERN(
      NODE("n"), EDGE("r", relationship, Direction::RIGHT), NODE("m"))));
  CheckPlan<CreateNode, CreateExpand>(*query);
}

TEST(TestLogicalPlanner, CreateMultipleNode) {
  // Test CREATE (n), (m)
  AstTreeStorage storage;
  auto query = QUERY(CREATE(PATTERN(NODE("n")), PATTERN(NODE("m"))));
  CheckPlan<CreateNode, CreateNode>(*query);
}

TEST(TestLogicalPlanner, CreateNodeExpandNode) {
  // Test CREATE (n) -[r :rel]-> (m), (l)
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto relationship = dba->edge_type("rel");
  auto query = QUERY(CREATE(
      PATTERN(NODE("n"), EDGE("r", relationship, Direction::RIGHT), NODE("m")),
      PATTERN(NODE("l"))));
  CheckPlan<CreateNode, CreateExpand, CreateNode>(*query);
}

TEST(TestLogicalPlanner, MatchCreateExpand) {
  // Test MATCH (n) CREATE (n) -[r :rel1]-> (m)
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto relationship = dba->edge_type("relationship");
  auto query =
      QUERY(MATCH(PATTERN(NODE("n"))),
            CREATE(PATTERN(NODE("n"), EDGE("r", relationship, Direction::RIGHT),
                           NODE("m"))));
  CheckPlan<ScanAll, CreateExpand>(*query);
}

TEST(TestLogicalPlanner, MatchLabeledNodes) {
  // Test MATCH (n :label) RETURN n AS n
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto label = dba->label("label");
  auto query =
      QUERY(MATCH(PATTERN(NODE("n", label))), RETURN(IDENT("n"), AS("n")));
  CheckPlan<ScanAll, NodeFilter, Produce>(*query);
}

TEST(TestLogicalPlanner, MatchPathReturn) {
  // Test MATCH (n) -[r :relationship]- (m) RETURN n AS n
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto relationship = dba->edge_type("relationship");
  auto query =
      QUERY(MATCH(PATTERN(NODE("n"), EDGE("r", relationship), NODE("m"))),
            RETURN(IDENT("n"), AS("n")));
  CheckPlan<ScanAll, Expand, EdgeFilter, Produce>(*query);
}

TEST(TestLogicalPlanner, MatchWhereReturn) {
  // Test MATCH (n) WHERE n.property < 42 RETURN n AS n
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto property = dba->property("property");
  auto query = QUERY(MATCH(PATTERN(NODE("n"))),
                     WHERE(LESS(PROPERTY_LOOKUP("n", property), LITERAL(42))),
                     RETURN(IDENT("n"), AS("n")));
  CheckPlan<ScanAll, Filter, Produce>(*query);
}

TEST(TestLogicalPlanner, MatchDelete) {
  // Test MATCH (n) DELETE n
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("n"))), DELETE(IDENT("n")));
  CheckPlan<ScanAll, Delete>(*query);
}

TEST(TestLogicalPlanner, MatchNodeSet) {
  // Test MATCH (n) SET n.prop = 42, n = n, n :label
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  auto label = dba->label("label");
  auto query = QUERY(MATCH(PATTERN(NODE("n"))),
                     SET(PROPERTY_LOOKUP("n", prop), LITERAL(42)),
                     SET("n", IDENT("n")), SET("n", {label}));
  CheckPlan<ScanAll, SetProperty, SetProperties, SetLabels>(*query);
}

TEST(TestLogicalPlanner, MatchRemove) {
  // Test MATCH (n) REMOVE n.prop REMOVE n :label
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  auto label = dba->label("label");
  auto query = QUERY(MATCH(PATTERN(NODE("n"))),
                     REMOVE(PROPERTY_LOOKUP("n", prop)), REMOVE("n", {label}));
  CheckPlan<ScanAll, RemoveProperty, RemoveLabels>(*query);
}

TEST(TestLogicalPlanner, MatchMultiPattern) {
  // Test MATCH (n) -[r]- (m), (j) -[e]- (i)
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m")),
                           PATTERN(NODE("j"), EDGE("e"), NODE("i"))));
  // We expect the expansions after the first to have a uniqueness filter in a
  // single MATCH clause.
  CheckPlan<ScanAll, Expand, ScanAll, Expand,
            ExpandUniquenessFilter<EdgeAccessor>>(*query);
}

TEST(TestLogicalPlanner, MatchMultiPatternSameStart) {
  // Test MATCH (n), (n) -[e]- (m)
  AstTreeStorage storage;
  auto query = QUERY(
      MATCH(PATTERN(NODE("n")), PATTERN(NODE("n"), EDGE("e"), NODE("m"))));
  // We expect the second pattern to generate only an Expand, since another
  // ScanAll would be redundant.
  CheckPlan<ScanAll, Expand>(*query);
}

TEST(TestLogicalPlanner, MatchMultiPatternSameExpandStart) {
  // Test MATCH (n) -[r]- (m), (m) -[e]- (l)
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m")),
                           PATTERN(NODE("m"), EDGE("e"), NODE("l"))));
  // We expect the second pattern to generate only an Expand. Another
  // ScanAll would be redundant, as it would generate the nodes obtained from
  // expansion. Additionally, a uniqueness filter is expected.
  CheckPlan<ScanAll, Expand, Expand, ExpandUniquenessFilter<EdgeAccessor>>(
      *query);
}

TEST(TestLogicalPlanner, MultiMatch) {
  // Test MATCH (n) -[r]- (m) MATCH (j) -[e]- (i) -[f]- (h)
  AstTreeStorage storage;
  auto query = QUERY(
      MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
      MATCH(PATTERN(NODE("j"), EDGE("e"), NODE("i"), EDGE("f"), NODE("h"))));
  // Multiple MATCH clauses form a Cartesian product, so the uniqueness should
  // not cross MATCH boundaries.
  CheckPlan<ScanAll, Expand, ScanAll, Expand, Expand,
            ExpandUniquenessFilter<EdgeAccessor>>(*query);
}

TEST(TestLogicalPlanner, MultiMatchSameStart) {
  // Test MATCH (n) MATCH (n) -[r]- (m)
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("n"))),
                     MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))));
  // Similar to MatchMultiPatternSameStart, we expect only Expand from second
  // MATCH clause.
  CheckPlan<ScanAll, Expand>(*query);
}

TEST(TestLogicalPlanner, MatchEdgeCycle) {
  // Test MATCH (n) -[r]- (m) -[r]- (j)
  AstTreeStorage storage;
  auto query = QUERY(
      MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"), EDGE("r"), NODE("j"))));
  // There is no ExpandUniquenessFilter for referencing the same edge.
  CheckPlan<ScanAll, Expand, Expand>(*query);
}

}  // namespace
