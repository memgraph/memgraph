#include <list>
#include <typeinfo>

#include "gtest/gtest.h"

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

  PlanChecker(std::list<size_t> types) : types_(types) {}

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

 private:
  void AssertType(const LogicalOperator &op) {
    ASSERT_FALSE(types_.empty());
    ASSERT_EQ(types_.back(), typeid(op).hash_code());
    types_.pop_back();
  }
  std::list<size_t> types_;
};

auto CheckPlan(query::Query &query, std::list<size_t> expected_types) {
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query.Accept(symbol_generator);
  auto plan = MakeLogicalPlan(query, symbol_table);
  PlanChecker plan_checker(expected_types);
  plan->Accept(plan_checker);
}

TEST(TestLogicalPlanner, MatchNodeReturn) {
  // Test MATCH (n) RETURN n AS n
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("n"))), RETURN(IDENT("n"), AS("n")));
  CheckPlan(*query, {typeid(ScanAll).hash_code(), typeid(Produce).hash_code()});
}

TEST(TestLogicalPlanner, CreateNodeReturn) {
  // Test CREATE (n) RETURN n AS n
  AstTreeStorage storage;
  auto query = QUERY(CREATE(PATTERN(NODE("n"))), RETURN(IDENT("n"), AS("n")));
  CheckPlan(*query,
            {typeid(CreateNode).hash_code(), typeid(Produce).hash_code()});
}

TEST(TestLogicalPlanner, CreateExpand) {
  // Test CREATE (n) -[r :rel1]-> (m)
  AstTreeStorage storage;
  std::string relationship("relationship");
  auto query = QUERY(CREATE(PATTERN(
      NODE("n"), EDGE("r", &relationship, Direction::RIGHT), NODE("m"))));
  CheckPlan(*query,
            {typeid(CreateNode).hash_code(), typeid(CreateExpand).hash_code()});
}

TEST(TestLogicalPlanner, CreateMultipleNode) {
  // Test CREATE (n), (m)
  AstTreeStorage storage;
  auto query = QUERY(CREATE(PATTERN(NODE("n")), PATTERN(NODE("m"))));
  CheckPlan(*query,
            {typeid(CreateNode).hash_code(), typeid(CreateNode).hash_code()});
}

TEST(TestLogicalPlanner, CreateNodeExpandNode) {
  // Test CREATE (n) -[r :rel]-> (m), (l)
  AstTreeStorage storage;
  std::string relationship("rel");
  auto query = QUERY(CREATE(
      PATTERN(NODE("n"), EDGE("r", &relationship, Direction::RIGHT), NODE("m")),
      PATTERN(NODE("l"))));
  CheckPlan(*query,
            {typeid(CreateNode).hash_code(), typeid(CreateExpand).hash_code(),
             typeid(CreateNode).hash_code()});
}

TEST(TestLogicalPlanner, MatchCreateExpand) {
  // Test MATCH (n) CREATE (n) -[r :rel1]-> (m)
  AstTreeStorage storage;
  std::string relationship("relationship");
  auto query = QUERY(
      MATCH(PATTERN(NODE("n"))),
      CREATE(PATTERN(NODE("n"), EDGE("r", &relationship, Direction::RIGHT),
                     NODE("m"))));
  CheckPlan(*query,
            {typeid(ScanAll).hash_code(), typeid(CreateExpand).hash_code()});
}

TEST(TestLogicalPlanner, MatchLabeledNodes) {
  // Test MATCH (n :label) RETURN n AS n
  AstTreeStorage storage;
  std::string label("label");
  auto query =
      QUERY(MATCH(PATTERN(NODE("n", &label))), RETURN(IDENT("n"), AS("n")));
  CheckPlan(*query,
            {typeid(ScanAll).hash_code(), typeid(NodeFilter).hash_code(),
             typeid(Produce).hash_code()});
}

TEST(TestLogicalPlanner, MatchPathReturn) {
  // Test MATCH (n) -[r :relationship]- (m) RETURN n AS n
  AstTreeStorage storage;
  std::string relationship("relationship");
  auto query =
      QUERY(MATCH(PATTERN(NODE("n"), EDGE("r", &relationship), NODE("m"))),
            RETURN(IDENT("n"), AS("n")));
  CheckPlan(*query,
            {typeid(ScanAll).hash_code(), typeid(Expand).hash_code(),
             typeid(EdgeFilter).hash_code(), typeid(Produce).hash_code()});
}

TEST(TestLogicalPlanner, MatchWhereReturn) {
  // Test MATCH (n) WHERE n.property < 42 RETURN n AS n
  AstTreeStorage storage;
  std::string property("property");
  auto query = QUERY(MATCH(PATTERN(NODE("n"))),
                     WHERE(LESS(PROPERTY_LOOKUP("n", &property), LITERAL(42))),
                     RETURN(IDENT("n"), AS("n")));
  CheckPlan(*query, {typeid(ScanAll).hash_code(), typeid(Filter).hash_code(),
                     typeid(Produce).hash_code()});
}

TEST(TestLogicalPlanner, MatchDelete) {
  // Test MATCH (n) DELETE n
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("n"))), DELETE(IDENT("n")));
  CheckPlan(*query, {typeid(ScanAll).hash_code(), typeid(Delete).hash_code()});
}

TEST(TestLogicalPlanner, MatchNodeSet) {
  // Test MATCH (n) SET n.prop = 42, n = n, n :label
  AstTreeStorage storage;
  std::string prop("prop");
  std::string label("label");
  auto query = QUERY(MATCH(PATTERN(NODE("n"))),
                     SET(PROPERTY_LOOKUP("n", &prop), LITERAL(42)),
                     SET("n", IDENT("n")), SET("n", {&label}));
  CheckPlan(*query,
            {typeid(ScanAll).hash_code(), typeid(SetProperty).hash_code(),
             typeid(SetProperties).hash_code(), typeid(SetLabels).hash_code()});
}

}
