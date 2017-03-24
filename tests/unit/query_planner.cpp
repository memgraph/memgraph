#include <list>
#include <typeinfo>

#include "gtest/gtest.h"

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/logical/operator.hpp"
#include "query/frontend/logical/planner.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"

#include "query_common.hpp"

using namespace query;
using Direction = EdgeAtom::Direction;

namespace {

class PlanChecker : public LogicalOperatorVisitor {
 public:
  using LogicalOperatorVisitor::Visit;
  using LogicalOperatorVisitor::PostVisit;

  PlanChecker(std::list<size_t> types) : types_(types) {}

  void Visit(CreateNode &op) override { AssertType(op); }
  void Visit(CreateExpand &op) override { AssertType(op); }
  void Visit(ScanAll &op) override { AssertType(op); }
  void Visit(Expand &op) override { AssertType(op); }
  void Visit(NodeFilter &op) override { AssertType(op); }
  void Visit(EdgeFilter &op) override { AssertType(op); }
  void Visit(Produce &op) override { AssertType(op); }

 private:
  void AssertType(const LogicalOperator &op) {
    ASSERT_FALSE(types_.empty());
    ASSERT_EQ(types_.back(), typeid(op).hash_code());
    types_.pop_back();
  }
  std::list<size_t> types_;
};

TEST(TestLogicalPlanner, MatchNodeReturn) {
  // Test MATCH (n) RETURN n AS n
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("n"))), RETURN(NEXPR("n", IDENT("n"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  auto plan = MakeLogicalPlan(*query, symbol_table);
  std::list<size_t> expected_types;
  expected_types.emplace_back(typeid(ScanAll).hash_code());
  expected_types.emplace_back(typeid(Produce).hash_code());
  PlanChecker plan_checker(expected_types);
  plan->Accept(plan_checker);
}

TEST(TestLogicalPlanner, CreateNodeReturn) {
  // Test CREATE (n) RETURN n AS n
  AstTreeStorage storage;
  auto query =
      QUERY(CREATE(PATTERN(NODE("n"))), RETURN(NEXPR("n", IDENT("n"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  auto plan = MakeLogicalPlan(*query, symbol_table);
  std::list<size_t> expected_types;
  expected_types.emplace_back(typeid(CreateNode).hash_code());
  expected_types.emplace_back(typeid(Produce).hash_code());
  PlanChecker plan_checker(expected_types);
  plan->Accept(plan_checker);
}

TEST(TestLogicalPlanner, CreateExpand) {
  // Test CREATE (n) -[r :rel1]-> (m)
  AstTreeStorage storage;
  std::string relationship("relationship");
  auto query = QUERY(CREATE(PATTERN(
      NODE("n"), EDGE("r", &relationship, Direction::RIGHT), NODE("m"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  auto plan = MakeLogicalPlan(*query, symbol_table);
  std::list<size_t> expected_types;
  expected_types.emplace_back(typeid(CreateNode).hash_code());
  expected_types.emplace_back(typeid(CreateExpand).hash_code());
  PlanChecker plan_checker(expected_types);
  plan->Accept(plan_checker);
}

TEST(TestLogicalPlanner, CreateMultipleNode) {
  // Test CREATE (n), (m)
  AstTreeStorage storage;
  auto query = QUERY(CREATE(PATTERN(NODE("n")), PATTERN(NODE("m"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  auto plan = MakeLogicalPlan(*query, symbol_table);
  std::list<size_t> expected_types;
  expected_types.emplace_back(typeid(CreateNode).hash_code());
  expected_types.emplace_back(typeid(CreateNode).hash_code());
  PlanChecker plan_checker(expected_types);
  plan->Accept(plan_checker);
}

TEST(TestLogicalPlanner, CreateNodeExpandNode) {
  // Test CREATE (n) -[r :rel]-> (m), (l)
  AstTreeStorage storage;
  std::string relationship("rel");
  auto query = QUERY(CREATE(
      PATTERN(NODE("n"), EDGE("r", &relationship, Direction::RIGHT), NODE("m")),
      PATTERN(NODE("l"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  auto plan = MakeLogicalPlan(*query, symbol_table);
  std::list<size_t> expected_types;
  expected_types.emplace_back(typeid(CreateNode).hash_code());
  expected_types.emplace_back(typeid(CreateExpand).hash_code());
  expected_types.emplace_back(typeid(CreateNode).hash_code());
  PlanChecker plan_checker(expected_types);
  plan->Accept(plan_checker);
}

TEST(TestLogicalPlanner, MatchCreateExpand) {
  // Test MATCH (n) CREATE (n) -[r :rel1]-> (m)
  AstTreeStorage storage;
  std::string relationship("relationship");
  auto query = QUERY(
      MATCH(PATTERN(NODE("n"))),
      CREATE(PATTERN(NODE("n"), EDGE("r", &relationship, Direction::RIGHT),
                     NODE("m"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  auto plan = MakeLogicalPlan(*query, symbol_table);
  std::list<size_t> expected_types;
  expected_types.emplace_back(typeid(ScanAll).hash_code());
  expected_types.emplace_back(typeid(CreateExpand).hash_code());
  PlanChecker plan_checker(expected_types);
  plan->Accept(plan_checker);
}

TEST(TestLogicalPlanner, MatchLabeledNodes) {
  // Test MATCH (n :label) RETURN n AS n
  AstTreeStorage storage;
  std::string label("label");
  auto query =
      QUERY(MATCH(PATTERN(NODE("n", &label))), RETURN(NEXPR("n", IDENT("n"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  auto plan = MakeLogicalPlan(*query, symbol_table);
  std::list<size_t> expected_types;
  expected_types.emplace_back(typeid(ScanAll).hash_code());
  expected_types.emplace_back(typeid(NodeFilter).hash_code());
  expected_types.emplace_back(typeid(Produce).hash_code());
  PlanChecker plan_checker(expected_types);
  plan->Accept(plan_checker);
}

TEST(TestLogicalPlanner, MatchPathReturn) {
  // Test MATCH (n) -[r :relationship]- (m) RETURN n AS n
  AstTreeStorage storage;
  std::string relationship("relationship");
  auto query =
      QUERY(MATCH(PATTERN(NODE("n"), EDGE("r", &relationship), NODE("m"))),
            RETURN(NEXPR("n", IDENT("n"))));
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query->Accept(symbol_generator);
  auto plan = MakeLogicalPlan(*query, symbol_table);
  std::list<size_t> expected_types;
  expected_types.emplace_back(typeid(ScanAll).hash_code());
  expected_types.emplace_back(typeid(Expand).hash_code());
  expected_types.emplace_back(typeid(EdgeFilter).hash_code());
  expected_types.emplace_back(typeid(Produce).hash_code());
  PlanChecker plan_checker(expected_types);
  plan->Accept(plan_checker);
}

}
