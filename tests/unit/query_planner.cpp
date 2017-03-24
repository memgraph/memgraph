#include <list>
#include <typeinfo>

#include "gtest/gtest.h"

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/logical/operator.hpp"
#include "query/frontend/logical/planner.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"

using namespace query;

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

TEST(TestLogicalPlanner, MatchNodeReturn) {
  // Test MATCH (n) RETURN n AS n
  AstTreeStorage storage;
  auto match = storage.Create<Match>();
  match->patterns_.emplace_back(GetPattern(storage, {"n"}));
  auto query = storage.query();
  query->clauses_.emplace_back(match);
  auto named_expr = storage.Create<NamedExpression>();
  named_expr->name_ = "n";
  named_expr->expression_ = storage.Create<Identifier>("n");
  auto ret = storage.Create<Return>();
  ret->named_expressions_.emplace_back(named_expr);
  query->clauses_.emplace_back(ret);
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
  auto create = storage.Create<Create>();
  create->patterns_.emplace_back(GetPattern(storage, {"n"}));
  auto query = storage.query();
  query->clauses_.emplace_back(create);
  auto named_expr = storage.Create<NamedExpression>();
  named_expr->name_ = "n";
  named_expr->expression_ = storage.Create<Identifier>("n");
  auto ret = storage.Create<Return>();
  ret->named_expressions_.emplace_back(named_expr);
  query->clauses_.emplace_back(ret);
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
  auto create = storage.Create<Create>();
  auto pattern = GetPattern(storage, {"n", "r", "m"});
  create->patterns_.emplace_back(pattern);
  auto edge_atom = dynamic_cast<EdgeAtom*>(pattern->atoms_[1]);
  edge_atom->direction_ = EdgeAtom::Direction::RIGHT;
  std::string relationship("relationship");
  edge_atom->edge_types_.emplace_back(&relationship);
  auto query = storage.query();
  query->clauses_.emplace_back(create);
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

TEST(TestLogicalPlanner, MatchCreateExpand) {
  // Test MATCH (n) CREATE (n) -[r :rel1]-> (m)
  AstTreeStorage storage;
  auto match = storage.Create<Match>();
  match->patterns_.emplace_back(GetPattern(storage, {"n"}));
  auto query = storage.query();
  query->clauses_.emplace_back(match);
  auto create = storage.Create<Create>();
  auto pattern = GetPattern(storage, {"n", "r", "m"});
  create->patterns_.emplace_back(pattern);
  auto edge_atom = dynamic_cast<EdgeAtom*>(pattern->atoms_[1]);
  edge_atom->direction_ = EdgeAtom::Direction::RIGHT;
  std::string relationship("relationship");
  edge_atom->edge_types_.emplace_back(&relationship);
  query->clauses_.emplace_back(create);
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
  auto pattern = storage.Create<Pattern>();
  auto node_atom = storage.Create<NodeAtom>(storage.Create<Identifier>("n"));
  std::string label("label");
  node_atom->labels_.emplace_back(&label);
  pattern->atoms_.emplace_back(node_atom);
  auto match = storage.Create<Match>();
  match->patterns_.emplace_back(pattern);
  auto query = storage.query();
  query->clauses_.emplace_back(match);
  auto named_expr = storage.Create<NamedExpression>();
  named_expr->name_ = "n";
  named_expr->expression_ = storage.Create<Identifier>("n");
  auto ret = storage.Create<Return>();
  ret->named_expressions_.emplace_back(named_expr);
  query->clauses_.emplace_back(ret);
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
  auto match = storage.Create<Match>();
  auto pattern = GetPattern(storage, {"n", "r", "m"});
  match->patterns_.emplace_back(pattern);
  auto edge_atom = dynamic_cast<EdgeAtom*>(pattern->atoms_[1]);
  std::string relationship("relationship");
  edge_atom->edge_types_.emplace_back(&relationship);
  auto query = storage.query();
  query->clauses_.emplace_back(match);
  auto named_expr = storage.Create<NamedExpression>();
  named_expr->name_ = "n";
  named_expr->expression_ = storage.Create<Identifier>("n");
  auto ret = storage.Create<Return>();
  ret->named_expressions_.emplace_back(named_expr);
  query->clauses_.emplace_back(ret);
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
