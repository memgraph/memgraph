#include <list>
#include <tuple>
#include <unordered_set>

#include "gtest/gtest.h"

#include "dbms/dbms.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/planner.hpp"

#include "query_common.hpp"

using namespace query::plan;
using query::AstTreeStorage;
using query::Symbol;
using query::SymbolTable;
using query::SymbolGenerator;
using Direction = query::EdgeAtom::Direction;

namespace {

class BaseOpChecker {
 public:
  virtual ~BaseOpChecker() {}

  virtual void CheckOp(LogicalOperator &, const SymbolTable &) = 0;
};

template <class TOp>
class OpChecker : public BaseOpChecker {
 public:
  void CheckOp(LogicalOperator &op, const SymbolTable &symbol_table) override {
    auto *expected_op = dynamic_cast<TOp *>(&op);
    ASSERT_TRUE(expected_op);
    ExpectOp(*expected_op, symbol_table);
  }

  virtual void ExpectOp(TOp &op, const SymbolTable &) {}
};

using ExpectCreateNode = OpChecker<CreateNode>;
using ExpectCreateExpand = OpChecker<CreateExpand>;
using ExpectDelete = OpChecker<Delete>;
using ExpectScanAll = OpChecker<ScanAll>;
using ExpectExpand = OpChecker<Expand>;
using ExpectNodeFilter = OpChecker<NodeFilter>;
using ExpectEdgeFilter = OpChecker<EdgeFilter>;
using ExpectFilter = OpChecker<Filter>;
using ExpectProduce = OpChecker<Produce>;
using ExpectSetProperty = OpChecker<SetProperty>;
using ExpectSetProperties = OpChecker<SetProperties>;
using ExpectSetLabels = OpChecker<SetLabels>;
using ExpectRemoveProperty = OpChecker<RemoveProperty>;
using ExpectRemoveLabels = OpChecker<RemoveLabels>;
template <class TAccessor>
using ExpectExpandUniquenessFilter =
    OpChecker<ExpandUniquenessFilter<TAccessor>>;
using ExpectSkip = OpChecker<Skip>;
using ExpectLimit = OpChecker<Limit>;
using ExpectOrderBy = OpChecker<OrderBy>;

class ExpectAccumulate : public OpChecker<Accumulate> {
 public:
  ExpectAccumulate(const std::unordered_set<Symbol, Symbol::Hash> &symbols)
      : symbols_(symbols) {}

  void ExpectOp(Accumulate &op, const SymbolTable &symbol_table) override {
    std::unordered_set<Symbol, Symbol::Hash> got_symbols(op.symbols().begin(),
                                                         op.symbols().end());
    EXPECT_EQ(symbols_, got_symbols);
  }

 private:
  const std::unordered_set<Symbol, Symbol::Hash> symbols_;
};

class ExpectAggregate : public OpChecker<Aggregate> {
 public:
  ExpectAggregate(const std::vector<query::Aggregation *> &aggregations,
                  const std::unordered_set<query::Expression *> &group_by)
      : aggregations_(aggregations), group_by_(group_by) {}

  void ExpectOp(Aggregate &op, const SymbolTable &symbol_table) override {
    auto aggr_it = aggregations_.begin();
    for (const auto &aggr_elem : op.aggregations()) {
      ASSERT_NE(aggr_it, aggregations_.end());
      auto aggr = *aggr_it++;
      auto expected =
          std::make_tuple(aggr->expression_, aggr->op_, symbol_table.at(*aggr));
      EXPECT_EQ(expected, aggr_elem);
    }
    EXPECT_EQ(aggr_it, aggregations_.end());
    auto got_group_by = std::unordered_set<query::Expression *>(
        op.group_by().begin(), op.group_by().end());
    EXPECT_EQ(group_by_, got_group_by);
  }

 private:
  const std::vector<query::Aggregation *> aggregations_;
  const std::unordered_set<query::Expression *> group_by_;
};

class PlanChecker : public LogicalOperatorVisitor {
 public:
  using LogicalOperatorVisitor::Visit;
  using LogicalOperatorVisitor::PostVisit;

  PlanChecker(const std::list<BaseOpChecker *> &checkers,
              const SymbolTable &symbol_table)
      : checkers_(checkers), symbol_table_(symbol_table) {}

  void Visit(CreateNode &op) override { CheckOp(op); }
  void Visit(CreateExpand &op) override { CheckOp(op); }
  void Visit(Delete &op) override { CheckOp(op); }
  void Visit(ScanAll &op) override { CheckOp(op); }
  void Visit(Expand &op) override { CheckOp(op); }
  void Visit(NodeFilter &op) override { CheckOp(op); }
  void Visit(EdgeFilter &op) override { CheckOp(op); }
  void Visit(Filter &op) override { CheckOp(op); }
  void Visit(Produce &op) override { CheckOp(op); }
  void Visit(SetProperty &op) override { CheckOp(op); }
  void Visit(SetProperties &op) override { CheckOp(op); }
  void Visit(SetLabels &op) override { CheckOp(op); }
  void Visit(RemoveProperty &op) override { CheckOp(op); }
  void Visit(RemoveLabels &op) override { CheckOp(op); }
  void Visit(ExpandUniquenessFilter<VertexAccessor> &op) override {
    CheckOp(op);
  }
  void Visit(ExpandUniquenessFilter<EdgeAccessor> &op) override { CheckOp(op); }
  void Visit(Accumulate &op) override { CheckOp(op); }
  void Visit(Aggregate &op) override { CheckOp(op); }
  void Visit(Skip &op) override { CheckOp(op); }
  void Visit(Limit &op) override { CheckOp(op); }
  void Visit(OrderBy &op) override { CheckOp(op); }

  std::list<BaseOpChecker *> checkers_;

 private:
  void CheckOp(LogicalOperator &op) {
    ASSERT_FALSE(checkers_.empty());
    checkers_.back()->CheckOp(op, symbol_table_);
    checkers_.pop_back();
  }

  const SymbolTable &symbol_table_;
};

auto MakeSymbolTable(query::Query &query) {
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query.Accept(symbol_generator);
  return symbol_table;
}

template <class... TChecker>
auto CheckPlan(LogicalOperator &plan, const SymbolTable &symbol_table,
               TChecker... checker) {
  std::list<BaseOpChecker *> checkers{&checker...};
  PlanChecker plan_checker(checkers, symbol_table);
  plan.Accept(plan_checker);
  EXPECT_TRUE(plan_checker.checkers_.empty());
}

template <class... TChecker>
auto CheckPlan(query::Query &query, TChecker... checker) {
  auto symbol_table = MakeSymbolTable(query);
  auto plan = MakeLogicalPlan(query, symbol_table);
  CheckPlan(*plan, symbol_table, checker...);
}

TEST(TestLogicalPlanner, MatchNodeReturn) {
  // Test MATCH (n) RETURN n AS n
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("n"))), RETURN(IDENT("n"), AS("n")));
  CheckPlan(*query, ExpectScanAll(), ExpectProduce());
}

TEST(TestLogicalPlanner, CreateNodeReturn) {
  // Test CREATE (n) RETURN n AS n
  AstTreeStorage storage;
  auto ident_n = IDENT("n");
  auto query = QUERY(CREATE(PATTERN(NODE("n"))), RETURN(ident_n, AS("n")));
  auto symbol_table = MakeSymbolTable(*query);
  auto acc = ExpectAccumulate({symbol_table.at(*ident_n)});
  auto plan = MakeLogicalPlan(*query, symbol_table);
  CheckPlan(*plan, symbol_table, ExpectCreateNode(), acc, ExpectProduce());
}

TEST(TestLogicalPlanner, CreateExpand) {
  // Test CREATE (n) -[r :rel1]-> (m)
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto relationship = dba->edge_type("relationship");
  auto query = QUERY(CREATE(PATTERN(
      NODE("n"), EDGE("r", relationship, Direction::RIGHT), NODE("m"))));
  CheckPlan(*query, ExpectCreateNode(), ExpectCreateExpand());
}

TEST(TestLogicalPlanner, CreateMultipleNode) {
  // Test CREATE (n), (m)
  AstTreeStorage storage;
  auto query = QUERY(CREATE(PATTERN(NODE("n")), PATTERN(NODE("m"))));
  CheckPlan(*query, ExpectCreateNode(), ExpectCreateNode());
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
  CheckPlan(*query, ExpectCreateNode(), ExpectCreateExpand(),
            ExpectCreateNode());
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
  CheckPlan(*query, ExpectScanAll(), ExpectCreateExpand());
}

TEST(TestLogicalPlanner, MatchLabeledNodes) {
  // Test MATCH (n :label) RETURN n AS n
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto label = dba->label("label");
  auto query =
      QUERY(MATCH(PATTERN(NODE("n", label))), RETURN(IDENT("n"), AS("n")));
  CheckPlan(*query, ExpectScanAll(), ExpectNodeFilter(), ExpectProduce());
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
  CheckPlan(*query, ExpectScanAll(), ExpectExpand(), ExpectEdgeFilter(),
            ExpectProduce());
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
  CheckPlan(*query, ExpectScanAll(), ExpectFilter(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchDelete) {
  // Test MATCH (n) DELETE n
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("n"))), DELETE(IDENT("n")));
  CheckPlan(*query, ExpectScanAll(), ExpectDelete());
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
  CheckPlan(*query, ExpectScanAll(), ExpectSetProperty(), ExpectSetProperties(),
            ExpectSetLabels());
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
  CheckPlan(*query, ExpectScanAll(), ExpectRemoveProperty(),
            ExpectRemoveLabels());
}

TEST(TestLogicalPlanner, MatchMultiPattern) {
  // Test MATCH (n) -[r]- (m), (j) -[e]- (i)
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m")),
                           PATTERN(NODE("j"), EDGE("e"), NODE("i"))));
  // We expect the expansions after the first to have a uniqueness filter in a
  // single MATCH clause.
  CheckPlan(*query, ExpectScanAll(), ExpectExpand(), ExpectScanAll(),
            ExpectExpand(), ExpectExpandUniquenessFilter<EdgeAccessor>());
}

TEST(TestLogicalPlanner, MatchMultiPatternSameStart) {
  // Test MATCH (n), (n) -[e]- (m)
  AstTreeStorage storage;
  auto query = QUERY(
      MATCH(PATTERN(NODE("n")), PATTERN(NODE("n"), EDGE("e"), NODE("m"))));
  // We expect the second pattern to generate only an Expand, since another
  // ScanAll would be redundant.
  CheckPlan(*query, ExpectScanAll(), ExpectExpand());
}

TEST(TestLogicalPlanner, MatchMultiPatternSameExpandStart) {
  // Test MATCH (n) -[r]- (m), (m) -[e]- (l)
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m")),
                           PATTERN(NODE("m"), EDGE("e"), NODE("l"))));
  // We expect the second pattern to generate only an Expand. Another
  // ScanAll would be redundant, as it would generate the nodes obtained from
  // expansion. Additionally, a uniqueness filter is expected.
  CheckPlan(*query, ExpectScanAll(), ExpectExpand(), ExpectExpand(),
            ExpectExpandUniquenessFilter<EdgeAccessor>());
}

TEST(TestLogicalPlanner, MultiMatch) {
  // Test MATCH (n) -[r]- (m) MATCH (j) -[e]- (i) -[f]- (h)
  AstTreeStorage storage;
  auto query = QUERY(
      MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
      MATCH(PATTERN(NODE("j"), EDGE("e"), NODE("i"), EDGE("f"), NODE("h"))));
  // Multiple MATCH clauses form a Cartesian product, so the uniqueness should
  // not cross MATCH boundaries.
  CheckPlan(*query, ExpectScanAll(), ExpectExpand(), ExpectScanAll(),
            ExpectExpand(), ExpectExpand(),
            ExpectExpandUniquenessFilter<EdgeAccessor>());
}

TEST(TestLogicalPlanner, MultiMatchSameStart) {
  // Test MATCH (n) MATCH (n) -[r]- (m)
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("n"))),
                     MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))));
  // Similar to MatchMultiPatternSameStart, we expect only Expand from second
  // MATCH clause.
  CheckPlan(*query, ExpectScanAll(), ExpectExpand());
}

TEST(TestLogicalPlanner, MatchEdgeCycle) {
  // Test MATCH (n) -[r]- (m) -[r]- (j)
  AstTreeStorage storage;
  auto query = QUERY(
      MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"), EDGE("r"), NODE("j"))));
  // There is no ExpandUniquenessFilter for referencing the same edge.
  CheckPlan(*query, ExpectScanAll(), ExpectExpand(), ExpectExpand());
}

TEST(TestLogicalPlanner, MatchWithReturn) {
  // Test MATCH (old) WITH old AS new RETURN new AS new
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("old"))), WITH(IDENT("old"), AS("new")),
                     RETURN(IDENT("new"), AS("new")));
  // No accumulation since we only do reads.
  CheckPlan(*query, ExpectScanAll(), ExpectProduce(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchWithWhereReturn) {
  // Test MATCH (old) WITH old AS new WHERE new.prop < 42 RETURN new AS new
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto query = QUERY(MATCH(PATTERN(NODE("old"))), WITH(IDENT("old"), AS("new")),
                     WHERE(LESS(PROPERTY_LOOKUP("new", prop), LITERAL(42))),
                     RETURN(IDENT("new"), AS("new")));
  // No accumulation since we only do reads.
  CheckPlan(*query, ExpectScanAll(), ExpectProduce(), ExpectFilter(),
            ExpectProduce());
}

TEST(TestLogicalPlanner, CreateMultiExpand) {
  // Test CREATE (n) -[r :r]-> (m), (n) - [p :p]-> (l)
  Dbms dbms;
  auto dba = dbms.active();
  auto r = dba->edge_type("r");
  auto p = dba->edge_type("p");
  AstTreeStorage storage;
  auto query = QUERY(
      CREATE(PATTERN(NODE("n"), EDGE("r", r, Direction::RIGHT), NODE("m")),
             PATTERN(NODE("n"), EDGE("p", p, Direction::RIGHT), NODE("l"))));
  CheckPlan(*query, ExpectCreateNode(), ExpectCreateExpand(),
            ExpectCreateExpand());
}

TEST(TestLogicalPlanner, MatchWithSumWhereReturn) {
  // Test MATCH (n) WITH SUM(n.prop) + 42 AS sum WHERE sum < 42
  //      RETURN sum AS result
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto sum = SUM(PROPERTY_LOOKUP("n", prop));
  auto literal = LITERAL(42);
  auto query =
      QUERY(MATCH(PATTERN(NODE("n"))), WITH(ADD(sum, literal), AS("sum")),
            WHERE(LESS(IDENT("sum"), LITERAL(42))),
            RETURN(IDENT("sum"), AS("result")));
  auto aggr = ExpectAggregate({sum}, {literal});
  CheckPlan(*query, ExpectScanAll(), aggr, ExpectProduce(), ExpectFilter(),
            ExpectProduce());
}

TEST(TestLogicalPlanner, MatchReturnSum) {
  // Test MATCH (n) RETURN SUM(n.prop1) AS sum, n.prop2 AS group
  Dbms dbms;
  auto dba = dbms.active();
  auto prop1 = dba->property("prop1");
  auto prop2 = dba->property("prop2");
  AstTreeStorage storage;
  auto sum = SUM(PROPERTY_LOOKUP("n", prop1));
  auto n_prop2 = PROPERTY_LOOKUP("n", prop2);
  auto query = QUERY(MATCH(PATTERN(NODE("n"))),
                     RETURN(sum, AS("sum"), n_prop2, AS("group")));
  auto aggr = ExpectAggregate({sum}, {n_prop2});
  CheckPlan(*query, ExpectScanAll(), aggr, ExpectProduce());
}

TEST(TestLogicalPlanner, CreateWithSum) {
  // Test CREATE (n) WITH SUM(n.prop) AS sum
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto n_prop = PROPERTY_LOOKUP("n", prop);
  auto sum = SUM(n_prop);
  auto query = QUERY(CREATE(PATTERN(NODE("n"))), WITH(sum, AS("sum")));
  auto symbol_table = MakeSymbolTable(*query);
  auto acc = ExpectAccumulate({symbol_table.at(*n_prop->expression_)});
  auto aggr = ExpectAggregate({sum}, {});
  auto plan = MakeLogicalPlan(*query, symbol_table);
  // We expect both the accumulation and aggregation because the part before
  // WITH updates the database.
  CheckPlan(*plan, symbol_table, ExpectCreateNode(), acc, aggr,
            ExpectProduce());
}

TEST(TestLogicalPlanner, MatchWithCreate) {
  // Test MATCH (n) WITH n AS a CREATE (a) -[r :r]-> (b)
  Dbms dbms;
  auto dba = dbms.active();
  auto r_type = dba->edge_type("r");
  AstTreeStorage storage;
  auto query =
      QUERY(MATCH(PATTERN(NODE("n"))), WITH(IDENT("n"), AS("a")),
            CREATE(PATTERN(NODE("a"), EDGE("r", r_type, Direction::RIGHT),
                           NODE("b"))));
  CheckPlan(*query, ExpectScanAll(), ExpectProduce(), ExpectCreateExpand());
}

TEST(TestLogicalPlanner, MatchReturnSkipLimit) {
  // Test MATCH (n) RETURN n SKIP 2 LIMIT 1
  AstTreeStorage storage;
  auto query =
      QUERY(MATCH(PATTERN(NODE("n"))),
            RETURN(IDENT("n"), AS("n"), SKIP(LITERAL(2)), LIMIT(LITERAL(1))));
  // A simple Skip and Limit combo which should come before Produce.
  CheckPlan(*query, ExpectScanAll(), ExpectSkip(), ExpectLimit(),
            ExpectProduce());
}

TEST(TestLogicalPlanner, CreateWithSkipReturnLimit) {
  // Test CREATE (n) WITH n AS m SKIP 2 RETURN m LIMIT 1
  AstTreeStorage storage;
  auto ident_n = IDENT("n");
  auto query = QUERY(CREATE(PATTERN(NODE("n"))),
                     WITH(ident_n, AS("m"), SKIP(LITERAL(2))),
                     RETURN(IDENT("m"), AS("m"), LIMIT(LITERAL(1))));
  auto symbol_table = MakeSymbolTable(*query);
  auto acc = ExpectAccumulate({symbol_table.at(*ident_n)});
  auto plan = MakeLogicalPlan(*query, symbol_table);
  // Since we have a write query, we need to have Accumulate, so Skip and Limit
  // need to come before it. This is a bit different than Neo4j, which optimizes
  // WITH followed by RETURN as a single RETURN clause. This would cause the
  // Limit operator to also appear before Accumulate, thus changing the
  // behaviour. We've decided to diverge from Neo4j here, for consistency sake.
  CheckPlan(*plan, symbol_table, ExpectCreateNode(), ExpectSkip(), acc,
            ExpectProduce(), ExpectLimit(), ExpectProduce());
}

TEST(TestLogicalPlanner, CreateReturnSumSkipLimit) {
  // Test CREATE (n) RETURN SUM(n.prop) AS s SKIP 2 LIMIT 1
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto n_prop = PROPERTY_LOOKUP("n", prop);
  auto sum = SUM(n_prop);
  auto query = QUERY(CREATE(PATTERN(NODE("n"))),
                     RETURN(sum, AS("s"), SKIP(LITERAL(2)), LIMIT(LITERAL(1))));
  auto symbol_table = MakeSymbolTable(*query);
  auto acc = ExpectAccumulate({symbol_table.at(*n_prop->expression_)});
  auto aggr = ExpectAggregate({sum}, {});
  auto plan = MakeLogicalPlan(*query, symbol_table);
  // We have a write query and aggregation, therefore Skip and Limit should come
  // after Accumulate and Aggregate.
  CheckPlan(*plan, symbol_table, ExpectCreateNode(), acc, aggr, ExpectSkip(),
            ExpectLimit(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchReturnOrderBy) {
  // Test MATCH (n) RETURN n ORDER BY n.prop
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto ret = RETURN(IDENT("n"), AS("n"), ORDER_BY(PROPERTY_LOOKUP("n", prop)));
  auto query = QUERY(MATCH(PATTERN(NODE("n"))), ret);
  CheckPlan(*query, ExpectScanAll(), ExpectProduce(), ExpectOrderBy());
}

TEST(TestLogicalPlanner, CreateWithOrderByWhere) {
  // Test CREATE (n) -[r :r]-> (m)
  //      WITH n AS new ORDER BY new.prop, r.prop WHERE m.prop < 42
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  auto r_type = dba->edge_type("r");
  AstTreeStorage storage;
  auto ident_n = IDENT("n");
  auto new_prop = PROPERTY_LOOKUP("new", prop);
  auto r_prop = PROPERTY_LOOKUP("r", prop);
  auto m_prop = PROPERTY_LOOKUP("m", prop);
  auto query =
      QUERY(CREATE(PATTERN(NODE("n"), EDGE("r", r_type, Direction::RIGHT),
                           NODE("m"))),
            WITH(ident_n, AS("new"), ORDER_BY(new_prop, r_prop)),
            WHERE(LESS(m_prop, LITERAL(42))));
  auto symbol_table = MakeSymbolTable(*query);
  // Since this is a write query, we expect to accumulate to old used symbols.
  auto acc = ExpectAccumulate({
      symbol_table.at(*ident_n),              // `n` in WITH
      symbol_table.at(*r_prop->expression_),  // `r` in ORDER BY
      symbol_table.at(*m_prop->expression_),  // `m` in WHERE
  });
  auto plan = MakeLogicalPlan(*query, symbol_table);
  CheckPlan(*plan, symbol_table, ExpectCreateNode(), ExpectCreateExpand(), acc,
            ExpectProduce(), ExpectFilter(), ExpectOrderBy());
}

TEST(TestLogicalPlanner, ReturnAddSumCountOrderBy) {
  // Test RETURN SUM(1) + COUNT(2) AS result ORDER BY result
  AstTreeStorage storage;
  auto sum = SUM(LITERAL(1));
  auto count = COUNT(LITERAL(2));
  auto query =
      QUERY(RETURN(ADD(sum, count), AS("result"), ORDER_BY(IDENT("result"))));
  auto aggr = ExpectAggregate({sum, count}, {});
  CheckPlan(*query, aggr, ExpectProduce(), ExpectOrderBy());
}

}  // namespace
