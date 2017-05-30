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

class PlanChecker : public HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;
  using HierarchicalLogicalOperatorVisitor::PostVisit;

  PlanChecker(const std::list<BaseOpChecker *> &checkers,
              const SymbolTable &symbol_table)
      : checkers_(checkers), symbol_table_(symbol_table) {}

#define PRE_VISIT(TOp)              \
  bool PreVisit(TOp &op) override { \
    CheckOp(op);                    \
    return true;                    \
  }

  PRE_VISIT(CreateNode);
  PRE_VISIT(CreateExpand);
  PRE_VISIT(Delete);
  PRE_VISIT(ScanAll);
  PRE_VISIT(ScanAllByLabel);
  PRE_VISIT(Expand);
  PRE_VISIT(Filter);
  PRE_VISIT(Produce);
  PRE_VISIT(SetProperty);
  PRE_VISIT(SetProperties);
  PRE_VISIT(SetLabels);
  PRE_VISIT(RemoveProperty);
  PRE_VISIT(RemoveLabels);
  PRE_VISIT(ExpandUniquenessFilter<VertexAccessor>);
  PRE_VISIT(ExpandUniquenessFilter<EdgeAccessor>);
  PRE_VISIT(Accumulate);
  PRE_VISIT(Aggregate);
  PRE_VISIT(Skip);
  PRE_VISIT(Limit);
  PRE_VISIT(OrderBy);
  bool PreVisit(Merge &op) override {
    CheckOp(op);
    op.input()->Accept(*this);
    return false;
  }
  bool PreVisit(Optional &op) override {
    CheckOp(op);
    op.input()->Accept(*this);
    return false;
  }
  PRE_VISIT(Unwind);
  PRE_VISIT(Distinct);

  bool Visit(Once &op) override {
    // Ignore checking Once, it is implicitly at the end.
    return true;
  }
#undef PRE_VISIT

  std::list<BaseOpChecker *> checkers_;

 private:
  void CheckOp(LogicalOperator &op) {
    ASSERT_FALSE(checkers_.empty());
    checkers_.back()->CheckOp(op, symbol_table_);
    checkers_.pop_back();
  }

  const SymbolTable &symbol_table_;
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
using ExpectScanAllByLabel = OpChecker<ScanAllByLabel>;
using ExpectExpand = OpChecker<Expand>;
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
using ExpectUnwind = OpChecker<Unwind>;
using ExpectDistinct = OpChecker<Distinct>;

class ExpectAccumulate : public OpChecker<Accumulate> {
 public:
  ExpectAccumulate(const std::unordered_set<Symbol> &symbols)
      : symbols_(symbols) {}

  void ExpectOp(Accumulate &op, const SymbolTable &symbol_table) override {
    std::unordered_set<Symbol> got_symbols(op.symbols().begin(),
                                           op.symbols().end());
    EXPECT_EQ(symbols_, got_symbols);
  }

 private:
  const std::unordered_set<Symbol> symbols_;
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

class ExpectMerge : public OpChecker<Merge> {
 public:
  ExpectMerge(const std::list<BaseOpChecker *> &on_match,
              const std::list<BaseOpChecker *> &on_create)
      : on_match_(on_match), on_create_(on_create) {}

  void ExpectOp(Merge &merge, const SymbolTable &symbol_table) override {
    PlanChecker check_match(on_match_, symbol_table);
    merge.merge_match()->Accept(check_match);
    PlanChecker check_create(on_create_, symbol_table);
    merge.merge_create()->Accept(check_create);
  }

 private:
  const std::list<BaseOpChecker *> &on_match_;
  const std::list<BaseOpChecker *> &on_create_;
};

class ExpectOptional : public OpChecker<Optional> {
 public:
  ExpectOptional(const std::list<BaseOpChecker *> &optional)
      : optional_(optional) {}

  void ExpectOp(Optional &optional, const SymbolTable &symbol_table) override {
    PlanChecker check_optional(optional_, symbol_table);
    optional.optional()->Accept(check_optional);
  }

 private:
  const std::list<BaseOpChecker *> &optional_;
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
auto CheckPlan(AstTreeStorage &storage, TChecker... checker) {
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto plan = MakeLogicalPlan(storage, symbol_table);
  CheckPlan(*plan, symbol_table, checker...);
}

TEST(TestLogicalPlanner, MatchNodeReturn) {
  // Test MATCH (n) RETURN n AS n
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"))), RETURN(IDENT("n"), AS("n")));
  CheckPlan(storage, ExpectScanAll(), ExpectProduce());
}

TEST(TestLogicalPlanner, CreateNodeReturn) {
  // Test CREATE (n) RETURN n AS n
  AstTreeStorage storage;
  auto ident_n = IDENT("n");
  auto query = QUERY(CREATE(PATTERN(NODE("n"))), RETURN(ident_n, AS("n")));
  auto symbol_table = MakeSymbolTable(*query);
  auto acc = ExpectAccumulate({symbol_table.at(*ident_n)});
  auto plan = MakeLogicalPlan(storage, symbol_table);
  CheckPlan(*plan, symbol_table, ExpectCreateNode(), acc, ExpectProduce());
}

TEST(TestLogicalPlanner, CreateExpand) {
  // Test CREATE (n) -[r :rel1]-> (m)
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto relationship = dba->edge_type("relationship");
  QUERY(CREATE(
      PATTERN(NODE("n"), EDGE("r", relationship, Direction::OUT), NODE("m"))));
  CheckPlan(storage, ExpectCreateNode(), ExpectCreateExpand());
}

TEST(TestLogicalPlanner, CreateMultipleNode) {
  // Test CREATE (n), (m)
  AstTreeStorage storage;
  QUERY(CREATE(PATTERN(NODE("n")), PATTERN(NODE("m"))));
  CheckPlan(storage, ExpectCreateNode(), ExpectCreateNode());
}

TEST(TestLogicalPlanner, CreateNodeExpandNode) {
  // Test CREATE (n) -[r :rel]-> (m), (l)
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto relationship = dba->edge_type("rel");
  QUERY(CREATE(
      PATTERN(NODE("n"), EDGE("r", relationship, Direction::OUT), NODE("m")),
      PATTERN(NODE("l"))));
  CheckPlan(storage, ExpectCreateNode(), ExpectCreateExpand(),
            ExpectCreateNode());
}

TEST(TestLogicalPlanner, MatchCreateExpand) {
  // Test MATCH (n) CREATE (n) -[r :rel1]-> (m)
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto relationship = dba->edge_type("relationship");
  QUERY(MATCH(PATTERN(NODE("n"))),
        CREATE(PATTERN(NODE("n"), EDGE("r", relationship, Direction::OUT),
                       NODE("m"))));
  CheckPlan(storage, ExpectScanAll(), ExpectCreateExpand());
}

TEST(TestLogicalPlanner, MatchLabeledNodes) {
  // Test MATCH (n :label) RETURN n AS n
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto label = dba->label("label");
  QUERY(MATCH(PATTERN(NODE("n", label))), RETURN(IDENT("n"), AS("n")));
  CheckPlan(storage, ExpectScanAllByLabel(), ExpectFilter(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchPathReturn) {
  // Test MATCH (n) -[r :relationship]- (m) RETURN n AS n
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto relationship = dba->edge_type("relationship");
  QUERY(MATCH(PATTERN(NODE("n"), EDGE("r", relationship), NODE("m"))),
        RETURN(IDENT("n"), AS("n")));
  CheckPlan(storage, ExpectScanAll(), ExpectExpand(), ExpectFilter(),
            ExpectProduce());
}

TEST(TestLogicalPlanner, MatchWhereReturn) {
  // Test MATCH (n) WHERE n.property < 42 RETURN n AS n
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto property = dba->property("property");
  QUERY(MATCH(PATTERN(NODE("n"))),
        WHERE(LESS(PROPERTY_LOOKUP("n", property), LITERAL(42))),
        RETURN(IDENT("n"), AS("n")));
  CheckPlan(storage, ExpectScanAll(), ExpectFilter(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchDelete) {
  // Test MATCH (n) DELETE n
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"))), DELETE(IDENT("n")));
  CheckPlan(storage, ExpectScanAll(), ExpectDelete());
}

TEST(TestLogicalPlanner, MatchNodeSet) {
  // Test MATCH (n) SET n.prop = 42, n = n, n :label
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  auto label = dba->label("label");
  QUERY(MATCH(PATTERN(NODE("n"))), SET(PROPERTY_LOOKUP("n", prop), LITERAL(42)),
        SET("n", IDENT("n")), SET("n", {label}));
  CheckPlan(storage, ExpectScanAll(), ExpectSetProperty(),
            ExpectSetProperties(), ExpectSetLabels());
}

TEST(TestLogicalPlanner, MatchRemove) {
  // Test MATCH (n) REMOVE n.prop REMOVE n :label
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  auto label = dba->label("label");
  QUERY(MATCH(PATTERN(NODE("n"))), REMOVE(PROPERTY_LOOKUP("n", prop)),
        REMOVE("n", {label}));
  CheckPlan(storage, ExpectScanAll(), ExpectRemoveProperty(),
            ExpectRemoveLabels());
}

TEST(TestLogicalPlanner, MatchMultiPattern) {
  // Test MATCH (n) -[r]- (m), (j) -[e]- (i) RETURN n
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m")),
              PATTERN(NODE("j"), EDGE("e"), NODE("i"))),
        RETURN(IDENT("n"), AS("n")));
  // We expect the expansions after the first to have a uniqueness filter in a
  // single MATCH clause.
  CheckPlan(storage, ExpectScanAll(), ExpectExpand(), ExpectScanAll(),
            ExpectExpand(), ExpectExpandUniquenessFilter<EdgeAccessor>(),
            ExpectProduce());
}

TEST(TestLogicalPlanner, MatchMultiPatternSameStart) {
  // Test MATCH (n), (n) -[e]- (m) RETURN n
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n")), PATTERN(NODE("n"), EDGE("e"), NODE("m"))),
        RETURN(IDENT("n"), AS("n")));
  // We expect the second pattern to generate only an Expand, since another
  // ScanAll would be redundant.
  CheckPlan(storage, ExpectScanAll(), ExpectExpand(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchMultiPatternSameExpandStart) {
  // Test MATCH (n) -[r]- (m), (m) -[e]- (l) RETURN n
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m")),
              PATTERN(NODE("m"), EDGE("e"), NODE("l"))),
        RETURN(IDENT("n"), AS("n")));
  // We expect the second pattern to generate only an Expand. Another
  // ScanAll would be redundant, as it would generate the nodes obtained from
  // expansion. Additionally, a uniqueness filter is expected.
  CheckPlan(storage, ExpectScanAll(), ExpectExpand(), ExpectExpand(),
            ExpectExpandUniquenessFilter<EdgeAccessor>(), ExpectProduce());
}

TEST(TestLogicalPlanner, MultiMatch) {
  // Test MATCH (n) -[r]- (m) MATCH (j) -[e]- (i) -[f]- (h) RETURN n
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
        MATCH(PATTERN(NODE("j"), EDGE("e"), NODE("i"), EDGE("f"), NODE("h"))),
        RETURN(IDENT("n"), AS("n")));
  // Multiple MATCH clauses form a Cartesian product, so the uniqueness should
  // not cross MATCH boundaries.
  CheckPlan(storage, ExpectScanAll(), ExpectExpand(), ExpectScanAll(),
            ExpectExpand(), ExpectExpand(),
            ExpectExpandUniquenessFilter<EdgeAccessor>(), ExpectProduce());
}

TEST(TestLogicalPlanner, MultiMatchSameStart) {
  // Test MATCH (n) MATCH (n) -[r]- (m) RETURN n
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"))),
        MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
        RETURN(IDENT("n"), AS("n")));
  // Similar to MatchMultiPatternSameStart, we expect only Expand from second
  // MATCH clause.
  CheckPlan(storage, ExpectScanAll(), ExpectExpand(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchExistingEdge) {
  // Test MATCH (n) -[r]- (m) -[r]- (j) RETURN n
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"), EDGE("r"), NODE("j"))),
        RETURN(IDENT("n"), AS("n")));
  // There is no ExpandUniquenessFilter for referencing the same edge.
  CheckPlan(storage, ExpectScanAll(), ExpectExpand(), ExpectExpand(),
            ExpectProduce());
}

TEST(TestLogicalPlanner, MultiMatchExistingEdgeOtherEdge) {
  // Test MATCH (n) -[r]- (m) MATCH (m) -[r]- (j) -[e]- (l) RETURN n
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
        MATCH(PATTERN(NODE("m"), EDGE("r"), NODE("j"), EDGE("e"), NODE("l"))),
        RETURN(IDENT("n"), AS("n")));
  // We need ExpandUniquenessFilter for edge `e` against `r` in second MATCH.
  CheckPlan(storage, ExpectScanAll(), ExpectExpand(), ExpectExpand(),
            ExpectExpand(), ExpectExpandUniquenessFilter<EdgeAccessor>(),
            ExpectProduce());
}

TEST(TestLogicalPlanner, MatchWithReturn) {
  // Test MATCH (old) WITH old AS new RETURN new AS new
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("old"))), WITH(IDENT("old"), AS("new")),
        RETURN(IDENT("new"), AS("new")));
  // No accumulation since we only do reads.
  CheckPlan(storage, ExpectScanAll(), ExpectProduce(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchWithWhereReturn) {
  // Test MATCH (old) WITH old AS new WHERE new.prop < 42 RETURN new AS new
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("old"))), WITH(IDENT("old"), AS("new")),
        WHERE(LESS(PROPERTY_LOOKUP("new", prop), LITERAL(42))),
        RETURN(IDENT("new"), AS("new")));
  // No accumulation since we only do reads.
  CheckPlan(storage, ExpectScanAll(), ExpectProduce(), ExpectFilter(),
            ExpectProduce());
}

TEST(TestLogicalPlanner, CreateMultiExpand) {
  // Test CREATE (n) -[r :r]-> (m), (n) - [p :p]-> (l)
  Dbms dbms;
  auto dba = dbms.active();
  auto r = dba->edge_type("r");
  auto p = dba->edge_type("p");
  AstTreeStorage storage;
  QUERY(CREATE(PATTERN(NODE("n"), EDGE("r", r, Direction::OUT), NODE("m")),
               PATTERN(NODE("n"), EDGE("p", p, Direction::OUT), NODE("l"))));
  CheckPlan(storage, ExpectCreateNode(), ExpectCreateExpand(),
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
  QUERY(MATCH(PATTERN(NODE("n"))), WITH(ADD(sum, literal), AS("sum")),
        WHERE(LESS(IDENT("sum"), LITERAL(42))),
        RETURN(IDENT("sum"), AS("result")));
  auto aggr = ExpectAggregate({sum}, {literal});
  CheckPlan(storage, ExpectScanAll(), aggr, ExpectProduce(), ExpectFilter(),
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
  QUERY(MATCH(PATTERN(NODE("n"))),
        RETURN(sum, AS("sum"), n_prop2, AS("group")));
  auto aggr = ExpectAggregate({sum}, {n_prop2});
  CheckPlan(storage, ExpectScanAll(), aggr, ExpectProduce());
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
  auto plan = MakeLogicalPlan(storage, symbol_table);
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
  QUERY(
      MATCH(PATTERN(NODE("n"))), WITH(IDENT("n"), AS("a")),
      CREATE(PATTERN(NODE("a"), EDGE("r", r_type, Direction::OUT), NODE("b"))));
  CheckPlan(storage, ExpectScanAll(), ExpectProduce(), ExpectCreateExpand());
}

TEST(TestLogicalPlanner, MatchReturnSkipLimit) {
  // Test MATCH (n) RETURN n SKIP 2 LIMIT 1
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"))),
        RETURN(IDENT("n"), AS("n"), SKIP(LITERAL(2)), LIMIT(LITERAL(1))));
  CheckPlan(storage, ExpectScanAll(), ExpectProduce(), ExpectSkip(),
            ExpectLimit());
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
  auto plan = MakeLogicalPlan(storage, symbol_table);
  // Since we have a write query, we need to have Accumulate. This is a bit
  // different than Neo4j 3.0, which optimizes WITH followed by RETURN as a
  // single RETURN clause and then moves Skip and Limit before Accumulate. This
  // causes different behaviour. A newer version of Neo4j does the same thing as
  // us here (but who knows if they change it again).
  CheckPlan(*plan, symbol_table, ExpectCreateNode(), acc, ExpectProduce(),
            ExpectSkip(), ExpectProduce(), ExpectLimit());
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
  auto plan = MakeLogicalPlan(storage, symbol_table);
  CheckPlan(*plan, symbol_table, ExpectCreateNode(), acc, aggr, ExpectProduce(),
            ExpectSkip(), ExpectLimit());
}

TEST(TestLogicalPlanner, MatchReturnOrderBy) {
  // Test MATCH (n) RETURN n ORDER BY n.prop
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto ret = RETURN(IDENT("n"), AS("n"), ORDER_BY(PROPERTY_LOOKUP("n", prop)));
  QUERY(MATCH(PATTERN(NODE("n"))), ret);
  CheckPlan(storage, ExpectScanAll(), ExpectProduce(), ExpectOrderBy());
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
  auto query = QUERY(
      CREATE(PATTERN(NODE("n"), EDGE("r", r_type, Direction::OUT), NODE("m"))),
      WITH(ident_n, AS("new"), ORDER_BY(new_prop, r_prop)),
      WHERE(LESS(m_prop, LITERAL(42))));
  auto symbol_table = MakeSymbolTable(*query);
  // Since this is a write query, we expect to accumulate to old used symbols.
  auto acc = ExpectAccumulate({
      symbol_table.at(*ident_n),              // `n` in WITH
      symbol_table.at(*r_prop->expression_),  // `r` in ORDER BY
      symbol_table.at(*m_prop->expression_),  // `m` in WHERE
  });
  auto plan = MakeLogicalPlan(storage, symbol_table);
  CheckPlan(*plan, symbol_table, ExpectCreateNode(), ExpectCreateExpand(), acc,
            ExpectProduce(), ExpectFilter(), ExpectOrderBy());
}

TEST(TestLogicalPlanner, ReturnAddSumCountOrderBy) {
  // Test RETURN SUM(1) + COUNT(2) AS result ORDER BY result
  AstTreeStorage storage;
  auto sum = SUM(LITERAL(1));
  auto count = COUNT(LITERAL(2));
  QUERY(RETURN(ADD(sum, count), AS("result"), ORDER_BY(IDENT("result"))));
  auto aggr = ExpectAggregate({sum, count}, {});
  CheckPlan(storage, aggr, ExpectProduce(), ExpectOrderBy());
}

TEST(TestLogicalPlanner, MatchMerge) {
  // Test MATCH (n) MERGE (n) -[r :r]- (m)
  //      ON MATCH SET n.prop = 42 ON CREATE SET m = n
  //      RETURN n AS n
  Dbms dbms;
  auto dba = dbms.active();
  auto r_type = dba->edge_type("r");
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto ident_n = IDENT("n");
  auto query =
      QUERY(MATCH(PATTERN(NODE("n"))),
            MERGE(PATTERN(NODE("n"), EDGE("r", r_type), NODE("m")),
                  ON_MATCH(SET(PROPERTY_LOOKUP("n", prop), LITERAL(42))),
                  ON_CREATE(SET("m", IDENT("n")))),
            RETURN(ident_n, AS("n")));
  std::list<BaseOpChecker *> on_match{new ExpectExpand(), new ExpectFilter(),
                                      new ExpectSetProperty()};
  std::list<BaseOpChecker *> on_create{new ExpectCreateExpand(),
                                       new ExpectSetProperties()};
  auto symbol_table = MakeSymbolTable(*query);
  // We expect Accumulate after Merge, because it is considered as a write.
  auto acc = ExpectAccumulate({symbol_table.at(*ident_n)});
  auto plan = MakeLogicalPlan(storage, symbol_table);
  CheckPlan(*plan, symbol_table, ExpectScanAll(),
            ExpectMerge(on_match, on_create), acc, ExpectProduce());
  for (auto &op : on_match) delete op;
  on_match.clear();
  for (auto &op : on_create) delete op;
  on_create.clear();
}

TEST(TestLogicalPlanner, MatchOptionalMatchWhereReturn) {
  // Test MATCH (n) OPTIONAL MATCH (n) -[r]- (m) WHERE m.prop < 42 RETURN r
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"))),
        OPTIONAL_MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
        WHERE(LESS(PROPERTY_LOOKUP("m", prop), LITERAL(42))),
        RETURN(IDENT("r"), AS("r")));
  std::list<BaseOpChecker *> optional{new ExpectScanAll(), new ExpectExpand(),
                                      new ExpectFilter()};
  CheckPlan(storage, ExpectScanAll(), ExpectOptional(optional),
            ExpectProduce());
}

TEST(TestLogicalPlanner, MatchUnwindReturn) {
  // Test MATCH (n) UNWIND [1,2,3] AS x RETURN n AS n, x AS x
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"))),
        UNWIND(LIST(LITERAL(1), LITERAL(2), LITERAL(3)), AS("x")),
        RETURN(IDENT("n"), AS("n"), IDENT("x"), AS("x")));
  CheckPlan(storage, ExpectScanAll(), ExpectUnwind(), ExpectProduce());
}

TEST(TestLogicalPlanner, ReturnDistinctOrderBySkipLimit) {
  // Test RETURN DISTINCT 1 ORDER BY 1 SKIP 1 LIMIT 1
  AstTreeStorage storage;
  QUERY(RETURN_DISTINCT(LITERAL(1), AS("1"), ORDER_BY(LITERAL(1)),
                        SKIP(LITERAL(1)), LIMIT(LITERAL(1))));
  CheckPlan(storage, ExpectProduce(), ExpectDistinct(), ExpectOrderBy(),
            ExpectSkip(), ExpectLimit());
}

TEST(TestLogicalPlanner, CreateWithDistinctSumWhereReturn) {
  // Test CREATE (n) WITH DISTINCT SUM(n.prop) AS s WHERE s < 42 RETURN s
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto node_n = NODE("n");
  auto sum = SUM(PROPERTY_LOOKUP("n", prop));
  auto query =
      QUERY(CREATE(PATTERN(node_n)), WITH_DISTINCT(sum, AS("s")),
            WHERE(LESS(IDENT("s"), LITERAL(42))), RETURN(IDENT("s"), AS("s")));
  auto symbol_table = MakeSymbolTable(*query);
  auto acc = ExpectAccumulate({symbol_table.at(*node_n->identifier_)});
  auto aggr = ExpectAggregate({sum}, {});
  auto plan = MakeLogicalPlan(storage, symbol_table);
  CheckPlan(*plan, symbol_table, ExpectCreateNode(), acc, aggr, ExpectProduce(),
            ExpectFilter(), ExpectDistinct(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchCrossReferenceVariable) {
  // Test MATCH (n {prop: m.prop}), (m {prop: n.prop}) RETURN n
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto node_n = NODE("n");
  auto m_prop = PROPERTY_LOOKUP("m", prop);
  node_n->properties_[prop] = m_prop;
  auto node_m = NODE("m");
  auto n_prop = PROPERTY_LOOKUP("n", prop);
  node_m->properties_[prop] = n_prop;
  QUERY(MATCH(PATTERN(node_n), PATTERN(node_m)), RETURN(IDENT("n"), AS("n")));
  // We expect both ScanAll to come before filters (2 are joined into one),
  // because they need to populate the symbol values.
  CheckPlan(storage, ExpectScanAll(), ExpectScanAll(), ExpectFilter(),
            ExpectProduce());
}

TEST(TestLogicalPlanner, MatchWhereBeforeExpand) {
  // Test MATCH (n) -[r]- (m) WHERE n.prop < 42 RETURN n
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
        WHERE(LESS(PROPERTY_LOOKUP("n", prop), LITERAL(42))),
        RETURN(IDENT("n"), AS("n")));
  // We expect Fitler to come immediately after ScanAll, since it only uses `n`.
  CheckPlan(storage, ExpectScanAll(), ExpectFilter(), ExpectExpand(),
            ExpectProduce());
}

TEST(TestLogicalPlanner, MultiMatchWhere) {
  // Test MATCH (n) -[r]- (m) MATCH (l) WHERE n.prop < 42 RETURN n
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
        MATCH(PATTERN(NODE("l"))),
        WHERE(LESS(PROPERTY_LOOKUP("n", prop), LITERAL(42))),
        RETURN(IDENT("n"), AS("n")));
  // Even though WHERE is in the second MATCH clause, we expect Filter to come
  // before second ScanAll, since it only uses the value from first ScanAll.
  CheckPlan(storage, ExpectScanAll(), ExpectFilter(), ExpectExpand(),
            ExpectScanAll(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchOptionalMatchWhere) {
  // Test MATCH (n) -[r]- (m) OPTIONAL MATCH (l) WHERE n.prop < 42 RETURN n
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
        OPTIONAL_MATCH(PATTERN(NODE("l"))),
        WHERE(LESS(PROPERTY_LOOKUP("n", prop), LITERAL(42))),
        RETURN(IDENT("n"), AS("n")));
  // Even though WHERE is in the second MATCH clause, and it uses the value from
  // first ScanAll, it must remain part of the Optional. It should come before
  // optional ScanAll.
  std::list<BaseOpChecker *> optional{new ExpectFilter(), new ExpectScanAll()};
  CheckPlan(storage, ExpectScanAll(), ExpectExpand(), ExpectOptional(optional),
            ExpectProduce());
}

TEST(TestLogicalPlanner, MatchReturnAsterisk) {
  // Test MATCH (n) -[e]- (m) RETURN *, m.prop
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto ret = RETURN(PROPERTY_LOOKUP("m", prop), AS("m.prop"));
  ret->body_.all_identifiers = true;
  auto query = QUERY(MATCH(PATTERN(NODE("n"), EDGE("e"), NODE("m"))), ret);
  auto symbol_table = MakeSymbolTable(*query);
  auto plan = MakeLogicalPlan(storage, symbol_table);
  CheckPlan(*plan, symbol_table, ExpectScanAll(), ExpectExpand(),
            ExpectProduce());
  std::vector<std::string> output_names;
  for (const auto &output_symbol : plan->OutputSymbols(symbol_table)) {
    output_names.emplace_back(output_symbol.name());
  }
  std::vector<std::string> expected_names{"e", "m", "n", "m.prop"};
  EXPECT_EQ(output_names, expected_names);
}

TEST(TestLogicalPlanner, MatchReturnAsteriskSum) {
  // Test MATCH (n) RETURN *, SUM(n.prop) AS s
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto sum = SUM(PROPERTY_LOOKUP("n", prop));
  auto ret = RETURN(sum, AS("s"));
  ret->body_.all_identifiers = true;
  auto query = QUERY(MATCH(PATTERN(NODE("n"))), ret);
  auto symbol_table = MakeSymbolTable(*query);
  auto plan = MakeLogicalPlan(storage, symbol_table);
  auto *produce = dynamic_cast<Produce *>(plan.get());
  ASSERT_TRUE(produce);
  const auto &named_expressions = produce->named_expressions();
  ASSERT_EQ(named_expressions.size(), 2);
  auto *expanded_ident =
      dynamic_cast<query::Identifier *>(named_expressions[0]->expression_);
  ASSERT_TRUE(expanded_ident);
  auto aggr = ExpectAggregate({sum}, {expanded_ident});
  CheckPlan(*plan, symbol_table, ExpectScanAll(), aggr, ExpectProduce());
  std::vector<std::string> output_names;
  for (const auto &output_symbol : plan->OutputSymbols(symbol_table)) {
    output_names.emplace_back(output_symbol.name());
  }
  std::vector<std::string> expected_names{"n", "s"};
  EXPECT_EQ(output_names, expected_names);
}

TEST(TestLogicalPlanner, UnwindMergeNodeProperty) {
  // Test UNWIND [1] AS i MERGE (n {prop: i})
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto node_n = NODE("n");
  node_n->properties_[prop] = IDENT("i");
  QUERY(UNWIND(LIST(LITERAL(1)), AS("i")), MERGE(PATTERN(node_n)));
  std::list<BaseOpChecker *> on_match{new ExpectScanAll(), new ExpectFilter()};
  std::list<BaseOpChecker *> on_create{new ExpectCreateNode()};
  CheckPlan(storage, ExpectUnwind(), ExpectMerge(on_match, on_create));
  for (auto &op : on_match) delete op;
  for (auto &op : on_create) delete op;
}

TEST(TestLogicalPlanner, MultipleOptionalMatchReturn) {
  // Test OPTIONAL MATCH (n) OPTIONAL MATCH (m) RETURN n
  AstTreeStorage storage;
  QUERY(OPTIONAL_MATCH(PATTERN(NODE("n"))), OPTIONAL_MATCH(PATTERN(NODE("m"))),
        RETURN(IDENT("n"), AS("n")));
  std::list<BaseOpChecker *> optional{new ExpectScanAll()};
  CheckPlan(storage, ExpectOptional(optional), ExpectOptional(optional),
            ExpectProduce());
}

}  // namespace
