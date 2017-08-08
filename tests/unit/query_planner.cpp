#include <list>
#include <tuple>
#include <unordered_set>

#include "gtest/gtest.h"

#include "database/dbms.hpp"
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
using Bound = ScanAllByLabelPropertyRange::Bound;

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
  PRE_VISIT(ScanAllByLabelPropertyValue);
  PRE_VISIT(ScanAllByLabelPropertyRange);
  PRE_VISIT(Expand);
  PRE_VISIT(ExpandVariable);
  PRE_VISIT(ExpandBreadthFirst);
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

  bool Visit(CreateIndex &op) override {
    CheckOp(op);
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
using ExpectExpandVariable = OpChecker<ExpandVariable>;
using ExpectExpandBreadthFirst = OpChecker<ExpandBreadthFirst>;
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

class ExpectScanAllByLabelPropertyValue
    : public OpChecker<ScanAllByLabelPropertyValue> {
 public:
  ExpectScanAllByLabelPropertyValue(
      GraphDbTypes::Label label,
      const std::pair<std::string, GraphDbTypes::Property> &prop_pair,
      query::Expression *expression)
      : label_(label), property_(prop_pair.second), expression_(expression) {}

  void ExpectOp(ScanAllByLabelPropertyValue &scan_all,
                const SymbolTable &) override {
    EXPECT_EQ(scan_all.label(), label_);
    EXPECT_EQ(scan_all.property(), property_);
    EXPECT_EQ(scan_all.expression(), expression_);
  }

 private:
  GraphDbTypes::Label label_;
  GraphDbTypes::Property property_;
  query::Expression *expression_;
};

class ExpectScanAllByLabelPropertyRange
    : public OpChecker<ScanAllByLabelPropertyRange> {
 public:
  ExpectScanAllByLabelPropertyRange(
      GraphDbTypes::Label label, GraphDbTypes::Property property,
      std::experimental::optional<Bound> lower_bound,
      std::experimental::optional<Bound> upper_bound)
      : label_(label),
        property_(property),
        lower_bound_(lower_bound),
        upper_bound_(upper_bound) {}

  void ExpectOp(ScanAllByLabelPropertyRange &scan_all,
                const SymbolTable &) override {
    EXPECT_EQ(scan_all.label(), label_);
    EXPECT_EQ(scan_all.property(), property_);
    if (lower_bound_) {
      ASSERT_TRUE(scan_all.lower_bound());
      EXPECT_EQ(scan_all.lower_bound()->value(), lower_bound_->value());
      EXPECT_EQ(scan_all.lower_bound()->type(), lower_bound_->type());
    }
    if (upper_bound_) {
      ASSERT_TRUE(scan_all.upper_bound());
      EXPECT_EQ(scan_all.upper_bound()->value(), upper_bound_->value());
      EXPECT_EQ(scan_all.upper_bound()->type(), upper_bound_->type());
    }
  }

 private:
  GraphDbTypes::Label label_;
  GraphDbTypes::Property property_;
  std::experimental::optional<Bound> lower_bound_;
  std::experimental::optional<Bound> upper_bound_;
};

class ExpectCreateIndex : public OpChecker<CreateIndex> {
 public:
  ExpectCreateIndex(GraphDbTypes::Label label, GraphDbTypes::Property property)
      : label_(label), property_(property) {}

  void ExpectOp(CreateIndex &create_index, const SymbolTable &) override {
    EXPECT_EQ(create_index.label(), label_);
    EXPECT_EQ(create_index.property(), property_);
  }

 private:
  GraphDbTypes::Label label_;
  GraphDbTypes::Property property_;
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
  Dbms dbms;
  auto plan =
      MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dbms.active());
  CheckPlan(*plan, symbol_table, checker...);
}

TEST(TestLogicalPlanner, MatchNodeReturn) {
  // Test MATCH (n) RETURN n
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"))), RETURN("n"));
  CheckPlan(storage, ExpectScanAll(), ExpectProduce());
}

TEST(TestLogicalPlanner, CreateNodeReturn) {
  // Test CREATE (n) RETURN n AS n
  AstTreeStorage storage;
  auto ident_n = IDENT("n");
  auto query = QUERY(CREATE(PATTERN(NODE("n"))), RETURN(ident_n, AS("n")));
  auto symbol_table = MakeSymbolTable(*query);
  auto acc = ExpectAccumulate({symbol_table.at(*ident_n)});
  Dbms dbms;
  auto plan =
      MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dbms.active());
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
  // Test MATCH (n :label) RETURN n
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto label = dba->label("label");
  QUERY(MATCH(PATTERN(NODE("n", label))), RETURN("n"));
  CheckPlan(storage, ExpectScanAllByLabel(), ExpectFilter(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchPathReturn) {
  // Test MATCH (n) -[r :relationship]- (m) RETURN n
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto relationship = dba->edge_type("relationship");
  QUERY(MATCH(PATTERN(NODE("n"), EDGE("r", relationship), NODE("m"))),
        RETURN("n"));
  CheckPlan(storage, ExpectScanAll(), ExpectExpand(), ExpectFilter(),
            ExpectProduce());
}

TEST(TestLogicalPlanner, MatchWhereReturn) {
  // Test MATCH (n) WHERE n.property < 42 RETURN n
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto property = dba->property("property");
  QUERY(MATCH(PATTERN(NODE("n"))),
        WHERE(LESS(PROPERTY_LOOKUP("n", property), LITERAL(42))), RETURN("n"));
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
        RETURN("n"));
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
        RETURN("n"));
  // We expect the second pattern to generate only an Expand, since another
  // ScanAll would be redundant.
  CheckPlan(storage, ExpectScanAll(), ExpectExpand(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchMultiPatternSameExpandStart) {
  // Test MATCH (n) -[r]- (m), (m) -[e]- (l) RETURN n
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m")),
              PATTERN(NODE("m"), EDGE("e"), NODE("l"))),
        RETURN("n"));
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
        RETURN("n"));
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
        MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))), RETURN("n"));
  // Similar to MatchMultiPatternSameStart, we expect only Expand from second
  // MATCH clause.
  CheckPlan(storage, ExpectScanAll(), ExpectExpand(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchExistingEdge) {
  // Test MATCH (n) -[r]- (m) -[r]- (j) RETURN n
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"), EDGE("r"), NODE("j"))),
        RETURN("n"));
  // There is no ExpandUniquenessFilter for referencing the same edge.
  CheckPlan(storage, ExpectScanAll(), ExpectExpand(), ExpectExpand(),
            ExpectProduce());
}

TEST(TestLogicalPlanner, MultiMatchExistingEdgeOtherEdge) {
  // Test MATCH (n) -[r]- (m) MATCH (m) -[r]- (j) -[e]- (l) RETURN n
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
        MATCH(PATTERN(NODE("m"), EDGE("r"), NODE("j"), EDGE("e"), NODE("l"))),
        RETURN("n"));
  // We need ExpandUniquenessFilter for edge `e` against `r` in second MATCH.
  CheckPlan(storage, ExpectScanAll(), ExpectExpand(), ExpectExpand(),
            ExpectExpand(), ExpectExpandUniquenessFilter<EdgeAccessor>(),
            ExpectProduce());
}

TEST(TestLogicalPlanner, MatchWithReturn) {
  // Test MATCH (old) WITH old AS new RETURN new
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("old"))), WITH("old", AS("new")), RETURN("new"));
  // No accumulation since we only do reads.
  CheckPlan(storage, ExpectScanAll(), ExpectProduce(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchWithWhereReturn) {
  // Test MATCH (old) WITH old AS new WHERE new.prop < 42 RETURN new
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("old"))), WITH("old", AS("new")),
        WHERE(LESS(PROPERTY_LOOKUP("new", prop), LITERAL(42))), RETURN("new"));
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
        WHERE(LESS(IDENT("sum"), LITERAL(42))), RETURN("sum", AS("result")));
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
  auto plan = MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dba);
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
      MATCH(PATTERN(NODE("n"))), WITH("n", AS("a")),
      CREATE(PATTERN(NODE("a"), EDGE("r", r_type, Direction::OUT), NODE("b"))));
  CheckPlan(storage, ExpectScanAll(), ExpectProduce(), ExpectCreateExpand());
}

TEST(TestLogicalPlanner, MatchReturnSkipLimit) {
  // Test MATCH (n) RETURN n SKIP 2 LIMIT 1
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"))),
        RETURN("n", SKIP(LITERAL(2)), LIMIT(LITERAL(1))));
  CheckPlan(storage, ExpectScanAll(), ExpectProduce(), ExpectSkip(),
            ExpectLimit());
}

TEST(TestLogicalPlanner, CreateWithSkipReturnLimit) {
  // Test CREATE (n) WITH n AS m SKIP 2 RETURN m LIMIT 1
  AstTreeStorage storage;
  auto ident_n = IDENT("n");
  auto query = QUERY(CREATE(PATTERN(NODE("n"))),
                     WITH(ident_n, AS("m"), SKIP(LITERAL(2))),
                     RETURN("m", LIMIT(LITERAL(1))));
  auto symbol_table = MakeSymbolTable(*query);
  auto acc = ExpectAccumulate({symbol_table.at(*ident_n)});
  Dbms dbms;
  auto plan =
      MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dbms.active());
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
  auto plan = MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dba);
  CheckPlan(*plan, symbol_table, ExpectCreateNode(), acc, aggr, ExpectProduce(),
            ExpectSkip(), ExpectLimit());
}

TEST(TestLogicalPlanner, MatchReturnOrderBy) {
  // Test MATCH (n) RETURN n ORDER BY n.prop
  Dbms dbms;
  auto dba = dbms.active();
  auto prop = dba->property("prop");
  AstTreeStorage storage;
  auto ret = RETURN("n", ORDER_BY(PROPERTY_LOOKUP("n", prop)));
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
  auto plan = MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dba);
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
  auto plan = MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dba);
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
        WHERE(LESS(PROPERTY_LOOKUP("m", prop), LITERAL(42))), RETURN("r"));
  std::list<BaseOpChecker *> optional{new ExpectScanAll(), new ExpectExpand(),
                                      new ExpectFilter()};
  CheckPlan(storage, ExpectScanAll(), ExpectOptional(optional),
            ExpectProduce());
}

TEST(TestLogicalPlanner, MatchUnwindReturn) {
  // Test MATCH (n) UNWIND [1,2,3] AS x RETURN n, x
  AstTreeStorage storage;
  QUERY(MATCH(PATTERN(NODE("n"))),
        UNWIND(LIST(LITERAL(1), LITERAL(2), LITERAL(3)), AS("x")),
        RETURN("n", "x"));
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
  auto query = QUERY(CREATE(PATTERN(node_n)), WITH_DISTINCT(sum, AS("s")),
                     WHERE(LESS(IDENT("s"), LITERAL(42))), RETURN("s"));
  auto symbol_table = MakeSymbolTable(*query);
  auto acc = ExpectAccumulate({symbol_table.at(*node_n->identifier_)});
  auto aggr = ExpectAggregate({sum}, {});
  auto plan = MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dba);
  CheckPlan(*plan, symbol_table, ExpectCreateNode(), acc, aggr, ExpectProduce(),
            ExpectFilter(), ExpectDistinct(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchCrossReferenceVariable) {
  // Test MATCH (n {prop: m.prop}), (m {prop: n.prop}) RETURN n
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
  QUERY(MATCH(PATTERN(node_n), PATTERN(node_m)), RETURN("n"));
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
        WHERE(LESS(PROPERTY_LOOKUP("n", prop), LITERAL(42))), RETURN("n"));
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
        WHERE(LESS(PROPERTY_LOOKUP("n", prop), LITERAL(42))), RETURN("n"));
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
        WHERE(LESS(PROPERTY_LOOKUP("n", prop), LITERAL(42))), RETURN("n"));
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
  auto plan = MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dba);
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
  auto plan = MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dba);
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
  AstTreeStorage storage;
  auto node_n = NODE("n");
  node_n->properties_[PROPERTY_PAIR("prop")] = IDENT("i");
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
        RETURN("n"));
  std::list<BaseOpChecker *> optional{new ExpectScanAll()};
  CheckPlan(storage, ExpectOptional(optional), ExpectOptional(optional),
            ExpectProduce());
}

TEST(TestLogicalPlanner, FunctionAggregationReturn) {
  // Test RETURN sqrt(SUM(2)) AS result, 42 AS group_by
  AstTreeStorage storage;
  auto sum = SUM(LITERAL(2));
  auto group_by_literal = LITERAL(42);
  QUERY(
      RETURN(FN("sqrt", sum), AS("result"), group_by_literal, AS("group_by")));
  auto aggr = ExpectAggregate({sum}, {group_by_literal});
  CheckPlan(storage, aggr, ExpectProduce());
}

TEST(TestLogicalPlanner, FunctionWithoutArguments) {
  // Test RETURN pi() AS pi
  AstTreeStorage storage;
  QUERY(RETURN(FN("pi"), AS("pi")));
  CheckPlan(storage, ExpectProduce());
}

TEST(TestLogicalPlanner, ListLiteralAggregationReturn) {
  // Test RETURN [SUM(2)] AS result, 42 AS group_by
  AstTreeStorage storage;
  auto sum = SUM(LITERAL(2));
  auto group_by_literal = LITERAL(42);
  QUERY(RETURN(LIST(sum), AS("result"), group_by_literal, AS("group_by")));
  auto aggr = ExpectAggregate({sum}, {group_by_literal});
  CheckPlan(storage, aggr, ExpectProduce());
}

TEST(TestLogicalPlanner, MapLiteralAggregationReturn) {
  // Test RETURN {sum: SUM(2)} AS result, 42 AS group_by
  AstTreeStorage storage; Dbms dbms;
  auto dba = dbms.active();
  auto sum = SUM(LITERAL(2));
  auto group_by_literal = LITERAL(42);
  QUERY(RETURN(MAP({PROPERTY_PAIR("sum"), sum}), AS("result"), group_by_literal,
               AS("group_by")));
  auto aggr = ExpectAggregate({sum}, {group_by_literal});
  CheckPlan(storage, aggr, ExpectProduce());
}

TEST(TestLogicalPlanner, EmptyListIndexAggregation) {
  // Test RETURN [][SUM(2)] AS result, 42 AS group_by
  AstTreeStorage storage;
  auto sum = SUM(LITERAL(2));
  auto empty_list = LIST();
  auto group_by_literal = LITERAL(42);
  QUERY(RETURN(storage.Create<query::ListIndexingOperator>(empty_list, sum),
               AS("result"), group_by_literal, AS("group_by")));
  // We expect to group by '42' and the empty list, because it is a
  // sub-expression of a binary operator which contains an aggregation. This is
  // similar to grouping by '1' in `RETURN 1 + SUM(2)`.
  auto aggr = ExpectAggregate({sum}, {empty_list, group_by_literal});
  CheckPlan(storage, aggr, ExpectProduce());
}

TEST(TestLogicalPlanner, ListSliceAggregationReturn) {
  // Test RETURN [1, 2][0..SUM(2)] AS result, 42 AS group_by
  AstTreeStorage storage;
  auto sum = SUM(LITERAL(2));
  auto list = LIST(LITERAL(1), LITERAL(2));
  auto group_by_literal = LITERAL(42);
  QUERY(RETURN(SLICE(list, LITERAL(0), sum), AS("result"), group_by_literal,
               AS("group_by")));
  // Similarly to EmptyListIndexAggregation test, we expect grouping by list and
  // '42', because slicing is an operator.
  auto aggr = ExpectAggregate({sum}, {list, group_by_literal});
  CheckPlan(storage, aggr, ExpectProduce());
}

TEST(TestLogicalPlanner, CreateIndex) {
  // Test CREATE INDEX ON :label(property)
  Dbms dbms;
  auto dba = dbms.active();
  auto label = dba->label("label");
  auto property = dba->property("property");
  AstTreeStorage storage;
  QUERY(CREATE_INDEX_ON(label, property));
  CheckPlan(storage, ExpectCreateIndex(label, property));
}

TEST(TestLogicalPlanner, AtomIndexedLabelProperty) {
  // Test MATCH (n :label {property: 42, not_indexed: 0}) RETURN n
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto label = dba->label("label");
  auto property = PROPERTY_PAIR("property");
  auto not_indexed = PROPERTY_PAIR("not_indexed");
  auto vertex = dba->insert_vertex();
  vertex.add_label(label);
  vertex.PropsSet(property.second, 42);
  dba->commit();
  dba = dbms.active();
  dba->BuildIndex(label, property.second);
  dba = dbms.active();
  auto node = NODE("n", label);
  auto lit_42 = LITERAL(42);
  node->properties_[property] = lit_42;
  node->properties_[not_indexed] = LITERAL(0);
  QUERY(MATCH(PATTERN(node)), RETURN("n"));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto plan = MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dba);
  CheckPlan(*plan, symbol_table,
            ExpectScanAllByLabelPropertyValue(label, property, lit_42),
            ExpectFilter(), ExpectProduce());
}

TEST(TestLogicalPlanner, AtomPropertyWhereLabelIndexing) {
  // Test MATCH (n {property: 42}) WHERE n.not_indexed AND n:label RETURN n
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto label = dba->label("label");
  auto property = PROPERTY_PAIR("property");
  auto not_indexed = PROPERTY_PAIR("not_indexed");
  dba->BuildIndex(label, property.second);
  dba = dbms.active();
  auto node = NODE("n");
  auto lit_42 = LITERAL(42);
  node->properties_[property] = lit_42;
  QUERY(MATCH(PATTERN(node)),
        WHERE(AND(PROPERTY_LOOKUP("n", not_indexed),
                  storage.Create<query::LabelsTest>(
                      IDENT("n"), std::vector<GraphDbTypes::Label>{label}))),
        RETURN("n"));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto plan = MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dba);
  CheckPlan(*plan, symbol_table,
            ExpectScanAllByLabelPropertyValue(label, property, lit_42),
            ExpectFilter(), ExpectProduce());
}

TEST(TestLogicalPlanner, WhereIndexedLabelProperty) {
  // Test MATCH (n :label) WHERE n.property = 42 RETURN n
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto label = dba->label("label");
  auto property = PROPERTY_PAIR("property");
  dba->BuildIndex(label, property.second);
  dba = dbms.active();
  auto lit_42 = LITERAL(42);
  QUERY(MATCH(PATTERN(NODE("n", label))),
        WHERE(EQ(PROPERTY_LOOKUP("n", property), lit_42)), RETURN("n"));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto plan = MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dba);
  CheckPlan(*plan, symbol_table,
            ExpectScanAllByLabelPropertyValue(label, property, lit_42),
            ExpectFilter(), ExpectProduce());
}

TEST(TestLogicalPlanner, BestPropertyIndexed) {
  // Test MATCH (n :label) WHERE n.property = 1 AND n.better = 42 RETURN n
  AstTreeStorage storage;
  Dbms dbms;
  auto dba = dbms.active();
  auto label = dba->label("label");
  auto property = dba->property("property");
  dba->BuildIndex(label, property);
  dba = dbms.active();
  // Add a vertex with :label+property combination, so that the best
  // :label+better remains empty and thus better choice.
  auto vertex = dba->insert_vertex();
  vertex.add_label(label);
  vertex.PropsSet(property, 1);
  dba->commit();
  dba = dbms.active();
  ASSERT_EQ(dba->vertices_count(label, property), 1);
  auto better = PROPERTY_PAIR("better");
  dba->BuildIndex(label, better.second);
  dba = dbms.active();
  ASSERT_EQ(dba->vertices_count(label, better.second), 0);
  auto lit_42 = LITERAL(42);
  QUERY(MATCH(PATTERN(NODE("n", label))),
        WHERE(AND(EQ(PROPERTY_LOOKUP("n", property), LITERAL(1)),
                  EQ(PROPERTY_LOOKUP("n", better), lit_42))),
        RETURN("n"));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto plan = MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dba);
  CheckPlan(*plan, symbol_table,
            ExpectScanAllByLabelPropertyValue(label, better, lit_42),
            ExpectFilter(), ExpectProduce());
}

TEST(TestLogicalPlanner, MultiPropertyIndexScan) {
  // Test MATCH (n :label1), (m :label2) WHERE n.prop1 = 1 AND m.prop2 = 2
  //      RETURN n, m
  Dbms dbms;
  auto dba = dbms.active();
  auto label1 = dba->label("label1");
  auto label2 = dba->label("label2");
  auto prop1 = PROPERTY_PAIR("prop1");
  auto prop2 = PROPERTY_PAIR("prop2");
  dba->BuildIndex(label1, prop1.second);
  dba = dbms.active();
  dba->BuildIndex(label2, prop2.second);
  dba = dbms.active();
  AstTreeStorage storage;
  auto lit_1 = LITERAL(1);
  auto lit_2 = LITERAL(2);
  QUERY(MATCH(PATTERN(NODE("n", label1)), PATTERN(NODE("m", label2))),
        WHERE(AND(EQ(PROPERTY_LOOKUP("n", prop1), lit_1),
                  EQ(PROPERTY_LOOKUP("m", prop2), lit_2))),
        RETURN("n", "m"));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto plan = MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dba);
  CheckPlan(*plan, symbol_table,
            ExpectScanAllByLabelPropertyValue(label1, prop1, lit_1),
            ExpectFilter(),
            ExpectScanAllByLabelPropertyValue(label2, prop2, lit_2),
            ExpectFilter(), ExpectProduce());
}

TEST(TestLogicalPlanner, WhereIndexedLabelPropertyRange) {
  // Test MATCH (n :label) WHERE n.property REL_OP 42 RETURN n
  // REL_OP is one of: `<`, `<=`, `>`, `>=`
  Dbms dbms;
  auto dba = dbms.active();
  auto label = dba->label("label");
  auto property = dba->property("property");
  dba->BuildIndex(label, property);
  dba = dbms.active();
  AstTreeStorage storage;
  auto lit_42 = LITERAL(42);
  auto n_prop = PROPERTY_LOOKUP("n", property);
  auto check_planned_range = [&label, &property, &dba](
      const auto &rel_expr, auto lower_bound, auto upper_bound) {
    // Shadow the first storage, so that the query is created in this one.
    AstTreeStorage storage;
    QUERY(MATCH(PATTERN(NODE("n", label))), WHERE(rel_expr), RETURN("n"));
    auto symbol_table = MakeSymbolTable(*storage.query());
    auto plan = MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dba);
    CheckPlan(*plan, symbol_table,
              ExpectScanAllByLabelPropertyRange(label, property, lower_bound,
                                                upper_bound),
              ExpectFilter(), ExpectProduce());
  };
  {
    // Test relation operators which form an upper bound for range.
    std::vector<std::pair<query::Expression *, Bound::Type>> upper_bound_rel_op{
        std::make_pair(LESS(n_prop, lit_42), Bound::Type::EXCLUSIVE),
        std::make_pair(LESS_EQ(n_prop, lit_42), Bound::Type::INCLUSIVE),
        std::make_pair(GREATER(lit_42, n_prop), Bound::Type::EXCLUSIVE),
        std::make_pair(GREATER_EQ(lit_42, n_prop), Bound::Type::INCLUSIVE)};
    for (const auto &rel_op : upper_bound_rel_op) {
      check_planned_range(rel_op.first, std::experimental::nullopt,
                          Bound(lit_42, rel_op.second));
    }
  }
  {
    // Test relation operators which form a lower bound for range.
    std::vector<std::pair<query::Expression *, Bound::Type>> lower_bound_rel_op{
        std::make_pair(LESS(lit_42, n_prop), Bound::Type::EXCLUSIVE),
        std::make_pair(LESS_EQ(lit_42, n_prop), Bound::Type::INCLUSIVE),
        std::make_pair(GREATER(n_prop, lit_42), Bound::Type::EXCLUSIVE),
        std::make_pair(GREATER_EQ(n_prop, lit_42), Bound::Type::INCLUSIVE)};
    for (const auto &rel_op : lower_bound_rel_op) {
      check_planned_range(rel_op.first, Bound(lit_42, rel_op.second),
                          std::experimental::nullopt);
    }
  }
}

TEST(TestLogicalPlanner, UnableToUsePropertyIndex) {
  // Test MATCH (n: label) WHERE n.property = n.property RETURN n
  Dbms dbms;
  auto dba = dbms.active();
  auto label = dba->label("label");
  auto property = dba->property("property");
  dba->BuildIndex(label, property);
  dba = dbms.active();
  AstTreeStorage storage;
  QUERY(
      MATCH(PATTERN(NODE("n", label))),
      WHERE(EQ(PROPERTY_LOOKUP("n", property), PROPERTY_LOOKUP("n", property))),
      RETURN("n"));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto plan = MakeLogicalPlan<RuleBasedPlanner>(storage, symbol_table, *dba);
  // We can only get ScanAllByLabelIndex, because we are comparing properties
  // with those on the same node.
  CheckPlan(*plan, symbol_table, ExpectScanAllByLabel(), ExpectFilter(),
            ExpectProduce());
}

TEST(TestLogicalPlanner, ReturnSumGroupByAll) {
  // Test RETURN sum([1,2,3]), all(x in [1] where x = 1)
  AstTreeStorage storage;
  auto sum = SUM(LIST(LITERAL(1), LITERAL(2), LITERAL(3)));
  auto *all = ALL("x", LIST(LITERAL(1)), WHERE(EQ(IDENT("x"), LITERAL(1))));
  QUERY(RETURN(sum, AS("sum"), all, AS("all")));
  auto aggr = ExpectAggregate({sum}, {all});
  CheckPlan(storage, aggr, ExpectProduce());
}

TEST(TestLogicalPlanner, MatchExpandVariable) {
  // Test MATCH (n) -[r *..3]-> (m) RETURN r
  AstTreeStorage storage;
  auto edge = EDGE("r");
  edge->has_range_ = true;
  edge->upper_bound_ = LITERAL(3);
  QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m"))), RETURN("r"));
  CheckPlan(storage, ExpectScanAll(), ExpectExpandVariable(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchExpandVariableNoBounds) {
  // Test MATCH (n) -[r *]-> (m) RETURN r
  AstTreeStorage storage;
  auto edge = EDGE("r");
  edge->has_range_ = true;
  QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m"))), RETURN("r"));
  CheckPlan(storage, ExpectScanAll(), ExpectExpandVariable(), ExpectProduce());
}

TEST(TestLogicalPlanner, MatchExpandVariableFiltered) {
  // Test MATCH (n) -[r :type * {prop: 42}]-> (m) RETURN r
  Dbms dbms;
  auto dba = dbms.active();
  auto type = dba->edge_type("type");
  auto prop = PROPERTY_PAIR("prop");
  AstTreeStorage storage;
  auto edge = EDGE("r", type);
  edge->has_range_ = true;
  edge->properties_[prop] = LITERAL(42);
  QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m"))), RETURN("r"));
  CheckPlan(storage, ExpectScanAll(), ExpectExpandVariable(), ExpectFilter(),
            ExpectProduce());
}

TEST(TestLogicalPlanner, UnwindMatchVariable) {
  // Test UNWIND [1,2,3] AS depth MATCH (n) -[r*d]-> (m) RETURN r
  AstTreeStorage storage;
  auto edge = EDGE("r", Direction::OUT);
  edge->has_range_ = true;
  edge->lower_bound_ = IDENT("d");
  edge->upper_bound_ = IDENT("d");
  QUERY(UNWIND(LIST(LITERAL(1), LITERAL(2), LITERAL(3)), AS("d")),
        MATCH(PATTERN(NODE("n"), edge, NODE("m"))), RETURN("r"));
  CheckPlan(storage, ExpectUnwind(), ExpectScanAll(), ExpectExpandVariable(),
            ExpectProduce());
}

TEST(TestLogicalPlanner, MatchBreadthFirst) {
  // Test MATCH (n) -bfs[r](r, n|n, 10)-> (m) RETURN r
  AstTreeStorage storage;
  auto *bfs = storage.Create<query::BreadthFirstAtom>(
      IDENT("r"), Direction::OUT, IDENT("r"), IDENT("n"), IDENT("n"),
      LITERAL(10));
  QUERY(MATCH(PATTERN(NODE("n"), bfs, NODE("m"))), RETURN("r"));
  CheckPlan(storage, ExpectScanAll(), ExpectExpandBreadthFirst(),
            ExpectProduce());
}

}  // namespace
