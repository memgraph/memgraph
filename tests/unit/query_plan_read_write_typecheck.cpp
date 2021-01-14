#include <gtest/gtest.h>

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/read_write_type_checker.hpp"

#include "query_common.hpp"

using namespace query;
using namespace query::plan;
using RWType = ReadWriteTypeChecker::RWType;

class ReadWriteTypeCheckTest : public ::testing::Test {
 protected:
  ReadWriteTypeCheckTest() : db(), dba(db.Access()) {}

  AstStorage storage;
  SymbolTable symbol_table;

  storage::Storage db;
  storage::Storage::Accessor dba;

  Symbol GetSymbol(std::string name) {
    return symbol_table.CreateSymbol(name, true);
  }

  void Check(LogicalOperator *root, RWType expected) {
    auto rw_type_checker = ReadWriteTypeChecker();
    rw_type_checker.InferRWType(*root);
    EXPECT_EQ(rw_type_checker.type, expected);
  }
};

TEST_F(ReadWriteTypeCheckTest, Once) {
  std::shared_ptr<LogicalOperator> op = std::make_shared<Once>();
  Check(op.get(), RWType::NONE);
}

TEST_F(ReadWriteTypeCheckTest, CreateNode) {
  std::shared_ptr<LogicalOperator> once = std::make_shared<Once>();
  std::shared_ptr<LogicalOperator> create_node =
      std::make_shared<CreateNode>(once, NodeCreationInfo());

  Check(create_node.get(), RWType::W);
}

TEST_F(ReadWriteTypeCheckTest, Filter) {
  std::shared_ptr<LogicalOperator> scan_all =
      std::make_shared<ScanAll>(nullptr, GetSymbol("node1"));
  std::shared_ptr<LogicalOperator> filter = std::make_shared<Filter>(
      scan_all,
      EQ(PROPERTY_LOOKUP("node1", dba.NameToProperty("prop")), LITERAL(0)));

  Check(filter.get(), RWType::R);
}

TEST_F(ReadWriteTypeCheckTest, Cartesian) {
  Symbol x = GetSymbol("x");
  std::shared_ptr<LogicalOperator> lhs = std::make_shared<plan::Unwind>(
      nullptr, LIST(LITERAL(1), LITERAL(2), LITERAL(3)), x);

  Symbol node = GetSymbol("node");
  std::shared_ptr<LogicalOperator> rhs =
      std::make_shared<ScanAll>(nullptr, node);

  std::shared_ptr<LogicalOperator> cartesian = std::make_shared<Cartesian>(
      lhs, std::vector<Symbol>{x}, rhs, std::vector<Symbol>{node});

  Check(cartesian.get(), RWType::R);
}

TEST_F(ReadWriteTypeCheckTest, Union) {
  Symbol x = GetSymbol("x");
  std::shared_ptr<LogicalOperator> lhs = std::make_shared<plan::Unwind>(
      nullptr, LIST(LITERAL(2), LITERAL(3), LITERAL(2)), x);

  Symbol node = GetSymbol("x");
  std::shared_ptr<LogicalOperator> rhs =
      std::make_shared<ScanAll>(nullptr, node);

  std::shared_ptr<LogicalOperator> union_op = std::make_shared<Union>(
      lhs, rhs, std::vector<Symbol>{GetSymbol("x")}, std::vector<Symbol>{x},
      std::vector<Symbol>{node});
  
  Check(union_op.get(), RWType::R);
}

