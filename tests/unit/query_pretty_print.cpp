// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cstddef>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "disk_test_utils.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/pretty_print.hpp"
#include "query_common.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/string.hpp"

using namespace memgraph::query;
using memgraph::query::test_common::ToString;
using testing::ElementsAre;
using testing::UnorderedElementsAre;

namespace {

template <typename StorageType>
class ExpressionPrettyPrinterTest : public ::testing::Test {
 public:
  const std::string testSuite = "query_pretty_print";
  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> db{new StorageType(config)};
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba{
      db->Access(memgraph::replication_coordination_glue::ReplicationRole::MAIN)};
  memgraph::query::DbAccessor dba{storage_dba.get()};
  AstStorage storage;

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_SUITE(ExpressionPrettyPrinterTest, StorageTypes);

TYPED_TEST(ExpressionPrettyPrinterTest, Literals) {
  // 1
  EXPECT_EQ(ToString(LITERAL(1)), "1");

  // "hello"
  EXPECT_EQ(ToString(LITERAL("hello")), "\"hello\"");

  // null
  EXPECT_EQ(ToString(LITERAL(TypedValue())), "null");

  // true
  EXPECT_EQ(ToString(LITERAL(true)), "true");

  // false
  EXPECT_EQ(ToString(LITERAL(false)), "false");

  // [1 null "hello"]
  std::vector<memgraph::storage::PropertyValue> values{memgraph::storage::PropertyValue(1),
                                                       memgraph::storage::PropertyValue(),
                                                       memgraph::storage::PropertyValue("hello")};
  EXPECT_EQ(ToString(LITERAL(memgraph::storage::PropertyValue(values))), "[1, null, \"hello\"]");

  // {hello: 1, there: 2}
  std::map<std::string, memgraph::storage::PropertyValue> map{{"hello", memgraph::storage::PropertyValue(1)},
                                                              {"there", memgraph::storage::PropertyValue(2)}};
  EXPECT_EQ(ToString(LITERAL(memgraph::storage::PropertyValue(map))), "{\"hello\": 1, \"there\": 2}");

  std::vector<memgraph::storage::PropertyValue> tt_vec{
      memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Duration, 1)),
      memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Duration, -2)),
      memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::LocalTime, 2)),
      memgraph::storage::PropertyValue(
          memgraph::storage::TemporalData(memgraph::storage::TemporalType::LocalDateTime, 3)),
      memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 4))};
  EXPECT_EQ(ToString(LITERAL(memgraph::storage::PropertyValue(tt_vec))),
            "[DURATION(\"P0DT0H0M0.000001S\"), DURATION(\"P0DT0H0M-0.000002S\"), LOCALTIME(\"00:00:00.000002\"), "
            "LOCALDATETIME(\"1970-01-01T00:00:00.000003\"), DATE(\"1970-01-01\")]");

  // map {literalEntry: 10, variableSelector: a, .map, .*}
  auto elements = std::unordered_map<memgraph::query::PropertyIx, memgraph::query::Expression *>{
      {this->storage.GetPropertyIx("literalEntry"), LITERAL(10)},
      {this->storage.GetPropertyIx("variableSelector"), IDENT("a")},
      {this->storage.GetPropertyIx("propertySelector"),
       PROPERTY_LOOKUP(this->dba, "map", PROPERTY_PAIR(this->dba, "hello"))},
      {this->storage.GetPropertyIx("allPropertiesSelector"), ALL_PROPERTIES_LOOKUP("map")}};
  EXPECT_EQ(ToString(MAP_PROJECTION(IDENT("map"), elements)),
            "(Identifier \"map\"){\"allPropertiesSelector\": .*, \"literalEntry\": 10, \"propertySelector\": "
            "(PropertyLookup (Identifier \"map\") \"hello\"), \"variableSelector\": (Identifier \"a\")}");
}

TYPED_TEST(ExpressionPrettyPrinterTest, Identifiers) {
  // x
  EXPECT_EQ(ToString(IDENT("x")), "(Identifier \"x\")");

  // hello_there
  EXPECT_EQ(ToString(IDENT("hello_there")), "(Identifier \"hello_there\")");
}

TYPED_TEST(ExpressionPrettyPrinterTest, Reducing) {
  // all(x in list where x.prop = 42)
  auto prop = this->dba.NameToProperty("prop");
  EXPECT_EQ(ToString(ALL("x", LITERAL(std::vector<memgraph::storage::PropertyValue>{}),
                         WHERE(EQ(PROPERTY_LOOKUP(this->dba, "x", prop), LITERAL(42))))),
            "(All (Identifier \"x\") [] (== (PropertyLookup "
            "(Identifier \"x\") \"prop\") 42))");

  // reduce(accumulator = initial_value, variable IN list | expression)
  EXPECT_EQ(ToString(REDUCE("accumulator", IDENT("initial_value"), "variable", IDENT("list"), IDENT("expression"))),
            "(Reduce (Identifier \"accumulator\") (Identifier \"initial_value\") "
            "(Identifier \"variable\") (Identifier \"list\") (Identifier "
            "\"expression\"))");
}

TYPED_TEST(ExpressionPrettyPrinterTest, UnaryOperators) {
  // not(false)
  EXPECT_EQ(ToString(NOT(LITERAL(false))), "(Not false)");

  // +1
  EXPECT_EQ(ToString(UPLUS(LITERAL(1))), "(+ 1)");

  // -1
  EXPECT_EQ(ToString(UMINUS(LITERAL(1))), "(- 1)");

  // null IS NULL
  EXPECT_EQ(ToString(IS_NULL(LITERAL(TypedValue()))), "(IsNull null)");
}

TYPED_TEST(ExpressionPrettyPrinterTest, BinaryOperators) {
  // and(null, 5)
  EXPECT_EQ(ToString(AND(LITERAL(TypedValue()), LITERAL(5))), "(And null 5)");

  // or(5, {hello: "there"}["hello"])
  EXPECT_EQ(ToString(OR(
                LITERAL(5),
                PROPERTY_LOOKUP(this->dba, MAP(std::make_pair(this->storage.GetPropertyIx("hello"), LITERAL("there"))),
                                "hello"))),
            "(Or 5 (PropertyLookup {\"hello\": \"there\"} \"hello\"))");

  // and(coalesce(null, 1), {hello: "there"})
  EXPECT_EQ(ToString(AND(COALESCE(LITERAL(TypedValue()), LITERAL(1)),
                         MAP(std::make_pair(this->storage.GetPropertyIx("hello"), LITERAL("there"))))),
            "(And (Coalesce [null, 1]) {\"hello\": \"there\"})");
}

TYPED_TEST(ExpressionPrettyPrinterTest, Coalesce) {
  // coalesce()
  EXPECT_EQ(ToString(COALESCE()), "(Coalesce [])");

  // coalesce(null, null)
  EXPECT_EQ(ToString(COALESCE(LITERAL(TypedValue()), LITERAL(TypedValue()))), "(Coalesce [null, null])");

  // coalesce(null, 2, 3)
  EXPECT_EQ(ToString(COALESCE(LITERAL(TypedValue()), LITERAL(2), LITERAL(3))), "(Coalesce [null, 2, 3])");

  // coalesce(null, 2, assert(false), 3)
  EXPECT_EQ(ToString(COALESCE(LITERAL(TypedValue()), LITERAL(2), FN("ASSERT", LITERAL(false)), LITERAL(3))),
            "(Coalesce [null, 2, (Function \"ASSERT\" [false]), 3])");

  // coalesce(null, assert(false))
  EXPECT_EQ(ToString(COALESCE(LITERAL(TypedValue()), FN("ASSERT", LITERAL(false)))),
            "(Coalesce [null, (Function \"ASSERT\" [false])])");

  // coalesce([null, null])
  EXPECT_EQ(ToString(COALESCE(LITERAL(TypedValue(std::vector<TypedValue>{TypedValue(), TypedValue()})))),
            "(Coalesce [[null, null]])");
}

TYPED_TEST(ExpressionPrettyPrinterTest, ParameterLookup) {
  // and($hello, $there)
  EXPECT_EQ(ToString(AND(PARAMETER_LOOKUP(1), PARAMETER_LOOKUP(2))), "(And (ParameterLookup 1) (ParameterLookup 2))");
}

TYPED_TEST(ExpressionPrettyPrinterTest, PropertyLookup) {
  // {hello: "there"}["hello"]
  EXPECT_EQ(ToString(PROPERTY_LOOKUP(
                this->dba, MAP(std::make_pair(this->storage.GetPropertyIx("hello"), LITERAL("there"))), "hello")),
            "(PropertyLookup {\"hello\": \"there\"} \"hello\")");
}

TYPED_TEST(ExpressionPrettyPrinterTest, NamedExpression) {
  // n AS 1
  EXPECT_EQ(ToString(NEXPR("n", LITERAL(1))), "(NamedExpression \"n\" 1)");
}
}  // namespace
