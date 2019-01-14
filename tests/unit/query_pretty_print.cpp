#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "database/single_node/graph_db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/pretty_print.hpp"
#include "query_common.hpp"
#include "utils/string.hpp"

using namespace query;
using query::test_common::ToList;
using query::test_common::ToString;
using testing::ElementsAre;
using testing::UnorderedElementsAre;

namespace {

struct ExpressionPrettyPrinterTest : public ::testing::Test {
  ExpressionPrettyPrinterTest() : pdba{db.Access()}, dba{*pdba} {}

  database::GraphDb db;
  std::unique_ptr<database::GraphDbAccessor> pdba;
  database::GraphDbAccessor &dba;
  AstStorage storage;
};

TEST_F(ExpressionPrettyPrinterTest, Literals) {
  // 1
  EXPECT_EQ(ToString(storage, LITERAL(1)), "1");

  // "hello"
  EXPECT_EQ(ToString(storage, LITERAL("hello")), "\"hello\"");

  // null
  EXPECT_EQ(ToString(storage, LITERAL(TypedValue::Null)), "null");

  // true
  EXPECT_EQ(ToString(storage, LITERAL(true)), "true");

  // false
  EXPECT_EQ(ToString(storage, LITERAL(false)), "false");

  // [1 null "hello"]
  EXPECT_EQ(ToString(storage, LITERAL((std::vector<PropertyValue>{
                                  1, PropertyValue::Null, "hello"}))),
            "[1, null, \"hello\"]");

  // {hello: 1, there: 2}
  EXPECT_EQ(ToString(storage, LITERAL((std::map<std::string, PropertyValue>{
                                  {"hello", 1}, {"there", 2}}))),
            "{\"hello\": 1, \"there\": 2}");
}

TEST_F(ExpressionPrettyPrinterTest, UnaryOperators) {
  // not(false)
  EXPECT_EQ(ToString(storage, NOT(LITERAL(false))), "(Not false)");

  // +1
  EXPECT_EQ(ToString(storage, UPLUS(LITERAL(1))), "(+ 1)");

  // -1
  EXPECT_EQ(ToString(storage, UMINUS(LITERAL(1))), "(- 1)");

  // null IS NULL
  EXPECT_EQ(ToString(storage, IS_NULL(LITERAL(TypedValue::Null))),
            "(IsNull null)");
}

TEST_F(ExpressionPrettyPrinterTest, BinaryOperators) {
  // and(null, 5)
  EXPECT_EQ(ToString(storage, AND(LITERAL(TypedValue::Null), LITERAL(5))),
            "(And null 5)");

  // or(5, {hello: "there"}["hello"])
  EXPECT_EQ(ToString(storage,
                     OR(LITERAL(5),
                        PROPERTY_LOOKUP(
                            MAP(std::make_pair(storage.GetPropertyIx("hello"),
                                               LITERAL("there"))),
                            "hello"))),
            "(Or 5 (PropertyLookup {\"hello\": \"there\"} \"hello\"))");

  // and(coalesce(null, 1), {hello: "there"})
  EXPECT_EQ(
      ToString(storage, AND(COALESCE(LITERAL(TypedValue::Null), LITERAL(1)),
                            MAP(std::make_pair(storage.GetPropertyIx("hello"),
                                               LITERAL("there"))))),
      "(And (Coalesce [null, 1]) {\"hello\": \"there\"})");
}

TEST_F(ExpressionPrettyPrinterTest, Coalesce) {
  // coalesce()
  EXPECT_EQ(ToString(storage, COALESCE()), "(Coalesce [])");

  // coalesce(null, null)
  EXPECT_EQ(ToString(storage, COALESCE(LITERAL(TypedValue::Null),
                                       LITERAL(TypedValue::Null))),
            "(Coalesce [null, null])");

  // coalesce(null, 2, 3)
  EXPECT_EQ(ToString(storage, COALESCE(LITERAL(TypedValue::Null), LITERAL(2),
                                       LITERAL(3))),
            "(Coalesce [null, 2, 3])");

  // coalesce(null, 2, assert(false), 3)
  EXPECT_EQ(
      ToString(storage, COALESCE(LITERAL(TypedValue::Null), LITERAL(2),
                                 FN("ASSERT", LITERAL(false)), LITERAL(3))),
      "(Coalesce [null, 2, (Function \"ASSERT\" [false]), 3])");

  // coalesce(null, assert(false))
  EXPECT_EQ(ToString(storage, COALESCE(LITERAL(TypedValue::Null),
                                       FN("ASSERT", LITERAL(false)))),
            "(Coalesce [null, (Function \"ASSERT\" [false])])");

  // coalesce([null, null])
  EXPECT_EQ(
      ToString(storage, COALESCE(LITERAL(TypedValue(std::vector<TypedValue>{
                            TypedValue::Null, TypedValue::Null})))),
      "(Coalesce [[null, null]])");
}

TEST_F(ExpressionPrettyPrinterTest, ParameterLookup) {
  // and($hello, $there)
  EXPECT_EQ(ToString(storage, AND(PARAMETER_LOOKUP(1), PARAMETER_LOOKUP(2))),
            "(And (ParameterLookup 1) (ParameterLookup 2))");
}

TEST_F(ExpressionPrettyPrinterTest, PropertyLookup) {
  // {hello: "there"}["hello"]
  EXPECT_EQ(
      ToString(storage, PROPERTY_LOOKUP(
                            MAP(std::make_pair(storage.GetPropertyIx("hello"),
                                               LITERAL("there"))),
                            "hello")),
      "(PropertyLookup {\"hello\": \"there\"} \"hello\")");
}

TEST_F(ExpressionPrettyPrinterTest, NamedExpression) {
  // n AS 1
  EXPECT_EQ(ToString(storage, NEXPR("n", LITERAL(1))),
            "(NamedExpression \"n\" 1)");
}

}  // namespace
