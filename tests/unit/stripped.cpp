//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 07.03.17.
//

#include "gtest/gtest.h"

#include "query/frontend/stripped.hpp"
#include "query/typed_value.hpp"

using namespace query;

void EXPECT_PROP_TRUE(const TypedValue& a) {
  EXPECT_TRUE(a.type() == TypedValue::Type::Bool && a.Value<bool>());
}

void EXPECT_PROP_EQ(const TypedValue& a, const TypedValue& b) {
  EXPECT_PROP_TRUE(a == b);
}

TEST(QueryStripper, NoLiterals) {
  StrippedQuery stripped("CREATE (n)");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "create ( n )");
}

TEST(QueryStripper, DecimalInteger) {
  StrippedQuery stripped("RETURN 42");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).first, 2);
  EXPECT_EQ(stripped.literals().At(0).second.Value<int64_t>(), 42);
  EXPECT_EQ(stripped.literals().AtTokenPosition(2).Value<int64_t>(), 42);
  EXPECT_EQ(stripped.query(), "return " + kStrippedIntToken);
}

TEST(QueryStripper, OctalInteger) {
  StrippedQuery stripped("RETURN 010");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).second.Value<int64_t>(), 8);
  EXPECT_EQ(stripped.query(), "return " + kStrippedIntToken);
}

TEST(QueryStripper, HexInteger) {
  StrippedQuery stripped("RETURN 0xa");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).second.Value<int64_t>(), 10);
  EXPECT_EQ(stripped.query(), "return " + kStrippedIntToken);
}

TEST(QueryStripper, RegularDecimal) {
  StrippedQuery stripped("RETURN 42.3");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_FLOAT_EQ(stripped.literals().At(0).second.Value<double>(), 42.3);
  EXPECT_EQ(stripped.query(), "return " + kStrippedDoubleToken);
}

TEST(QueryStripper, ExponentDecimal) {
  StrippedQuery stripped("RETURN 4e2");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_FLOAT_EQ(stripped.literals().At(0).second.Value<double>(), 4e2);
  EXPECT_EQ(stripped.query(), "return " + kStrippedDoubleToken);
}

TEST(QueryStripper, StringLiteral) {
  StrippedQuery stripped("RETURN 'something'");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).second.Value<std::string>(), "something");
  EXPECT_EQ(stripped.query(), "return " + kStrippedStringToken);
}

TEST(QueryStripper, BoolLiteral) {
  StrippedQuery stripped("RETURN true");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_PROP_EQ(stripped.literals().At(0).second, TypedValue(true));
  EXPECT_EQ(stripped.query(), "return " + kStrippedBooleanToken);
}

TEST(QueryStripper, ListLiteral) {
  StrippedQuery stripped("MATCH (n) RETURN [n, n.prop]");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "match ( n ) return [ n , n . prop ]");
}

TEST(QueryStripper, MapLiteral) {
  StrippedQuery stripped("MATCH (n) RETURN {val: n}");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "match ( n ) return { val : n }");
}

TEST(QueryStripper, RangeLiteral) {
  StrippedQuery stripped("MATCH (n)-[*2..3]-() RETURN n");
  EXPECT_EQ(stripped.literals().size(), 2);
  EXPECT_EQ(stripped.literals().At(0).second.Value<int64_t>(), 2);
  EXPECT_EQ(stripped.literals().At(1).second.Value<int64_t>(), 3);
  EXPECT_EQ(stripped.query(), "match ( n ) - [ * " + kStrippedIntToken +
                                  " .. " + kStrippedIntToken +
                                  " ] - ( ) return n");
}
