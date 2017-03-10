//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 07.03.17.
//

#include "gtest/gtest.h"

#include "query/stripper.hpp"

void EXPECT_PROP_TRUE(const TypedValue& a) {
  EXPECT_TRUE(a.type() == TypedValue::Type::Bool && a.Value<bool>());
}

void EXPECT_PROP_EQ(const TypedValue& a, const TypedValue& b) {
  EXPECT_PROP_TRUE(a == b);
}

TEST(QueryStripper, NoLiterals) {
  StrippedQuery stripped = query::Strip("CREATE (n)");
  EXPECT_EQ(stripped.arguments.Size(), 0);
  EXPECT_EQ(stripped.query, "create ( n ) ");
}

TEST(QueryStripper, DecimalInteger) {
  StrippedQuery stripped = query::Strip("RETURN 42");
  EXPECT_EQ(stripped.arguments.Size(), 1);
  EXPECT_EQ(stripped.arguments.At(0).Value<int64_t>(), 42);
  EXPECT_EQ(stripped.query, "return $stripped_arg_0 ");
}

TEST(QueryStripper, OctalInteger) {
  StrippedQuery stripped = query::Strip("RETURN 010");
  EXPECT_EQ(stripped.arguments.Size(), 1);
  EXPECT_EQ(stripped.arguments.At(0).Value<int64_t>(), 8);
  EXPECT_EQ(stripped.query, "return $stripped_arg_0 ");
}

TEST(QueryStripper, HexInteger) {
  StrippedQuery stripped = query::Strip("RETURN 0xa");
  EXPECT_EQ(stripped.arguments.Size(), 1);
  EXPECT_EQ(stripped.arguments.At(0).Value<int64_t>(), 10);
  EXPECT_EQ(stripped.query, "return $stripped_arg_0 ");
}

TEST(QueryStripper, RegularDecimal) {
  StrippedQuery stripped = query::Strip("RETURN 42.3");
  EXPECT_EQ(stripped.arguments.Size(), 1);
  EXPECT_FLOAT_EQ(stripped.arguments.At(0).Value<double>(), 42.3);
  EXPECT_EQ(stripped.query, "return $stripped_arg_0 ");
}

TEST(QueryStripper, ExponentDecimal) {
  StrippedQuery stripped = query::Strip("RETURN 4e2");
  EXPECT_EQ(stripped.arguments.Size(), 1);
  EXPECT_FLOAT_EQ(stripped.arguments.At(0).Value<double>(), 4e2);
  EXPECT_EQ(stripped.query, "return $stripped_arg_0 ");
}

TEST(QueryStripper, StringLiteral) {
  StrippedQuery stripped = query::Strip("RETURN 'something'");
  EXPECT_EQ(stripped.arguments.Size(), 1);
  EXPECT_EQ(stripped.arguments.At(0).Value<std::string>(), "'something'");
  EXPECT_EQ(stripped.query, "return $stripped_arg_0 ");
}

TEST(QueryStripper, BoolLiteral) {
  StrippedQuery stripped = query::Strip("RETURN true");
  EXPECT_EQ(stripped.arguments.Size(), 1);
  EXPECT_PROP_EQ(stripped.arguments.At(0), TypedValue(true));
  EXPECT_EQ(stripped.query, "return $stripped_arg_0 ");
}

TEST(QueryStripper, ListLiteral) {
  StrippedQuery stripped = query::Strip("MATCH (n) RETURN [n, n.prop]");
  EXPECT_EQ(stripped.arguments.Size(), 0);
  EXPECT_EQ(stripped.query, "match ( n ) return [ n , n . prop ] ");
}

TEST(QueryStripper, MapLiteral) {
  StrippedQuery stripped = query::Strip("MATCH (n) RETURN {val: n}");
  EXPECT_EQ(stripped.arguments.Size(), 0);
  EXPECT_EQ(stripped.query, "match ( n ) return { val : n } ");
}

TEST(QueryStripper, RangeLiteral) {
  StrippedQuery stripped = query::Strip("MATCH (n)-[*2..3]-() RETURN n");
  EXPECT_EQ(stripped.arguments.Size(), 0);
  EXPECT_EQ(stripped.query, "match ( n ) - [ * 2 .. 3 ] - ( ) return n ");
}

