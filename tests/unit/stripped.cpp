//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 07.03.17.
//

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "query/exceptions.hpp"
#include "query/frontend/stripped.hpp"
#include "query/typed_value.hpp"

using namespace query;

namespace {

using testing::Pair;
using testing::UnorderedElementsAre;

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

TEST(QueryStripper, ZeroInteger) {
  StrippedQuery stripped("RETURN 0");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).first, 2);
  EXPECT_EQ(stripped.literals().At(0).second.Value<int64_t>(), 0);
  EXPECT_EQ(stripped.literals().AtTokenPosition(2).Value<int64_t>(), 0);
  EXPECT_EQ(stripped.query(), "return " + kStrippedIntToken);
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

TEST(QueryStripper, ExponentDecimal2) {
  StrippedQuery stripped("RETURN 4e-2");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_FLOAT_EQ(stripped.literals().At(0).second.Value<double>(), 4e-2);
  EXPECT_EQ(stripped.query(), "return " + kStrippedDoubleToken);
}

TEST(QueryStripper, ExponentDecimal3) {
  StrippedQuery stripped("RETURN 0.1e-2");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_FLOAT_EQ(stripped.literals().At(0).second.Value<double>(), 0.1e-2);
  EXPECT_EQ(stripped.query(), "return " + kStrippedDoubleToken);
}

TEST(QueryStripper, ExponentDecimal4) {
  StrippedQuery stripped("RETURN .1e-2");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_FLOAT_EQ(stripped.literals().At(0).second.Value<double>(), .1e-2);
  EXPECT_EQ(stripped.query(), "return " + kStrippedDoubleToken);
}

TEST(QueryStripper, SymbolicNameStartingWithE) {
  StrippedQuery stripped("RETURN e1");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "return e1");
}

TEST(QueryStripper, StringLiteral) {
  StrippedQuery stripped("RETURN 'something'");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).second.Value<std::string>(), "something");
  EXPECT_EQ(stripped.query(), "return " + kStrippedStringToken);
}

TEST(QueryStripper, StringLiteral2) {
  StrippedQuery stripped("RETURN 'so\\'me'");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).second.Value<std::string>(), "so'me");
  EXPECT_EQ(stripped.query(), "return " + kStrippedStringToken);
}

TEST(QueryStripper, StringLiteral3) {
  StrippedQuery stripped("RETURN \"so\\\"me'\"");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).second.Value<std::string>(), "so\"me'");
  EXPECT_EQ(stripped.query(), "return " + kStrippedStringToken);
}

TEST(QueryStripper, StringLiteral4) {
  StrippedQuery stripped("RETURN '\\u1Aa4'");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).second.Value<std::string>(), u8"\u1Aa4");
  EXPECT_EQ(stripped.query(), "return " + kStrippedStringToken);
}

TEST(QueryStripper, StringLiteralIllegalEscapedSequence) {
  EXPECT_THROW(StrippedQuery("RETURN 'so\\x'"), LexingException);
  EXPECT_THROW(StrippedQuery("RETURN 'so\\uabc'"), LexingException);
}

TEST(QueryStripper, TrueLiteral) {
  StrippedQuery stripped("RETURN trUE");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_PROP_EQ(stripped.literals().At(0).second, TypedValue(true));
  EXPECT_EQ(stripped.query(), "return " + kStrippedBooleanToken);
}

TEST(QueryStripper, FalseLiteral) {
  StrippedQuery stripped("RETURN fAlse");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_PROP_EQ(stripped.literals().At(0).second, TypedValue(false));
  EXPECT_EQ(stripped.query(), "return " + kStrippedBooleanToken);
}

TEST(QueryStripper, NullLiteral) {
  StrippedQuery stripped("RETURN NuLl");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "return null");
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

TEST(QueryStripper, EscapedName) {
  StrippedQuery stripped("MATCH (n:`mirko``slavko`)");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "match ( n : `mirko``slavko` )");
}

TEST(QueryStripper, UnescapedName) {
  StrippedQuery stripped("MATCH (n:peropero)");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "match ( n : peropero )");
}

TEST(QueryStripper, UnescapedName2) {
  StrippedQuery stripped(u8"MATCH (n:\uffd5\u04c2\u04c2pero\u0078pe)");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), u8"match ( n : \uffd5\u04c2\u04c2pero\u0078pe )");
}

TEST(QueryStripper, MixedCaseKeyword) {
  StrippedQuery stripped("MaTch (n:peropero)");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "match ( n : peropero )");
}

TEST(QueryStripper, BlockComment) {
  StrippedQuery stripped("MaTch (n:/**fhf/gf\n\r\n//fjhf*/peropero)");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "match ( n : peropero )");
}

TEST(QueryStripper, LineComment1) {
  StrippedQuery stripped("MaTch (n:peropero) // komentar\nreturn n");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "match ( n : peropero ) return n");
}

TEST(QueryStripper, LineComment2) {
  StrippedQuery stripped("MaTch (n:peropero) // komentar\r\nreturn n");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "match ( n : peropero ) return n");
}

TEST(QueryStripper, LineComment3) {
  StrippedQuery stripped("MaTch (n:peropero) return n // komentar");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "match ( n : peropero ) return n");
}

TEST(QueryStripper, LineComment4) {
  StrippedQuery stripped("MaTch (n:peropero) return n // komentar\r");
  EXPECT_EQ(stripped.literals().size(), 0);
  // Didn't manage to parse comment because it ends with \r.
  EXPECT_EQ(stripped.query(), "match ( n : peropero ) return n / / komentar");
}

TEST(QueryStripper, Spaces) {
  StrippedQuery stripped(u8"RETURN \r\n\u202f\t\u2007  NuLl");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "return null");
}

TEST(QueryStripper, OtherTokens) {
  StrippedQuery stripped("++=...");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "+ += .. .");
}

TEST(QueryStripper, NamedExpression) {
  StrippedQuery stripped("RETURN 2   + 3");
  EXPECT_THAT(stripped.named_expressions(),
              UnorderedElementsAre(Pair(2, "2   + 3")));
}

TEST(QueryStripper, AliasedNamedExpression) {
  StrippedQuery stripped("RETURN 2   + 3 AS x");
  EXPECT_THAT(stripped.named_expressions(), UnorderedElementsAre());
}

TEST(QueryStripper, MultipleNamedExpressions) {
  StrippedQuery stripped("RETURN 2   + 3, x as s, x, n.x");
  EXPECT_THAT(
      stripped.named_expressions(),
      UnorderedElementsAre(Pair(2, "2   + 3"), Pair(18, "x"), Pair(22, "n.x")));
}

TEST(QueryStripper, ReturnOrderBy) {
  StrippedQuery stripped("RETURN 2   + 3 ORDER BY n.x, x");
  EXPECT_THAT(stripped.named_expressions(),
              UnorderedElementsAre(Pair(2, "2   + 3")));
}

TEST(QueryStripper, ReturnSkip) {
  StrippedQuery stripped("RETURN 2   + 3 SKIP 10");
  EXPECT_THAT(stripped.named_expressions(),
              UnorderedElementsAre(Pair(2, "2   + 3")));
}

TEST(QueryStripper, ReturnLimit) {
  StrippedQuery stripped("RETURN 2   + 3 LIMIT 12");
  EXPECT_THAT(stripped.named_expressions(),
              UnorderedElementsAre(Pair(2, "2   + 3")));
}

TEST(QueryStripper, ReturnListsAndFunctionCalls) {
  StrippedQuery stripped("RETURN [1,2,[3, 4] , 5], f(1, 2), 3");
  EXPECT_THAT(stripped.named_expressions(),
              UnorderedElementsAre(Pair(2, "[1,2,[3, 4] , 5]"),
                                   Pair(30, "f(1, 2)"), Pair(44, "3")));
}
}
