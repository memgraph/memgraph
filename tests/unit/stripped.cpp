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

//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 07.03.17.
//

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "query/exceptions.hpp"
#include "query/frontend/stripped.hpp"
#include "query/typed_value.hpp"

using namespace memgraph::query;
using namespace memgraph::query::frontend;

namespace {

using testing::Pair;
using testing::UnorderedElementsAre;

void EXPECT_PROP_TRUE(const TypedValue &a) { EXPECT_TRUE(a.type() == TypedValue::Type::Bool && a.ValueBool()); }

void EXPECT_PROP_EQ(const TypedValue &a, const TypedValue &b) { EXPECT_PROP_TRUE(a == b); }

void EXPECT_PROP_EQ(const memgraph::storage::PropertyValue &a, const TypedValue &b) {
  EXPECT_PROP_EQ(TypedValue(a), b);
}

TEST(QueryStripper, NoLiterals) {
  StrippedQuery stripped("CREATE (n)");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "CREATE ( n )");
}

TEST(QueryStripper, ZeroInteger) {
  StrippedQuery stripped("RETURN 0");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).first, 1);
  EXPECT_EQ(stripped.literals().At(0).second.ValueInt(), 0);
  EXPECT_EQ(stripped.literals().AtTokenPosition(1).ValueInt(), 0);
  EXPECT_EQ(stripped.query(), "RETURN " + kStrippedIntToken);
}

TEST(QueryStripper, DecimalInteger) {
  StrippedQuery stripped("RETURN 42");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).first, 1);
  EXPECT_EQ(stripped.literals().At(0).second.ValueInt(), 42);
  EXPECT_EQ(stripped.literals().AtTokenPosition(1).ValueInt(), 42);
  EXPECT_EQ(stripped.query(), "RETURN " + kStrippedIntToken);
}

TEST(QueryStripper, OctalInteger) {
  StrippedQuery stripped("RETURN 010");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).second.ValueInt(), 8);
  EXPECT_EQ(stripped.query(), "RETURN " + kStrippedIntToken);
}

TEST(QueryStripper, HexInteger) {
  StrippedQuery stripped("RETURN 0xa");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).second.ValueInt(), 10);
  EXPECT_EQ(stripped.query(), "RETURN " + kStrippedIntToken);
}

TEST(QueryStripper, RegularDecimal) {
  StrippedQuery stripped("RETURN 42.3");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_FLOAT_EQ(stripped.literals().At(0).second.ValueDouble(), 42.3);
  EXPECT_EQ(stripped.query(), "RETURN " + kStrippedDoubleToken);
}

TEST(QueryStripper, ExponentDecimal) {
  StrippedQuery stripped("RETURN 4e2");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_FLOAT_EQ(stripped.literals().At(0).second.ValueDouble(), 4e2);
  EXPECT_EQ(stripped.query(), "RETURN " + kStrippedDoubleToken);
}

TEST(QueryStripper, ExponentDecimal2) {
  StrippedQuery stripped("RETURN 4e-2");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_FLOAT_EQ(stripped.literals().At(0).second.ValueDouble(), 4e-2);
  EXPECT_EQ(stripped.query(), "RETURN " + kStrippedDoubleToken);
}

TEST(QueryStripper, ExponentDecimal3) {
  StrippedQuery stripped("RETURN 0.1e-2");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_FLOAT_EQ(stripped.literals().At(0).second.ValueDouble(), 0.1e-2);
  EXPECT_EQ(stripped.query(), "RETURN " + kStrippedDoubleToken);
}

TEST(QueryStripper, ExponentDecimal4) {
  StrippedQuery stripped("RETURN .1e-2");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_FLOAT_EQ(stripped.literals().At(0).second.ValueDouble(), .1e-2);
  EXPECT_EQ(stripped.query(), "RETURN " + kStrippedDoubleToken);
}

TEST(QueryStripper, SymbolicNameStartingWithE) {
  StrippedQuery stripped("RETURN e1");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "RETURN e1");
}

TEST(QueryStripper, StringLiteral) {
  StrippedQuery stripped("RETURN 'something'");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).second.ValueString(), "something");
  EXPECT_EQ(stripped.query(), "RETURN " + kStrippedStringToken);
}

TEST(QueryStripper, StringLiteral2) {
  StrippedQuery stripped("RETURN 'so\\'me'");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).second.ValueString(), "so'me");
  EXPECT_EQ(stripped.query(), "RETURN " + kStrippedStringToken);
}

TEST(QueryStripper, StringLiteral3) {
  StrippedQuery stripped("RETURN \"so\\\"me'\"");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).second.ValueString(), "so\"me'");
  EXPECT_EQ(stripped.query(), "RETURN " + kStrippedStringToken);
}

TEST(QueryStripper, StringLiteral4) {
  StrippedQuery stripped("RETURN '\\u1Aa4'");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).second.ValueString(),
            "\xE1\xAA\xA4");  // "u8"\u1Aa4
  EXPECT_EQ(stripped.query(), "RETURN " + kStrippedStringToken);
}

TEST(QueryStripper, HighSurrogateAlone) { ASSERT_THROW(StrippedQuery("RETURN '\\udeeb'"), SemanticException); }

TEST(QueryStripper, LowSurrogateAlone) { ASSERT_THROW(StrippedQuery("RETURN '\\ud83d'"), SemanticException); }

TEST(QueryStripper, Surrogates) {
  StrippedQuery stripped("RETURN '\\ud83d\\udeeb'");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_EQ(stripped.literals().At(0).second.ValueString(),
            "\xF0\x9F\x9B\xAB");  // u8"\U0001f6eb"
  EXPECT_EQ(stripped.query(), "RETURN " + kStrippedStringToken);
}

TEST(QueryStripper, StringLiteralIllegalEscapedSequence) {
  EXPECT_THROW(StrippedQuery("RETURN 'so\\x'"), LexingException);
  EXPECT_THROW(StrippedQuery("RETURN 'so\\uabc'"), LexingException);
}

TEST(QueryStripper, TrueLiteral) {
  StrippedQuery stripped("RETURN trUE");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_PROP_EQ(stripped.literals().At(0).second, TypedValue(true));
  EXPECT_EQ(stripped.query(), "RETURN " + kStrippedBooleanToken);
}

TEST(QueryStripper, FalseLiteral) {
  StrippedQuery stripped("RETURN fAlse");
  EXPECT_EQ(stripped.literals().size(), 1);
  EXPECT_PROP_EQ(stripped.literals().At(0).second, TypedValue(false));
  EXPECT_EQ(stripped.query(), "RETURN " + kStrippedBooleanToken);
}

TEST(QueryStripper, NullLiteral) {
  StrippedQuery stripped("RETURN NuLl");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "RETURN NuLl");
}

TEST(QueryStripper, ListLiteral) {
  StrippedQuery stripped("MATCH (n) RETURN [n, n.prop]");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "MATCH ( n ) RETURN [ n , n . prop ]");
}

TEST(QueryStripper, MapLiteral) {
  StrippedQuery stripped("MATCH (n) RETURN {val: n}");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "MATCH ( n ) RETURN { val : n }");
}

TEST(QueryStripper, MapProjectionLiteral) {
  StrippedQuery stripped("WITH 0 as var MATCH (n) RETURN n {.x, var, key: 'a'}");
  EXPECT_EQ(stripped.literals().size(), 2);
  EXPECT_EQ(stripped.query(), "WITH 0 as var MATCH ( n ) RETURN n { . x , var , key : \"a\" }");
}

TEST(QueryStripper, RangeLiteral) {
  StrippedQuery stripped("MATCH (n)-[*2..3]-() RETURN n");
  EXPECT_EQ(stripped.literals().size(), 2);
  EXPECT_EQ(stripped.literals().At(0).second.ValueInt(), 2);
  EXPECT_EQ(stripped.literals().At(1).second.ValueInt(), 3);
  EXPECT_EQ(stripped.query(),
            "MATCH ( n ) - [ * " + kStrippedIntToken + " .. " + kStrippedIntToken + " ] - ( ) RETURN n");
}

TEST(QueryStripper, EscapedName) {
  StrippedQuery stripped("MATCH (n:`mirko``slavko`)");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "MATCH ( n : `mirko``slavko` )");
}

TEST(QueryStripper, UnescapedName) {
  StrippedQuery stripped("MATCH (n:peropero)");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "MATCH ( n : peropero )");
}

TEST(QueryStripper, UnescapedName2) {
  // using u8string this string is u8"\uffd5\u04c2\u04c2pero\u0078pe"
  StrippedQuery stripped("MATCH (n:\xEF\xBF\x95\xD3\x82\xD3\x82pero\x78pe)");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "MATCH ( n : \xEF\xBF\x95\xD3\x82\xD3\x82pero\x78pe )");
}

TEST(QueryStripper, MixedCaseKeyword) {
  StrippedQuery stripped("MaTch (n:peropero)");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "MaTch ( n : peropero )");
}

TEST(QueryStripper, BlockComment) {
  StrippedQuery stripped("MaTch (n:/**fhf/gf\n\r\n//fjhf*/peropero)");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "MaTch ( n : peropero )");
}

TEST(QueryStripper, LineComment1) {
  StrippedQuery stripped("MaTch (n:peropero) // komentar\nreturn n");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "MaTch ( n : peropero ) return n");
}

TEST(QueryStripper, LineComment2) {
  StrippedQuery stripped("MaTch (n:peropero) // komentar\r\nreturn n");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "MaTch ( n : peropero ) return n");
}

TEST(QueryStripper, LineComment3) {
  StrippedQuery stripped("MaTch (n:peropero) return n // komentar");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "MaTch ( n : peropero ) return n");
}

TEST(QueryStripper, LineComment4) {
  StrippedQuery stripped("MaTch (n:peropero) return n // komentar\r");
  EXPECT_EQ(stripped.literals().size(), 0);
  // Didn't manage to parse comment because it ends with \r.
  EXPECT_EQ(stripped.query(), "MaTch ( n : peropero ) return n / / komentar");
}

TEST(QueryStripper, LineComment5) {
  {
    StrippedQuery stripped("MaTch (n:peropero) return n//");
    EXPECT_EQ(stripped.literals().size(), 0);
    EXPECT_EQ(stripped.query(), "MaTch ( n : peropero ) return n");
  }
  {
    StrippedQuery stripped("MATCH (n) MATCH (n)-[*bfs]->(m) RETURN n;\n//");
    EXPECT_EQ(stripped.literals().size(), 0);
    EXPECT_EQ(stripped.query(), "MATCH ( n ) MATCH ( n ) - [ * bfs ] - > ( m ) RETURN n ;");
  }
}

TEST(QueryStripper, Spaces) {
  // using u8string this string is u8"\u202f"
  StrippedQuery stripped("RETURN \r\n\xE2\x80\xAF\t\xE2\x80\x87  NuLl");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "RETURN NuLl");
}

TEST(QueryStripper, OtherTokens) {
  StrippedQuery stripped("++=...");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "+ += .. .");
}

TEST(QueryStripper, NamedExpression) {
  StrippedQuery stripped("RETURN 2   + 3");
  EXPECT_THAT(stripped.named_expressions(), UnorderedElementsAre(Pair(1, "2   + 3")));
}

TEST(QueryStripper, AliasedNamedExpression) {
  StrippedQuery stripped("RETURN 2   + 3 AS x");
  EXPECT_THAT(stripped.named_expressions(), UnorderedElementsAre());
}

TEST(QueryStripper, MultipleNamedExpressions) {
  StrippedQuery stripped("RETURN 2   + 3, x as s, x, n.x");
  EXPECT_THAT(stripped.named_expressions(), UnorderedElementsAre(Pair(1, "2   + 3"), Pair(9, "x"), Pair(11, "n.x")));
}

TEST(QueryStripper, ReturnOrderBy) {
  StrippedQuery stripped("RETURN 2   + 3 ORDER BY n.x, x");
  EXPECT_THAT(stripped.named_expressions(), UnorderedElementsAre(Pair(1, "2   + 3")));
}

TEST(QueryStripper, ReturnSkip) {
  StrippedQuery stripped("RETURN 2   + 3 SKIP 10");
  EXPECT_THAT(stripped.named_expressions(), UnorderedElementsAre(Pair(1, "2   + 3")));
}

TEST(QueryStripper, ReturnLimit) {
  StrippedQuery stripped("RETURN 2   + 3 LIMIT 12");
  EXPECT_THAT(stripped.named_expressions(), UnorderedElementsAre(Pair(1, "2   + 3")));
}

TEST(QueryStripper, ReturnListsAndFunctionCalls) {
  StrippedQuery stripped("RETURN [1,2,[3, 4] , 5], f(1, 2), 3");
  EXPECT_THAT(stripped.named_expressions(),
              UnorderedElementsAre(Pair(1, "[1,2,[3, 4] , 5]"), Pair(15, "f(1, 2)"), Pair(22, "3")));
}

TEST(QueryStripper, Parameters) {
  StrippedQuery stripped("RETURN $123, $pero, $`mirko ``slavko`");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "RETURN $123 , $pero , $`mirko ``slavko`");
  EXPECT_THAT(stripped.parameters(), UnorderedElementsAre(Pair(1, "123"), Pair(4, "pero"), Pair(7, "mirko `slavko")));
  EXPECT_THAT(stripped.named_expressions(),
              UnorderedElementsAre(Pair(1, "$123"), Pair(4, "$pero"), Pair(7, "$`mirko ``slavko`")));
}

TEST(QueryStripper, KeywordInNamedExpression) {
  StrippedQuery stripped("RETURN CoUnT(n)");
  EXPECT_EQ(stripped.literals().size(), 0);
  EXPECT_EQ(stripped.query(), "RETURN CoUnT ( n )");
  EXPECT_THAT(stripped.named_expressions(), UnorderedElementsAre(Pair(1, "CoUnT(n)")));
}

TEST(QueryStripper, UnionMultipleReturnStatementsAliasedExpression) {
  StrippedQuery stripped("RETURN 1 AS X UNION RETURN 2 AS X");
  EXPECT_THAT(stripped.named_expressions(), UnorderedElementsAre());
}

TEST(QueryStripper, UnionMultipleReturnStatementsNamedExpressions) {
  StrippedQuery stripped("RETURN x UNION RETURN x");
  EXPECT_THAT(stripped.named_expressions(), UnorderedElementsAre(Pair(1, "x"), Pair(4, "x")));
}

TEST(QueryStripper, UnionAllMultipleReturnStatementsNamedExpressions) {
  StrippedQuery stripped("RETURN x UNION ALL RETURN x");
  EXPECT_THAT(stripped.named_expressions(), UnorderedElementsAre(Pair(1, "x"), Pair(5, "x")));
}

TEST(QueryStripper, QueryReturnMap) {
  StrippedQuery stripped("RETURN {a: 1, b: 'foo'}");
  EXPECT_THAT(stripped.named_expressions(), UnorderedElementsAre(Pair(1, "{a: 1, b: 'foo'}")));
}

TEST(QueryStripper, QueryReturnMapProjection) {
  StrippedQuery stripped("RETURN a {.prop, var, key: 2}");
  EXPECT_THAT(stripped.named_expressions(), UnorderedElementsAre(Pair(1, "a {.prop, var, key: 2}")));
}

TEST(QueryStripper, QuerySemicolonEndingQuery1) {
  StrippedQuery stripped("RETURN 1;");
  EXPECT_THAT(stripped.named_expressions(), UnorderedElementsAre(Pair(1, "1")));
}

TEST(QueryStripper, QuerySemicolonEndingQuery2) {
  StrippedQuery stripped("RETURN 42   ;");
  EXPECT_THAT(stripped.named_expressions(), UnorderedElementsAre(Pair(1, "42")));
}

TEST(QueryStripper, CreateTriggerQuery) {
  static constexpr std::string_view execute_query{
      "      MATCH  (execute:Node)    RETURN / *test comment */    execute \"test\""};
  {
    SCOPED_TRACE("Everything after EXECUTE keyword in CREATE TRIGGER should not be stripped");

    {
      SCOPED_TRACE("Query starting with CREATE keyword");
      StrippedQuery stripped(
          fmt::format("CREATE TRIGGER execute  /*test*/ ON CREATE BEFORE COMMIT EXECUTE{}", execute_query));
      EXPECT_EQ(stripped.query(),
                fmt::format("CREATE TRIGGER execute ON CREATE BEFORE COMMIT EXECUTE {}", execute_query));
    }

    {
      SCOPED_TRACE("Query starting with comments and spaces");
      StrippedQuery stripped(fmt::format(
          "/*comment*/    \n\n //other comment\nCREATE TRIGGER execute AFTER COMMIT EXECUTE{}", execute_query));
      EXPECT_EQ(stripped.query(), fmt::format("CREATE TRIGGER execute AFTER COMMIT EXECUTE {}", execute_query));
    }

    {
      SCOPED_TRACE("Query with comments and spaces between CREATE and TRIGGER");
      StrippedQuery stripped(fmt::format(
          "/*comment*/    \n\n //other comment\nCREATE //some comment \n   TRIGGER execute AFTER COMMIT EXECUTE{}",
          execute_query));
      EXPECT_EQ(stripped.query(), fmt::format("CREATE TRIGGER execute AFTER COMMIT EXECUTE {}", execute_query));
    }
  }
  {
    SCOPED_TRACE("Execute keyword should still be allowed in other queries");
    StrippedQuery stripped("MATCH (execute:Node)   //comment \n  RETURN  /* test comment */  execute");
    EXPECT_EQ(stripped.query(), "MATCH ( execute : Node ) RETURN execute");
    EXPECT_THAT(stripped.named_expressions(), UnorderedElementsAre(Pair(7, "execute")));
  }
}

}  // namespace
