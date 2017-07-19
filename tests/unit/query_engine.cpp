#include "communication/result_stream_faker.hpp"
#include "database/dbms.hpp"
#include "database/graph_db_accessor.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "query/engine.hpp"
#include "query/exceptions.hpp"
#include "query/typed_value.hpp"
#include "query_common.hpp"

// TODO: This is not a unit test, but tests/integration dir is chaotic at the
// moment. After tests refactoring is done, move/rename this.

namespace {

// Run query with different ast twice to see if query executes correctly when
// ast is read from cache.
TEST(QueryEngine, AstCache) {
  QueryEngine<ResultStreamFaker> engine;
  Dbms dbms;
  {
    ResultStreamFaker stream;
    auto dba = dbms.active();
    engine.Run("RETURN 2 + 3", *dba, stream, {});
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "2 + 3");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<int64_t>(), 5);
  }
  {
    // Cached ast, different literals.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    engine.Run("RETURN 5 + 4", *dba, stream, {});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<int64_t>(), 9);
  }
  {
    // Different ast (because of different types).
    ResultStreamFaker stream;
    auto dba = dbms.active();
    engine.Run("RETURN 5.5 + 4", *dba, stream, {});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<double>(), 9.5);
  }
  {
    // Cached ast, same literals.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    engine.Run("RETURN 2 + 3", *dba, stream, {});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<int64_t>(), 5);
  }
  {
    // Cached ast, different literals.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    engine.Run("RETURN 10.5 + 1", *dba, stream, {});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<double>(), 11.5);
  }
  {
    // Cached ast, same literals, different whitespaces.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    engine.Run("RETURN  10.5 + 1", *dba, stream, {});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<double>(), 11.5);
  }
  {
    // Cached ast, same literals, different named header.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    engine.Run("RETURN  10.5+1", *dba, stream, {});
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "10.5+1");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<double>(), 11.5);
  }
}

// Run query with same ast multiple times with different parameters.
TEST(QueryEngine, Parameters) {
  QueryEngine<ResultStreamFaker> engine;
  Dbms dbms;
  {
    ResultStreamFaker stream;
    auto dba = dbms.active();
    engine.Run("RETURN $2 + $`a b`", *dba, stream, {{"2", 10}, {"a b", 15}});
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "$2 + $`a b`");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<int64_t>(), 25);
  }
  {
    // Not needed parameter.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    engine.Run("RETURN $2 + $`a b`", *dba, stream,
               {{"2", 10}, {"a b", 15}, {"c", 10}});
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "$2 + $`a b`");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<int64_t>(), 25);
  }
  {
    // Cached ast, different parameters.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    engine.Run("RETURN $2 + $`a b`", *dba, stream,
               {{"2", "da"}, {"a b", "ne"}});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<std::string>(), "dane");
  }
  {
    // Non-primitive literal.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    engine.Run("RETURN $2", *dba, stream,
               {{"2", std::vector<query::TypedValue>{5, 2, 3}}});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    auto result = query::test_common::ToInt64List(
        stream.GetResults()[0][0].Value<std::vector<query::TypedValue>>());
    ASSERT_THAT(result, testing::ElementsAre(5, 2, 3));
  }
  {
    // Cached ast, unprovided parameter.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    ASSERT_THROW(engine.Run("RETURN $2 + $`a b`", *dba, stream,
                            {{"2", "da"}, {"ab", "ne"}}),
                 query::UnprovidedParameterError);
  }
}
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
