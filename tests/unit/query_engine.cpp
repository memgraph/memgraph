#include "communication/result_stream_faker.hpp"
#include "database/graph_db_accessor.hpp"
#include "dbms/dbms.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "query/engine.hpp"

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
    engine.Run("RETURN 2 + 3", *dba, stream);
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<int64_t>(), 5);
  }
  {
    // Cached ast, different literals.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    engine.Run("RETURN 5 + 4", *dba, stream);
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<int64_t>(), 9);
  }
  {
    // Different ast (because of different types).
    ResultStreamFaker stream;
    auto dba = dbms.active();
    engine.Run("RETURN 5.5 + 4", *dba, stream);
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<double>(), 9.5);
  }
  {
    // Cached ast, same literals.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    engine.Run("RETURN 2 + 3", *dba, stream);
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<int64_t>(), 5);
  }
  {
    // Cached ast, different literals.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    engine.Run("RETURN 10.5 + 1", *dba, stream);
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<double>(), 11.5);
  }
}
}
