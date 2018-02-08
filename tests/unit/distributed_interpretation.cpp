#include "gtest/gtest.h"

#include "database/graph_db.hpp"
#include "distributed_common.hpp"
#include "query/interpreter.hpp"
#include "query_common.hpp"
#include "query_plan_common.hpp"

using namespace distributed;
using namespace database;

TEST_F(DistributedGraphDbTest, RemotePullTest) {
  using Interpreter = query::Interpreter;
  std::map<std::string, query::TypedValue> params = {};

  GraphDbAccessor dba(master());

  ResultStreamFaker result;
  Interpreter interpreter_;
  interpreter_("OPTIONAL MATCH(n) UNWIND(RANGE(0, 20)) AS X RETURN 1", dba,
               params, false)
      .PullAll(result);

  // Three instances (master + 2 workers) with 21 result each.
  uint expected_results = 3U * 21;
  ASSERT_EQ(result.GetHeader().size(), 1U);
  EXPECT_EQ(result.GetHeader()[0], "1");
  ASSERT_EQ(result.GetResults().size(), expected_results);

  for (uint i = 0; i < expected_results; ++i) {
    ASSERT_EQ(result.GetResults()[i].size(), 1U);
    ASSERT_EQ(result.GetResults()[i][0].Value<int64_t>(), 1);
  }
}

TEST_F(DistributedGraphDbTest, RemotePullNoResultsTest) {
  using Interpreter = query::Interpreter;
  std::map<std::string, query::TypedValue> params = {};

  GraphDbAccessor dba(master());

  ResultStreamFaker result;
  Interpreter interpreter_;
  interpreter_("MATCH (n) RETURN n", dba, params, false).PullAll(result);

  ASSERT_EQ(result.GetHeader().size(), 1U);
  EXPECT_EQ(result.GetHeader()[0], "n");
  ASSERT_EQ(result.GetResults().size(), 0U);
}
