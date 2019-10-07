#include <glog/logging.h>
#include <gtest/gtest.h>

#include "communication/result_stream_faker.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"

DECLARE_int32(query_execution_time_sec);

TEST(TransactionTimeout, TransactionTimeout) {
  FLAGS_query_execution_time_sec = 3;
  database::GraphDb db;
  query::Interpreter::InterpreterContext interpreter_context;
  query::Interpreter interpreter(&interpreter_context);
  auto interpret = [&](auto &dba, const std::string &query) {
    query::DbAccessor query_dba(&dba);
    ResultStreamFaker<query::TypedValue> stream;
    interpreter(query, &query_dba, {}, false, utils::NewDeleteResource())
        .PullAll(stream);
  };
  {
    auto dba = db.Access();
    interpret(dba, "MATCH (n) RETURN n");
  }
  {
    auto dba = db.Access();
    std::this_thread::sleep_for(std::chrono::seconds(5));
    ASSERT_THROW(interpret(dba, "MATCH (n) RETURN n"), query::HintedAbortError);
  }
  {
    auto dba = db.Access();
    interpret(dba, "MATCH (n) RETURN n");
  }
}
