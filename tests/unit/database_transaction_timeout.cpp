#include <glog/logging.h>
#include "communication/result_stream_faker.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"

#include <gtest/gtest.h>

DECLARE_int32(query_execution_time_sec);

TEST(TransactionTimeout, TransactionTimeout) {
  FLAGS_query_execution_time_sec = 3;
  GraphDb db;
  query::Interpreter interpreter;
  {
    ResultStreamFaker stream;
    GraphDbAccessor dba(db);
    interpreter.Interpret("MATCH (n) RETURN n", dba, stream, {}, false);
  }
  {
    ResultStreamFaker stream;
    GraphDbAccessor dba(db);
    std::this_thread::sleep_for(std::chrono::seconds(5));
    ASSERT_THROW(
        interpreter.Interpret("MATCH (n) RETURN n", dba, stream, {}, false),
        query::HintedAbortError);
  }
  {
    ResultStreamFaker stream;
    GraphDbAccessor dba(db);
    interpreter.Interpret("MATCH (n) RETURN n", dba, stream, {}, false);
  }
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
