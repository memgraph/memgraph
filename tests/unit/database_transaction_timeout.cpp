#include <glog/logging.h>
#include "communication/result_stream_faker.hpp"
#include "database/dbms.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"

#include <gtest/gtest.h>

DECLARE_int32(query_execution_time_sec);

TEST(TransactionTimeout, TransactionTimeout) {
  FLAGS_query_execution_time_sec = 3;
  Dbms dbms;
  query::Interpreter interpreter;
  {
    ResultStreamFaker stream;
    auto dba1 = dbms.active();
    interpreter.Interpret("MATCH (n) RETURN n", *dba1, stream, {}, false);
  }
  {
    ResultStreamFaker stream;
    auto dba2 = dbms.active();
    std::this_thread::sleep_for(std::chrono::seconds(5));
    ASSERT_THROW(
        interpreter.Interpret("MATCH (n) RETURN n", *dba2, stream, {}, false),
        query::HintedAbortError);
  }
  {
    ResultStreamFaker stream;
    auto dba3 = dbms.active();
    interpreter.Interpret("MATCH (n) RETURN n", *dba3, stream, {}, false);
  }
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
