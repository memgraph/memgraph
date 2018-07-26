#include <glog/logging.h>
#include <gtest/gtest.h>

#include "communication/result_stream_faker.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"

DECLARE_int32(query_execution_time_sec);

TEST(TransactionTimeout, TransactionTimeout) {
  FLAGS_query_execution_time_sec = 3;
  database::SingleNode db;
  query::Interpreter interpreter{db};
  auto interpret = [&](auto &dba, const std::string &query) {
    ResultStreamFaker<query::TypedValue> stream;
    interpreter(query, dba, {}, false).PullAll(stream);
  };
  {
    auto dba = db.Access();
    interpret(*dba, "MATCH (n) RETURN n");
  }
  {
    auto dba = db.Access();
    std::this_thread::sleep_for(std::chrono::seconds(5));
    ASSERT_THROW(interpret(*dba, "MATCH (n) RETURN n"),
                 query::HintedAbortError);
  }
  {
    auto dba = db.Access();
    interpret(*dba, "MATCH (n) RETURN n");
  }
}
