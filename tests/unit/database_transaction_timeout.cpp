#include <glog/logging.h>
#include <gtest/gtest.h>

#include "query/exceptions.hpp"
#include "query/interpreter.hpp"

DECLARE_int32(query_execution_time_sec);

TEST(TransactionTimeout, TransactionTimeout) {
  FLAGS_query_execution_time_sec = 3;
  database::GraphDb db;

  auto dba = db.Access();
  std::this_thread::sleep_for(std::chrono::seconds(5));
  ASSERT_TRUE(dba.should_abort());
}
