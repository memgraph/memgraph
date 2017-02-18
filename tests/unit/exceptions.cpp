#include "gtest/gtest.h"

#include "utils/exceptions/basic_exception.hpp"
#include "utils/exceptions/stacktrace_exception.hpp"

void i_will_throw() { throw BasicException("this is not ok"); }

void bar() { i_will_throw(); }

void foo() { bar(); }

void i_will_throw_stacktrace_exception() {
  throw StacktraceException("this is not {}", "ok!");
}

void bar_stacktrace() { i_will_throw_stacktrace_exception(); }

void foo_stacktrace() { bar_stacktrace(); }

TEST(ExceptionsTest, ThrowBasicAndStackExceptions) {
  ASSERT_THROW(foo(), BasicException);
  ASSERT_THROW(foo_stacktrace(), StacktraceException);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
