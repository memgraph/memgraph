#include <gtest/gtest.h>
#include <utils/exceptions.hpp>

void i_will_throw() { throw utils::BasicException("this is not ok"); }

void bar() { i_will_throw(); }

void foo() { bar(); }

void i_will_throw_stacktrace_exception() { throw utils::StacktraceException("this is not {}", "ok!"); }

void bar_stacktrace() { i_will_throw_stacktrace_exception(); }

void foo_stacktrace() { bar_stacktrace(); }

TEST(ExceptionsTest, ThrowBasicAndStackExceptions) {
  ASSERT_THROW(foo(), utils::BasicException);
  ASSERT_THROW(foo_stacktrace(), utils::StacktraceException);
}
