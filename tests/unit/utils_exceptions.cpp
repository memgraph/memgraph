// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>
#include <utils/exceptions.hpp>

void i_will_throw() { throw memgraph::utils::BasicException("this is not ok"); }

void bar() { i_will_throw(); }

void foo() { bar(); }

void i_will_throw_stacktrace_exception() { throw memgraph::utils::StacktraceException("this is not {}", "ok!"); }

void bar_stacktrace() { i_will_throw_stacktrace_exception(); }

void foo_stacktrace() { bar_stacktrace(); }

TEST(ExceptionsTest, ThrowBasicAndStackExceptions) {
  ASSERT_THROW(foo(), memgraph::utils::BasicException);
  ASSERT_THROW(foo_stacktrace(), memgraph::utils::StacktraceException);
}
