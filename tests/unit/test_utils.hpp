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

#pragma once

#include <memory>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "mg_procedure.h"
#include "query/procedure/mg_procedure_impl.hpp"

namespace test_utils {
using MgpValueOwningPtr = std::unique_ptr<mgp_value, void (*)(mgp_value *)>;

MgpValueOwningPtr CreateValueOwningPtr(mgp_value *value) { return MgpValueOwningPtr(value, &mgp_value_destroy); }

template <typename TResult, typename TFunc, typename... TArgs>
TResult ExpectNoError(const char *file, int line, TFunc func, TArgs &&...args) {
  static_assert(std::is_trivially_copyable_v<TFunc>);
  static_assert((std::is_trivially_copyable_v<std::remove_reference_t<TArgs>> && ...));
  TResult result{};
  EXPECT_EQ(func(args..., &result), mgp_error::MGP_ERROR_NO_ERROR) << fmt::format("Source of error: {}:{}", file, line);
  return result;
}
}  // namespace test_utils

#define EXPECT_MGP_NO_ERROR(type, ...) test_utils::ExpectNoError<type>(__FILE__, __LINE__, __VA_ARGS__)

#define EXPECT_THROW_WITH_MSG(statement, expected_exception, msg_checker) \
  EXPECT_THROW(                                                           \
      {                                                                   \
        try {                                                             \
          statement;                                                      \
        } catch (const expected_exception &e) {                           \
          EXPECT_NO_FATAL_FAILURE(msg_checker(e.what()));                 \
          throw;                                                          \
        }                                                                 \
      },                                                                  \
      expected_exception);
