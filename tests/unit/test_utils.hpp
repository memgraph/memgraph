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
  EXPECT_EQ(func(args..., &result), MGP_ERROR_NO_ERROR) << fmt::format("Source of error: {} at line {}", file, line);
  return result;
}
}  // namespace test_utils

#define EXPECT_MGP_NO_ERROR(type, ...) test_utils::ExpectNoError<type>(__FILE__, __LINE__, __VA_ARGS__)