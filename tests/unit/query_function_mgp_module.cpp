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

#include <functional>
#include <sstream>
#include <string_view>

#include "query/procedure/mg_procedure_impl.hpp"

#include "test_utils.hpp"

static mgp_value *DummyCallback(mgp_list *, mgp_func_context *, mgp_memory *) {
  mgp_value *result{};
  return result;
}

TEST(Module, InvalidFunctionRegistration) {
  mgp_module module(utils::NewDeleteResource());
  mgp_func *func{nullptr};
  // Other test cases are covered within the procedure API. This is only sanity check
  EXPECT_EQ(mgp_module_add_function(&module, "dashes-not-supported", DummyCallback, &func), MGP_ERROR_INVALID_ARGUMENT);
}

TEST(Module, RegisterSameFunctionMultipleTimes) {
  mgp_module module(utils::NewDeleteResource());
  mgp_func *func{nullptr};
  EXPECT_EQ(module.functions.find("same_name"), module.functions.end());
  EXPECT_EQ(mgp_module_add_function(&module, "same_name", DummyCallback, &func), MGP_ERROR_NO_ERROR);
  EXPECT_NE(module.functions.find("same_name"), module.functions.end());
  EXPECT_EQ(mgp_module_add_function(&module, "same_name", DummyCallback, &func), MGP_ERROR_LOGIC_ERROR);
  EXPECT_EQ(mgp_module_add_function(&module, "same_name", DummyCallback, &func), MGP_ERROR_LOGIC_ERROR);
  EXPECT_NE(module.functions.find("same_name"), module.functions.end());
}

TEST(Module, CaseSensitiveFunctionNames) {
  mgp_module module(utils::NewDeleteResource());
  mgp_func *func{nullptr};
  EXPECT_EQ(mgp_module_add_function(&module, "not_same", DummyCallback, &func), MGP_ERROR_NO_ERROR);
  EXPECT_EQ(mgp_module_add_function(&module, "NoT_saME", DummyCallback, &func), MGP_ERROR_NO_ERROR);
  EXPECT_EQ(mgp_module_add_function(&module, "NOT_SAME", DummyCallback, &func), MGP_ERROR_NO_ERROR);
  EXPECT_EQ(module.functions.size(), 3U);
}
