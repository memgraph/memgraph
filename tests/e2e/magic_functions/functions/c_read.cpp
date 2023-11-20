// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <functional>
#include <stdexcept>

#include "mg_procedure.h"

#include "utils/on_scope_exit.hpp"

namespace {
void ReturnFunctionArgument(struct mgp_list *args, mgp_func_context *ctx, mgp_func_result *result,
                            struct mgp_memory *memory) {
  mgp_value *value{nullptr};
  auto err_code = mgp_list_at(args, 0, &value);
  if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
    static_cast<void>(mgp_func_result_set_error_msg(result, "Failed to fetch list!", memory));
    return;
  }

  err_code = mgp_func_result_set_value(result, value, memory);
  if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
    static_cast<void>(mgp_func_result_set_error_msg(result, "Failed to construct return value!", memory));
    return;
  }
}

void ReturnOptionalArgument(struct mgp_list *args, mgp_func_context *ctx, mgp_func_result *result,
                            struct mgp_memory *memory) {
  mgp_value *value{nullptr};
  auto err_code = mgp_list_at(args, 0, &value);
  if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
    static_cast<void>(mgp_func_result_set_error_msg(result, "Failed to fetch list!", memory));
    return;
  }

  err_code = mgp_func_result_set_value(result, value, memory);
  if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
    static_cast<void>(mgp_func_result_set_error_msg(result, "Failed to construct return value!", memory));
    return;
  }
}

double GetElementFromArg(struct mgp_list *args, int index) {
  mgp_value *value{nullptr};
  if (mgp_list_at(args, index, &value) != mgp_error::MGP_ERROR_NO_ERROR) {
    throw std::runtime_error("Error while argument fetching.");
  }

  double result;
  int is_int;
  static_cast<void>(mgp_value_is_int(value, &is_int));

  if (is_int) {
    int64_t result_int;
    static_cast<void>(mgp_value_get_int(value, &result_int));
    result = static_cast<double>(result_int);
  } else {
    static_cast<void>(mgp_value_get_double(value, &result));
  }
  return result;
}

void AddTwoNumbers(struct mgp_list *args, mgp_func_context *ctx, mgp_func_result *result, struct mgp_memory *memory) {
  double first = 0;
  double second = 0;
  try {
    first = GetElementFromArg(args, 0);
    second = GetElementFromArg(args, 1);
  } catch (...) {
    static_cast<void>(mgp_func_result_set_error_msg(result, "Unable to fetch the result!", memory));
    return;
  }

  mgp_value *value{nullptr};
  auto summation = first + second;
  static_cast<void>(mgp_value_make_double(summation, memory, &value));
  memgraph::utils::OnScopeExit delete_summation_value([&value] { mgp_value_destroy(value); });

  auto err_code = mgp_func_result_set_value(result, value, memory);
  if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
    static_cast<void>(mgp_func_result_set_error_msg(result, "Failed to construct return value!", memory));
  }
}

void ReturnNull(struct mgp_list *args, mgp_func_context *ctx, mgp_func_result *result, struct mgp_memory *memory) {
  mgp_value *value{nullptr};
  static_cast<void>(mgp_value_make_null(memory, &value));
  memgraph::utils::OnScopeExit delete_null([&value] { mgp_value_destroy(value); });

  auto err_code = mgp_func_result_set_value(result, value, memory);
  if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
    static_cast<void>(mgp_func_result_set_error_msg(result, "Failed to fetch list!", memory));
  }
}
}  // namespace

// Each module needs to define mgp_init_module function.
// Here you can register multiple functions/procedures your module supports.
extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  {
    mgp_func *func{nullptr};
    auto err_code = mgp_module_add_function(module, "return_function_argument", ReturnFunctionArgument, &func);
    if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
      return 1;
    }

    mgp_type *type_any{nullptr};
    static_cast<void>(mgp_type_any(&type_any));
    err_code = mgp_func_add_arg(func, "argument", type_any);
    if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
      return 1;
    }
  }

  {
    mgp_func *func{nullptr};
    auto err_code = mgp_module_add_function(module, "return_optional_argument", ReturnOptionalArgument, &func);
    if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
      return 1;
    }

    mgp_value *default_value{nullptr};
    static_cast<void>(mgp_value_make_int(42, memory, &default_value));
    memgraph::utils::OnScopeExit delete_summation_value([&default_value] { mgp_value_destroy(default_value); });

    mgp_type *type_int{nullptr};
    static_cast<void>(mgp_type_int(&type_int));
    err_code = mgp_func_add_opt_arg(func, "opt_argument", type_int, default_value);
    if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
      return 1;
    }
  }

  {
    mgp_func *func{nullptr};
    auto err_code = mgp_module_add_function(module, "add_two_numbers", AddTwoNumbers, &func);
    if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
      return 1;
    }

    mgp_type *type_number{nullptr};
    static_cast<void>(mgp_type_number(&type_number));
    err_code = mgp_func_add_arg(func, "first", type_number);
    if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
      return 1;
    }
    err_code = mgp_func_add_arg(func, "second", type_number);
    if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
      return 1;
    }
  }

  {
    mgp_func *func{nullptr};
    auto err_code = mgp_module_add_function(module, "return_null", ReturnNull, &func);
    if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
      return 1;
    }
  }

  return 0;
}

// This is an optional function if you need to release any resources before the
// module is unloaded. You will probably need this if you acquired some
// resources in mgp_init_module.
extern "C" int mgp_shutdown_module() { return 0; }
