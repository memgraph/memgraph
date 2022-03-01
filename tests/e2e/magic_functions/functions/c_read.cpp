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

#include "mg_procedure.h"

static mgp_func_result *return_function_argument(struct mgp_list *args, mgp_func_context *ctx,
                                                 struct mgp_memory *memory) {
  mgp_func_result *result{};

  mgp_value *value{};
  auto err_code = mgp_list_at(args, 0, &value);
  if (err_code != MGP_ERROR_NO_ERROR) {
    mgp_func_result_error("Failed to fetch list!", memory, &result);
    return result;
  }

  err_code = mgp_func_result_value(value, memory, &result);
  if (err_code != MGP_ERROR_NO_ERROR) {
    mgp_func_result_error("Failed to construct return value!", memory, &result);
    return result;
  }
  return result;
}

double get_element_from_arg(struct mgp_list *args, int index) {
  mgp_value *value{};
  mgp_list_at(args, index, &value);

  double result;
  int is_int;
  mgp_value_is_int(value, &is_int);

  if (is_int) {
    int64_t result_int;
    mgp_value_get_int(value, &result_int);
    result = static_cast<double>(result_int);
  } else {
    mgp_value_get_double(value, &result);
  }
  return result;
}

static mgp_func_result *add_two_numbers(struct mgp_list *args, mgp_func_context *ctx, struct mgp_memory *memory) {
  mgp_func_result *result{};

  auto first = get_element_from_arg(args, 0);
  auto second = get_element_from_arg(args, 1);

  mgp_value *value{};
  auto summation = first + second;
  mgp_value_make_double(summation, memory, &value);

  auto err_code = mgp_func_result_value(value, memory, &result);
  if (err_code != MGP_ERROR_NO_ERROR) {
    mgp_func_result_error("Failed to construct return value!", memory, &result);
    return result;
  }
  return result;
}

static mgp_func_result *return_null(struct mgp_list *args, mgp_func_context *ctx, struct mgp_memory *memory) {
  mgp_func_result *result{};

  mgp_value *value{};
  mgp_value_make_null(memory, &value);
  auto err_code = mgp_func_result_value(value, memory, &result);
  if (err_code != MGP_ERROR_NO_ERROR) {
    mgp_func_result_error("Failed to fetch list!", memory, &result);
    return result;
  }

  return result;
}

// Each module needs to define mgp_init_module function.
// Here you can register multiple procedures your module supports.
extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  {
    mgp_func *func;
    auto err_code = mgp_module_add_function(module, "return_function_argument", return_function_argument, &func);
    if (err_code != MGP_ERROR_NO_ERROR) {
      return 1;
    }

    mgp_type *type_any{};
    mgp_type_any(&type_any);
    err_code = mgp_func_add_arg(func, "argument", type_any);
    if (err_code != MGP_ERROR_NO_ERROR) {
      return 1;
    }
  }

  {
    mgp_func *func;
    auto err_code = mgp_module_add_function(module, "add_two_numbers", add_two_numbers, &func);
    if (err_code != MGP_ERROR_NO_ERROR) {
      return 1;
    }

    mgp_type *type_number{};
    mgp_type_number(&type_number);
    err_code = mgp_func_add_arg(func, "first", type_number);
    if (err_code != MGP_ERROR_NO_ERROR) {
      return 1;
    }
    err_code = mgp_func_add_arg(func, "second", type_number);
    if (err_code != MGP_ERROR_NO_ERROR) {
      return 1;
    }
  }

  {
    mgp_func *func;
    auto err_code = mgp_module_add_function(module, "return_null", return_null, &func);
    if (err_code != MGP_ERROR_NO_ERROR) {
      return 1;
    }
  }

  return 0;
}

// This is an optional function if you need to release any resources before the
// module is unloaded. You will probably need this if you acquired some
// resources in mgp_init_module.
extern "C" int mgp_shutdown_module() { return 0; }
