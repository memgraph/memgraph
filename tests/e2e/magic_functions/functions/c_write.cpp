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

static mgp_func_result *try_to_write(struct mgp_list *args, mgp_func_context *ctx, struct mgp_memory *memory) {
  mgp_func_result *result{};

  mgp_value *value{};
  mgp_vertex *vertex{};
  mgp_list_at(args, 0, &value);
  mgp_value_get_vertex(value, &vertex);

  const char *name;
  mgp_list_at(args, 1, &value);
  mgp_value_get_string(value, &name);

  mgp_list_at(args, 2, &value);

  auto err_code = mgp_vertex_set_property(vertex, name, value);  // This should set an error
  if (err_code != MGP_ERROR_NO_ERROR) {
    mgp_func_result_error("Cannot set property in the function!", memory, &result);
    return result;
  }

  err_code = mgp_func_result_value(value, memory, &result);
  if (err_code != MGP_ERROR_NO_ERROR) {
    mgp_func_result_error("Failed to construct return value!", memory, &result);
    return result;
  }
  return result;
}

// Each module needs to define mgp_init_module function.
// Here you can register multiple procedures your module supports.
extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  {
    mgp_func *func;
    auto err_code = mgp_module_add_function(module, "try_to_write", try_to_write, &func);
    if (err_code != MGP_ERROR_NO_ERROR) {
      return 1;
    }

    mgp_type *type_vertex{};
    mgp_type_node(&type_vertex);
    err_code = mgp_func_add_arg(func, "argument", type_vertex);
    if (err_code != MGP_ERROR_NO_ERROR) {
      return 1;
    }

    mgp_type *type_string{};
    mgp_type_string(&type_string);
    err_code = mgp_func_add_arg(func, "name", type_string);
    if (err_code != MGP_ERROR_NO_ERROR) {
      return 1;
    }

    mgp_type *any_type{};
    mgp_type_any(&any_type);
    mgp_type *nullable_type{};
    mgp_type_nullable(any_type, &nullable_type);
    err_code = mgp_func_add_arg(func, "value", nullable_type);
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
