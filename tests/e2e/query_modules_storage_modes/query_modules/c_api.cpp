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

#include <chrono>
#include <thread>

#include "_mgp.hpp"
#include "mg_exceptions.hpp"
#include "mg_procedure.h"

constexpr char const *kFunctionPassNode = "pass_node";
constexpr char const *kProcedurePassNodeWithId = "get";

constexpr char const *kFieldNode = "node";
constexpr char const *kFieldId = "id";

void PassNode(mgp_list *args, mgp_func_context *ctx, mgp_func_result *result, mgp_memory *memory) {
  mgp_value *value{nullptr};
  auto err_code = mgp_list_at(args, 0, &value);
  if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
    static_cast<void>(mgp_func_result_set_error_msg(result, "Failed to fetch list!", memory));
    return;
  }
  std::this_thread::sleep_for(std::chrono::seconds(1));
  err_code = mgp_func_result_set_value(result, value, memory);
  if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
    static_cast<void>(mgp_func_result_set_error_msg(result, "Failed to construct return value!", memory));
    return;
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp_func *func{nullptr};
    auto err_code = mgp_module_add_function(module, kFunctionPassNode, PassNode, &func);
    if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
      return 1;
    }

    err_code = mgp_func_add_arg(func, kFieldNode, mgp::type_node());
    if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
      return 1;
    }
  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
