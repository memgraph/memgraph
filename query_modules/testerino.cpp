// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <mgp.hpp>
#include <string>

thread_local std::optional<mgp_memory *> mgp::MemoryDispatcher::current_memory;

void isEqual(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);

  // Using C API
  auto arg0 = mgp::list_at(args, 0);
  auto arg1 = mgp::list_at(args, 1);
  // validation
  mgp::value_is_string(arg0) && mgp::value_is_string(arg1);

  auto s1 = std::string_view{mgp::value_get_string(arg0)};
  auto s2 = std::string_view{mgp::value_get_string(arg1)};
  auto result = mgp::Result(res);
  result.SetValue(s1 == s2);

  // Using C++ API

  // const auto arguments = mgp::List(args);
  // auto result = mgp::Result(res);
  //
  // std::string output = std::string(arguments[0].ValueString());
  // std::string input = std::string(arguments[1].ValueString());
  //
  // result.SetValue(output == input);
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);

    mgp::AddFunction(isEqual, "is_equal",
                     {mgp::Parameter("output", mgp::Type::String), mgp::Parameter("input", mgp::Type::String)}, module,
                     memory);
  } catch (const std::exception &e) {
    return 1;
  }
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
