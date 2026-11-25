// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cstdint>
#include <optional>
#include <string_view>

#include <mgp.hpp>

#include "algorithm/bitwise.hpp"

namespace {

std::optional<int64_t> OptionalInt(const mgp::Value &value) {
  if (value.IsNull()) return std::nullopt;
  return value.ValueInt();
}

std::optional<std::string_view> OptionalString(const mgp::Value &value) {
  if (value.IsNull()) return std::nullopt;
  return value.ValueString();
}

void Op(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    // Kept alive by name: the operator's string_view points into its value.
    const auto a = arguments[0];
    const auto op = arguments[1];
    const auto b = arguments[2];

    const auto value = Bitwise::Op(OptionalInt(a), OptionalString(op), OptionalInt(b));
    if (value) {
      result.SetValue(*value);
    } else {
      result.SetValue();
    }
  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
  }
}

}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};

    // Registered through the C API so that every argument can be NULL and reach the body (INTEGER?, STRING?),
    // which the high-level mgp::AddFunction/Parameter cannot express. NULL arguments yield NULL rather than an error.
    auto *func = mgp::module_add_function(module, Bitwise::kProcedureOp, Op);
    mgp::func_add_arg(func, Bitwise::kArgumentA, mgp::type_nullable(mgp::type_int()));
    mgp::func_add_arg(func, Bitwise::kArgumentOperator, mgp::type_nullable(mgp::type_string()));
    mgp::func_add_arg(func, Bitwise::kArgumentB, mgp::type_nullable(mgp::type_int()));
  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
