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

#include <mgp.hpp>
#include <string>

namespace Mgps {

constexpr std::string_view kProcedureValidate = "validate";
constexpr std::string_view kParameterPredicate = "predicate";
constexpr std::string_view kParameterMessage = "message";
constexpr std::string_view kParameterParams = "params";

void Validate(mgp_list *args, mgp_graph * /*memgraph_graph*/, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto predicate = arguments[0].ValueBool();

  if (!predicate) {
    return;
  }

  auto message = std::string(arguments[1].ValueString());
  const auto params = arguments[2].ValueList();

  // Replace format specifiers: %d (int), %f (double), %s (string)
  std::size_t pos = 0;
  for (const auto &param : params) {
    pos = message.find('%', pos);
    if (pos == std::string::npos || pos + 1 >= message.size()) break;

    const char spec = message[pos + 1];
    if (!std::isalpha(spec)) {
      ++pos;
      continue;
    }

    std::string replacement;
    switch (spec) {
      case 'd':
        replacement = std::to_string(param.ValueInt());
        break;
      case 'f':
        replacement = std::to_string(param.ValueDouble());
        break;
      case 's':
        replacement = std::string(param.ValueString());
        break;
      default:
        static_cast<void>(
            mgp_result_set_error_msg(result, ("Unsupported format specifier: %" + std::string(1, spec)).c_str()));
        return;
    }
    message.replace(pos, 2, replacement);
    pos += replacement.size();
  }

  static_cast<void>(mgp_result_set_error_msg(result, message.c_str()));
}

}  // namespace Mgps

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};

    AddProcedure(Mgps::Validate,
                 Mgps::kProcedureValidate,
                 mgp::ProcedureType::Write,
                 {mgp::Parameter(Mgps::kParameterPredicate, mgp::Type::Bool),
                  mgp::Parameter(Mgps::kParameterMessage, mgp::Type::String),
                  mgp::Parameter(Mgps::kParameterParams, {mgp::Type::List, mgp::Type::Any})},
                 {},  // No return values - void procedure
                 module,
                 memory);
  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
