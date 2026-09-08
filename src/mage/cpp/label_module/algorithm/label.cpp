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

#include "label.hpp"

// NOLINTNEXTLINE(misc-unused-parameters)
void Label::Exists(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    bool exists = false;

    const auto label = arguments[1].ValueString();
    if (arguments[0].IsNode()) {
      const auto node = arguments[0].ValueNode();
      exists = node.HasLabel(label);
    }
    result.SetValue(exists);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}
