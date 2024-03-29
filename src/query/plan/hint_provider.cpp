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

#include "hint_provider.hpp"

namespace memgraph::query::plan {

std::vector<std::string> ProvidePlanHints(const LogicalOperator *plan_root, const SymbolTable &symbol_table) {
  PlanHintsProvider plan_hinter(symbol_table);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  const_cast<LogicalOperator *>(plan_root)->Accept(plan_hinter);

  return plan_hinter.hints();
}

}  // namespace memgraph::query::plan
