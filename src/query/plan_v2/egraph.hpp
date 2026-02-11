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

#pragma once

#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "storage/v2/property_value.hpp"
#include "strong_type/strong_type.hpp"

namespace memgraph::query::plan {
class LogicalOperator;
}

namespace memgraph::query::plan::v2 {

using eclass = strong::type<std::uint32_t, struct eclass_>;

struct egraph {
  egraph();
  egraph(egraph &&) noexcept;
  egraph &operator=(egraph &&) noexcept;
  ~egraph();  // required because pimpl

  // Public API for creating egraph nodes
  auto MakeOnce() -> eclass;
  auto MakeSymbol(int32_t position, std::string_view name) -> eclass;
  auto MakeLiteral(storage::ExternalPropertyValue const &value) -> eclass;
  auto MakeParameterLookup(int32_t position) -> eclass;
  auto MakeBind(eclass input, eclass sym, eclass expr) -> eclass;
  auto MakeIdentifier(eclass sym) -> eclass;
  auto MakeOutputs(eclass input, std::vector<eclass> named_outputs) -> eclass;
  auto MakeNamedOutput(std::string_view name, eclass sym, eclass expr) -> eclass;

 private:
  struct impl;
  std::unique_ptr<impl> pimpl_;

  // Friend accessor for internal implementation access
  friend struct internal;
};
}  // namespace memgraph::query::plan::v2
