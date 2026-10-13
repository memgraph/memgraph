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
#include <string>
#include <unordered_map>

#include "query/frontend/ast/ast.hpp"

namespace memgraph::query::frontend {

// Token positions of `$name` parameters, mapped to their unescaped names. Stripped literals are not in here.
using ParameterNames = std::unordered_map<int32_t, std::string>;

// When a projection aggregates, ORDER BY and WHERE only see the projected items, so any part of them that repeats a
// projected item's expression is replaced with a reference to that item.
void ReferToProjectedItems(ReturnBody &body, Where *where, ParameterNames const &parameter_names, AstStorage &storage);

}  // namespace memgraph::query::frontend
