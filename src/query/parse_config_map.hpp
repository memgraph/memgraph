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

#include <functional>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>

namespace memgraph::query {

class Expression;
class TypedValue;
template <class TResult>
class ExpressionVisitor;

auto ParseConfigMap(std::unordered_map<Expression *, Expression *> const &config_map,
                    ExpressionVisitor<TypedValue> &evaluator)
    -> std::optional<std::map<std::string, std::string, std::less<>>>;

}  // namespace memgraph::query
