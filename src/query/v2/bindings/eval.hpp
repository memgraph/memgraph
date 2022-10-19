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

#pragma once

#include "query/v2/bindings/bindings.hpp"

#include "expr/interpret/eval.hpp"
#include "query/v2/bindings/typed_value.hpp"
#include "query/v2/context.hpp"
#include "query/v2/conversions.hpp"
#include "query/v2/db_accessor.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/conversions.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/view.hpp"

namespace memgraph::msgs {
class ShardRequestManagerInterface;
} // namespace memgraph::msgs

namespace memgraph::query::v2 {

inline const auto lam = [](const auto &val) { return ValueToTypedValue(val); };
namespace detail {
class Callable {
 public:
  auto operator()(const memgraph::storage::v3::PropertyValue &val) const {
    return memgraph::storage::v3::PropertyToTypedValue<TypedValue>(val);
  };
  auto operator()(const msgs::Value &val, memgraph::msgs::ShardRequestManagerInterface *manager) const {
    return ValueToTypedValue(val, manager);
  };
};

}  // namespace detail
using ExpressionEvaluator = memgraph::expr::ExpressionEvaluator<
    TypedValue, memgraph::query::v2::EvaluationContext, memgraph::msgs::ShardRequestManagerInterface, storage::v3::View,
    storage::v3::LabelId, msgs::Value, detail::Callable, memgraph::storage::v3::Error, memgraph::expr::QueryEngineTag>;

}  // namespace memgraph::query::v2
