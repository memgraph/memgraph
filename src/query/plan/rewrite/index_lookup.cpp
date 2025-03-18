// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/plan/rewrite/index_lookup.hpp"
#include "query/interpret/eval.hpp"
#include "storage/v2/property_value_utils.hpp"
#include "utils/flag_validation.hpp"

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int64(query_vertex_count_to_expand_existing, 10,
                       "Maximum count of indexed vertices which provoke "
                       "indexed lookup and then expand to existing, instead of "
                       "a regular expand. Default is 10, to turn off use -1.",
                       FLAG_IN_RANGE(-1, std::numeric_limits<std::int64_t>::max()));

namespace memgraph::query::plan {

auto ExpressionRange::Equal(Expression *value) -> ExpressionRange {
  return {Type::EQUAL, utils::MakeBoundInclusive(value), std::nullopt};
}

auto ExpressionRange::RegexMatch() -> ExpressionRange { return {Type::REGEX_MATCH, std::nullopt, std::nullopt}; }

auto ExpressionRange::Range(std::optional<utils::Bound<Expression *>> lower,
                            std::optional<utils::Bound<Expression *>> upper) -> ExpressionRange {
  return {Type::RANGE, std::move(lower), std::move(upper)};
}

auto ExpressionRange::In(Expression *value) -> ExpressionRange {
  return {Type::IN, utils::MakeBoundInclusive(value), std::nullopt};
}

auto ExpressionRange::IsNotNull() -> ExpressionRange { return {Type::IS_NOT_NULL, std::nullopt, std::nullopt}; }

auto ExpressionRange::evaluate(ExpressionEvaluator &evaluator, AstStorage &storage) -> storage::PropertyValueRange {
  auto const to_bounded_property_value = [&](auto &value) -> utils::Bound<storage::PropertyValue> {
    auto const &property_value = storage::PropertyValue(value->value()->Accept(evaluator));
    return utils::Bound<storage::PropertyValue>(property_value, value->type());
  };

  switch (type_) {
    case Type::EQUAL:
    case Type::IN: {
      auto bounded_property_value = to_bounded_property_value(lower_);
      return storage::PropertyValueRange::Bounded(bounded_property_value, bounded_property_value);
    }

    case Type::REGEX_MATCH: {
      auto empty_string = storage::PropertyValue("");
      auto upper_bound = storage::UpperBoundForType(storage::PropertyValueType::String);
      return storage::PropertyValueRange::Bounded(utils::MakeBoundInclusive(empty_string), upper_bound);
    }

    case Type::RANGE: {
      // @TODO note that in `TryConvertToBound`, if the expression evaluates
      // to certain types, will can fail early as values of such types are not
      // ordered.
      return storage::PropertyValueRange::Bounded(to_bounded_property_value(lower_), to_bounded_property_value(upper_));
    }

    case Type::IS_NOT_NULL: {
      return storage::PropertyValueRange::IsNotNull();
    }
  }

  return storage::PropertyValueRange::IsNotNull();
}

ExpressionRange::ExpressionRange(Type type, std::optional<utils::Bound<Expression *>> lower,
                                 std::optional<utils::Bound<Expression *>> upper)
    : type_{type}, lower_{std::move(lower)}, upper_{std::move(upper)} {}

}  // namespace memgraph::query::plan
