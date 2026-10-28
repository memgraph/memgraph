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

#include "query/interpret/eval.hpp"

#include "query/auth_checker.hpp"
#include "query/graph.hpp"
#include "query/virtual_graph.hpp"

#include <ranges>
#include <regex>

namespace memgraph::query {

namespace r = std::ranges;
namespace rv = r::views;

namespace {
template <r::input_range R>
TypedValue RangeToTypedValueList(R &&range, utils::MemoryResource *memory) {
  utils::pmr::vector<TypedValue> out(memory);
  if constexpr (r::sized_range<R>) out.reserve(r::size(range));
  for (const auto &item : range) out.emplace_back(TypedValue(item, memory));
  return {std::move(out), memory};
}
}  // namespace

int64_t EvaluateInt(ExpressionVisitor<TypedValue> &eval, Expression *expr, std::string_view what) {
  TypedValue value = expr->Accept(eval);
  try {
    return value.ValueInt();
  } catch (TypedValueException &e) {
    throw QueryRuntimeException(std::string(what) + " must be an int");
  }
}

std::optional<int64_t> EvaluateUint(ExpressionVisitor<TypedValue> &eval, Expression *expr, std::string_view what) {
  if (!expr) {
    return std::nullopt;
  }

  TypedValue value = expr->Accept(eval);
  try {
    auto value_uint = value.ValueInt();
    if (value_uint < 0) {
      throw QueryRuntimeException(std::string(what) + " must be a non-negative integer");
    }
    return value_uint;
  } catch (TypedValueException &e) {
    throw QueryRuntimeException(std::string(what) + " must be a non-negative integer");
  }
}

std::optional<int64_t> EvaluateHopsLimit(ExpressionVisitor<TypedValue> &eval, Expression *expr) {
  return EvaluateUint(eval, expr, "Hops limit");
}

std::optional<int64_t> EvaluateCommitFrequency(ExpressionVisitor<TypedValue> &eval, Expression *expr) {
  return EvaluateUint(eval, expr, "Commit frequency");
}

std::optional<int64_t> EvaluateDeleteBufferSize(ExpressionVisitor<TypedValue> &eval, Expression *expr) {
  return EvaluateUint(eval, expr, "Delete buffer size");
}

std::optional<size_t> EvaluateMemoryLimit(ExpressionVisitor<TypedValue> &eval, Expression *memory_limit,
                                          size_t memory_scale) {
  if (!memory_limit) return std::nullopt;
  auto limit_value = memory_limit->Accept(eval);
  if (!limit_value.IsInt() || limit_value.ValueInt() <= 0)
    throw QueryRuntimeException("Memory limit must be a non-negative integer.");
  size_t limit = limit_value.ValueInt();
  if (std::numeric_limits<size_t>::max() / memory_scale < limit) throw QueryRuntimeException("Memory limit overflow.");
  return limit * memory_scale;
}

TypedValue ExpressionEvaluator::Visit(RegexMatch &regex_match) {
  auto target_string_value = regex_match.string_expr_->Accept(*this);
  if (target_string_value.IsNull()) {
    return TypedValue(ctx_->memory);
  }
  if (target_string_value.type() != TypedValue::Type::String) {
    // Instead of error, we return Null which makes it compatible in case we
    // use indexed lookup which filters out any non-string properties.
    // Assuming a property lookup is the target_string_value.
    return TypedValue(ctx_->memory);
  }

  auto valid_regex_str = [&](TypedValue const &regex) {
    if (regex.IsNull()) {
      return false;
    }
    if (regex.type() != TypedValue::Type::String) {
      throw QueryRuntimeException("Regular expression must evaluate to a string, got {}.", regex.type());
    }
    return true;
  };

  auto build_regex = [](std::pmr::string const &str) {
    try {
      return std::regex{str};
    } catch (const std::regex_error &e) {
      throw QueryRuntimeException("Regex error in '{}': {}", str, e.what());
    }
  };

  if (frame_change_collector_) {
    const auto cached_id = memgraph::utils::GetFrameChangeId(regex_match);
    if (frame_change_collector_->IsRegexKeyTracked(cached_id)) {
      auto cached_value_ref = frame_change_collector_->TryGetRegexCachedValue(cached_id);
      if (!cached_value_ref) {
        // Check only first time if everything is okay, later when we use
        // cache there is no need to check again as we did check first time
        const auto regex_str = regex_match.regex_->Accept(*this);

        if (!valid_regex_str(regex_str)) {
          return TypedValue(ctx_->memory);
        }
        auto cached_value = frame_change_collector_->AddRegexKey(cached_id, build_regex(regex_str.ValueString()));
        cached_value_ref = cached_value;
      }
      return TypedValue(std::regex_match(target_string_value.ValueString(), cached_value_ref->get()), ctx_->memory);
    }
  }

  // When caching is not an option, we need to evaluate regex literal every time
  auto regex_value = regex_match.regex_->Accept(*this);
  if (!valid_regex_str(regex_value)) {
    return TypedValue(ctx_->memory);
  }
  return TypedValue(std::regex_match(target_string_value.ValueString(), build_regex(regex_value.ValueString())),
                    ctx_->memory);
}

TypedValue ExpressionEvaluator::Visit(AllPropertiesLookup &all_properties_lookup) {
  TypedValue::TMap result(ctx_->memory);

  auto expression_result = all_properties_lookup.expression_->Accept(*this);
  switch (expression_result.type()) {
    case TypedValue::Type::Null:
      return TypedValue(ctx_->memory);
    case TypedValue::Type::Vertex: {
      for (const auto &[property_id, value] : GetAllProperties(expression_result.ValueVertex())) {
        auto typed_value = TypedValue(value, GetNameIdMapper(), ctx_->memory);
        result.emplace(TypedValue::TString(dba_->PropertyToName(property_id), ctx_->memory), typed_value);
      }
      return {result, ctx_->memory};
    }
    case TypedValue::Type::Edge: {
      for (const auto &[property_id, value] : GetAllProperties(expression_result.ValueEdge())) {
        auto typed_value = TypedValue(value, GetNameIdMapper(), ctx_->memory);
        result.emplace(TypedValue::TString(dba_->PropertyToName(property_id), ctx_->memory), typed_value);
      }
      return {result, ctx_->memory};
    }
    case TypedValue::Type::VirtualEdge: {
      for (auto const &[prop_id, prop_value] : expression_result.ValueVirtualEdge().Properties()) {
        result.emplace(TypedValue::TString(dba_->PropertyToName(prop_id), ctx_->memory),
                       TypedValue(prop_value, GetNameIdMapper(), ctx_->memory));
      }
      return {result, ctx_->memory};
    }
    case TypedValue::Type::Map: {
      for (auto &[name, value] : expression_result.ValueMap()) {
        result.emplace(name, value);
      }
      return {result, ctx_->memory};
    }
    case TypedValue::Type::Duration: {
      const auto &dur = expression_result.ValueDuration();
      result.emplace(TypedValue::TString("day", ctx_->memory), TypedValue(dur.Days(), ctx_->memory));
      result.emplace(TypedValue::TString("hour", ctx_->memory), TypedValue(dur.SubDaysAsHours(), ctx_->memory));
      result.emplace(TypedValue::TString("minute", ctx_->memory), TypedValue(dur.SubDaysAsMinutes(), ctx_->memory));
      result.emplace(TypedValue::TString("second", ctx_->memory), TypedValue(dur.SubDaysAsSeconds(), ctx_->memory));
      result.emplace(TypedValue::TString("millisecond", ctx_->memory),
                     TypedValue(dur.SubDaysAsMilliseconds(), ctx_->memory));
      result.emplace(TypedValue::TString("microseconds", ctx_->memory),
                     TypedValue(dur.SubDaysAsMicroseconds(), ctx_->memory));
      result.emplace(TypedValue::TString("nanoseconds", ctx_->memory),
                     TypedValue(dur.SubDaysAsNanoseconds(), ctx_->memory));
      return {result, ctx_->memory};
    }
    case TypedValue::Type::Date: {
      const auto &date = expression_result.ValueDate();
      result.emplace(TypedValue::TString("year", ctx_->memory), TypedValue(date.year, ctx_->memory));
      result.emplace(TypedValue::TString("month", ctx_->memory), TypedValue(date.month, ctx_->memory));
      result.emplace(TypedValue::TString("day", ctx_->memory), TypedValue(date.day, ctx_->memory));
      return {result, ctx_->memory};
    }
    case TypedValue::Type::LocalTime: {
      const auto &lt = expression_result.ValueLocalTime();
      result.emplace(TypedValue::TString("hour", ctx_->memory), TypedValue(lt.hour, ctx_->memory));
      result.emplace(TypedValue::TString("minute", ctx_->memory), TypedValue(lt.minute, ctx_->memory));
      result.emplace(TypedValue::TString("second", ctx_->memory), TypedValue(lt.second, ctx_->memory));
      result.emplace(TypedValue::TString("millisecond", ctx_->memory), TypedValue(lt.millisecond, ctx_->memory));
      result.emplace(TypedValue::TString("microsecond", ctx_->memory), TypedValue(lt.microsecond, ctx_->memory));
      return {result, ctx_->memory};
    }
    case TypedValue::Type::LocalDateTime: {
      const auto &ldt = expression_result.ValueLocalDateTime();
      const auto &date = ldt.date();
      const auto &lt = ldt.local_time();
      result.emplace(TypedValue::TString("year", ctx_->memory), TypedValue(date.year, ctx_->memory));
      result.emplace(TypedValue::TString("month", ctx_->memory), TypedValue(date.month, ctx_->memory));
      result.emplace(TypedValue::TString("day", ctx_->memory), TypedValue(date.day, ctx_->memory));
      result.emplace(TypedValue::TString("hour", ctx_->memory), TypedValue(lt.hour, ctx_->memory));
      result.emplace(TypedValue::TString("minute", ctx_->memory), TypedValue(lt.minute, ctx_->memory));
      result.emplace(TypedValue::TString("second", ctx_->memory), TypedValue(lt.second, ctx_->memory));
      result.emplace(TypedValue::TString("millisecond", ctx_->memory), TypedValue(lt.millisecond, ctx_->memory));
      result.emplace(TypedValue::TString("microsecond", ctx_->memory), TypedValue(lt.microsecond, ctx_->memory));
      return {result, ctx_->memory};
    }
    case TypedValue::Type::ZonedDateTime: {
      throw QueryRuntimeException("Can't coerce `{}` to Map.", expression_result.ValueZonedDateTime().ToString());
    }
    case TypedValue::Type::Point2d: {
      auto const &point_2d = expression_result.ValuePoint2d();
      result.emplace(TypedValue::TString("x", ctx_->memory), TypedValue(point_2d.x(), ctx_->memory));
      result.emplace(TypedValue::TString("y", ctx_->memory), TypedValue(point_2d.y(), ctx_->memory));
      result.emplace(TypedValue::TString("srid", ctx_->memory),
                     TypedValue(storage::CrsToSrid(point_2d.crs()).value_of(), ctx_->memory));
      return {result, ctx_->memory};
    }
    case TypedValue::Type::Point3d: {
      auto const &point_3d = expression_result.ValuePoint3d();
      result.emplace(TypedValue::TString("x", ctx_->memory), TypedValue(point_3d.x(), ctx_->memory));
      result.emplace(TypedValue::TString("y", ctx_->memory), TypedValue(point_3d.y(), ctx_->memory));
      result.emplace(TypedValue::TString("z", ctx_->memory), TypedValue(point_3d.z(), ctx_->memory));
      result.emplace(TypedValue::TString("srid", ctx_->memory),
                     TypedValue(storage::CrsToSrid(point_3d.crs()).value_of(), ctx_->memory));
      return {result, ctx_->memory};
    }
    case TypedValue::Type::VirtualNode: {
      for (auto const &[prop_id, prop_value] : expression_result.ValueVirtualNode().Properties()) {
        result.emplace(TypedValue::TString(dba_->PropertyToName(prop_id), ctx_->memory),
                       TypedValue(prop_value, GetNameIdMapper(), ctx_->memory));
      }
      return {result, ctx_->memory};
    }
    case TypedValue::Type::Graph: {
      const auto &graph = expression_result.ValueGraph();
      result.emplace(TypedValue::TString("nodes", ctx_->memory), RangeToTypedValueList(graph.vertices(), ctx_->memory));
      result.emplace(TypedValue::TString("edges", ctx_->memory), RangeToTypedValueList(graph.edges(), ctx_->memory));
      return {result, ctx_->memory};
    }
    case TypedValue::Type::VirtualGraph: {
      const auto &vg = expression_result.ValueVirtualGraph();
      result.emplace(TypedValue::TString("nodes", ctx_->memory),
                     RangeToTypedValueList(vg.nodes() | rv::values |
                                               rv::transform([](const auto &sp) -> const VirtualNode & { return *sp; }),
                                           ctx_->memory));
      result.emplace(TypedValue::TString("edges", ctx_->memory), RangeToTypedValueList(vg.edges(), ctx_->memory));
      return {result, ctx_->memory};
    }

    default:
      throw QueryRuntimeException(
          "Only nodes, edges, maps, temporal types, points, and graphs have properties to be looked up.");
  }
}

TypedValue ExpressionEvaluator::Visit(PropertyLookup &property_lookup) {
  ReferenceExpressionEvaluator referenceExpressionEvaluator(frame_, ctx_);

  TypedValue const *expression_result_ptr = property_lookup.expression_->Accept(referenceExpressionEvaluator);
  TypedValue expression_result;

  if (nullptr == expression_result_ptr) {
    expression_result = property_lookup.expression_->Accept(*this);
    expression_result_ptr = &expression_result;
  }
  // A cartesian point has x and y where a geographic one has longitude and latitude, and
  // naming the one to use instead is more help than reporting the name as unknown.
  auto field_error = [](std::string_view prop_name, bool geographic, std::string_view kind) {
    if (!geographic) {
      if (prop_name == "longitude")
        return QueryRuntimeException("Use x instead of longitude for cartesian point types");
      if (prop_name == "latitude") return QueryRuntimeException("Use y instead of latitude for cartesian point types");
      if (prop_name == "height") return QueryRuntimeException("Use z instead of height for cartesian point types");
    }
    return QueryRuntimeException("Invalid property name {} for {}", prop_name, kind);
  };
  auto maybe_graph = [this](const auto &graph, const auto &prop_name) -> std::optional<TypedValue> {
    if (prop_name == "nodes") return RangeToTypedValueList(graph.vertices(), ctx_->memory);
    if (prop_name == "edges") return RangeToTypedValueList(graph.edges(), ctx_->memory);
    return std::nullopt;
  };
  auto maybe_virtual_graph = [this](const auto &vg, const auto &prop_name) -> std::optional<TypedValue> {
    if (prop_name == "nodes") {
      return RangeToTypedValueList(
          vg.nodes() | rv::values | rv::transform([](const auto &sp) -> const VirtualNode & { return *sp; }),
          ctx_->memory);
    }
    if (prop_name == "edges") return RangeToTypedValueList(vg.edges(), ctx_->memory);
    return std::nullopt;
  };
  switch (expression_result_ptr->type()) {
    case TypedValue::Type::Null:
      return TypedValue(ctx_->memory);
    case TypedValue::Type::Vertex:
      if (property_lookup.evaluation_mode_ == PropertyLookup::EvaluationMode::GET_ALL_PROPERTIES) {
        return CachedPropertyValue<RecordIdentity::Kind::Vertex>(expression_result_ptr->ValueVertex(), property_lookup);
      } else {
        return {GetProperty(expression_result_ptr->ValueVertex(), property_lookup.property_),
                GetNameIdMapper(),
                ctx_->memory};
      }
    case TypedValue::Type::Edge:
      if (property_lookup.evaluation_mode_ == PropertyLookup::EvaluationMode::GET_ALL_PROPERTIES) {
        return CachedPropertyValue<RecordIdentity::Kind::Edge>(expression_result_ptr->ValueEdge(), property_lookup);
      } else {
        return {GetProperty(expression_result_ptr->ValueEdge(), property_lookup.property_),
                GetNameIdMapper(),
                ctx_->memory};
      }
    case TypedValue::Type::VirtualEdge: {
      auto prop_id = dba_->NameToProperty(property_lookup.property_.name);
      auto prop_value = expression_result_ptr->ValueVirtualEdge().GetProperty(prop_id);
      if (prop_value.IsNull()) return TypedValue(ctx_->memory);
      return {std::move(prop_value), GetNameIdMapper(), ctx_->memory};
    }
    case TypedValue::Type::VirtualNode: {
      auto prop_id = dba_->NameToProperty(property_lookup.property_.name);
      auto prop_value = expression_result_ptr->ValueVirtualNode().GetProperty(prop_id);
      if (prop_value.IsNull()) return TypedValue(ctx_->memory);
      return {std::move(prop_value), GetNameIdMapper(), ctx_->memory};
    }
    case TypedValue::Type::Map: {
      auto &map = expression_result_ptr->ValueMap();
      auto found = map.find(property_lookup.property_.name.c_str());
      if (found == map.end()) return TypedValue(ctx_->memory);
      return {found->second, ctx_->memory};
    }
    case TypedValue::Type::Duration: {
      const auto &prop_name = property_lookup.property_.name;
      if (auto read = storage::ReadFieldNamed(expression_result_ptr->ValueDuration(), prop_name)) {
        return {*read, GetNameIdMapper(), ctx_->memory};
      }
      throw field_error(prop_name, true, "Duration");
    }
    case TypedValue::Type::Date: {
      const auto &prop_name = property_lookup.property_.name;
      if (auto read = storage::ReadFieldNamed(expression_result_ptr->ValueDate(), prop_name)) {
        return {*read, GetNameIdMapper(), ctx_->memory};
      }
      throw field_error(prop_name, true, "Date");
    }
    case TypedValue::Type::LocalTime: {
      const auto &prop_name = property_lookup.property_.name;
      if (auto read = storage::ReadFieldNamed(expression_result_ptr->ValueLocalTime(), prop_name)) {
        return {*read, GetNameIdMapper(), ctx_->memory};
      }
      throw field_error(prop_name, true, "LocalTime");
    }
    case TypedValue::Type::LocalDateTime: {
      const auto &prop_name = property_lookup.property_.name;
      if (auto read = storage::ReadFieldNamed(expression_result_ptr->ValueLocalDateTime(), prop_name)) {
        return {*read, GetNameIdMapper(), ctx_->memory};
      }
      throw field_error(prop_name, true, "LocalDateTime");
    }
    case TypedValue::Type::ZonedDateTime: {
      const auto &prop_name = property_lookup.property_.name;
      if (auto read = storage::ReadFieldNamed(expression_result_ptr->ValueZonedDateTime(), prop_name)) {
        return {*read, GetNameIdMapper(), ctx_->memory};
      }
      throw field_error(prop_name, true, "ZonedDateTime");
    }
    case TypedValue::Type::Point2d: {
      const auto &prop_name = property_lookup.property_.name;
      const auto &point_2d = expression_result_ptr->ValuePoint2d();
      if (auto read = storage::ReadFieldNamed(point_2d, prop_name)) {
        return {*read, GetNameIdMapper(), ctx_->memory};
      }
      throw field_error(prop_name, point_2d.crs() == storage::CoordinateReferenceSystem::WGS84_2d, "Point2d");
    }
    case TypedValue::Type::Point3d: {
      const auto &prop_name = property_lookup.property_.name;
      const auto &point_3d = expression_result_ptr->ValuePoint3d();
      if (auto read = storage::ReadFieldNamed(point_3d, prop_name)) {
        return {*read, GetNameIdMapper(), ctx_->memory};
      }
      throw field_error(prop_name, point_3d.crs() == storage::CoordinateReferenceSystem::WGS84_3d, "Point3d");
    }
    case TypedValue::Type::Graph: {
      const auto &prop_name = property_lookup.property_.name;
      const auto &graph = expression_result_ptr->ValueGraph();
      if (auto graph_field = maybe_graph(graph, prop_name); graph_field) {
        return {*graph_field, ctx_->memory};
      }
      throw QueryRuntimeException("Invalid property name {} for Graph", prop_name);
    }
    case TypedValue::Type::VirtualGraph: {
      const auto &prop_name = property_lookup.property_.name;
      const auto &vg = expression_result_ptr->ValueVirtualGraph();
      if (auto vg_field = maybe_virtual_graph(vg, prop_name); vg_field) {
        return {*vg_field, ctx_->memory};
      }
      throw QueryRuntimeException("Invalid property name {} for VirtualGraph", prop_name);
    }
    default:
      throw QueryRuntimeException(
          "Only nodes, edges, maps, temporal types and graphs have properties to be looked up.");
  }
}

}  // namespace memgraph::query
