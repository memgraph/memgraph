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

#include "query/interpret/eval.hpp"

#include "query/graph.hpp"

#include <regex>

namespace memgraph::query {

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
  auto regex_value = regex_match.regex_->Accept(*this);
  if (target_string_value.IsNull() || regex_value.IsNull()) {
    return TypedValue(ctx_->memory);
  }
  if (regex_value.type() != TypedValue::Type::String) {
    throw QueryRuntimeException("Regular expression must evaluate to a string, got {}.", regex_value.type());
  }
  if (target_string_value.type() != TypedValue::Type::String) {
    // Instead of error, we return Null which makes it compatible in case we
    // use indexed lookup which filters out any non-string properties.
    // Assuming a property lookup is the target_string_value.
    return TypedValue(ctx_->memory);
  }
  const auto &target_string = target_string_value.ValueString();
  try {
    std::regex const regex(regex_value.ValueString());
    return TypedValue(std::regex_match(target_string, regex), ctx_->memory);
  } catch (const std::regex_error &e) {
    throw QueryRuntimeException("Regex error in '{}': {}", regex_value.ValueString(), e.what());
  }
}
TypedValue ExpressionEvaluator::Visit(AllPropertiesLookup &all_properties_lookup) {
  TypedValue::TMap result(ctx_->memory);

  auto expression_result = all_properties_lookup.expression_->Accept(*this);
  switch (expression_result.type()) {
    case TypedValue::Type::Null:
      return TypedValue(ctx_->memory);
    case TypedValue::Type::Vertex: {
      for (const auto properties = *expression_result.ValueVertex().Properties(view_);
           const auto &[property_id, value] : properties) {
        auto typed_value = TypedValue(value, GetNameIdMapper(), ctx_->memory);
        result.emplace(TypedValue::TString(dba_->PropertyToName(property_id), ctx_->memory), typed_value);
      }
      return {result, ctx_->memory};
    }
    case TypedValue::Type::Edge: {
      for (const auto properties = *expression_result.ValueEdge().Properties(view_);
           const auto &[property_id, value] : properties) {
        auto typed_value = TypedValue(value, GetNameIdMapper(), ctx_->memory);
        result.emplace(TypedValue::TString(dba_->PropertyToName(property_id), ctx_->memory), typed_value);
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
    case TypedValue::Type::Graph: {
      const auto &graph = expression_result.ValueGraph();
      utils::pmr::vector<TypedValue> vertices(ctx_->memory);
      vertices.reserve(graph.vertices().size());
      for (const auto &v : graph.vertices()) {
        vertices.emplace_back(v);
      }
      result.emplace(TypedValue::TString("nodes", ctx_->memory), TypedValue(std::move(vertices), ctx_->memory));

      utils::pmr::vector<TypedValue> edges(ctx_->memory);
      edges.reserve(graph.edges().size());
      for (const auto &e : graph.edges()) {
        edges.emplace_back(e);
      }
      result.emplace(TypedValue::TString("edges", ctx_->memory), TypedValue(std::move(edges), ctx_->memory));

      return {result, ctx_->memory};
    }

    default:
      throw QueryRuntimeException(
          "Only nodes, edges, maps, temporal types, points, and graphs have properties to be looked up.");
  }
}
TypedValue ExpressionEvaluator::Visit(PropertyLookup &property_lookup) {
  ReferenceExpressionEvaluator referenceExpressionEvaluator(frame_, symbol_table_, ctx_);

  TypedValue const *expression_result_ptr = property_lookup.expression_->Accept(referenceExpressionEvaluator);
  TypedValue expression_result;

  if (nullptr == expression_result_ptr) {
    expression_result = property_lookup.expression_->Accept(*this);
    expression_result_ptr = &expression_result;
  }
  auto maybe_date = [this](const auto &date, const auto &prop_name) -> std::optional<TypedValue> {
    if (prop_name == "year") {
      return TypedValue(date.year, ctx_->memory);
    }
    if (prop_name == "month") {
      return TypedValue(date.month, ctx_->memory);
    }
    if (prop_name == "day") {
      return TypedValue(date.day, ctx_->memory);
    }
    return std::nullopt;
  };
  auto maybe_local_time = [this](const auto &lt, const auto &prop_name) -> std::optional<TypedValue> {
    if (prop_name == "hour") {
      return TypedValue(lt.hour, ctx_->memory);
    }
    if (prop_name == "minute") {
      return TypedValue(lt.minute, ctx_->memory);
    }
    if (prop_name == "second") {
      return TypedValue(lt.second, ctx_->memory);
    }
    if (prop_name == "millisecond") {
      return TypedValue(lt.millisecond, ctx_->memory);
    }
    if (prop_name == "microsecond") {
      return TypedValue(lt.microsecond, ctx_->memory);
    }
    return std::nullopt;
  };
  auto maybe_duration = [this](const auto &dur, const auto &prop_name) -> std::optional<TypedValue> {
    if (prop_name == "day") {
      return TypedValue(dur.Days(), ctx_->memory);
    }
    if (prop_name == "hour") {
      return TypedValue(dur.SubDaysAsHours(), ctx_->memory);
    }
    if (prop_name == "minute") {
      return TypedValue(dur.SubDaysAsMinutes(), ctx_->memory);
    }
    if (prop_name == "second") {
      return TypedValue(dur.SubDaysAsSeconds(), ctx_->memory);
    }
    if (prop_name == "millisecond") {
      return TypedValue(dur.SubDaysAsMilliseconds(), ctx_->memory);
    }
    if (prop_name == "microsecond") {
      return TypedValue(dur.SubDaysAsMicroseconds(), ctx_->memory);
    }
    if (prop_name == "nanosecond") {
      return TypedValue(dur.SubDaysAsNanoseconds(), ctx_->memory);
    }
    return std::nullopt;
  };
  auto maybe_zoned_date_time = [this](const auto &zdt, const auto &prop_name) -> std::optional<TypedValue> {
    if (prop_name == "year") {
      return TypedValue(zdt.LocalYear(), ctx_->memory);
    }
    if (prop_name == "month") {
      return TypedValue(utils::MemcpyCast<int64_t>(zdt.LocalMonth()), ctx_->memory);
    }
    if (prop_name == "day") {
      return TypedValue(utils::MemcpyCast<int64_t>(zdt.LocalDay()), ctx_->memory);
    }
    if (prop_name == "hour") {
      return TypedValue(zdt.LocalHour(), ctx_->memory);
    }
    if (prop_name == "minute") {
      return TypedValue(zdt.LocalMinute(), ctx_->memory);
    }
    if (prop_name == "second") {
      return TypedValue(zdt.LocalSecond(), ctx_->memory);
    }
    if (prop_name == "millisecond") {
      return TypedValue(zdt.LocalMillisecond(), ctx_->memory);
    }
    if (prop_name == "microsecond") {
      return TypedValue(zdt.LocalMicrosecond(), ctx_->memory);
    }
    if (prop_name == "timezone") {
      return TypedValue(zdt.GetTimezone().ToString(), ctx_->memory);
    }
    return std::nullopt;
  };
  auto maybe_point2d = [this](const auto &point_2d, const auto &prop_name) -> std::optional<TypedValue> {
    auto is_wgs = point_2d.crs() == storage::CoordinateReferenceSystem::WGS84_2d;
    if (prop_name == "x") {
      return TypedValue(point_2d.x(), ctx_->memory);
    }
    if (prop_name == "longitude") {
      if (!is_wgs) throw QueryRuntimeException("Use x instead of longitude for cartesian point types");
      return TypedValue(point_2d.x(), ctx_->memory);
    }
    if (prop_name == "y") {
      return TypedValue(point_2d.y(), ctx_->memory);
    }
    if (prop_name == "latitude") {
      if (!is_wgs) throw QueryRuntimeException("Use y instead of latitude for cartesian point types");
      return TypedValue(point_2d.y(), ctx_->memory);
    }
    if (prop_name == "crs") {
      return TypedValue(storage::CrsToString(point_2d.crs()), ctx_->memory);
    }
    if (prop_name == "srid") {
      return TypedValue(storage::CrsToSrid(point_2d.crs()).value_of(), ctx_->memory);
    }
    return std::nullopt;
  };
  auto maybe_point3d = [this](const auto &point_3d, const auto &prop_name) -> std::optional<TypedValue> {
    auto is_wgs = point_3d.crs() == storage::CoordinateReferenceSystem::WGS84_3d;
    if (prop_name == "x") {
      return TypedValue(point_3d.x(), ctx_->memory);
    }
    if (prop_name == "longitude") {
      if (!is_wgs) throw QueryRuntimeException("Use x instead of longitude for cartesian point types");
      return TypedValue(point_3d.x(), ctx_->memory);
    }
    if (prop_name == "y") {
      return TypedValue(point_3d.y(), ctx_->memory);
    }
    if (prop_name == "latitude") {
      if (!is_wgs) throw QueryRuntimeException("Use y instead of latitude for cartesian point types");
      return TypedValue(point_3d.y(), ctx_->memory);
    }
    if (prop_name == "z") {
      return TypedValue(point_3d.z(), ctx_->memory);
    }
    if (prop_name == "height") {
      if (!is_wgs) throw QueryRuntimeException("Use z instead of height for cartesian point types");
      return TypedValue(point_3d.z(), ctx_->memory);
    }
    if (prop_name == "crs") {
      return TypedValue(storage::CrsToString(point_3d.crs()), ctx_->memory);
    }
    if (prop_name == "srid") {
      return TypedValue(storage::CrsToSrid(point_3d.crs()).value_of(), ctx_->memory);
    }
    return std::nullopt;
  };
  auto maybe_graph = [this](const auto &graph, const auto &prop_name) -> std::optional<TypedValue> {
    if (prop_name == "nodes") {
      utils::pmr::vector<TypedValue> vertices(ctx_->memory);
      vertices.reserve(graph.vertices().size());
      for (const auto &v : graph.vertices()) {
        vertices.emplace_back(TypedValue(v, ctx_->memory));
      }
      return TypedValue(vertices, ctx_->memory);
    }
    if (prop_name == "edges") {
      utils::pmr::vector<TypedValue> edges(ctx_->memory);
      edges.reserve(graph.edges().size());
      for (const auto &e : graph.edges()) {
        edges.emplace_back(TypedValue(e, ctx_->memory));
      }
      return TypedValue(edges, ctx_->memory);
    }
    return std::nullopt;
  };
  switch (expression_result_ptr->type()) {
    case TypedValue::Type::Null:
      return TypedValue(ctx_->memory);
    case TypedValue::Type::Vertex:
      if (property_lookup.evaluation_mode_ == PropertyLookup::EvaluationMode::GET_ALL_PROPERTIES) {
        auto symbol_pos = static_cast<Identifier *>(property_lookup.expression_)->symbol_pos_;
        if (!property_lookup_cache_.contains(symbol_pos)) {
          property_lookup_cache_.emplace(symbol_pos, GetAllProperties(expression_result_ptr->ValueVertex()));
        }

        auto property_id = ctx_->properties[property_lookup.property_.ix];
        if (property_lookup_cache_[symbol_pos].contains(property_id)) {
          return {property_lookup_cache_[symbol_pos][property_id], GetNameIdMapper(), ctx_->memory};
        }
        return TypedValue(ctx_->memory);
      } else {
        return {GetProperty(expression_result_ptr->ValueVertex(), property_lookup.property_), GetNameIdMapper(),
                ctx_->memory};
      }
    case TypedValue::Type::Edge:
      if (property_lookup.evaluation_mode_ == PropertyLookup::EvaluationMode::GET_ALL_PROPERTIES) {
        auto symbol_pos = static_cast<Identifier *>(property_lookup.expression_)->symbol_pos_;
        if (!property_lookup_cache_.contains(symbol_pos)) {
          property_lookup_cache_.emplace(symbol_pos, GetAllProperties(expression_result_ptr->ValueEdge()));
        }

        auto property_id = ctx_->properties[property_lookup.property_.ix];
        if (property_lookup_cache_[symbol_pos].contains(property_id)) {
          return {property_lookup_cache_[symbol_pos][property_id], GetNameIdMapper(), ctx_->memory};
        }
        return TypedValue(ctx_->memory);
      } else {
        return {GetProperty(expression_result_ptr->ValueEdge(), property_lookup.property_), GetNameIdMapper(),
                ctx_->memory};
      }
    case TypedValue::Type::Map: {
      auto &map = expression_result_ptr->ValueMap();
      auto found = map.find(property_lookup.property_.name.c_str());
      if (found == map.end()) return TypedValue(ctx_->memory);
      return {found->second, ctx_->memory};
    }
    case TypedValue::Type::Duration: {
      const auto &prop_name = property_lookup.property_.name;
      const auto &dur = expression_result_ptr->ValueDuration();
      if (auto dur_field = maybe_duration(dur, prop_name); dur_field) {
        return {*dur_field, ctx_->memory};
      }
      throw QueryRuntimeException("Invalid property name {} for Duration", prop_name);
    }
    case TypedValue::Type::Date: {
      const auto &prop_name = property_lookup.property_.name;
      const auto &date = expression_result_ptr->ValueDate();
      if (auto date_field = maybe_date(date, prop_name); date_field) {
        return {*date_field, ctx_->memory};
      }
      throw QueryRuntimeException("Invalid property name {} for Date", prop_name);
    }
    case TypedValue::Type::LocalTime: {
      const auto &prop_name = property_lookup.property_.name;
      const auto &lt = expression_result_ptr->ValueLocalTime();
      if (auto lt_field = maybe_local_time(lt, prop_name); lt_field) {
        return std::move(*lt_field);
      }
      throw QueryRuntimeException("Invalid property name {} for LocalTime", prop_name);
    }
    case TypedValue::Type::LocalDateTime: {
      const auto &prop_name = property_lookup.property_.name;
      const auto &ldt = expression_result_ptr->ValueLocalDateTime();
      if (auto date_field = maybe_date(ldt.date(), prop_name); date_field) {
        return std::move(*date_field);
      }
      if (auto lt_field = maybe_local_time(ldt.local_time(), prop_name); lt_field) {
        return {*lt_field, ctx_->memory};
      }
      throw QueryRuntimeException("Invalid property name {} for LocalDateTime", prop_name);
    }
    case TypedValue::Type::ZonedDateTime: {
      const auto &prop_name = property_lookup.property_.name;
      const auto &zdt = expression_result_ptr->ValueZonedDateTime();
      if (auto zdt_field = maybe_zoned_date_time(zdt, prop_name); zdt_field) {
        return {*zdt_field, ctx_->memory};
      }
      throw QueryRuntimeException("Invalid property name {} for ZonedDateTime", prop_name);
    }
    case TypedValue::Type::Point2d: {
      const auto &prop_name = property_lookup.property_.name;
      const auto &point_2d = expression_result_ptr->ValuePoint2d();
      if (auto point_2d_field = maybe_point2d(point_2d, prop_name); point_2d_field) {
        return {*point_2d_field, ctx_->memory};
      }
      throw QueryRuntimeException("Invalid property name {} for Point2d", prop_name);
    }
    case TypedValue::Type::Point3d: {
      const auto &prop_name = property_lookup.property_.name;
      const auto &point_3d = expression_result_ptr->ValuePoint3d();
      if (auto point_3d_field = maybe_point3d(point_3d, prop_name); point_3d_field) {
        return {*point_3d_field, ctx_->memory};
      }
      throw QueryRuntimeException("Invalid property name {} for Point3d", prop_name);
    }
    case TypedValue::Type::Graph: {
      const auto &prop_name = property_lookup.property_.name;
      const auto &graph = expression_result_ptr->ValueGraph();
      if (auto graph_field = maybe_graph(graph, prop_name); graph_field) {
        return {*graph_field, ctx_->memory};
      }
      throw QueryRuntimeException("Invalid property name {} for Graph", prop_name);
    }
    default:
      throw QueryRuntimeException(
          "Only nodes, edges, maps, temporal types and graphs have properties to be looked up.");
  }
}
}  // namespace memgraph::query
