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

#include <sstream>
#include <string>

#include "query/typed_value.hpp"
#include "utils/algorithm.hpp"
#include "utils/temporal.hpp"

/// Functions that convert types to a `std::string` representation of it. The
/// `TAccessor` supplied must have the functions `NameToLabel`, `LabelToName`,
/// `NameToProperty`, `PropertyToName`, `NameToEdgeType` and `EdgeTypeToName`.
/// For example, both `memgraph::storage::Storage` and `Storage::Accessor` will
/// be appropriate.

template <class TAccessor>
inline std::string ToString(const memgraph::query::VertexAccessor &vertex, const TAccessor &acc) {
  std::ostringstream os;
  os << "V(";
  auto maybe_labels = vertex.Labels(memgraph::storage::View::NEW);
  MG_ASSERT(maybe_labels.HasValue());
  memgraph::utils::PrintIterable(os, *maybe_labels, ":",
                                 [&](auto &stream, auto label) { stream << acc.LabelToName(label); });
  if (maybe_labels->size() > 0) os << " ";
  os << "{";
  auto maybe_properties = vertex.Properties(memgraph::storage::View::NEW);
  MG_ASSERT(maybe_properties.HasValue());
  memgraph::utils::PrintIterable(os, *maybe_properties, ", ", [&](auto &stream, const auto &pair) {
    stream << acc.PropertyToName(pair.first) << ": " << pair.second;
  });
  os << "})";
  return os.str();
}

template <class TAccessor>
inline std::string ToString(const memgraph::query::EdgeAccessor &edge, const TAccessor &acc) {
  std::ostringstream os;
  os << "E[" << acc.EdgeTypeToName(edge.EdgeType());
  os << " {";
  auto maybe_properties = edge.Properties(memgraph::storage::View::NEW);
  MG_ASSERT(maybe_properties.HasValue());
  memgraph::utils::PrintIterable(os, *maybe_properties, ", ", [&](auto &stream, const auto &pair) {
    stream << acc.PropertyToName(pair.first) << ": " << pair.second;
  });
  os << "}]";
  return os.str();
}

template <class TAccessor>
inline std::string ToString(const memgraph::query::Path &path, const TAccessor &acc) {
  std::ostringstream os;
  const auto &vertices = path.vertices();
  const auto &edges = path.edges();
  MG_ASSERT(vertices.empty(), "Attempting to stream out an invalid path");
  os << ToString(vertices[0], acc);
  for (size_t i = 0; i < edges.size(); ++i) {
    bool arrow_to_left = vertices[i] == edges[i].To();
    if (arrow_to_left) os << "<";
    os << "-" << ToString(edges[i], acc) << "-";
    if (!arrow_to_left) os << ">";
    os << ToString(vertices[i + 1], acc);
  }
  return os.str();
}

// TODO(antonio2368): Define printing of dates
inline std::string ToString(const memgraph::utils::Date) { return ""; }

inline std::string ToString(const memgraph::utils::LocalTime) { return ""; }

inline std::string ToString(const memgraph::utils::LocalDateTime) { return ""; }

inline std::string ToString(const memgraph::utils::Duration) { return ""; }

template <class TAccessor>
inline std::string ToString(const memgraph::query::TypedValue &value, const TAccessor &acc) {
  std::ostringstream os;
  switch (value.type()) {
    case memgraph::query::TypedValue::Type::Null:
      os << "null";
      break;
    case memgraph::query::TypedValue::Type::Bool:
      os << (value.ValueBool() ? "true" : "false");
      break;
    case memgraph::query::TypedValue::Type::Int:
      os << value.ValueInt();
      break;
    case memgraph::query::TypedValue::Type::Double:
      os << value.ValueDouble();
      break;
    case memgraph::query::TypedValue::Type::String:
      os << value.ValueString();
      break;
    case memgraph::query::TypedValue::Type::List:
      os << "[";
      memgraph::utils::PrintIterable(os, value.ValueList(), ", ",
                                     [&](auto &stream, const auto &item) { stream << ToString(item, acc); });
      os << "]";
      break;
    case memgraph::query::TypedValue::Type::Map:
      os << "{";
      memgraph::utils::PrintIterable(os, value.ValueMap(), ", ", [&](auto &stream, const auto &pair) {
        stream << pair.first << ": " << ToString(pair.second, acc);
      });
      os << "}";
      break;
    case memgraph::query::TypedValue::Type::Vertex:
      os << ToString(value.ValueVertex(), acc);
      break;
    case memgraph::query::TypedValue::Type::Edge:
      os << ToString(value.ValueEdge(), acc);
      break;
    case memgraph::query::TypedValue::Type::Path:
      os << ToString(value.ValuePath(), acc);
      break;
    case memgraph::query::TypedValue::Type::Date:
      os << ToString(value.ValueDate());
      break;
    case memgraph::query::TypedValue::Type::LocalTime:
      os << ToString(value.ValueLocalTime());
      break;
    case memgraph::query::TypedValue::Type::LocalDateTime:
      os << ToString(value.ValueLocalDateTime());
      break;
    case memgraph::query::TypedValue::Type::Duration:
      os << ToString(value.ValueDuration());
      break;
    case memgraph::query::TypedValue::Type::Graph:
      throw std::logic_error{"Not implemented"};
  }
  return os.str();
}
