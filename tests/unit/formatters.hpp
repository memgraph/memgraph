#pragma once

#include <sstream>
#include <string>

#include "query/temporal.hpp"
#include "query/typed_value.hpp"
#include "utils/algorithm.hpp"

/// Functions that convert types to a `std::string` representation of it. The
/// `TAccessor` supplied must have the functions `NameToLabel`, `LabelToName`,
/// `NameToProperty`, `PropertyToName`, `NameToEdgeType` and `EdgeTypeToName`.
/// For example, both `storage::Storage` and `storage::Storage::Accessor` will
/// be apropriate.

template <class TAccessor>
inline std::string ToString(const query::VertexAccessor &vertex, const TAccessor &acc) {
  std::ostringstream os;
  os << "V(";
  auto maybe_labels = vertex.Labels(storage::View::NEW);
  MG_ASSERT(maybe_labels.HasValue());
  utils::PrintIterable(os, *maybe_labels, ":", [&](auto &stream, auto label) { stream << acc.LabelToName(label); });
  if (maybe_labels->size() > 0) os << " ";
  os << "{";
  auto maybe_properties = vertex.Properties(storage::View::NEW);
  MG_ASSERT(maybe_properties.HasValue());
  utils::PrintIterable(os, *maybe_properties, ", ", [&](auto &stream, const auto &pair) {
    stream << acc.PropertyToName(pair.first) << ": " << pair.second;
  });
  os << "})";
  return os.str();
}

template <class TAccessor>
inline std::string ToString(const query::EdgeAccessor &edge, const TAccessor &acc) {
  std::ostringstream os;
  os << "E[" << acc.EdgeTypeToName(edge.EdgeType());
  os << " {";
  auto maybe_properties = edge.Properties(storage::View::NEW);
  MG_ASSERT(maybe_properties.HasValue());
  utils::PrintIterable(os, *maybe_properties, ", ", [&](auto &stream, const auto &pair) {
    stream << acc.PropertyToName(pair.first) << ": " << pair.second;
  });
  os << "}]";
  return os.str();
}

template <class TAccessor>
inline std::string ToString(const query::Path &path, const TAccessor &acc) {
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
inline std::string ToString(const query::Date) { return ""; }

inline std::string ToString(const query::LocalTime) { return ""; }

inline std::string ToString(const query::LocalDateTime) { return ""; }

inline std::string ToString(const query::Duration) { return ""; }

template <class TAccessor>
inline std::string ToString(const query::TypedValue &value, const TAccessor &acc) {
  std::ostringstream os;
  switch (value.type()) {
    case query::TypedValue::Type::Null:
      os << "null";
      break;
    case query::TypedValue::Type::Bool:
      os << (value.ValueBool() ? "true" : "false");
      break;
    case query::TypedValue::Type::Int:
      os << value.ValueInt();
      break;
    case query::TypedValue::Type::Double:
      os << value.ValueDouble();
      break;
    case query::TypedValue::Type::String:
      os << value.ValueString();
      break;
    case query::TypedValue::Type::List:
      os << "[";
      utils::PrintIterable(os, value.ValueList(), ", ",
                           [&](auto &stream, const auto &item) { stream << ToString(item, acc); });
      os << "]";
      break;
    case query::TypedValue::Type::Map:
      os << "{";
      utils::PrintIterable(os, value.ValueMap(), ", ", [&](auto &stream, const auto &pair) {
        stream << pair.first << ": " << ToString(pair.second, acc);
      });
      os << "}";
      break;
    case query::TypedValue::Type::Vertex:
      os << ToString(value.ValueVertex(), acc);
      break;
    case query::TypedValue::Type::Edge:
      os << ToString(value.ValueEdge(), acc);
      break;
    case query::TypedValue::Type::Path:
      os << ToString(value.ValuePath(), acc);
      break;
    case query::TypedValue::Type::Date:
      os << ToString(value.ValueDate());
      break;
    case query::TypedValue::Type::LocalTime:
      os << ToString(value.ValueLocalTime());
      break;
    case query::TypedValue::Type::LocalDateTime:
      os << ToString(value.ValueLocalDateTime());
      break;
    case query::TypedValue::Type::Duration:
      os << ToString(value.ValueDuration());
      break;
  }
  return os.str();
}
