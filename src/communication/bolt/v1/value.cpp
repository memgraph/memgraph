// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "communication/bolt/v1/value.hpp"

#include "utils/algorithm.hpp"
#include "utils/string.hpp"

namespace memgraph::communication::bolt {

#define DEF_GETTER_BY_VAL(type, value_type, field)   \
  value_type &Value::Value##type() {                 \
    if (type_ != Type::type) throw ValueException(); \
    return field;                                    \
  }                                                  \
  value_type Value::Value##type() const {            \
    if (type_ != Type::type) throw ValueException(); \
    return field;                                    \
  }

DEF_GETTER_BY_VAL(Bool, bool, bool_v)
DEF_GETTER_BY_VAL(Int, int64_t, int_v)
DEF_GETTER_BY_VAL(Double, double, double_v)

#undef DEF_GETTER_BY_VAL

#define DEF_GETTER_BY_REF(type, value_type, field)   \
  value_type &Value::Value##type() {                 \
    if (type_ != Type::type) throw ValueException(); \
    return field;                                    \
  }                                                  \
  const value_type &Value::Value##type() const {     \
    if (type_ != Type::type) throw ValueException(); \
    return field;                                    \
  }

DEF_GETTER_BY_REF(String, std::string, string_v)
DEF_GETTER_BY_REF(List, std::vector<Value>, list_v)
using map_t = std::map<std::string, Value>;
DEF_GETTER_BY_REF(Map, map_t, map_v)
DEF_GETTER_BY_REF(Vertex, Vertex, vertex_v)
DEF_GETTER_BY_REF(Edge, Edge, edge_v)
DEF_GETTER_BY_REF(UnboundedEdge, UnboundedEdge, unbounded_edge_v)
DEF_GETTER_BY_REF(Path, Path, path_v)
DEF_GETTER_BY_REF(Date, utils::Date, date_v)
DEF_GETTER_BY_REF(LocalTime, utils::LocalTime, local_time_v)
DEF_GETTER_BY_REF(LocalDateTime, utils::LocalDateTime, local_date_time_v)
DEF_GETTER_BY_REF(Duration, utils::Duration, duration_v)

#undef DEF_GETTER_BY_REF

Value::Value(const Value &other) : type_(other.type_) {
  switch (other.type_) {
    case Type::Null:
      return;
    case Type::Bool:
      this->bool_v = other.bool_v;
      return;
    case Type::Int:
      this->int_v = other.int_v;
      return;
    case Type::Double:
      this->double_v = other.double_v;
      return;
    case Type::String:
      new (&string_v) std::string(other.string_v);
      return;
    case Type::List:
      new (&list_v) std::vector<Value>(other.list_v);
      return;
    case Type::Map:
      new (&map_v) std::map<std::string, Value>(other.map_v);
      return;
    case Type::Vertex:
      new (&vertex_v) Vertex(other.vertex_v);
      return;
    case Type::Edge:
      new (&edge_v) Edge(other.edge_v);
      return;
    case Type::UnboundedEdge:
      new (&unbounded_edge_v) UnboundedEdge(other.unbounded_edge_v);
      return;
    case Type::Path:
      new (&path_v) Path(other.path_v);
      return;
    case Type::Date:
      new (&date_v) utils::Date(other.date_v);
      return;
    case Type::LocalTime:
      new (&local_time_v) utils::LocalTime(other.local_time_v);
      return;
    case Type::LocalDateTime:
      new (&local_date_time_v) utils::LocalDateTime(other.local_date_time_v);
      return;
    case Type::Duration:
      new (&duration_v) utils::Duration(other.duration_v);
      return;
  }
}

Value &Value::operator=(const Value &other) {
  if (this != &other) {
    this->~Value();
    // set the type of this
    type_ = other.type_;

    switch (other.type_) {
      case Type::Null:
        return *this;
      case Type::Bool:
        this->bool_v = other.bool_v;
        return *this;
      case Type::Int:
        this->int_v = other.int_v;
        return *this;
      case Type::Double:
        this->double_v = other.double_v;
        return *this;
      case Type::String:
        new (&string_v) std::string(other.string_v);
        return *this;
      case Type::List:
        new (&list_v) std::vector<Value>(other.list_v);
        return *this;
      case Type::Map:
        new (&map_v) std::map<std::string, Value>(other.map_v);
        return *this;
      case Type::Vertex:
        new (&vertex_v) Vertex(other.vertex_v);
        return *this;
      case Type::Edge:
        new (&edge_v) Edge(other.edge_v);
        return *this;
      case Type::UnboundedEdge:
        new (&unbounded_edge_v) UnboundedEdge(other.unbounded_edge_v);
        return *this;
      case Type::Path:
        new (&path_v) Path(other.path_v);
        return *this;
      case Type::Date:
        new (&date_v) utils::Date(other.date_v);
        return *this;
      case Type::LocalTime:
        new (&local_time_v) utils::LocalTime(other.local_time_v);
        return *this;
      case Type::LocalDateTime:
        new (&local_date_time_v) utils::LocalDateTime(other.local_date_time_v);
        return *this;
      case Type::Duration:
        new (&duration_v) utils::Duration(other.duration_v);
        return *this;
    }
  }
  return *this;
}

Value::Value(Value &&other) noexcept : type_(other.type_) {
  switch (other.type_) {
    case Type::Null:
      break;
    case Type::Bool:
      this->bool_v = other.bool_v;
      break;
    case Type::Int:
      this->int_v = other.int_v;
      break;
    case Type::Double:
      this->double_v = other.double_v;
      break;
    case Type::String:
      new (&string_v) std::string(std::move(other.string_v));
      break;
    case Type::List:
      new (&list_v) std::vector<Value>(std::move(other.list_v));
      break;
    case Type::Map:
      new (&map_v) std::map<std::string, Value>(std::move(other.map_v));
      break;
    case Type::Vertex:
      new (&vertex_v) Vertex(std::move(other.vertex_v));
      break;
    case Type::Edge:
      new (&edge_v) Edge(std::move(other.edge_v));
      break;
    case Type::UnboundedEdge:
      new (&unbounded_edge_v) UnboundedEdge(std::move(other.unbounded_edge_v));
      break;
    case Type::Path:
      new (&path_v) Path(std::move(other.path_v));
      break;
    case Type::Date:
      new (&date_v) utils::Date(other.date_v);
      break;
    case Type::LocalTime:
      new (&local_time_v) utils::LocalTime(other.local_time_v);
      break;
    case Type::LocalDateTime:
      new (&local_date_time_v) utils::LocalDateTime(other.local_date_time_v);
      break;
    case Type::Duration:
      new (&duration_v) utils::Duration(other.duration_v);
      break;
  }

  // reset the type of other
  other.~Value();
  other.type_ = Type::Null;
}

Value &Value::operator=(Value &&other) noexcept {
  if (this != &other) {
    this->~Value();
    // set the type of this
    type_ = other.type_;

    switch (other.type_) {
      case Type::Null:
        break;
      case Type::Bool:
        this->bool_v = other.bool_v;
        break;
      case Type::Int:
        this->int_v = other.int_v;
        break;
      case Type::Double:
        this->double_v = other.double_v;
        break;
      case Type::String:
        new (&string_v) std::string(std::move(other.string_v));
        break;
      case Type::List:
        new (&list_v) std::vector<Value>(std::move(other.list_v));
        break;
      case Type::Map:
        new (&map_v) std::map<std::string, Value>(std::move(other.map_v));
        break;
      case Type::Vertex:
        new (&vertex_v) Vertex(std::move(other.vertex_v));
        break;
      case Type::Edge:
        new (&edge_v) Edge(std::move(other.edge_v));
        break;
      case Type::UnboundedEdge:
        new (&unbounded_edge_v) UnboundedEdge(std::move(other.unbounded_edge_v));
        break;
      case Type::Path:
        new (&path_v) Path(std::move(other.path_v));
        break;
      case Type::Date:
        new (&date_v) utils::Date(other.date_v);
        break;
      case Type::LocalTime:
        new (&local_time_v) utils::LocalTime(other.local_time_v);
        break;
      case Type::LocalDateTime:
        new (&local_date_time_v) utils::LocalDateTime(other.local_date_time_v);
        break;
      case Type::Duration:
        new (&duration_v) utils::Duration(other.duration_v);
        break;
    }

    // reset the type of other
    other.~Value();
    other.type_ = Type::Null;
  }
  return *this;
}

Value::~Value() {
  switch (type_) {
    // destructor for primitive types does nothing
    case Type::Null:
    case Type::Bool:
    case Type::Int:
    case Type::Double:
      return;

    // we need to call destructors for non primitive types since we used
    // placement new
    case Type::String:
      // Clang fails to compile ~std::string. It seems it is a bug in some
      // versions of clang. using namespace std statement solves the issue.
      using namespace std;
      string_v.~string();
      return;
    case Type::List:
      using namespace std;
      list_v.~vector<Value>();
      return;
    case Type::Map:
      using namespace std;
      map_v.~map<std::string, Value>();
      return;
    case Type::Vertex:
      vertex_v.~Vertex();
      return;
    case Type::Edge:
      edge_v.~Edge();
      return;
    case Type::UnboundedEdge:
      unbounded_edge_v.~UnboundedEdge();
      return;
    case Type::Path:
      path_v.~Path();
      return;
    case Type::Date:
      date_v.~Date();
      return;
    case Type::LocalTime:
      local_time_v.~LocalTime();
      return;
    case Type::LocalDateTime:
      local_date_time_v.~LocalDateTime();
      return;
    case Type::Duration:
      duration_v.~Duration();
      return;
  }
}

std::ostream &operator<<(std::ostream &os, const Vertex &vertex) {
  os << "(";
  if (vertex.labels.size() > 0) {
    os << ":";
  }
  utils::PrintIterable(os, vertex.labels, ":", [&](auto &stream, auto label) { stream << label; });
  if (vertex.labels.size() > 0 && vertex.properties.size() > 0) {
    os << " ";
  }
  if (vertex.properties.size() > 0) {
    os << "{";
    utils::PrintIterable(os, vertex.properties, ", ",
                         [&](auto &stream, const auto &pair) { stream << pair.first << ": " << pair.second; });
    os << "}";
  }
  if (!vertex.element_id.empty()) {
    os << " element_id: " << vertex.element_id;
  }
  return os << ")";
}

std::ostream &operator<<(std::ostream &os, const Edge &edge) {
  os << "[" << edge.type;
  if (edge.properties.size() > 0) {
    os << " {";
    utils::PrintIterable(os, edge.properties, ", ",
                         [&](auto &stream, const auto &pair) { stream << pair.first << ": " << pair.second; });
    os << "}";
  }
  return os << "]";
}

std::ostream &operator<<(std::ostream &os, const UnboundedEdge &edge) {
  os << "[" << edge.type;
  if (edge.properties.size() > 0) {
    os << " {";
    utils::PrintIterable(os, edge.properties, ", ",
                         [&](auto &stream, const auto &pair) { stream << pair.first << ": " << pair.second; });
    os << "}";
  }
  return os << "]";
}

std::ostream &operator<<(std::ostream &os, const Path &path) {
  os << path.vertices[0];
  for (auto it = path.indices.begin(); it != path.indices.end();) {
    auto edge_ind = *it++;
    auto vertex_ind = *it++;
    bool arrow_to_right = true;
    if (edge_ind < 0) {
      arrow_to_right = false;
      edge_ind = -edge_ind;
    }

    if (!arrow_to_right) os << "<";
    os << "-" << path.edges[edge_ind - 1] << "-";
    if (arrow_to_right) os << ">";
    os << path.vertices[vertex_ind];
  }

  return os;
}

std::ostream &operator<<(std::ostream &os, const Value &value) {
  switch (value.type_) {
    case Value::Type::Null:
      return os << "Null";
    case Value::Type::Bool:
      return os << (value.ValueBool() ? "true" : "false");
    case Value::Type::Int:
      return os << value.ValueInt();
    case Value::Type::Double:
      return os << value.ValueDouble();
    case Value::Type::String:
      return os << utils::Escape(value.ValueString());
    case Value::Type::List:
      os << "[";
      utils::PrintIterable(os, value.ValueList());
      return os << "]";
    case Value::Type::Map:
      os << "{";
      utils::PrintIterable(os, value.ValueMap(), ", ",
                           [](auto &stream, const auto &pair) { stream << pair.first << ": " << pair.second; });
      return os << "}";
    case Value::Type::Vertex:
      return os << value.ValueVertex();
    case Value::Type::Edge:
      return os << value.ValueEdge();
    case Value::Type::UnboundedEdge:
      return os << value.ValueUnboundedEdge();
    case Value::Type::Path:
      return os << value.ValuePath();
    case Value::Type::Date:
      return os << value.ValueDate();
    case Value::Type::LocalTime:
      return os << value.ValueLocalTime();
    case Value::Type::LocalDateTime:
      return os << value.ValueLocalDateTime();
    case Value::Type::Duration:
      return os << value.ValueDuration();
  }
}

std::ostream &operator<<(std::ostream &os, const Value::Type type) {
  switch (type) {
    case Value::Type::Null:
      return os << "null";
    case Value::Type::Bool:
      return os << "bool";
    case Value::Type::Int:
      return os << "int";
    case Value::Type::Double:
      return os << "double";
    case Value::Type::String:
      return os << "string";
    case Value::Type::List:
      return os << "list";
    case Value::Type::Map:
      return os << "map";
    case Value::Type::Vertex:
      return os << "vertex";
    case Value::Type::Edge:
      return os << "edge";
    case Value::Type::UnboundedEdge:
      return os << "unbounded_edge";
    case Value::Type::Path:
      return os << "path";
    case Value::Type::Date:
      return os << "date";
    case Value::Type::LocalTime:
      return os << "local_time";
    case Value::Type::LocalDateTime:
      return os << "local_date_time";
    case Value::Type::Duration:
      return os << "duration";
  }
}
}  // namespace memgraph::communication::bolt
