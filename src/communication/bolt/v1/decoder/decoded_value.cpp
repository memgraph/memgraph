#include "communication/bolt/v1/decoder/decoded_value.hpp"

namespace communication::bolt {

#define DEF_GETTER_BY_VAL(type, value_type, field)          \
  value_type DecodedValue::Value##type() const {            \
    if (type_ != Type::type) throw DecodedValueException(); \
    return field;                                           \
  }

DEF_GETTER_BY_VAL(Bool, bool, bool_v)
DEF_GETTER_BY_VAL(Int, int64_t, int_v)
DEF_GETTER_BY_VAL(Double, double, double_v)

#undef DEF_GETTER_BY_VAL

#define DEF_GETTER_BY_REF(type, value_type, field)          \
  value_type &DecodedValue::Value##type() {                 \
    if (type_ != Type::type) throw DecodedValueException(); \
    return field;                                           \
  }                                                         \
  const value_type &DecodedValue::Value##type() const {     \
    if (type_ != Type::type) throw DecodedValueException(); \
    return field;                                           \
  }

DEF_GETTER_BY_REF(String, std::string, string_v)
DEF_GETTER_BY_REF(List, std::vector<DecodedValue>, list_v)
using map_t = std::map<std::string, DecodedValue>;
DEF_GETTER_BY_REF(Map, map_t, map_v)
DEF_GETTER_BY_REF(Vertex, DecodedVertex, vertex_v)
DEF_GETTER_BY_REF(Edge, DecodedEdge, edge_v)
DEF_GETTER_BY_REF(UnboundedEdge, DecodedUnboundedEdge, unbounded_edge_v)
DEF_GETTER_BY_REF(Path, DecodedPath, path_v)

#undef DEF_GETTER_BY_REF

DecodedValue::DecodedValue(const DecodedValue &other) : type_(other.type_) {
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
      new (&list_v) std::vector<DecodedValue>(other.list_v);
      return;
    case Type::Map:
      new (&map_v) std::map<std::string, DecodedValue>(other.map_v);
      return;
    case Type::Vertex:
      new (&vertex_v) DecodedVertex(other.vertex_v);
      return;
    case Type::Edge:
      new (&edge_v) DecodedEdge(other.edge_v);
      return;
    case Type::UnboundedEdge:
      new (&unbounded_edge_v) DecodedUnboundedEdge(other.unbounded_edge_v);
      return;
    case Type::Path:
      new (&path_v) DecodedPath(other.path_v);
      return;
  }
  permanent_fail("Unsupported DecodedValue::Type");
}

DecodedValue &DecodedValue::operator=(const DecodedValue &other) {
  if (this != &other) {
    this->~DecodedValue();
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
        new (&list_v) std::vector<DecodedValue>(other.list_v);
        return *this;
      case Type::Map:
        new (&map_v) std::map<std::string, DecodedValue>(other.map_v);
        return *this;
      case Type::Vertex:
        new (&vertex_v) DecodedVertex(other.vertex_v);
        return *this;
      case Type::Edge:
        new (&edge_v) DecodedEdge(other.edge_v);
        return *this;
      case Type::UnboundedEdge:
        new (&unbounded_edge_v) DecodedUnboundedEdge(other.unbounded_edge_v);
        return *this;
      case Type::Path:
        new (&path_v) DecodedPath(other.path_v);
        return *this;
    }
    permanent_fail("Unsupported DecodedValue::Type");
  }
  return *this;
}

DecodedValue::~DecodedValue() {
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
      list_v.~vector<DecodedValue>();
      return;
    case Type::Map:
      using namespace std;
      map_v.~map<std::string, DecodedValue>();
      return;
    case Type::Vertex:
      vertex_v.~DecodedVertex();
      return;
    case Type::Edge:
      edge_v.~DecodedEdge();
      return;
    case Type::UnboundedEdge:
      unbounded_edge_v.~DecodedUnboundedEdge();
      return;
    case Type::Path:
      path_v.~DecodedPath();
      return;
  }
  permanent_fail("Unsupported DecodedValue::Type");
}

DecodedValue::operator query::TypedValue() const {
  switch (type_) {
    case Type::Null:
      return query::TypedValue::Null;
    case Type::Bool:
      return query::TypedValue(bool_v);
    case Type::Int:
      return query::TypedValue(int_v);
    case Type::Double:
      return query::TypedValue(double_v);
    case Type::String:
      return query::TypedValue(string_v);
    case Type::List:
      return query::TypedValue(
          std::vector<query::TypedValue>(list_v.begin(), list_v.end()));
    case Type::Map:
      return query::TypedValue(
          std::map<std::string, query::TypedValue>(map_v.begin(), map_v.end()));
    case Type::Vertex:
    case Type::Edge:
    case Type::UnboundedEdge:
    case Type::Path:
      throw DecodedValueException(
          "Unsupported conversion from DecodedValue to TypedValue");
  }
}

std::ostream &operator<<(std::ostream &os, const DecodedVertex &vertex) {
  os << "V(";
  PrintIterable(os, vertex.labels, ":",
                [&](auto &stream, auto label) { stream << label; });
  os << " {";
  PrintIterable(os, vertex.properties, ", ",
                [&](auto &stream, const auto &pair) {
                  stream << pair.first << ": " << pair.second;
                });
  return os << "})";
}

std::ostream &operator<<(std::ostream &os, const DecodedEdge &edge) {
  os << "E[" << edge.type;
  os << " {";
  PrintIterable(os, edge.properties, ", ", [&](auto &stream, const auto &pair) {
    stream << pair.first << ": " << pair.second;
  });
  return os << "}]";
}

std::ostream &operator<<(std::ostream &os, const DecodedUnboundedEdge &edge) {
  os << "E[" << edge.type;
  os << " {";
  PrintIterable(os, edge.properties, ", ", [&](auto &stream, const auto &pair) {
    stream << pair.first << ": " << pair.second;
  });
  return os << "}]";
}

std::ostream &operator<<(std::ostream &os, const DecodedPath &path) {
  os << path.vertices[0];
  debug_assert(path.indices.size() % 2 == 0,
               "Must have even number of indices");
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

std::ostream &operator<<(std::ostream &os, const DecodedValue &value) {
  switch (value.type_) {
    case DecodedValue::Type::Null:
      return os << "Null";
    case DecodedValue::Type::Bool:
      return os << (value.ValueBool() ? "true" : "false");
    case DecodedValue::Type::Int:
      return os << value.ValueInt();
    case DecodedValue::Type::Double:
      return os << value.ValueDouble();
    case DecodedValue::Type::String:
      return os << value.ValueString();
    case DecodedValue::Type::List:
      os << "[";
      PrintIterable(os, value.ValueList());
      return os << "]";
    case DecodedValue::Type::Map:
      os << "{";
      PrintIterable(os, value.ValueMap(), ", ",
                    [](auto &stream, const auto &pair) {
                      stream << pair.first << ": " << pair.second;
                    });
      return os << "}";
    case DecodedValue::Type::Vertex:
      return os << value.ValueVertex();
    case DecodedValue::Type::Edge:
      return os << value.ValueEdge();
    case DecodedValue::Type::UnboundedEdge:
      return os << value.ValueUnboundedEdge();
    case DecodedValue::Type::Path:
      return os << value.ValuePath();
  }
  permanent_fail("Unsupported DecodedValue::Type");
}

std::ostream &operator<<(std::ostream &os, const DecodedValue::Type type) {
  switch (type) {
    case DecodedValue::Type::Null:
      return os << "null";
    case DecodedValue::Type::Bool:
      return os << "bool";
    case DecodedValue::Type::Int:
      return os << "int";
    case DecodedValue::Type::Double:
      return os << "double";
    case DecodedValue::Type::String:
      return os << "string";
    case DecodedValue::Type::List:
      return os << "list";
    case DecodedValue::Type::Map:
      return os << "map";
    case DecodedValue::Type::Vertex:
      return os << "vertex";
    case DecodedValue::Type::Edge:
      return os << "edge";
    case DecodedValue::Type::UnboundedEdge:
      return os << "unbounded_edge";
    case DecodedValue::Type::Path:
      return os << "path";
  }
  permanent_fail("Unsupported DecodedValue::Type");
}
}
