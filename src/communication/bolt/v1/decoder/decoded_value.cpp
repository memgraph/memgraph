#include "communication/bolt/v1/decoder/decoded_value.hpp"

namespace communication::bolt {

bool DecodedValue::ValueBool() const {
  if (type_ != Type::Bool) {
    throw DecodedValueException();
  }
  return bool_v;
}

int64_t DecodedValue::ValueInt() const {
  if (type_ != Type::Int) {
    throw DecodedValueException();
  }
  return int_v;
}

double DecodedValue::ValueDouble() const {
  if (type_ != Type::Double) {
    throw DecodedValueException();
  }
  return double_v;
}

const std::string &DecodedValue::ValueString() const {
  if (type_ != Type::String) {
    throw DecodedValueException();
  }
  return string_v;
}

const std::vector<DecodedValue> &DecodedValue::ValueList() const {
  if (type_ != Type::List) {
    throw DecodedValueException();
  }
  return list_v;
}

const std::map<std::string, DecodedValue> &DecodedValue::ValueMap() const {
  if (type_ != Type::Map) {
    throw DecodedValueException();
  }
  return map_v;
}

const DecodedVertex &DecodedValue::ValueVertex() const {
  if (type_ != Type::Vertex) {
    throw DecodedValueException();
  }
  return vertex_v;
}

const DecodedEdge &DecodedValue::ValueEdge() const {
  if (type_ != Type::Edge) {
    throw DecodedValueException();
  }
  return edge_v;
}

bool &DecodedValue::ValueBool() {
  if (type_ != Type::Bool) {
    throw DecodedValueException();
  }
  return bool_v;
}

int64_t &DecodedValue::ValueInt() {
  if (type_ != Type::Int) {
    throw DecodedValueException();
  }
  return int_v;
}

double &DecodedValue::ValueDouble() {
  if (type_ != Type::Double) {
    throw DecodedValueException();
  }
  return double_v;
}

std::string &DecodedValue::ValueString() {
  if (type_ != Type::String) {
    throw DecodedValueException();
  }
  return string_v;
}

std::vector<DecodedValue> &DecodedValue::ValueList() {
  if (type_ != Type::List) {
    throw DecodedValueException();
  }
  return list_v;
}

std::map<std::string, DecodedValue> &DecodedValue::ValueMap() {
  if (type_ != Type::Map) {
    throw DecodedValueException();
  }
  return map_v;
}

DecodedVertex &DecodedValue::ValueVertex() {
  if (type_ != Type::Vertex) {
    throw DecodedValueException();
  }
  return vertex_v;
}

DecodedEdge &DecodedValue::ValueEdge() {
  if (type_ != Type::Edge) {
    throw DecodedValueException();
  }
  return edge_v;
}

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
    default:
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
  }
  permanent_fail("Unsupported DecodedValue::Type");
}
}
