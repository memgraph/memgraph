#pragma once

#include <map>
#include <string>
#include <vector>

#include "query/typed_value.hpp"
#include "storage/property_value.hpp"
#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"

namespace communication::bolt {

/** Forward declaration of DecodedValue class. */
class DecodedValue;

/**
 * Structure used when reading a Vertex with the decoder.
 * The decoder writes data into this structure.
 */
struct DecodedVertex {
  int64_t id;
  std::vector<std::string> labels;
  std::map<std::string, DecodedValue> properties;
};

/**
 * Structure used when reading an Edge with the decoder.
 * The decoder writes data into this structure.
 */
struct DecodedEdge {
  int64_t id;
  int64_t from;
  int64_t to;
  std::string type;
  std::map<std::string, DecodedValue> properties;
};

/**
 * Structure used when reading an UnboundEdge with the decoder.
 * The decoder writes data into this structure.
 */
struct DecodedUnboundedEdge {
  int64_t id;
  std::string type;
  std::map<std::string, DecodedValue> properties;
};

/**
 * Structure used when reading a Path with the decoder.
 * The decoder writes data into this structure.
 */
struct DecodedPath {
  std::vector<DecodedVertex> vertices;
  std::vector<DecodedUnboundedEdge> edges;
  std::vector<int64_t> indices;
};

/**
 * DecodedValue provides an encapsulation arround TypedValue, DecodedVertex
 * and DecodedEdge. This is necessary because TypedValue stores vertices and
 * edges as our internal accessors. Because of that the Bolt decoder can't
 * decode vertices and edges directly into a TypedValue so a DecodedValue is
 * used instead.
 */
class DecodedValue {
 public:
  /** Default constructor, makes Null */
  DecodedValue() : type_(Type::Null) {}

  /** Types that can be stored in a DecodedValue. */
  // TODO: Path isn't supported yet!
  enum class Type : unsigned {
    Null,
    Bool,
    Int,
    Double,
    String,
    List,
    Map,
    Vertex,
    Edge,
    UnboundedEdge,
    Path
  };

  // constructors for primitive types
  DecodedValue(bool value) : type_(Type::Bool) { bool_v = value; }
  DecodedValue(int value) : type_(Type::Int) { int_v = value; }
  DecodedValue(int64_t value) : type_(Type::Int) { int_v = value; }
  DecodedValue(double value) : type_(Type::Double) { double_v = value; }

  // constructors for non-primitive types
  DecodedValue(const std::string &value) : type_(Type::String) {
    new (&string_v) std::string(value);
  }
  DecodedValue(const std::vector<DecodedValue> &value) : type_(Type::List) {
    new (&list_v) std::vector<DecodedValue>(value);
  }
  DecodedValue(const std::map<std::string, DecodedValue> &value)
      : type_(Type::Map) {
    new (&map_v) std::map<std::string, DecodedValue>(value);
  }
  DecodedValue(const DecodedVertex &value) : type_(Type::Vertex) {
    new (&vertex_v) DecodedVertex(value);
  }
  DecodedValue(const DecodedEdge &value) : type_(Type::Edge) {
    new (&edge_v) DecodedEdge(value);
  }
  DecodedValue(const DecodedUnboundedEdge &value) : type_(Type::UnboundedEdge) {
    new (&unbounded_edge_v) DecodedUnboundedEdge(value);
  }
  DecodedValue(const DecodedPath &value) : type_(Type::Path) {
    new (&path_v) DecodedPath(value);
  }

  DecodedValue &operator=(const DecodedValue &other);
  DecodedValue(const DecodedValue &other);
  ~DecodedValue();

  Type type() const { return type_; }

#define DECL_GETTER_BY_VALUE(type, value_type) \
  value_type &Value##type();                   \
  value_type Value##type() const;

  DECL_GETTER_BY_VALUE(Bool, bool)
  DECL_GETTER_BY_VALUE(Int, int64_t)
  DECL_GETTER_BY_VALUE(Double, double)

#undef DECL_GETTER_BY_VALUE

#define DECL_GETTER_BY_REFERENCE(type, value_type) \
  value_type &Value##type();                       \
  const value_type &Value##type() const;

  DECL_GETTER_BY_REFERENCE(String, std::string)
  DECL_GETTER_BY_REFERENCE(List, std::vector<DecodedValue>)
  using map_t = std::map<std::string, DecodedValue>;
  DECL_GETTER_BY_REFERENCE(Map, map_t)
  DECL_GETTER_BY_REFERENCE(Vertex, DecodedVertex)
  DECL_GETTER_BY_REFERENCE(Edge, DecodedEdge)
  DECL_GETTER_BY_REFERENCE(UnboundedEdge, DecodedUnboundedEdge)
  DECL_GETTER_BY_REFERENCE(Path, DecodedPath)

#undef DECL_GETTER_BY_REFERNCE

#define TYPE_CHECKER(type) \
  bool Is##type() const { return type_ == Type::type; }

  TYPE_CHECKER(Bool)
  TYPE_CHECKER(Int)
  TYPE_CHECKER(Double)
  TYPE_CHECKER(String)
  TYPE_CHECKER(List)
  TYPE_CHECKER(Map)
  TYPE_CHECKER(Vertex)
  TYPE_CHECKER(Edge)
  TYPE_CHECKER(UnboundedEdge)
  TYPE_CHECKER(Path)

#undef TYPE_CHECKER

  operator query::TypedValue() const;
  // PropertyValue operator must be explicit to prevent ambiguity.
  explicit operator PropertyValue() const;

  friend std::ostream &operator<<(std::ostream &os, const DecodedValue &value);

 private:
  Type type_;

  // storage for the value of the property
  union {
    bool bool_v;
    int64_t int_v;
    double double_v;
    std::string string_v;
    std::vector<DecodedValue> list_v;
    std::map<std::string, DecodedValue> map_v;
    DecodedVertex vertex_v;
    DecodedEdge edge_v;
    DecodedUnboundedEdge unbounded_edge_v;
    DecodedPath path_v;
  };
};

/**
 * An exception raised by the DecodedValue system.
 */
class DecodedValueException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  DecodedValueException()
      : BasicException("Incompatible template param and type!") {}
};

/**
 * Output operators.
 */
std::ostream &operator<<(std::ostream &os, const DecodedVertex &vertex);
std::ostream &operator<<(std::ostream &os, const DecodedEdge &edge);
std::ostream &operator<<(std::ostream &os, const DecodedUnboundedEdge &edge);
std::ostream &operator<<(std::ostream &os, const DecodedPath &path);
std::ostream &operator<<(std::ostream &os, const DecodedValue &value);
std::ostream &operator<<(std::ostream &os, const DecodedValue::Type type);
}  // namespace communication::bolt
