#pragma once

#include <map>
#include <string>
#include <vector>

#include "query/typed_value.hpp"
#include "storage/property_value.hpp"
#include "utils/algorithm.hpp"
#include "utils/assert.hpp"
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
    Edge
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

  DecodedValue &operator=(const DecodedValue &other);
  DecodedValue(const DecodedValue &other);
  ~DecodedValue();

  Type type() const { return type_; }

  // constant getters
  bool ValueBool() const;
  int64_t ValueInt() const;
  double ValueDouble() const;
  const std::string &ValueString() const;
  const std::vector<DecodedValue> &ValueList() const;
  const std::map<std::string, DecodedValue> &ValueMap() const;
  const DecodedVertex &ValueVertex() const;
  const DecodedEdge &ValueEdge() const;

  // getters
  bool &ValueBool();
  int64_t &ValueInt();
  double &ValueDouble();
  std::string &ValueString();
  std::vector<DecodedValue> &ValueList();
  std::map<std::string, DecodedValue> &ValueMap();
  DecodedVertex &ValueVertex();
  DecodedEdge &ValueEdge();

  // conversion function to TypedValue
  operator query::TypedValue() const;

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
std::ostream &operator<<(std::ostream &os, const DecodedValue &value);
}
