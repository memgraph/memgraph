#pragma once

#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "storage/edge_accessor.hpp"
#include "storage/property_value.hpp"
#include "storage/vertex_accessor.hpp"
#include "traversal/path.hpp"
#include "utils/exceptions/stacktrace_exception.hpp"
#include "utils/total_ordering.hpp"
#include "utils/underlying_cast.hpp"

namespace query {

typedef traversal_template::Path<VertexAccessor, EdgeAccessor> Path;

// TODO: Neo4j does overflow checking. Should we also implement it?
/**
 * Encapsulation of a value and it's type encapsulated in a class that has no
 * compiled-time info about that type.
 *
 * Values can be of a number of predefined types that are enumerated in
 * TypedValue::Type. Each such type corresponds to exactly one C++ type.
 */
class TypedValue : public TotalOrdering<TypedValue, TypedValue, TypedValue> {
 public:
  /** Private default constructor, makes Null */
  TypedValue() : type_(Type::Null) {}

 public:
  /** A value type. Each type corresponds to exactly one C++ type */
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
    Path
  };

  // single static reference to Null, used whenever Null should be returned
  static const TypedValue Null;

  // constructors for primitive types
  TypedValue(bool value) : type_(Type::Bool) { bool_v = value; }
  TypedValue(int value) : type_(Type::Int) { int_v = value; }
  TypedValue(int64_t value) : type_(Type::Int) { int_v = value; }
  TypedValue(double value) : type_(Type::Double) { double_v = value; }

  // conversion function to PropertyValue
  operator PropertyValue() const;

  /// constructors for non-primitive types
  TypedValue(const std::string &value) : type_(Type::String) {
    new (&string_v) std::string(value);
  }
  TypedValue(const char *value) : type_(Type::String) {
    new (&string_v) std::string(value);
  }
  TypedValue(const std::vector<TypedValue> &value) : type_(Type::List) {
    new (&list_v) std::vector<TypedValue>(value);
  }
  TypedValue(const std::map<std::string, TypedValue> &value)
      : type_(Type::Map) {
    new (&map_v) std::map<std::string, TypedValue>(value);
  }
  TypedValue(const VertexAccessor &vertex) : type_(Type::Vertex) {
    new (&vertex_v) VertexAccessor(vertex);
  }
  TypedValue(const EdgeAccessor &edge) : type_(Type::Edge) {
    new (&edge_v) EdgeAccessor(edge);
  }
  TypedValue(const Path &path) : type_(Type::Path) { new (&path_v) Path(path); }
  TypedValue(const PropertyValue &value);

  // assignment ops
  TypedValue &operator=(const TypedValue &other);

  TypedValue(const TypedValue &other);
  ~TypedValue();

  Type type() const { return type_; }
  /**
   * Returns the value of the property as given type T.
   * The behavior of this function is undefined if
   * T does not correspond to this property's type_.
   *
   * @tparam T Type to interpret the value as.
   * @return The value as type T.
   */
  template <typename T>
  T &Value();
  template <typename T>
  const T &Value() const;

  friend std::ostream &operator<<(std::ostream &stream, const TypedValue &prop);

 private:
  // storage for the value of the property
  union {
    bool bool_v;
    int64_t int_v;
    double double_v;
    // Since this is used in query runtime, size of union is not critical so
    // string and vector are used instead of pointers. It requires copy of data,
    // but most of algorithms (concatenations, serialisation...) has linear time
    // complexity so it shouldn't be a problem. This is maybe even faster
    // because of data locality.
    std::string string_v;
    std::vector<TypedValue> list_v;
    // clang doesn't allow unordered_map to have incomplete type as value so we
    // we use map.
    std::map<std::string, TypedValue> map_v;
    VertexAccessor vertex_v;
    EdgeAccessor edge_v;
    Path path_v;
  };

  /**
   * The Type of property.
   */
  Type type_;
};

/**
 * An exception raised by the TypedValue system. Typically when
 * trying to perform operations (such as addition) on TypedValues
 * of incompatible Types.
 */
class TypedValueException : public StacktraceException {
 public:
  using ::StacktraceException::StacktraceException;
};

// comparison operators
// they return TypedValue because Null can be returned
TypedValue operator==(const TypedValue &a, const TypedValue &b);
TypedValue operator<(const TypedValue &a, const TypedValue &b);
TypedValue operator!(const TypedValue &a);

// arithmetic operators
TypedValue operator-(const TypedValue &a);
TypedValue operator+(const TypedValue &a);
TypedValue operator+(const TypedValue &a, const TypedValue &b);
TypedValue operator-(const TypedValue &a, const TypedValue &b);
TypedValue operator/(const TypedValue &a, const TypedValue &b);
TypedValue operator*(const TypedValue &a, const TypedValue &b);
TypedValue operator%(const TypedValue &a, const TypedValue &b);

// binary bool operators
TypedValue operator&&(const TypedValue &a, const TypedValue &b);
TypedValue operator||(const TypedValue &a, const TypedValue &b);
// binary bool xor, not power operator
// Be careful: since ^ is binary operator and || and && are logical operators
// they have different priority in c++.
TypedValue operator^(const TypedValue &a, const TypedValue &b);

// stream output
std::ostream &operator<<(std::ostream &os, const TypedValue::Type type);

}  // namespace query
