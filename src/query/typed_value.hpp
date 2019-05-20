#pragma once

#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "query/path.hpp"
#include "storage/common/types/property_value.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/exceptions.hpp"

namespace query {

// TODO: Neo4j does overflow checking. Should we also implement it?
/**
 * Encapsulation of a value and it's type encapsulated in a class that has no
 * compiled-time info about that type.
 *
 * Values can be of a number of predefined types that are enumerated in
 * TypedValue::Type. Each such type corresponds to exactly one C++ type.
 */
class TypedValue {
 public:
  /** Custom TypedValue equality function that returns a bool
   * (as opposed to returning TypedValue as the default equality does).
   * This implementation treats two nulls as being equal and null
   * not being equal to everything else.
   */
  struct BoolEqual {
    bool operator()(const TypedValue &left, const TypedValue &right) const;
  };

  /** Hash operator for TypedValue.
   *
   * Not injecting into std
   * due to linking problems. If the implementation is in this header,
   * then it implicitly instantiates TypedValue::Value<T> before
   * explicit instantiation in .cpp file. If the implementation is in
   * the .cpp file, it won't link.
   */
  struct Hash {
    size_t operator()(const TypedValue &value) const;
  };

  /**
   * Unordered set of TypedValue items. Can contain at most one Null element,
   * and treats an integral and floating point value as same if they are equal
   * in the floating point domain (TypedValue operator== behaves the same).
   * */
  using unordered_set = std::unordered_set<TypedValue, Hash, BoolEqual>;
  using value_map_t = std::map<std::string, TypedValue>;

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

  TypedValue(const TypedValue &other);
  TypedValue(TypedValue &&other) noexcept;

  // constructors for primitive types
  TypedValue(bool value) : type_(Type::Bool) { bool_v = value; }
  TypedValue(int value) : type_(Type::Int) { int_v = value; }
  TypedValue(int64_t value) : type_(Type::Int) { int_v = value; }
  TypedValue(double value) : type_(Type::Double) { double_v = value; }

  // conversion function to PropertyValue
  explicit operator PropertyValue() const;

  // copy constructors for non-primitive types
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

  // move constructors for non-primitive types
  TypedValue(std::string &&value) noexcept : type_(Type::String) {
    new (&string_v) std::string(std::move(value));
  }
  TypedValue(std::vector<TypedValue> &&value) noexcept : type_(Type::List) {
    new (&list_v) std::vector<TypedValue>(std::move(value));
  }
  TypedValue(std::map<std::string, TypedValue> &&value) noexcept
      : type_(Type::Map) {
    new (&map_v) std::map<std::string, TypedValue>(std::move(value));
  }
  TypedValue(VertexAccessor &&vertex) noexcept : type_(Type::Vertex) {
    new (&vertex_v) VertexAccessor(std::move(vertex));
  }
  TypedValue(EdgeAccessor &&edge) noexcept : type_(Type::Edge) {
    new (&edge_v) EdgeAccessor(std::move(edge));
  }
  TypedValue(Path &&path) noexcept : type_(Type::Path) {
    new (&path_v) Path(std::move(path));
  }
  TypedValue(PropertyValue &&value) noexcept;

  // copy assignment operators
  TypedValue &operator=(const char *);
  TypedValue &operator=(int);
  TypedValue &operator=(bool);
  TypedValue &operator=(int64_t);
  TypedValue &operator=(double);
  TypedValue &operator=(const std::string &);
  TypedValue &operator=(const std::vector<TypedValue> &);
  TypedValue &operator=(const TypedValue::value_map_t &);
  TypedValue &operator=(const VertexAccessor &);
  TypedValue &operator=(const EdgeAccessor &);
  TypedValue &operator=(const Path &);
  TypedValue &operator=(const TypedValue &);

  // move assignment operators
  TypedValue &operator=(std::string &&) noexcept;
  TypedValue &operator=(std::vector<TypedValue> &&) noexcept;
  TypedValue &operator=(TypedValue::value_map_t &&) noexcept;
  TypedValue &operator=(VertexAccessor &&) noexcept;
  TypedValue &operator=(EdgeAccessor &&) noexcept;
  TypedValue &operator=(Path &&) noexcept;
  TypedValue &operator=(TypedValue &&) noexcept;

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

// TODO consider adding getters for primitives by value (and not by ref)

#define DECLARE_VALUE_AND_TYPE_GETTERS(type_param, field)          \
  /** Gets the value of type field. Throws if value is not field*/ \
  type_param &Value##field();                                      \
  /** Gets the value of type field. Throws if value is not field*/ \
  const type_param &Value##field() const;                          \
  /** Checks if it's the value is of the given type */             \
  bool Is##field() const;

  DECLARE_VALUE_AND_TYPE_GETTERS(bool, Bool)
  DECLARE_VALUE_AND_TYPE_GETTERS(int64_t, Int)
  DECLARE_VALUE_AND_TYPE_GETTERS(double, Double)
  DECLARE_VALUE_AND_TYPE_GETTERS(std::string, String)
  DECLARE_VALUE_AND_TYPE_GETTERS(std::vector<TypedValue>, List)
  DECLARE_VALUE_AND_TYPE_GETTERS(value_map_t, Map)
  DECLARE_VALUE_AND_TYPE_GETTERS(VertexAccessor, Vertex)
  DECLARE_VALUE_AND_TYPE_GETTERS(EdgeAccessor, Edge)
  DECLARE_VALUE_AND_TYPE_GETTERS(Path, Path)

#undef DECLARE_VALUE_AND_TYPE_GETTERS

  /**  Checks if value is a TypedValue::Null. */
  bool IsNull() const;

  /** Convenience function for checking if this TypedValue is either
   * an integer or double */
  bool IsNumeric() const;

  /** Convenience function for checking if this TypedValue can be converted into
   * PropertyValue */
  bool IsPropertyValue() const;

  friend std::ostream &operator<<(std::ostream &stream, const TypedValue &prop);

 private:
  void DestroyValue();

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
class TypedValueException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
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

inline TypedValue operator!=(const TypedValue &a, const TypedValue &b) {
  return !(a == b);
}

inline TypedValue operator<=(const TypedValue &a, const TypedValue &b) {
  return a < b || a == b;
}

inline TypedValue operator>(const TypedValue &a, const TypedValue &b) {
  return !(a <= b);
}

inline TypedValue operator>=(const TypedValue &a, const TypedValue &b) {
  return !(a < b);
}

}  // namespace query
