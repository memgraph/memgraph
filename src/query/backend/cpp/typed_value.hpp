#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "utils/exceptions/stacktrace_exception.hpp"
#include "utils/total_ordering.hpp"
#include "utils/underlying_cast.hpp"
#include "storage/property_value.hpp"

/**
 * Encapsulation of a value and it's type encapsulated in a class that has no
 * compiled-time info about that type.
 *
 * Values can be of a number of predefined types that are enumerated in
 * TypedValue::Type. Each such type corresponds to exactly one C++ type.
 */
class TypedValue : public TotalOrdering<TypedValue, TypedValue, TypedValue> {
 private:
  /** Private default constructor, makes Null */
  TypedValue() : type_(Type::Null) {}

 public:
  /** A value type. Each type corresponds to exactly one C++ type */
  enum class Type : unsigned { Null, String, Bool, Int, Double, List };

  // single static reference to Null, used whenever Null should be returned
  static const TypedValue Null;

  // constructors for primitive types
  TypedValue(bool value) : type_(Type::Bool) { bool_v = value; }
  TypedValue(int value) : type_(Type::Int) { int_v = value; }
  TypedValue(double value) : type_(Type::Double) { double_v = value; }

  // conversion function to PropertyValue
  operator PropertyValue() const;

  /// constructors for non-primitive types (shared pointers)
  TypedValue(const std::string& value) : type_(Type::String) {
    new (&string_v) std::string(value);
  }
  TypedValue(const char* value) : type_(Type::String) {
    new (&string_v) std::string(value);
  }
  TypedValue(const std::vector<TypedValue>& value) : type_(Type::List) {
    new (&list_v) std::vector<TypedValue>(value);
  }
  TypedValue(const PropertyValue& value);

  // assignment ops
  TypedValue& operator=(const TypedValue& other);

  TypedValue(const TypedValue& other);
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
  T Value() const;

  friend std::ostream& operator<<(std::ostream& stream, const TypedValue& prop);

 private:
  // storage for the value of the property
  union {
    bool bool_v;
    int int_v;
    double double_v;
    // Since this is used in query runtime, size of union is not critical so
    // string and vector are used instead of pointers. It requires copy of data,
    // but most of algorithms (concatenations, serialisation...) has linear time
    // complexity so it shouldn't be a problem. This is maybe even faster
    // because of data locality.
    std::string string_v;
    std::vector<TypedValue> list_v;
    // Node, Edge, Path, Map missing.
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
TypedValue operator==(const TypedValue& a, const TypedValue& b);
TypedValue operator<(const TypedValue& a, const TypedValue& b);
TypedValue operator!(const TypedValue& a);

// arithmetic operators
TypedValue operator-(const TypedValue& a);
TypedValue operator+(const TypedValue& a, const TypedValue& b);
TypedValue operator-(const TypedValue& a, const TypedValue& b);
TypedValue operator/(const TypedValue& a, const TypedValue& b);
TypedValue operator*(const TypedValue& a, const TypedValue& b);
TypedValue operator%(const TypedValue& a, const TypedValue& b);

// binary bool operators
TypedValue operator&&(const TypedValue& a, const TypedValue& b);
TypedValue operator||(const TypedValue& a, const TypedValue& b);

// stream output
std::ostream& operator<<(std::ostream& os, const TypedValue::Type type);
