#pragma once

#include <memory>
#include <string>
#include <cassert>
#include <iostream>
#include <vector>

#include "utils/underlying_cast.hpp"
#include "utils/total_ordering.hpp"
#include "utils/exceptions/basic_exception.hpp"

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
  enum class Type : unsigned {
    Null,
    String,
    Bool,
    Int,
    Float
  };

  // single static reference to Null, used whenever Null should be returned
  static const TypedValue Null;

  // constructors for primitive types
  TypedValue(bool value) : type_(Type::Bool) { bool_v = value; }
  TypedValue(int value) : type_(Type::Int) { int_v = value; }
  TypedValue(float value) : type_(Type::Float) { float_v = value; }

  /// constructors for non-primitive types (shared pointers)
  TypedValue(const std::string &value) : type_(Type::String) {
    new (&string_v) std::shared_ptr<std::string>(new std::string(value));
  }
  TypedValue(const char* value) : type_(Type::String) {
    new (&string_v) std::shared_ptr<std::string>(new std::string(value));
  }

  // assignment ops
  TypedValue& operator=(TypedValue& other);
  TypedValue& operator=(TypedValue&& other);

   TypedValue(const TypedValue& other);
  ~TypedValue();

  /**
   * Returns the value of the property as given type T.
   * The behavior of this function is undefined if
   * T does not correspond to this property's type_.
   *
   * @tparam T Type to interpret the value as.
   * @return The value as type T.
   */
  template <typename T> T Value() const;

  friend std::ostream& operator<<(std::ostream& stream, const TypedValue& prop);

  /**
   * The Type of property.
   */
  const Type type_;

private:
  // storage for the value of the property
  union {
    bool bool_v;
    int int_v;
    float float_v;
    std::shared_ptr<std::string> string_v;
  };
};

/**
 * An exception raised by the TypedValue system. Typically when
 * trying to perform operations (such as addition) on TypedValues
 * of incompatible Types.
 */
class TypedValueException : public BasicException {

public:
  using ::BasicException::BasicException;
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
std::ostream &operator<<(std::ostream &os, const TypedValue::Type type);
