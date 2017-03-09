#pragma once

#include <cassert>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "utils/exceptions/stacktrace_exception.hpp"
#include "utils/total_ordering.hpp"
#include "utils/underlying_cast.hpp"

/**
 * Encapsulation of a value and it's type encapsulated in a class that has no
 * compiled-time info about that type.
 *
 * Values can be of a number of predefined types that are enumerated in
 * PropertyValue::Type. Each such type corresponds to exactly one C++ type.
 */
class PropertyValue {
 private:
  /** Private default constructor, makes Null */
  PropertyValue() : type_(Type::Null) {}

 public:
  /** A value type. Each type corresponds to exactly one C++ type */
  enum class Type : unsigned { Null, String, Bool, Int, Double, List };

  // single static reference to Null, used whenever Null should be returned
  static const PropertyValue Null;

  // constructors for primitive types
  PropertyValue(bool value) : type_(Type::Bool) { bool_v = value; }
  PropertyValue(int value) : type_(Type::Int) { int_v = value; }
  PropertyValue(int64_t value) : type_(Type::Int) { int_v = value; }
  PropertyValue(double value) : type_(Type::Double) { double_v = value; }

  /// constructors for non-primitive types (shared pointers)
  PropertyValue(const std::string& value) : type_(Type::String) {
    new (&string_v) std::shared_ptr<std::string>(new std::string(value));
  }
  PropertyValue(const char* value) : type_(Type::String) {
    new (&string_v) std::shared_ptr<std::string>(new std::string(value));
  }
  PropertyValue(const std::vector<PropertyValue>& value) : type_(Type::List) {
    new (&list_v) std::shared_ptr<std::vector<PropertyValue>>(
        new std::vector<PropertyValue>(value));
  }

  // assignment op
  PropertyValue& operator=(const PropertyValue& other);

  PropertyValue(const PropertyValue& other);
  ~PropertyValue();

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

  friend std::ostream& operator<<(std::ostream& stream,
                                  const PropertyValue& prop);

 private:
  // storage for the value of the property
  union {
    bool bool_v;
    int64_t int_v;
    double double_v;
    std::shared_ptr<std::string> string_v;
    // We support lists of values of different types, neo4j supports lists of
    // values of the same type.
    std::shared_ptr<std::vector<PropertyValue>> list_v;
  };

  /**
   * The Type of property.
   */
  Type type_;
};

/**
 * An exception raised by the PropertyValue system. Typically when
 * trying to perform operations (such as addition) on PropertyValues
 * of incompatible Types.
 */
class PropertyValueException : public StacktraceException {
 public:
  using ::StacktraceException::StacktraceException;
};

// stream output
std::ostream& operator<<(std::ostream& os, const PropertyValue::Type type);
