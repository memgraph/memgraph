#pragma once

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "utils/exceptions.hpp"

/**
 * An exception raised by the PropertyValue system. Typically when
 * trying to perform operations (such as addition) on PropertyValues
 * of incompatible Types.
 */
class PropertyValueException : public utils::StacktraceException {
 public:
  using utils::StacktraceException::StacktraceException;
};

/**
 * Encapsulation of a value and its type in a class that has no compile-time
 * info about the type.
 *
 * Values can be of a number of predefined types that are enumerated in
 * PropertyValue::Type. Each such type corresponds to exactly one C++ type.
 */
class PropertyValue {
 public:
  /** A value type. Each type corresponds to exactly one C++ type */
  enum class Type : unsigned { Null, String, Bool, Int, Double, List, Map };

  /** Checks if the given PropertyValue::Types are comparable */
  static bool AreComparableTypes(Type a, Type b) {
    auto is_numeric = [](const Type t) {
      return t == Type::Int || t == Type::Double;
    };

    return a == b || (is_numeric(a) && is_numeric(b));
  }

  // default constructor, makes Null
  PropertyValue() : type_(Type::Null) {}

  // constructors for primitive types
  explicit PropertyValue(bool value) : type_(Type::Bool) { bool_v = value; }
  explicit PropertyValue(int value) : type_(Type::Int) { int_v = value; }
  explicit PropertyValue(int64_t value) : type_(Type::Int) { int_v = value; }
  explicit PropertyValue(double value) : type_(Type::Double) {
    double_v = value;
  }

  // constructors for non-primitive types
  explicit PropertyValue(const std::string &value) : type_(Type::String) {
    new (&string_v) std::string(value);
  }
  explicit PropertyValue(const char *value) : type_(Type::String) {
    new (&string_v) std::string(value);
  }
  explicit PropertyValue(const std::vector<PropertyValue> &value)
      : type_(Type::List) {
    new (&list_v) std::vector<PropertyValue>(value);
  }
  explicit PropertyValue(const std::map<std::string, PropertyValue> &value)
      : type_(Type::Map) {
    new (&map_v) std::map<std::string, PropertyValue>(value);
  }

  // move constructors for non-primitive types
  explicit PropertyValue(std::string &&value) noexcept : type_(Type::String) {
    new (&string_v) std::string(std::move(value));
  }
  explicit PropertyValue(std::vector<PropertyValue> &&value) noexcept
      : type_(Type::List) {
    new (&list_v) std::vector<PropertyValue>(std::move(value));
  }
  explicit PropertyValue(std::map<std::string, PropertyValue> &&value) noexcept
      : type_(Type::Map) {
    new (&map_v) std::map<std::string, PropertyValue>(std::move(value));
  }

  PropertyValue &operator=(const PropertyValue &other);
  PropertyValue &operator=(PropertyValue &&other) noexcept;

  PropertyValue(const PropertyValue &other);
  PropertyValue(PropertyValue &&other) noexcept;
  ~PropertyValue();

  Type type() const { return type_; }

  bool IsNull() const { return type_ == Type::Null; }

  bool ValueBool() const {
    if (type_ != Type::Bool) {
      throw PropertyValueException("This value isn't a bool!");
    }
    return bool_v;
  }

  int64_t ValueInt() const {
    if (type_ != Type::Int) {
      throw PropertyValueException("This value isn't a int!");
    }
    return int_v;
  }

  double ValueDouble() const {
    if (type_ != Type::Double) {
      throw PropertyValueException("This value isn't a double!");
    }
    return double_v;
  }

  const std::string &ValueString() const {
    if (type_ != Type::String) {
      throw PropertyValueException("The value isn't a string!");
    }
    return string_v;
  }

  const std::vector<PropertyValue> &ValueList() const {
    if (type_ != Type::List) {
      throw PropertyValueException("The value isn't a list!");
    }
    return list_v;
  }

  const std::map<std::string, PropertyValue> &ValueMap() const {
    if (type_ != Type::Map) {
      throw PropertyValueException("The value isn't a map!");
    }
    return map_v;
  }

  std::string &ValueString() {
    if (type_ != Type::String) {
      throw PropertyValueException("The value isn't a string!");
    }
    return string_v;
  }

  std::vector<PropertyValue> &ValueList() {
    if (type_ != Type::List) {
      throw PropertyValueException("The value isn't a list!");
    }
    return list_v;
  }

  std::map<std::string, PropertyValue> &ValueMap() {
    if (type_ != Type::Map) {
      throw PropertyValueException("The value isn't a map!");
    }
    return map_v;
  }

 private:
  void DestroyValue();

  // storage for the value of the property
  union {
    bool bool_v;
    int64_t int_v;
    double double_v;
    std::string string_v;
    // We support lists of values of different types, neo4j supports lists of
    // values of the same type.
    std::vector<PropertyValue> list_v;
    std::map<std::string, PropertyValue> map_v;
  };

  /**
   * The Type of property.
   */
  Type type_;
};

// stream output
std::ostream &operator<<(std::ostream &os, const PropertyValue::Type type);
std::ostream &operator<<(std::ostream &os, const PropertyValue &value);

// comparison
bool operator==(const PropertyValue &first, const PropertyValue &second);
bool operator<(const PropertyValue &first, const PropertyValue &second);
