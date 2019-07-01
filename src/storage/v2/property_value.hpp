#pragma once

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"

namespace storage {

/// An exception raised by the PropertyValue. Typically when trying to perform
/// operations (such as addition) on PropertyValues of incompatible Types.
class PropertyValueException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

/// Encapsulation of a value and its type in a class that has no compile-time
/// info about the type.
///
/// Values can be of a number of predefined types that are enumerated in
/// PropertyValue::Type. Each such type corresponds to exactly one C++ type.
class PropertyValue {
 public:
  enum class Type : uint8_t { Null, Bool, Int, Double, String, List, Map };

  // default constructor, makes Null
  PropertyValue() : type_(Type::Null) {}

  // constructors for primitive types
  explicit PropertyValue(bool value) : type_(Type::Bool) { bool_v = value; }
  explicit PropertyValue(int value) : type_(Type::Int) { int_v = value; }
  explicit PropertyValue(int64_t value) : type_(Type::Int) { int_v = value; }
  explicit PropertyValue(double value) : type_(Type::Double) {
    double_v = value;
  }

  // copy constructors for non-primitive types
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

  // copy constructor
  PropertyValue(const PropertyValue &other) : type_(other.type_) {
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
        new (&list_v) std::vector<PropertyValue>(other.list_v);
        return;
      case Type::Map:
        new (&map_v) std::map<std::string, PropertyValue>(other.map_v);
        return;
    }
  }

  // move constructor
  PropertyValue(PropertyValue &&other) noexcept : type_(other.type_) {
    switch (other.type_) {
      case Type::Null:
        break;
      case Type::Bool:
        this->bool_v = other.bool_v;
        break;
      case Type::Int:
        this->int_v = other.int_v;
        break;
      case Type::Double:
        this->double_v = other.double_v;
        break;
      case Type::String:
        new (&string_v) std::string(std::move(other.string_v));
        break;
      case Type::List:
        new (&list_v) std::vector<PropertyValue>(std::move(other.list_v));
        break;
      case Type::Map:
        new (&map_v)
            std::map<std::string, PropertyValue>(std::move(other.map_v));
        break;
    }

    // reset the type of other
    other.DestroyValue();
    other.type_ = Type::Null;
  }

  // copy assignment
  PropertyValue &operator=(const PropertyValue &other) {
    if (this == &other) return *this;

    DestroyValue();
    type_ = other.type_;

    switch (other.type_) {
      case Type::Null:
        break;
      case Type::Bool:
        this->bool_v = other.bool_v;
        break;
      case Type::Int:
        this->int_v = other.int_v;
        break;
      case Type::Double:
        this->double_v = other.double_v;
        break;
      case Type::String:
        new (&string_v) std::string(other.string_v);
        break;
      case Type::List:
        new (&list_v) std::vector<PropertyValue>(other.list_v);
        break;
      case Type::Map:
        new (&map_v) std::map<std::string, PropertyValue>(other.map_v);
        break;
    }

    return *this;
  }

  // move assignment
  PropertyValue &operator=(PropertyValue &&other) noexcept {
    if (this == &other) return *this;

    DestroyValue();
    type_ = other.type_;

    switch (other.type_) {
      case Type::Null:
        break;
      case Type::Bool:
        this->bool_v = other.bool_v;
        break;
      case Type::Int:
        this->int_v = other.int_v;
        break;
      case Type::Double:
        this->double_v = other.double_v;
        break;
      case Type::String:
        new (&string_v) std::string(std::move(other.string_v));
        break;
      case Type::List:
        new (&list_v) std::vector<PropertyValue>(std::move(other.list_v));
        break;
      case Type::Map:
        new (&map_v)
            std::map<std::string, PropertyValue>(std::move(other.map_v));
        break;
    }

    // reset the type of other
    other.DestroyValue();
    other.type_ = Type::Null;

    return *this;
  }

  // TODO: Implement copy assignment operators for primitive types.
  // TODO: Implement copy and move assignment operators for non-primitive types.

  ~PropertyValue() { DestroyValue(); }

  Type type() const { return type_; }

  // type checkers
  bool IsNull() const { return type_ == Type::Null; }
  bool IsBool() const { return type_ == Type::Bool; }
  bool IsInt() const { return type_ == Type::Int; }
  bool IsDouble() const { return type_ == Type::Double; }
  bool IsString() const { return type_ == Type::String; }
  bool IsList() const { return type_ == Type::List; }
  bool IsMap() const { return type_ == Type::Map; }

  // value getters for primitive types
  bool ValueBool() const {
    if (type_ != Type::Bool) {
      throw PropertyValueException("The value isn't a bool!");
    }
    return bool_v;
  }
  int64_t ValueInt() const {
    if (type_ != Type::Int) {
      throw PropertyValueException("The value isn't an int!");
    }
    return int_v;
  }
  double ValueDouble() const {
    if (type_ != Type::Double) {
      throw PropertyValueException("The value isn't a double!");
    }
    return double_v;
  }

  // const value getters for non-primitive types
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

  // reference value getters for non-primitive types
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
  void DestroyValue() {
    switch (type_) {
      // destructor for primitive types does nothing
      case Type::Null:
      case Type::Bool:
      case Type::Int:
      case Type::Double:
        return;

      // destructor for non primitive types since we used placement new
      case Type::String:
        // Clang fails to compile ~std::string. It seems it is a bug in some
        // versions of clang. Using namespace std statement solves the issue.
        using namespace std;
        string_v.~string();
        return;
      case Type::List:
        list_v.~vector();
        return;
      case Type::Map:
        map_v.~map();
        return;
    }
  }

  union {
    bool bool_v;
    int64_t int_v;
    double double_v;
    std::string string_v;
    std::vector<PropertyValue> list_v;
    std::map<std::string, PropertyValue> map_v;
  };

  Type type_;
};

// stream output
inline std::ostream &operator<<(std::ostream &os,
                                const PropertyValue::Type type) {
  switch (type) {
    case PropertyValue::Type::Null:
      return os << "null";
    case PropertyValue::Type::Bool:
      return os << "bool";
    case PropertyValue::Type::Int:
      return os << "int";
    case PropertyValue::Type::Double:
      return os << "double";
    case PropertyValue::Type::String:
      return os << "string";
    case PropertyValue::Type::List:
      return os << "list";
    case PropertyValue::Type::Map:
      return os << "map";
  }
}
inline std::ostream &operator<<(std::ostream &os, const PropertyValue &value) {
  switch (value.type()) {
    case PropertyValue::Type::Null:
      return os << "null";
    case PropertyValue::Type::Bool:
      return os << (value.ValueBool() ? "true" : "false");
    case PropertyValue::Type::Int:
      return os << value.ValueInt();
    case PropertyValue::Type::Double:
      return os << value.ValueDouble();
    case PropertyValue::Type::String:
      return os << value.ValueString();
    case PropertyValue::Type::List:
      os << "[";
      utils::PrintIterable(os, value.ValueList());
      return os << "]";
    case PropertyValue::Type::Map:
      os << "{";
      utils::PrintIterable(os, value.ValueMap(), ", ",
                           [](auto &stream, const auto &pair) {
                             stream << pair.first << ": " << pair.second;
                           });
      return os << "}";
  }
}

// comparison
inline bool operator==(const PropertyValue &first,
                       const PropertyValue &second) {
  if (first.type() != second.type()) return false;
  switch (first.type()) {
    case PropertyValue::Type::Null:
      return true;
    case PropertyValue::Type::Bool:
      return first.ValueBool() == second.ValueBool();
    case PropertyValue::Type::Int:
      return first.ValueInt() == second.ValueInt();
    case PropertyValue::Type::Double:
      return first.ValueDouble() == second.ValueDouble();
    case PropertyValue::Type::String:
      return first.ValueString() == second.ValueString();
    case PropertyValue::Type::List:
      return first.ValueList() == second.ValueList();
    case PropertyValue::Type::Map:
      return first.ValueMap() == second.ValueMap();
  }
}
inline bool operator<(const PropertyValue &first, const PropertyValue &second) {
  if (first.type() != second.type()) return first.type() < second.type();
  switch (first.type()) {
    case PropertyValue::Type::Null:
      return false;
    case PropertyValue::Type::Bool:
      return first.ValueBool() < second.ValueBool();
    case PropertyValue::Type::Int:
      return first.ValueInt() < second.ValueInt();
    case PropertyValue::Type::Double:
      return first.ValueDouble() < second.ValueDouble();
    case PropertyValue::Type::String:
      return first.ValueString() < second.ValueString();
    case PropertyValue::Type::List:
      return first.ValueList() < second.ValueList();
    case PropertyValue::Type::Map:
      return first.ValueMap() < second.ValueMap();
  }
}

}  // namespace storage
