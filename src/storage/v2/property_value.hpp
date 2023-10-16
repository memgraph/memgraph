// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <fmt/ostream.h>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "storage/v2/temporal.hpp"
#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"

namespace memgraph::storage {

/// An exception raised by the PropertyValue. Typically when trying to perform
/// operations (such as addition) on PropertyValues of incompatible Types.
class PropertyValueException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(PropertyValueException)
};

/// Encapsulation of a value and its type in a class that has no compile-time
/// info about the type.
///
/// Values can be of a number of predefined types that are enumerated in
/// PropertyValue::Type. Each such type corresponds to exactly one C++ type.
class PropertyValue {
 public:
  /// A value type, each type corresponds to exactly one C++ type.
  enum class Type : uint8_t {
    Null = 0,
    Bool = 1,
    Int = 2,
    Double = 3,
    String = 4,
    List = 5,
    Map = 6,
    TemporalData = 7
  };

  static bool AreComparableTypes(Type a, Type b) {
    return (a == b) || (a == Type::Int && b == Type::Double) || (a == Type::Double && b == Type::Int);
  }

  /// Make a Null value
  PropertyValue() : type_(Type::Null) {}

  // constructors for primitive types
  explicit PropertyValue(const bool value) : type_(Type::Bool) { bool_v = value; }
  explicit PropertyValue(const int value) : type_(Type::Int) { int_v = value; }
  explicit PropertyValue(const int64_t value) : type_(Type::Int) { int_v = value; }
  explicit PropertyValue(const double value) : type_(Type::Double) { double_v = value; }
  explicit PropertyValue(const TemporalData value) : type_{Type::TemporalData} { temporal_data_v = value; }

  // copy constructors for non-primitive types
  /// @throw std::bad_alloc
  explicit PropertyValue(const std::string &value) : type_(Type::String) { new (&string_v) std::string(value); }
  /// @throw std::bad_alloc
  /// @throw std::length_error if length of value exceeds
  ///        std::string::max_length().
  explicit PropertyValue(const char *value) : type_(Type::String) { new (&string_v) std::string(value); }
  /// @throw std::bad_alloc
  explicit PropertyValue(const std::vector<PropertyValue> &value) : type_(Type::List) {
    new (&list_v) std::vector<PropertyValue>(value);
  }
  /// @throw std::bad_alloc
  explicit PropertyValue(const std::map<std::string, PropertyValue> &value) : type_(Type::Map) {
    new (&map_v) std::map<std::string, PropertyValue>(value);
  }

  // move constructors for non-primitive types
  explicit PropertyValue(std::string &&value) noexcept : type_(Type::String) {
    new (&string_v) std::string(std::move(value));
  }
  explicit PropertyValue(std::vector<PropertyValue> &&value) noexcept : type_(Type::List) {
    new (&list_v) std::vector<PropertyValue>(std::move(value));
  }
  explicit PropertyValue(std::map<std::string, PropertyValue> &&value) noexcept : type_(Type::Map) {
    new (&map_v) std::map<std::string, PropertyValue>(std::move(value));
  }

  // copy constructor
  /// @throw std::bad_alloc
  PropertyValue(const PropertyValue &other);

  // move constructor
  PropertyValue(PropertyValue &&other) noexcept;

  // copy assignment
  /// @throw std::bad_alloc
  PropertyValue &operator=(const PropertyValue &other);

  // move assignment
  PropertyValue &operator=(PropertyValue &&other) noexcept;
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
  bool IsTemporalData() const { return type_ == Type::TemporalData; }

  // value getters for primitive types
  /// @throw PropertyValueException if value isn't of correct type.
  bool ValueBool() const {
    if (type_ != Type::Bool) {
      throw PropertyValueException("The value isn't a bool!");
    }
    return bool_v;
  }
  /// @throw PropertyValueException if value isn't of correct type.
  int64_t ValueInt() const {
    if (type_ != Type::Int) {
      throw PropertyValueException("The value isn't an int!");
    }
    return int_v;
  }
  /// @throw PropertyValueException if value isn't of correct type.
  double ValueDouble() const {
    if (type_ != Type::Double) {
      throw PropertyValueException("The value isn't a double!");
    }
    return double_v;
  }

  /// @throw PropertyValueException if value isn't of correct type.
  TemporalData ValueTemporalData() const {
    if (type_ != Type::TemporalData) {
      throw PropertyValueException("The value isn't a temporal data!");
    }

    return temporal_data_v;
  }

  // const value getters for non-primitive types
  /// @throw PropertyValueException if value isn't of correct type.
  const std::string &ValueString() const {
    if (type_ != Type::String) {
      throw PropertyValueException("The value isn't a string!");
    }
    return string_v;
  }

  /// @throw PropertyValueException if value isn't of correct type.
  const std::vector<PropertyValue> &ValueList() const {
    if (type_ != Type::List) {
      throw PropertyValueException("The value isn't a list!");
    }
    return list_v;
  }

  /// @throw PropertyValueException if value isn't of correct type.
  const std::map<std::string, PropertyValue> &ValueMap() const {
    if (type_ != Type::Map) {
      throw PropertyValueException("The value isn't a map!");
    }
    return map_v;
  }

  // reference value getters for non-primitive types
  /// @throw PropertyValueException if value isn't of correct type.
  std::string &ValueString() {
    if (type_ != Type::String) {
      throw PropertyValueException("The value isn't a string!");
    }
    return string_v;
  }

  /// @throw PropertyValueException if value isn't of correct type.
  std::vector<PropertyValue> &ValueList() {
    if (type_ != Type::List) {
      throw PropertyValueException("The value isn't a list!");
    }
    return list_v;
  }

  /// @throw PropertyValueException if value isn't of correct type.
  std::map<std::string, PropertyValue> &ValueMap() {
    if (type_ != Type::Map) {
      throw PropertyValueException("The value isn't a map!");
    }
    return map_v;
  }

 private:
  void DestroyValue() noexcept;

  union {
    bool bool_v;
    int64_t int_v;
    double double_v;
    std::string string_v;
    std::vector<PropertyValue> list_v;
    std::map<std::string, PropertyValue> map_v;
    TemporalData temporal_data_v;
  };

  Type type_;
};

// stream output
/// @throw anything std::ostream::operator<< may throw.
inline std::ostream &operator<<(std::ostream &os, const PropertyValue::Type type) {
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
    case PropertyValue::Type::TemporalData:
      return os << "temporal data";
  }
}
/// @throw anything std::ostream::operator<< may throw.
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
                           [](auto &stream, const auto &pair) { stream << pair.first << ": " << pair.second; });
      return os << "}";
    case PropertyValue::Type::TemporalData:
      return os << fmt::format("type: {}, microseconds: {}", TemporalTypeTostring(value.ValueTemporalData().type),
                               value.ValueTemporalData().microseconds);
  }
}

// NOTE: The logic in this function *MUST* be equal to the logic in
// `PropertyStore::ComparePropertyValue`. If you change this operator make sure
// to change the function so that they have identical functionality.
inline bool operator==(const PropertyValue &first, const PropertyValue &second) noexcept {
  if (!PropertyValue::AreComparableTypes(first.type(), second.type())) return false;
  switch (first.type()) {
    case PropertyValue::Type::Null:
      return true;
    case PropertyValue::Type::Bool:
      return first.ValueBool() == second.ValueBool();
    case PropertyValue::Type::Int:
      if (second.type() == PropertyValue::Type::Double) {
        return first.ValueInt() == second.ValueDouble();
      } else {
        return first.ValueInt() == second.ValueInt();
      }
    case PropertyValue::Type::Double:
      if (second.type() == PropertyValue::Type::Double) {
        return first.ValueDouble() == second.ValueDouble();
      } else {
        return first.ValueDouble() == second.ValueInt();
      }
    case PropertyValue::Type::String:
      return first.ValueString() == second.ValueString();
    case PropertyValue::Type::List:
      return first.ValueList() == second.ValueList();
    case PropertyValue::Type::Map:
      return first.ValueMap() == second.ValueMap();
    case PropertyValue::Type::TemporalData:
      return first.ValueTemporalData() == second.ValueTemporalData();
  }
}

/// NOLINTNEXTLINE(bugprone-exception-escape)
inline bool operator<(const PropertyValue &first, const PropertyValue &second) noexcept {
  if (!PropertyValue::AreComparableTypes(first.type(), second.type())) return first.type() < second.type();
  switch (first.type()) {
    case PropertyValue::Type::Null:
      return false;
    case PropertyValue::Type::Bool:
      return first.ValueBool() < second.ValueBool();
    case PropertyValue::Type::Int:
      if (second.type() == PropertyValue::Type::Double) {
        return first.ValueInt() < second.ValueDouble();
      } else {
        return first.ValueInt() < second.ValueInt();
      }
    case PropertyValue::Type::Double:
      if (second.type() == PropertyValue::Type::Double) {
        return first.ValueDouble() < second.ValueDouble();
      } else {
        return first.ValueDouble() < second.ValueInt();
      }
    case PropertyValue::Type::String:
      return first.ValueString() < second.ValueString();
    case PropertyValue::Type::List:
      return first.ValueList() < second.ValueList();
    case PropertyValue::Type::Map:
      return first.ValueMap() < second.ValueMap();
    case PropertyValue::Type::TemporalData:
      return first.ValueTemporalData() < second.ValueTemporalData();
  }
}

/// NOLINTNEXTLINE(bugprone-exception-escape)
inline bool operator>(const PropertyValue &first, const PropertyValue &second) noexcept { return second < first; }

inline PropertyValue::PropertyValue(const PropertyValue &other) : type_(other.type_) {
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
    case Type::TemporalData:
      this->temporal_data_v = other.temporal_data_v;
      return;
  }
}

inline PropertyValue::PropertyValue(PropertyValue &&other) noexcept : type_(other.type_) {
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
      new (&map_v) std::map<std::string, PropertyValue>(std::move(other.map_v));
      break;
    case Type::TemporalData:
      this->temporal_data_v = other.temporal_data_v;
      break;
  }

  // reset the type of other
  other.DestroyValue();
  other.type_ = Type::Null;
}

inline PropertyValue &PropertyValue::operator=(const PropertyValue &other) {
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
    case Type::TemporalData:
      this->temporal_data_v = other.temporal_data_v;
      break;
  }

  return *this;
}

inline PropertyValue &PropertyValue::operator=(PropertyValue &&other) noexcept {
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
      new (&map_v) std::map<std::string, PropertyValue>(std::move(other.map_v));
      break;
    case Type::TemporalData:
      this->temporal_data_v = other.temporal_data_v;
      break;
  }

  // reset the type of other
  other.DestroyValue();
  other.type_ = Type::Null;

  return *this;
}

inline void PropertyValue::DestroyValue() noexcept {
  switch (type_) {
    // destructor for primitive types does nothing
    case Type::Null:
    case Type::Bool:
    case Type::Int:
    case Type::Double:
    case Type::TemporalData:
      return;

    // destructor for non primitive types since we used placement new
    case Type::String:
      std::destroy_at(&string_v);
      return;
    case Type::List:
      std::destroy_at(&list_v);
      return;
    case Type::Map:
      std::destroy_at(&map_v);
      return;
  }
}

}  // namespace memgraph::storage

#if FMT_VERSION > 90000
template <>
class fmt::formatter<memgraph::storage::PropertyValue> : public fmt::ostream_formatter {};
template <>
class fmt::formatter<memgraph::storage::PropertyValue::Type> : public fmt::ostream_formatter {};
#endif
