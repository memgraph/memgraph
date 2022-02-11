// Copyright 2021 Memgraph Ltd.
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

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "storage/v2/temporal.hpp"
#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/map.hpp"
#include "utils/pmr/string.hpp"
#include "utils/pmr/vector.hpp"

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

  using TString = utils::pmr::string;
  using TVector = utils::pmr::vector<PropertyValue>;
  using TMap = utils::pmr::map<utils::pmr::string, PropertyValue>;

  using allocator_type = utils::Allocator<PropertyValue>;

  static bool AreComparableTypes(Type a, Type b) {
    return (a == b) || (a == Type::Int && b == Type::Double) || (a == Type::Double && b == Type::Int);
  }

  /// Make a Null value
  PropertyValue() : type_(Type::Null) {}

  // constructors for primitive types
  explicit PropertyValue(const bool value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : type_(Type::Bool), memory_{memory} {
    bool_v = value;
  }
  explicit PropertyValue(const int value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : type_(Type::Int), memory_{memory} {
    int_v = value;
  }
  explicit PropertyValue(const int64_t value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : type_(Type::Int), memory_{memory} {
    int_v = value;
  }
  explicit PropertyValue(const double value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : type_(Type::Double), memory_{memory} {
    double_v = value;
  }
  explicit PropertyValue(const TemporalData value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : type_{Type::TemporalData}, memory_{memory} {
    temporal_data_v = value;
  }

  // copy constructors for non-primitive types
  /// @throw std::bad_alloc
  explicit PropertyValue(const std::string &value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : type_(Type::String), memory_{memory} {
    new (&string_v) TString(value, memory);
  }
  /// @throw std::bad_alloc
  /// @throw std::length_error if length of value exceeds
  ///        std::string::max_length().
  explicit PropertyValue(const char *value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : type_(Type::String), memory_{memory} {
    new (&string_v) TString(value, memory);
  }

  /**
   * Construct a copy of other.
   * utils::MemoryResource is obtained by calling
   * std::allocator_traits<>::
   *     select_on_container_copy_construction(other.get_allocator()).
   * Since we use utils::Allocator, which does not propagate, this means that
   * memory_ will be the default utils::NewDeleteResource().
   */
  explicit PropertyValue(const TString &other)
      : PropertyValue(other,
                      std::allocator_traits<utils::Allocator<PropertyValue>>::select_on_container_copy_construction(
                          other.get_allocator())
                          .GetMemoryResource()) {}

  /** Construct a copy using the given utils::MemoryResource */
  PropertyValue(const TString &other, utils::MemoryResource *memory) : type_(Type::String), memory_{memory} {
    new (&string_v) TString(other, memory_);
  }

  /** Construct a copy using the given utils::MemoryResource */
  explicit PropertyValue(const std::vector<PropertyValue> &value,
                         utils::MemoryResource *memory = utils::NewDeleteResource())
      : type_(Type::List), memory_(memory) {
    new (&list_v) TVector(memory_);
    list_v.reserve(value.size());
    list_v.assign(value.begin(), value.end());
  }

  /**
   * Construct a copy of other.
   * utils::MemoryResource is obtained by calling
   * std::allocator_traits<>::
   *     select_on_container_copy_construction(other.get_allocator()).
   * Since we use utils::Allocator, which does not propagate, this means that
   * memory_ will be the default utils::NewDeleteResource().
   */
  explicit PropertyValue(const TVector &other)
      : PropertyValue(other,
                      std::allocator_traits<utils::Allocator<PropertyValue>>::select_on_container_copy_construction(
                          other.get_allocator())
                          .GetMemoryResource()) {}

  /** Construct a copy using the given utils::MemoryResource */
  PropertyValue(const TVector &value, utils::MemoryResource *memory) : type_(Type::List), memory_(memory) {
    new (&list_v) TVector(value, memory_);
  }

  /** Construct a copy using the given utils::MemoryResource */
  explicit PropertyValue(const std::map<std::string, PropertyValue> &value,
                         utils::MemoryResource *memory = utils::NewDeleteResource())
      : type_(Type::Map), memory_{memory} {
    new (&map_v) TMap(memory_);
    for (const auto &kv : value) map_v.emplace(kv.first, kv.second);
  }
  /**
   * Construct a copy of other.
   * utils::MemoryResource is obtained by calling
   * std::allocator_traits<>::
   *     select_on_container_copy_construction(other.get_allocator()).
   * Since we use utils::Allocator, which does not propagate, this means that
   * memory_ will be the default utils::NewDeleteResource().
   */
  explicit PropertyValue(const TMap &other)
      : PropertyValue(other,
                      std::allocator_traits<utils::Allocator<PropertyValue>>::select_on_container_copy_construction(
                          other.get_allocator())
                          .GetMemoryResource()) {}

  /** Construct a copy using the given utils::MemoryResource */
  PropertyValue(const TMap &value, utils::MemoryResource *memory) : type_(Type::Map), memory_{memory} {
    new (&map_v) TMap(value, memory_);
  }
  /**
   * Construct with the value of other.
   * utils::MemoryResource is obtained from other. After the move, other will be
   * left in unspecified state.
   */
  explicit PropertyValue(TString &&other) noexcept
      : PropertyValue(std::move(other), other.get_allocator().GetMemoryResource()) {}

  /**
   * Construct with the value of other and use the given MemoryResource
   * After the move, other will be left in unspecified state.
   */
  PropertyValue(TString &&other, utils::MemoryResource *memory) : type_(Type::String), memory_{memory} {
    new (&string_v) TString(std::move(other), memory_);
  }

  /**
   * Perform an element-wise move using default utils::NewDeleteResource().
   * Other will be not be empty, though elements may be Null.
   */
  explicit PropertyValue(std::vector<PropertyValue> &&other)
      : PropertyValue(std::move(other), utils::NewDeleteResource()) {}

  /**
   * Perform an element-wise move of the other and use the given MemoryResource.
   * Other will be not be left empty, though elements may be Null.
   */
  PropertyValue(std::vector<PropertyValue> &&other, utils::MemoryResource *memory)
      : type_(Type::List), memory_{memory} {
    new (&list_v) TVector(memory_);
    list_v.reserve(other.size());
    // std::vector<PropertyValue> has std::allocator and there's no move
    // constructor for std::vector using different allocator types. Since
    // std::allocator is not propagated to elements, it is possible that some
    // PropertyValue elements have a MemoryResource that is the same as the one we
    // are given. In such a case we would like to move those PropertyValue
    // instances, so we use move_iterator.
    list_v.assign(std::make_move_iterator(other.begin()), std::make_move_iterator(other.end()));
  }
  /**
   * Construct with the value of other.
   * utils::MemoryResource is obtained from other. After the move, other will be
   * left empty.
   */
  explicit PropertyValue(TVector &&other) noexcept
      : PropertyValue(std::move(other), other.get_allocator().GetMemoryResource()) {}

  /**
   * Construct with the value of other and use the given MemoryResource.
   * If `other.get_allocator() != *memory`, this call will perform an
   * element-wise move and other is not guaranteed to be empty.
   */
  PropertyValue(TVector &&other, utils::MemoryResource *memory) : type_(Type::List), memory_{memory} {
    new (&list_v) TVector(std::move(other), memory_);
  }

  /**
   * Perform an element-wise move using default utils::NewDeleteResource().
   * Other will not be left empty, i.e. keys will exist but their values may
   * be Null.
   */
  explicit PropertyValue(std::map<std::string, PropertyValue> &&other)
      : PropertyValue(std::move(other), utils::NewDeleteResource()) {}

  /**
   * Perform an element-wise move using the given MemoryResource.
   * Other will not be left empty, i.e. keys will exist but their values may
   * be Null.
   */
  PropertyValue(std::map<std::string, PropertyValue> &&other, utils::MemoryResource *memory)
      : type_(Type::Map), memory_{memory} {
    new (&map_v) TMap(memory_);
    for (auto &kv : other) map_v.emplace(kv.first, std::move(kv.second));
  }

  /**
   * Construct with the value of other.
   * utils::MemoryResource is obtained from other. After the move, other will be
   * left empty.
   */
  explicit PropertyValue(TMap &&other) noexcept
      : PropertyValue(std::move(other), other.get_allocator().GetMemoryResource()) {}

  // copy constructor
  /// @throw std::bad_alloc
  PropertyValue(const PropertyValue &other);
  PropertyValue(const PropertyValue &other, utils::MemoryResource *memory);

  // move constructor
  PropertyValue(PropertyValue &&other) noexcept;
  PropertyValue(PropertyValue &&other, utils::MemoryResource *memory) noexcept;

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
  const TString &ValueString() const {
    if (type_ != Type::String) {
      throw PropertyValueException("The value isn't a string!");
    }
    return string_v;
  }

  /// @throw PropertyValueException if value isn't of correct type.
  const TVector &ValueList() const {
    if (type_ != Type::List) {
      throw PropertyValueException("The value isn't a list!");
    }
    return list_v;
  }

  /// @throw PropertyValueException if value isn't of correct type.
  const TMap &ValueMap() const {
    if (type_ != Type::Map) {
      throw PropertyValueException("The value isn't a map!");
    }
    return map_v;
  }

  // reference value getters for non-primitive types
  /// @throw PropertyValueException if value isn't of correct type.
  TString &ValueString() {
    if (type_ != Type::String) {
      throw PropertyValueException("The value isn't a string!");
    }
    return string_v;
  }

  /// @throw PropertyValueException if value isn't of correct type.
  TVector &ValueList() {
    if (type_ != Type::List) {
      throw PropertyValueException("The value isn't a list!");
    }
    return list_v;
  }

  /// @throw PropertyValueException if value isn't of correct type.
  TMap &ValueMap() {
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
    TString string_v;
    TVector list_v;
    TMap map_v;
    TemporalData temporal_data_v;
  };

  Type type_;
  utils::MemoryResource *memory_{utils::NewDeleteResource()};
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
inline PropertyValue::PropertyValue(const PropertyValue &other)
    : PropertyValue(
          other,
          std::allocator_traits<utils::Allocator<PropertyValue>>::select_on_container_copy_construction(other.memory_)
              .GetMemoryResource()) {}

inline PropertyValue::PropertyValue(const PropertyValue &other, utils::MemoryResource *memory)
    : type_(other.type_), memory_{memory} {
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
      new (&string_v) TString(other.string_v, memory_);
      return;
    case Type::List:
      new (&list_v) TVector(other.list_v, memory_);
      return;
    case Type::Map:
      new (&map_v) TMap(other.map_v, memory_);
      return;
    case Type::TemporalData:
      this->temporal_data_v = other.temporal_data_v;
      return;
  }
}

inline PropertyValue::PropertyValue(PropertyValue &&other) noexcept : PropertyValue(std::move(other), other.memory_) {}

inline PropertyValue::PropertyValue(PropertyValue &&other, utils::MemoryResource *memory) noexcept
    : type_(other.type_), memory_{memory} {
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
      new (&string_v) TString(std::move(other.string_v), memory_);
      break;
    case Type::List:
      new (&list_v) TVector(std::move(other.list_v), memory_);
      break;
    case Type::Map:
      new (&map_v) TMap(std::move(other.map_v), memory_);
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
      new (&string_v) TString(other.string_v, memory_);
      break;
    case Type::List:
      new (&list_v) TVector(other.list_v, memory_);
      break;
    case Type::Map:
      new (&map_v) TMap(other.map_v, memory_);
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
      new (&string_v) TString(std::move(other.string_v), memory_);
      break;
    case Type::List:
      new (&list_v) TVector(std::move(other.list_v), memory_);
      break;
    case Type::Map:
      new (&map_v) TMap(std::move(other.map_v), memory_);
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

}  // namespace storage
