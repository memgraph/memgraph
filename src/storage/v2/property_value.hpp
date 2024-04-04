// Copyright 2024 Memgraph Ltd.
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

#include <boost/container/flat_map.hpp>

namespace memgraph::storage {

/// An exception raised by the PropertyValue. Typically when trying to perform
/// operations (such as addition) on PropertyValues of incompatible Types.
class PropertyValueException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(PropertyValueException)
};

enum class PropertyValueType : uint8_t {
  Null = 0,
  Bool = 1,
  Int = 2,
  Double = 3,
  String = 4,
  List = 5,
  Map = 6,
  TemporalData = 7
};

/// Encapsulation of a value and its type in a class that has no compile-time
/// info about the type.
///
/// Values can be of a number of predefined types that are enumerated in
/// PropertyValue::Type. Each such type corresponds to exactly one C++ type.
template <typename Alloc>
class PropertyValueImpl {
 public:
  /// A value type, each type corresponds to exactly one C++ type.
  using Type = PropertyValueType;

  using allocator_type = std::allocator_traits<Alloc>::template rebind_alloc<PropertyValueImpl>;

  using string_t = std::basic_string<char, std::char_traits<char>,
                                     typename std::allocator_traits<allocator_type>::template rebind_alloc<char>>;
  using map_t = boost::container::flat_map<
      string_t, PropertyValueImpl, std::less<>,
      typename std::allocator_traits<allocator_type>::template rebind_alloc<std::pair<string_t, PropertyValueImpl>>>;
  using list_t = std::vector<PropertyValueImpl, allocator_type>;

 private:
  template <Type type, typename T>
  struct primative_ut {
    explicit primative_ut(T val, Alloc alloc) : alloc_{alloc}, val_{val} {}
    primative_ut(primative_ut const &) = default;
    primative_ut(primative_ut &&) = default;
    primative_ut &operator=(primative_ut const &other) {
      val_ = other.val_;
      if constexpr (std::allocator_traits<Alloc>::propagate_on_container_copy_assignment::value) {
        alloc_ = other.alloc_;
      }
      return *this;
    };
    primative_ut &operator=(primative_ut &&other) noexcept {
      val_ = other.val_;
      if constexpr (std::allocator_traits<Alloc>::propagate_on_container_move_assignment ::value) {
        alloc_ = other.alloc_;
      }
      return *this;
    };

    Type type_ = type;
    [[no_unique_address]] allocator_type alloc_;
    T val_;
  };

  struct null_ut {
    explicit null_ut(Alloc alloc) : alloc_{alloc} {}
    null_ut(null_ut const &) = default;
    null_ut(null_ut &&) = default;
    null_ut &operator=(null_ut const &other) {
      if constexpr (std::allocator_traits<Alloc>::propagate_on_container_copy_assignment::value) {
        alloc_ = other.alloc_;
      }
      return *this;
    };
    null_ut &operator=(null_ut &&other) noexcept {
      if constexpr (std::allocator_traits<Alloc>::propagate_on_container_move_assignment ::value) {
        alloc_ = other.alloc_;
      }
      return *this;
    };

    Type type_ = Type::Null;
    [[no_unique_address]] allocator_type alloc_;
  };

  using bool_ut = primative_ut<Type::Bool, bool>;
  using int_ut = primative_ut<Type::Int, int64_t>;
  using double_ut = primative_ut<Type::Double, double>;
  using temporal_data_ut = primative_ut<Type::TemporalData, TemporalData>;

  struct string_ut {
    explicit string_ut(string_t val) : val_{std::move(val)} {}
    string_ut(string_ut const &) = default;
    string_ut(string_ut &&) = default;
    string_ut &operator=(string_ut const &) = default;
    string_ut &operator=(string_ut &&) = default;

    Type type_ = Type::String;
    string_t val_;
  };
  struct list_ut {
    explicit list_ut(list_t val) : val_{std::move(val)} {}
    list_ut(list_ut const &) = default;
    list_ut(list_ut &&) = default;
    list_ut &operator=(list_ut const &) = default;
    list_ut &operator=(list_ut &&) = default;

    Type type_ = Type::List;
    list_t val_;
  };
  struct map_ut {
    explicit map_ut(map_t val) : val_{std::move(val)} {}
    map_ut(map_ut const &) = default;
    map_ut(map_ut &&) = default;
    map_ut &operator=(map_ut const &) = default;
    map_ut &operator=(map_ut &&) = default;

    Type type_ = Type::Map;
    map_t val_;
  };

 public:
  static bool AreComparableTypes(Type a, Type b) {
    return (a == b) || (a == Type::Int && b == Type::Double) || (a == Type::Double && b == Type::Int);
  }

  /// Make a Null value
  PropertyValueImpl(allocator_type const &alloc = allocator_type{}) : null_v{alloc} {}

  template <typename Alloc2>
  friend class PropertyValueImpl;

  /// Copy accross allocators
  template <typename Alloc2>
  requires(!std::same_as<Alloc, Alloc2>)
      PropertyValueImpl(PropertyValueImpl<Alloc2> const &other, allocator_type const &alloc = allocator_type{}) {
    switch (other.type_) {
      case Type::Null:
        std::construct_at(&null_v, alloc);
        return;
      case Type::Bool:
        std::construct_at(&bool_v, other.bool_v.val_, alloc);
        return;
      case Type::Int:
        std::construct_at(&int_v, other.int_v.val_, alloc);
        return;
      case Type::Double:
        std::construct_at(&double_v, other.double_v.val_, alloc);
        return;
      case Type::TemporalData:
        std::construct_at(&temporal_data_v, other.temporal_data_v.val_, alloc);
        return;
      case Type::String: {
        std::construct_at(&string_v, string_t(other.string_v.val_, alloc));
        return;
      }
      case Type::List: {
        auto converted_val = list_t(alloc);
        converted_val.reserve(other.list_v.val_.size());
        for (auto const &val : other.list_v.val_) {
          converted_val.emplace_back(PropertyValueImpl{val, alloc});
        }
        std::construct_at(&list_v, std::move(converted_val));
        return;
      }
      case Type::Map: {
        auto converted_val = map_t(alloc);
        converted_val.reserve(other.map_v.val_.size());
        for (auto const &[k, v] : other.map_v.val_) {
          converted_val.emplace(string_t(k, alloc), PropertyValueImpl(v, alloc));
        }
        std::construct_at(&map_v, std::move(converted_val));
        return;
      }
    }
  }

  // constructors for primitive types
  explicit PropertyValueImpl(const bool value, allocator_type const &alloc = allocator_type{}) : bool_v{value, alloc} {}
  explicit PropertyValueImpl(const int value, allocator_type const &alloc = allocator_type{}) : int_v{value, alloc} {}
  explicit PropertyValueImpl(const int64_t value, allocator_type const &alloc = allocator_type{})
      : int_v{value, alloc} {}
  explicit PropertyValueImpl(const double value, allocator_type const &alloc = allocator_type{})
      : double_v{value, alloc} {}
  explicit PropertyValueImpl(const TemporalData value, allocator_type const &alloc = allocator_type{})
      : temporal_data_v{value, alloc} {}

  // copy constructors for non-primitive types
  /// @throw std::bad_alloc
  explicit PropertyValueImpl(string_t &&value) : string_v{std::move(value)} {}
  explicit PropertyValueImpl(string_t const &value, allocator_type const &alloc = allocator_type{})
      : string_v{string_t{value, alloc}} {}
  /// @throw std::bad_alloc
  /// @throw std::length_error if length of value exceeds
  ///        std::string::max_length().
  explicit PropertyValueImpl(std::string_view value, allocator_type const &alloc = allocator_type{})
      : string_v{string_t{value, alloc}} {}
  explicit PropertyValueImpl(char const *value, allocator_type const &alloc = allocator_type{})
      : string_v{string_t{value, alloc}} {}
  /// @throw std::bad_alloc
  explicit PropertyValueImpl(list_t &&value) : list_v{std::move(value)} {}
  explicit PropertyValueImpl(list_t const &value, allocator_type const &alloc = allocator_type{})
      : list_v{list_t(value, alloc)} {}
  /// @throw std::bad_alloc
  explicit PropertyValueImpl(map_t &&value) : map_v{std::move(value)} {}
  explicit PropertyValueImpl(map_t const &value, allocator_type const &alloc = allocator_type{})
      : map_v{map_t(value, alloc)} {}

  PropertyValueImpl(const PropertyValueImpl &other);
  PropertyValueImpl(const PropertyValueImpl &other, allocator_type const &alloc);

  PropertyValueImpl(PropertyValueImpl &&other) noexcept;

  PropertyValueImpl &operator=(const PropertyValueImpl &other);

  PropertyValueImpl &operator=(PropertyValueImpl &&other) noexcept(
      std::allocator_traits<Alloc>::is_always_equal::value ||
      std::allocator_traits<Alloc>::propagate_on_container_move_assignment::value);
  // TODO: Implement copy assignment operators for primitive types.
  // TODO: Implement copy and move assignment operators for non-primitive types.

  auto get_allocator() const -> Alloc;

  ~PropertyValueImpl();

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
    if (type_ != Type::Bool) [[unlikely]] {
      throw PropertyValueException("The value isn't a bool!");
    }
    return bool_v.val_;
  }
  /// @throw PropertyValueException if value isn't of correct type.
  int64_t ValueInt() const {
    if (type_ != Type::Int) [[unlikely]] {
      throw PropertyValueException("The value isn't an int!");
    }
    return int_v.val_;
  }
  /// @throw PropertyValueException if value isn't of correct type.
  double ValueDouble() const {
    if (type_ != Type::Double) [[unlikely]] {
      throw PropertyValueException("The value isn't a double!");
    }
    return double_v.val_;
  }

  /// @throw PropertyValueException if value isn't of correct type.
  TemporalData ValueTemporalData() const {
    if (type_ != Type::TemporalData) [[unlikely]] {
      throw PropertyValueException("The value isn't a temporal data!");
    }

    return temporal_data_v.val_;
  }

  // const value getters for non-primitive types
  /// @throw PropertyValueException if value isn't of correct type.
  auto ValueString() const -> string_t const & {
    if (type_ != Type::String) [[unlikely]] {
      throw PropertyValueException("The value isn't a string!");
    }
    return string_v.val_;
  }

  /// @throw PropertyValueException if value isn't of correct type.
  auto ValueList() const -> list_t const & {
    if (type_ != Type::List) [[unlikely]] {
      throw PropertyValueException("The value isn't a list!");
    }
    return list_v.val_;
  }

  /// @throw PropertyValueException if value isn't of correct type.
  auto ValueMap() const -> map_t const & {
    if (type_ != Type::Map) [[unlikely]] {
      throw PropertyValueException("The value isn't a map!");
    }
    return map_v.val_;
  }

  // reference value getters for non-primitive types
  /// @throw PropertyValueException if value isn't of correct type.
  auto ValueString() -> string_t & {
    if (type_ != Type::String) [[unlikely]] {
      throw PropertyValueException("The value isn't a string!");
    }
    return string_v.val_;
  }

  /// @throw PropertyValueException if value isn't of correct type.
  auto ValueList() -> list_t & {
    if (type_ != Type::List) [[unlikely]] {
      throw PropertyValueException("The value isn't a list!");
    }
    return list_v.val_;
  }

  /// @throw PropertyValueException if value isn't of correct type.
  auto ValueMap() -> map_t & {
    if (type_ != Type::Map) [[unlikely]] {
      throw PropertyValueException("The value isn't a map!");
    }
    return map_v.val_;
  }

 private:
  // NOTE: this may look strange but it is for better data layout
  //       https://eel.is/c++draft/class.union#general-note-1

  union {
    Type type_;
    null_ut null_v;
    bool_ut bool_v;
    int_ut int_v;
    double_ut double_v;
    temporal_data_ut temporal_data_v;
    string_ut string_v;
    list_ut list_v;
    map_ut map_v;
  };
};

template <typename Alloc>
inline PropertyValueImpl<Alloc>::~PropertyValueImpl() {
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
      std::destroy_at(&string_v.val_);
      return;
    case Type::List:
      std::destroy_at(&list_v.val_);
      return;
    case Type::Map:
      std::destroy_at(&map_v.val_);
      return;
  }
}

template <typename Alloc>
inline auto PropertyValueImpl<Alloc>::get_allocator() const -> Alloc {
  if constexpr (std::allocator_traits<Alloc>::is_always_equal::value) {
    return Alloc{};
  } else {
    switch (type_) {
      case Type::Null:
        return null_v.alloc_;
      case Type::Bool:
        return bool_v.alloc_;
      case Type::Int:
        return int_v.alloc_;
      case Type::Double:
        return double_v.alloc_;
      case Type::TemporalData:
        return temporal_data_v.alloc_;

      case Type::String:
        return string_v.val_.get_allocator();
      case Type::List:
        return list_v.val_.get_allocator();
      case Type::Map:
        return map_v.val_.get_allocator();
    }
  }
}

using PropertyValue = PropertyValueImpl<std::allocator<std::byte>>;

namespace pmr {
using PropertyValue = PropertyValueImpl<std::pmr::polymorphic_allocator<std::byte>>;
static_assert(std::uses_allocator_v<PropertyValue, std::pmr::polymorphic_allocator<PropertyValue>>);
}  // namespace pmr

static_assert(sizeof(PropertyValue) == 40);
static_assert(sizeof(pmr::PropertyValue) == 48);
static_assert(alignof(pmr::PropertyValue) == 8);
static_assert(alignof(PropertyValue) == 8);

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
      if (second.type() == PropertyValue::Type::Double) [[unlikely]] {
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
      if (second.type() == PropertyValue::Type::Double) [[unlikely]] {
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

template <typename Alloc>
inline PropertyValueImpl<Alloc>::PropertyValueImpl(PropertyValueImpl const &other) {
  switch (other.type_) {
    case Type::Null:
      std::construct_at(&null_v, other.null_v);
      return;
    case Type::Bool:
      std::construct_at(&bool_v, other.bool_v);
      return;
    case Type::Int:
      std::construct_at(&int_v, other.int_v);
      return;
    case Type::Double:
      std::construct_at(&double_v, other.double_v);
      return;
    case Type::String:
      std::construct_at(&string_v, other.string_v);
      return;
    case Type::List:
      std::construct_at(&list_v, other.list_v);
      return;
    case Type::Map:
      std::construct_at(&map_v, other.map_v);
      return;
    case Type::TemporalData:
      std::construct_at(&temporal_data_v, other.temporal_data_v);
      return;
  }
}

template <typename Alloc>
inline PropertyValueImpl<Alloc>::PropertyValueImpl(PropertyValueImpl const &other, allocator_type const &alloc) {
  switch (other.type_) {
    case Type::Null:
      std::construct_at(&null_v, alloc);
      return;
    case Type::Bool:
      std::construct_at(&bool_v, other.bool_v.val_, alloc);
      return;
    case Type::Int:
      std::construct_at(&int_v, other.int_v.val_, alloc);
      return;
    case Type::Double:
      std::construct_at(&double_v, other.double_v.val_, alloc);
      return;
    case Type::TemporalData:
      std::construct_at(&temporal_data_v, other.temporal_data_v.val_, alloc);
      return;
    case Type::String:
      std::construct_at(&string_v, string_t(other.string_v.val_, alloc));
      return;
    case Type::List:
      std::construct_at(&list_v, list_t(other.list_v.val_, alloc));
      return;
    case Type::Map:
      std::construct_at(&map_v, map_t(other.map_v.val_, alloc));
      return;
  }
}

template <typename Alloc>
inline PropertyValueImpl<Alloc>::PropertyValueImpl(PropertyValueImpl &&other) noexcept : type_(other.type_) {
  switch (other.type_) {
    case Type::Null:
      std::construct_at(&null_v, std::move(other.null_v));
      return;
    case Type::Bool:
      std::construct_at(&bool_v, std::move(other.bool_v));
      return;
    case Type::Int:
      std::construct_at(&int_v, std::move(other.int_v));
      return;
    case Type::Double:
      std::construct_at(&double_v, std::move(other.double_v));
      return;
    case Type::String:
      std::construct_at(&string_v, std::move(other.string_v));
      return;
    case Type::List:
      std::construct_at(&list_v, std::move(other.list_v));
      return;
    case Type::Map:
      std::construct_at(&map_v, std::move(other.map_v));
      return;
    case Type::TemporalData:
      std::construct_at(&temporal_data_v, std::move(other.temporal_data_v));
      return;
  }
}

template <typename Alloc>
inline PropertyValueImpl<Alloc> &PropertyValueImpl<Alloc>::operator=(PropertyValueImpl const &other) {
  if (type_ == other.type_) {
    if (this == &other) [[unlikely]]
      return *this;
    switch (other.type_) {
      case Type::Null:
        null_v = other.null_v;
        break;
      case Type::Bool:
        bool_v = other.bool_v;
        break;
      case Type::Int:
        int_v = other.int_v;
        break;
      case Type::Double:
        double_v = other.double_v;
        break;
      case Type::TemporalData:
        temporal_data_v = other.temporal_data_v;
        break;
      case Type::String:
        string_v.val_ = other.string_v.val_;
        break;
      case Type::List:
        list_v.val_ = other.list_v.val_;
        break;
      case Type::Map:
        map_v.val_ = other.map_v.val_;
        break;
    }
    return *this;
  } else {
    if constexpr (std::allocator_traits<Alloc>::propagate_on_container_copy_assignment::value) {
      std::destroy_at(this);
      return *std::construct_at(std::launder(this), other);
    } else {
      auto alloc = get_allocator();
      std::destroy_at(this);
      if (alloc == other.get_allocator()) {
        return *std::construct_at(std::launder(this), other);
      } else {
        return *std::construct_at(std::launder(this), other, alloc);
      }
    }
  }
}

template <typename Alloc>
inline PropertyValueImpl<Alloc> &PropertyValueImpl<Alloc>::operator=(PropertyValueImpl &&other) noexcept(
    std::allocator_traits<Alloc>::is_always_equal::value ||
    std::allocator_traits<Alloc>::propagate_on_container_move_assignment::value) {
  if (type_ == other.type_) {
    if (this == &other) [[unlikely]]
      return *this;

    switch (other.type_) {
      case Type::Null:
        null_v = std::move(other.null_v);
        break;
      case Type::Bool:
        bool_v = std::move(other.bool_v);
        break;
      case Type::Int:
        int_v = std::move(other.int_v);
        break;
      case Type::Double:
        double_v = std::move(other.double_v);
        break;
      case Type::TemporalData:
        temporal_data_v = std::move(other.temporal_data_v);
        break;
      case Type::String:
        string_v.val_ = std::move(other.string_v.val_);
        break;
      case Type::List:
        list_v.val_ = std::move(other.list_v.val_);
        break;
      case Type::Map:
        map_v.val_ = std::move(other.map_v.val_);
        break;
    }
    return *this;
  } else {
    auto alloc = get_allocator();
    std::destroy_at(this);
    if constexpr (std::allocator_traits<Alloc>::propagate_on_container_move_assignment::value) {
      return *std::construct_at(std::launder(this), std::move(other));
    } else {
      if (alloc == other.get_allocator()) {
        return *std::construct_at(std::launder(this), std::move(other));
      } else {
        return *std::construct_at(std::launder(this), other, alloc);
      }
    }
  }
}

}  // namespace memgraph::storage
