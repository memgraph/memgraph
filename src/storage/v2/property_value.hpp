// Copyright 2025 Memgraph Ltd.
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

#include <iosfwd>
#include <string>
#include <vector>

#include "storage/v2/enum.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"
#include "utils/fnv.hpp"

#include <boost/container/flat_map.hpp>
#include "range/v3/all.hpp"

namespace memgraph::storage {

/// An exception raised by the PropertyValue. Typically when trying to perform
/// operations (such as addition) on PropertyValues of incompatible Types.
class PropertyValueException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(PropertyValueException)
};

// These are durable, do not chage there values
enum class PropertyValueType : uint8_t {
  Null = 0,
  Bool = 1,
  Int = 2,
  Double = 3,
  String = 4,
  List = 5,
  Map = 6,
  TemporalData = 7,
  ZonedTemporalData = 8,
  Enum = 9,
  Point2d = 10,
  Point3d = 11,
};

inline bool AreComparableTypes(PropertyValueType a, PropertyValueType b) {
  return (a == b) || (a == PropertyValueType::Int && b == PropertyValueType::Double) ||
         (a == PropertyValueType::Double && b == PropertyValueType::Int);
}

/// Encapsulation of a value and its type in a class that has no compile-time
/// info about the type.
///
/// Values can be of a number of predefined types that are enumerated in
/// PropertyValue::Type. Each such type corresponds to exactly one C++ type.
template <typename Alloc>
class PropertyValueImpl {
 public:
  using allocator_type = Alloc;
  using alloc_trait = std::allocator_traits<allocator_type>;

  /// A value type, each type corresponds to exactly one C++ type.
  using Type = PropertyValueType;

  using string_t = std::basic_string<char, std::char_traits<char>, typename alloc_trait::template rebind_alloc<char>>;

  using map_t =
      boost::container::flat_map<PropertyId, PropertyValueImpl, std::less<>,
                                 typename alloc_trait::template rebind_alloc<std::pair<PropertyId, PropertyValueImpl>>>;

  using list_t = std::vector<PropertyValueImpl, typename alloc_trait::template rebind_alloc<PropertyValueImpl>>;

  /// Make a Null value
  PropertyValueImpl(allocator_type const &alloc = allocator_type{}) : alloc_{alloc}, type_(Type::Null) {}

  // constructors for primitive types
  explicit PropertyValueImpl(const bool value, allocator_type const &alloc = allocator_type{})
      : alloc_{alloc}, bool_v{.val_ = value} {}
  explicit PropertyValueImpl(const int value, allocator_type const &alloc = allocator_type{})
      : alloc_{alloc}, int_v{.val_ = value} {}
  explicit PropertyValueImpl(const int64_t value, allocator_type const &alloc = allocator_type{})
      : alloc_{alloc}, int_v{.val_ = value} {}
  explicit PropertyValueImpl(const double value, allocator_type const &alloc = allocator_type{})
      : alloc_{alloc}, double_v{.val_ = value} {}
  explicit PropertyValueImpl(const TemporalData value, allocator_type const &alloc = allocator_type{})
      : alloc_{alloc}, temporal_data_v{.val_ = value} {}
  explicit PropertyValueImpl(const ZonedTemporalData value, allocator_type const &alloc = allocator_type{})
      : alloc_{alloc}, zoned_temporal_data_v{.val_ = value} {}
  explicit PropertyValueImpl(const Enum value, allocator_type const &alloc = allocator_type{})
      : alloc_{alloc}, enum_data_v{.val_ = value} {}
  explicit PropertyValueImpl(const Point2d value, allocator_type const &alloc = allocator_type{})
      : alloc_{alloc}, point2d_data_v{.val_ = value} {}
  explicit PropertyValueImpl(const Point3d value, allocator_type const &alloc = allocator_type{})
      : alloc_{alloc}, point3d_data_v{.val_ = value} {}

  // copy constructors for non-primitive types
  /// @throw std::bad_alloc
  explicit PropertyValueImpl(string_t const &value) : alloc_{value.get_allocator()}, string_v{.val_ = value} {}
  explicit PropertyValueImpl(string_t &&value) : alloc_{value.get_allocator()}, string_v{.val_ = std::move(value)} {}
  explicit PropertyValueImpl(string_t const &value, allocator_type const &alloc)
      : alloc_{alloc}, string_v{.val_ = string_t{value, alloc}} {}
  explicit PropertyValueImpl(string_t &&value, allocator_type const &alloc)
      : alloc_{alloc}, string_v{.val_ = string_t{std::move(value), alloc}} {}

  /// @throw std::bad_alloc
  /// @throw std::length_error if length of value exceeds
  ///        std::string::max_length().
  explicit PropertyValueImpl(std::string_view value, allocator_type const &alloc = allocator_type{})
      : alloc_{alloc}, string_v{.val_ = string_t{value, alloc}} {}
  explicit PropertyValueImpl(char const *value, allocator_type const &alloc = allocator_type{})
      : alloc_{alloc}, string_v{.val_ = string_t{value, alloc}} {}

  /// @throw std::bad_alloc
  explicit PropertyValueImpl(list_t const &value) : alloc_{value.get_allocator()}, list_v{.val_ = value} {}
  explicit PropertyValueImpl(list_t &&value) : alloc_{value.get_allocator()}, list_v{.val_ = std::move(value)} {}
  explicit PropertyValueImpl(list_t const &value, allocator_type const &alloc)
      : alloc_{alloc}, list_v{.val_ = list_t{value, alloc}} {}
  explicit PropertyValueImpl(list_t &&value, allocator_type const &alloc)
      : alloc_{alloc}, list_v{.val_ = list_t{std::move(value), alloc}} {}

  /// @throw std::bad_alloc
  explicit PropertyValueImpl(map_t const &value) : alloc_{value.get_allocator()}, map_v{.val_ = value} {}
  explicit PropertyValueImpl(map_t &&value) : alloc_{value.get_allocator()}, map_v{.val_ = std::move(value)} {}
  explicit PropertyValueImpl(map_t const &value, allocator_type const &alloc)
      : alloc_{alloc}, map_v{.val_ = map_t{value, alloc}} {}
  explicit PropertyValueImpl(map_t &&value, allocator_type const &alloc)
      : alloc_{alloc}, map_v{.val_ = map_t{std::move(value), alloc}} {}

  // copy constructor
  /// @throw std::bad_alloc
  PropertyValueImpl(const PropertyValueImpl &other);
  PropertyValueImpl(const PropertyValueImpl &other, allocator_type const &alloc);

  // move constructor
  PropertyValueImpl(PropertyValueImpl &&other) noexcept;
  PropertyValueImpl(PropertyValueImpl &&other, allocator_type const &alloc) noexcept;

  // copy assignment
  /// @throw std::bad_alloc
  PropertyValueImpl &operator=(const PropertyValueImpl &other);

  // move assignment
  PropertyValueImpl &operator=(PropertyValueImpl &&other) noexcept(
      alloc_trait::is_always_equal::value || alloc_trait::propagate_on_container_move_assignment::value);

  // TODO: Implement copy assignment operators for primitive types.
  // TODO: Implement copy and move assignment operators for non-primitive types.

  template <typename AllocOther>
  friend class PropertyValueImpl;

  /// Copy accross allocators
  template <typename AllocOther>
  requires(!std::same_as<allocator_type, AllocOther>)
      PropertyValueImpl(PropertyValueImpl<AllocOther> const &other, allocator_type const &alloc = allocator_type{})
      : alloc_{alloc}, type_{other.type_} {
    switch (other.type_) {
      case Type::Null:
        return;
      case Type::Bool:
        bool_v.val_ = other.bool_v.val_;
        return;
      case Type::Int:
        int_v.val_ = other.int_v.val_;
        return;
      case Type::Double:
        double_v.val_ = other.double_v.val_;
        return;
      case Type::String:
        alloc_trait::construct(alloc_, &string_v.val_, other.string_v.val_);
        return;
      case Type::List:
        alloc_trait::construct(alloc_, &list_v.val_, other.list_v.val_.begin(), other.list_v.val_.end());
        return;
      case Type::Map:
        alloc_trait::construct(alloc_, &map_v.val_, other.map_v.val_.begin(), other.map_v.val_.end());
        return;
      case Type::TemporalData:
        temporal_data_v.val_ = other.temporal_data_v.val_;
        return;
      case Type::ZonedTemporalData:
        zoned_temporal_data_v.val_ = other.zoned_temporal_data_v.val_;
        return;
      case Type::Enum:
        enum_data_v.val_ = other.enum_data_v.val_;
        return;
      case Type::Point2d:
        point2d_data_v.val_ = other.point2d_data_v.val_;
        return;
      case Type::Point3d:
        point3d_data_v.val_ = other.point3d_data_v.val_;
        return;
    }
  }

  ~PropertyValueImpl() {
    switch (type_) {
      // destructor for primitive types does nothing
      case Type::Null:
      case Type::Bool:
      case Type::Int:
      case Type::Double:
      case Type::TemporalData:
      case Type::ZonedTemporalData:
        // Do nothing: std::chrono::time_zone* pointers reference immutable values from the external tz DB
      case Type::Enum:
      case Type::Point2d:
      case Type::Point3d:
        return;
      // destructor for non primitive types since we used placement new
      case Type::String:
        alloc_trait::destroy(alloc_, &string_v.val_);
        return;
      case Type::List:
        alloc_trait::destroy(alloc_, &list_v.val_);
        return;
      case Type::Map:
        alloc_trait::destroy(alloc_, &map_v.val_);
        return;
    }
  }

  Type type() const { return type_; }

  // type checkers
  bool IsNull() const { return type_ == Type::Null; }
  bool IsBool() const { return type_ == Type::Bool; }
  bool IsInt() const { return type_ == Type::Int; }
  bool IsDouble() const { return type_ == Type::Double; }
  bool IsString() const { return type_ == Type::String; }
  bool IsList() const { return type_ == Type::List; }
  bool IsMap() const { return type_ == Type::Map; }
  bool IsEnum() const { return type_ == Type::Enum; }
  bool IsTemporalData() const { return type_ == Type::TemporalData; }
  bool IsZonedTemporalData() const { return type_ == Type::ZonedTemporalData; }
  bool IsPoint2d() const { return type_ == Type::Point2d; }
  bool IsPoint3d() const { return type_ == Type::Point3d; }

  // value getters for primitive types
  /// @throw PropertyValueException if value isn't of correct type.
  bool ValueBool() const {
    if (type_ != Type::Bool) [[unlikely]] {
      throw PropertyValueException("The value isn't a bool!");
    }
    return bool_v.val_;
  }
  /// @throw PropertyValueException if value isn't of correct type.
  auto ValueInt() const -> int64_t {
    if (type_ != Type::Int) [[unlikely]] {
      throw PropertyValueException("The value isn't an int!");
    }
    return int_v.val_;
  }
  /// @throw PropertyValueException if value isn't of correct type.
  auto ValueDouble() const -> double {
    if (type_ != Type::Double) [[unlikely]] {
      throw PropertyValueException("The value isn't a double!");
    }
    return double_v.val_;
  }

  /// @throw PropertyValueException if value isn't of correct type.
  auto ValueTemporalData() const -> TemporalData {
    if (type_ != Type::TemporalData) [[unlikely]] {
      throw PropertyValueException("The value isn't a temporal data!");
    }

    return temporal_data_v.val_;
  }

  auto ValueZonedTemporalData() const -> ZonedTemporalData {
    if (type_ != Type::ZonedTemporalData) [[unlikely]] {
      throw PropertyValueException("The value isn't a zoned temporal datum!");
    }

    return zoned_temporal_data_v.val_;
  }

  auto ValueEnum() const -> Enum {
    if (type_ != Type::Enum) [[unlikely]] {
      throw PropertyValueException("The value isn't an enum!");
    }

    return enum_data_v.val_;
  }

  auto ValuePoint2d() const -> Point2d {
    if (type_ != Type::Point2d) [[unlikely]] {
      throw PropertyValueException("The value isn't a 2d point!");
    }

    return point2d_data_v.val_;
  }

  auto ValuePoint3d() const -> Point3d {
    if (type_ != Type::Point3d) [[unlikely]] {
      throw PropertyValueException("The value isn't a 3d point!");
    }

    return point3d_data_v.val_;
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
  [[no_unique_address]] allocator_type alloc_;

  // NOTE: this may look strange but it is for better data layout
  //       https://eel.is/c++draft/class.union#general-note-1
  union {
    Type type_;
    struct {
      Type type_ = Type::Bool;
      bool val_;
    } bool_v;
    struct {
      Type type_ = Type::Int;
      int64_t val_;
    } int_v;
    struct {
      Type type_ = Type::Double;
      double val_;
    } double_v;
    struct {
      Type type_ = Type::String;
      string_t val_;
    } string_v;
    struct {
      Type type_ = Type::List;
      list_t val_;
    } list_v;
    struct {
      Type type_ = Type::Map;
      map_t val_;
    } map_v;
    struct {
      Type type_ = Type::TemporalData;
      TemporalData val_;
    } temporal_data_v;
    struct {
      Type type_ = Type::ZonedTemporalData;
      ZonedTemporalData val_;
    } zoned_temporal_data_v;  // FYI: current largest member at 40B
    struct {
      Type type_ = Type::Enum;
      Enum val_;
    } enum_data_v;
    struct {
      Type type_ = Type::Point2d;
      Point2d val_;
    } point2d_data_v;
    struct {
      Type type_ = Type::Point3d;
      Point3d val_;
    } point3d_data_v;
  };
};

using PropertyValue = PropertyValueImpl<std::allocator<std::byte>>;
namespace pmr {
using PropertyValue = PropertyValueImpl<std::pmr::polymorphic_allocator<std::byte>>;
}

struct ExtendedPropertyType {
  PropertyValueType type{PropertyValueType::Null};
  TemporalType temporal_type{};
  EnumTypeId enum_type{};

  ExtendedPropertyType() {}
  explicit ExtendedPropertyType(PropertyValueType type) : type{type} {}
  explicit ExtendedPropertyType(TemporalType temporal_type)
      : type{PropertyValueType::TemporalData}, temporal_type{temporal_type} {}
  explicit ExtendedPropertyType(EnumTypeId enum_type) : type{PropertyValueType::Enum}, enum_type{enum_type} {}
  explicit ExtendedPropertyType(const PropertyValue &val) : type{val.type()} {
    if (type == PropertyValueType::TemporalData) {
      temporal_type = val.ValueTemporalData().type;
    }
    if (type == PropertyValueType::Enum) {
      enum_type = val.ValueEnum().type_id();
    }
  }

  bool operator==(const ExtendedPropertyType &other) const {
    return type == other.type && temporal_type == other.temporal_type && enum_type == other.enum_type;
  }
};

static_assert(sizeof(PropertyValue) == 40);
static_assert(sizeof(pmr::PropertyValue) == 56);

// NOTE: The logic in this function *MUST* be equal to the logic in
// `PropertyStore::ComparePropertyValue`. If you change this operator make sure
// to change the function so that they have identical functionality.
template <typename Alloc, typename Alloc2>
inline auto operator<=>(const PropertyValueImpl<Alloc> &first, const PropertyValueImpl<Alloc2> &second) noexcept
    -> std::weak_ordering {
  if (!AreComparableTypes(first.type(), second.type())) return first.type() <=> second.type();

  auto to_weak_order = [](std::partial_ordering o) {
    if (o == std::partial_ordering::equivalent) {
      return std::weak_ordering::equivalent;
    }
    if (o == std::partial_ordering::less) {
      return std::weak_ordering::less;
    }
    if (o == std::partial_ordering::greater) {
      return std::weak_ordering::greater;
    }
    // DANGER: TODO: check is this possible and what it should mean
    return std::weak_ordering::less;
  };

  switch (first.type()) {
    case PropertyValueType::Null:
      return std::weak_ordering::equivalent;
    case PropertyValueType::Bool:
      return first.ValueBool() <=> second.ValueBool();
    case PropertyValueType::Int:
      if (second.type() == PropertyValueType::Int) {
        return first.ValueInt() <=> second.ValueInt();
      } else {
        return to_weak_order(first.ValueInt() <=> second.ValueDouble());
      }
    case PropertyValueType::Double:
      if (second.type() == PropertyValueType::Double) {
        return to_weak_order(first.ValueDouble() <=> second.ValueDouble());
      } else {
        return to_weak_order(first.ValueDouble() <=> second.ValueInt());
      }
    case PropertyValueType::String:
      // using string_view for allocator agnostic compare
      return std::string_view{first.ValueString()} <=> second.ValueString();
    case PropertyValueType::List: {
      auto const &l1 = first.ValueList();
      auto const &l2 = second.ValueList();
      auto const three_way_cmp = [](PropertyValueImpl<Alloc> const &v1, PropertyValueImpl<Alloc2> const &v2) {
        return v1 <=> v2;
      };
      return std::lexicographical_compare_three_way(l1.begin(), l1.end(), l2.begin(), l2.end(), three_way_cmp);
    }
    case PropertyValueType::Map: {
      auto const &m1 = first.ValueMap();
      auto const &m2 = second.ValueMap();
      if (m1.size() != m2.size()) return m1.size() <=> m2.size();
      for (auto &&[v1, v2] : ranges::views::zip(m1, m2)) {
        auto key_cmp_res = v1.first <=> v2.first;
        if (key_cmp_res != std::weak_ordering::equivalent) return key_cmp_res;
        auto val_cmp_res = v1.second <=> v2.second;
        if (val_cmp_res != std::weak_ordering::equivalent) return val_cmp_res;
      }
      return std::weak_ordering::equivalent;
    }
    case PropertyValueType::TemporalData:
      return first.ValueTemporalData() <=> second.ValueTemporalData();
    case PropertyValueType::ZonedTemporalData:
      return first.ValueZonedTemporalData() <=> second.ValueZonedTemporalData();
    case PropertyValueType::Enum:
      return first.ValueEnum() <=> second.ValueEnum();
    case PropertyValueType::Point2d:
      return to_weak_order(first.ValuePoint2d() <=> second.ValuePoint2d());
    case PropertyValueType::Point3d:
      return to_weak_order(first.ValuePoint3d() <=> second.ValuePoint3d());
  }
}

template <typename Alloc, typename Alloc2>
inline bool operator==(const PropertyValueImpl<Alloc> &first, const PropertyValueImpl<Alloc2> &second) noexcept {
  return is_eq(first <=> second);
}

template <typename Alloc>
inline PropertyValueImpl<Alloc>::PropertyValueImpl(const PropertyValueImpl &other)
    : PropertyValueImpl{other, other.alloc_} {}

template <typename Alloc>
inline PropertyValueImpl<Alloc>::PropertyValueImpl(const PropertyValueImpl &other, allocator_type const &alloc)
    : alloc_{alloc}, type_(other.type_) {
  switch (other.type_) {
    case Type::Null:
      return;
    case Type::Bool:
      bool_v.val_ = other.bool_v.val_;
      return;
    case Type::Int:
      int_v.val_ = other.int_v.val_;
      return;
    case Type::Double:
      double_v.val_ = other.double_v.val_;
      return;
    case Type::String:
      alloc_trait::construct(alloc_, &string_v.val_, other.string_v.val_);
      return;
    case Type::List:
      alloc_trait::construct(alloc_, &list_v.val_, other.list_v.val_);
      return;
    case Type::Map:
      alloc_trait::construct(alloc_, &map_v.val_, other.map_v.val_);
      return;
    case Type::TemporalData:
      temporal_data_v.val_ = other.temporal_data_v.val_;
      return;
    case Type::ZonedTemporalData:
      zoned_temporal_data_v.val_ = other.zoned_temporal_data_v.val_;
      return;
    case Type::Enum:
      enum_data_v.val_ = other.enum_data_v.val_;
      return;
    case Type::Point2d:
      point2d_data_v.val_ = other.point2d_data_v.val_;
      return;
    case Type::Point3d:
      point3d_data_v.val_ = other.point3d_data_v.val_;
      return;
  }
}

template <typename Alloc>
inline PropertyValueImpl<Alloc>::PropertyValueImpl(PropertyValueImpl &&other) noexcept
    : PropertyValueImpl{std::move(other), other.alloc_} {}

template <typename Alloc>
inline PropertyValueImpl<Alloc>::PropertyValueImpl(PropertyValueImpl &&other, allocator_type const &alloc) noexcept
    : alloc_{alloc}, type_(other.type_) {
  switch (type_) {
    case Type::Null:
      break;
    case Type::Bool:
      bool_v.val_ = other.bool_v.val_;
      break;
    case Type::Int:
      int_v.val_ = other.int_v.val_;
      break;
    case Type::Double:
      double_v.val_ = other.double_v.val_;
      break;
    case Type::String:
      alloc_trait::construct(alloc_, &string_v.val_, std::move(other.string_v.val_));
      break;
    case Type::List:
      alloc_trait::construct(alloc_, &list_v.val_, std::move(other.list_v.val_));
      break;
    case Type::Map:
      alloc_trait::construct(alloc_, &map_v.val_, std::move(other.map_v.val_));
      break;
    case Type::TemporalData:
      temporal_data_v.val_ = other.temporal_data_v.val_;
      break;
    case Type::ZonedTemporalData:
      zoned_temporal_data_v.val_ = other.zoned_temporal_data_v.val_;
      break;
    case Type::Enum:
      enum_data_v.val_ = other.enum_data_v.val_;
      break;
    case Type::Point2d:
      point2d_data_v.val_ = other.point2d_data_v.val_;
      break;
    case Type::Point3d:
      point3d_data_v.val_ = other.point3d_data_v.val_;
      break;
  }
}

template <typename Alloc>
inline auto PropertyValueImpl<Alloc>::operator=(PropertyValueImpl const &other) -> PropertyValueImpl & {
  auto do_copy = [&]() -> PropertyValueImpl<allocator_type> & {
    // if same type try assignment
    if (type_ == other.type_) {
      if (this == &other) return *this;
      switch (other.type_) {
        case Type::Null:
          break;
        case Type::Bool:
          bool_v.val_ = other.bool_v.val_;
          break;
        case Type::Int:
          int_v.val_ = other.int_v.val_;
          break;
        case Type::Double:
          double_v.val_ = other.double_v.val_;
          break;
        case Type::String:
          string_v.val_ = string_t{other.string_v.val_, alloc_};
          break;
        case Type::List:
          list_v.val_ = list_t(other.list_v.val_, alloc_);
          break;
        case Type::Map:
          map_v.val_ = map_t(other.map_v.val_, alloc_);
          break;
        case Type::TemporalData:
          temporal_data_v.val_ = other.temporal_data_v.val_;
          break;
        case Type::ZonedTemporalData:
          zoned_temporal_data_v.val_ = other.zoned_temporal_data_v.val_;
          break;
        case Type::Enum:
          enum_data_v.val_ = other.enum_data_v.val_;
          break;
        case Type::Point2d:
          point2d_data_v.val_ = other.point2d_data_v.val_;
          break;
        case Type::Point3d:
          point3d_data_v.val_ = other.point3d_data_v.val_;
          break;
      }
      return *this;
    } else {
      alloc_trait::destroy(alloc_, this);
      try {
        auto *new_this = std::launder(this);
        alloc_trait::construct(alloc_, new_this, other);
        return *new_this;
      } catch (...) {
        type_ = Type::Null;
        throw;
      }
    }
  };

  if constexpr (alloc_trait::is_always_equal::value) {
    return do_copy();
  } else {
    if (other.alloc_ == alloc_) {
      return do_copy();
    } else {
      if constexpr (alloc_trait::propagate_on_container_copy_assignment::value) {
        auto oldalloc = alloc_;
        try {
          alloc_ = other.alloc_;
          return do_copy();
        } catch (...) {
          alloc_ = oldalloc;
          throw;
        }
      }
      return do_copy();
    }
  }
}

template <typename Alloc>
inline auto PropertyValueImpl<Alloc>::operator=(PropertyValueImpl &&other) noexcept(
    alloc_trait::is_always_equal::value || alloc_trait::propagate_on_container_move_assignment::value)
    -> PropertyValueImpl<allocator_type> & {
  auto do_move = [&]() -> PropertyValueImpl<allocator_type> & {
    if (type_ == other.type_) {
      // maybe the same object, check if no work is required
      if (this == &other) return *this;

      switch (type_) {
        case Type::Null:
          break;
        case Type::Bool:
          bool_v.val_ = other.bool_v.val_;
          break;
        case Type::Int:
          int_v.val_ = other.int_v.val_;
          break;
        case Type::Double:
          double_v.val_ = other.double_v.val_;
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
        case Type::TemporalData:
          temporal_data_v.val_ = other.temporal_data_v.val_;
          break;
        case Type::ZonedTemporalData:
          zoned_temporal_data_v.val_ = other.zoned_temporal_data_v.val_;
          break;
        case Type::Enum:
          enum_data_v.val_ = other.enum_data_v.val_;
          break;
        case Type::Point2d:
          point2d_data_v.val_ = other.point2d_data_v.val_;
          break;
        case Type::Point3d:
          point3d_data_v.val_ = other.point3d_data_v.val_;
          break;
      }
      return *this;
    } else {
      alloc_trait::destroy(alloc_, this);
      try {
        auto *new_this = std::launder(this);
        alloc_trait::construct(alloc_, new_this, std::move(other));
        return *new_this;
      } catch (...) {
        type_ = Type::Null;
        throw;
      }
    }
  };

  if constexpr (alloc_trait::is_always_equal::value) {
    return do_move();
  } else {
    if (other.alloc_ == alloc_) {
      return do_move();
    } else {
      if constexpr (alloc_trait::propagate_on_container_move_assignment::value) {
        std::swap(alloc_, other.alloc_);
        return do_move();  //???
      } else {
        // fall back to copy
        return operator=(other);
      }
    }
  }
}

template <typename Alloc>
class IntermediatePropertyValueImpl {
 public:
  using allocator_type = Alloc;
  using alloc_trait = std::allocator_traits<allocator_type>;
  using string_t = typename PropertyValueImpl<Alloc>::string_t;

  /// A value type, each type corresponds to exactly one C++ type.
  using Type = PropertyValueType;

  using list_t = std::vector<IntermediatePropertyValueImpl,
                             typename alloc_trait::template rebind_alloc<IntermediatePropertyValueImpl>>;

  using map_t = boost::container::flat_map<
      string_t, IntermediatePropertyValueImpl, std::less<>,
      typename alloc_trait::template rebind_alloc<std::pair<string_t, IntermediatePropertyValueImpl>>>;

  IntermediatePropertyValueImpl(allocator_type const &alloc = {}) : alloc_(alloc), type_(PropertyValueType::Null) {}

  IntermediatePropertyValueImpl(bool b, allocator_type const &alloc = {}) : alloc_(alloc), bool_v{.val_ = b} {}

  IntermediatePropertyValueImpl(int64_t i, allocator_type const &alloc = {}) : alloc_(alloc), int_v{.val_ = i} {}

  IntermediatePropertyValueImpl(int i, allocator_type const &alloc = {}) : alloc_(alloc), int_v{.val_ = i} {}

  IntermediatePropertyValueImpl(double d, allocator_type const &alloc = {}) : alloc_(alloc), double_v{.val_ = d} {}

  IntermediatePropertyValueImpl(string_t s, allocator_type const &alloc = {})
      : alloc_(alloc), string_v{.val_ = std::move(s)} {}

  IntermediatePropertyValueImpl(list_t l, allocator_type const &alloc = {})
      : alloc_(alloc), list_v{.val_ = std::move(l)} {}

  IntermediatePropertyValueImpl(map_t m, allocator_type const &alloc = {})
      : alloc_(alloc), map_v{.val_ = std::move(m)} {}

  IntermediatePropertyValueImpl(TemporalData td, allocator_type const &alloc = {})
      : alloc_(alloc), temporal_data_v{.val_ = td} {}

  IntermediatePropertyValueImpl(ZonedTemporalData ztd, allocator_type const &alloc = {})
      : alloc_(alloc), zoned_temporal_data_v{.val_ = ztd} {}

  IntermediatePropertyValueImpl(Enum e, allocator_type const &alloc = {}) : alloc_(alloc), enum_data_v{.val_ = e} {}

  IntermediatePropertyValueImpl(Point2d p2d, allocator_type const &alloc = {})
      : alloc_(alloc), point2d_data_v{.val_ = p2d} {}

  IntermediatePropertyValueImpl(Point3d p3d, allocator_type const &alloc = {})
      : alloc_(alloc), point3d_data_v{.val_ = p3d} {}

  inline IntermediatePropertyValueImpl(const IntermediatePropertyValueImpl &other)
      : IntermediatePropertyValueImpl{other, other.alloc_} {}

  inline IntermediatePropertyValueImpl(const IntermediatePropertyValueImpl &other, allocator_type const &alloc)
      : alloc_{alloc}, type_(other.type_) {
    switch (other.type_) {
      case Type::Null:
        return;
      case Type::Bool:
        bool_v.val_ = other.bool_v.val_;
        return;
      case Type::Int:
        int_v.val_ = other.int_v.val_;
        return;
      case Type::Double:
        double_v.val_ = other.double_v.val_;
        return;
      case Type::String:
        alloc_trait::construct(alloc_, &string_v.val_, other.string_v.val_);
        return;
      case Type::List:
        alloc_trait::construct(alloc_, &list_v.val_, other.list_v.val_);
        return;
      case Type::Map:
        alloc_trait::construct(alloc_, &map_v.val_, other.map_v.val_);
        return;
      case Type::TemporalData:
        temporal_data_v.val_ = other.temporal_data_v.val_;
        return;
      case Type::ZonedTemporalData:
        zoned_temporal_data_v.val_ = other.zoned_temporal_data_v.val_;
        return;
      case Type::Enum:
        enum_data_v.val_ = other.enum_data_v.val_;
        return;
      case Type::Point2d:
        point2d_data_v.val_ = other.point2d_data_v.val_;
        return;
      case Type::Point3d:
        point3d_data_v.val_ = other.point3d_data_v.val_;
        return;
    }
  }

  inline IntermediatePropertyValueImpl(IntermediatePropertyValueImpl &&other) noexcept
      : IntermediatePropertyValueImpl{std::move(other), other.alloc_} {}

  inline IntermediatePropertyValueImpl(IntermediatePropertyValueImpl &&other, allocator_type const &alloc) noexcept
      : alloc_{alloc}, type_(other.type_) {
    switch (type_) {
      case Type::Null:
        break;
      case Type::Bool:
        bool_v.val_ = other.bool_v.val_;
        break;
      case Type::Int:
        int_v.val_ = other.int_v.val_;
        break;
      case Type::Double:
        double_v.val_ = other.double_v.val_;
        break;
      case Type::String:
        alloc_trait::construct(alloc_, &string_v.val_, std::move(other.string_v.val_));
        break;
      case Type::List:
        alloc_trait::construct(alloc_, &list_v.val_, std::move(other.list_v.val_));
        break;
      case Type::Map:
        alloc_trait::construct(alloc_, &map_v.val_, std::move(other.map_v.val_));
        break;
      case Type::TemporalData:
        temporal_data_v.val_ = other.temporal_data_v.val_;
        break;
      case Type::ZonedTemporalData:
        zoned_temporal_data_v.val_ = other.zoned_temporal_data_v.val_;
        break;
      case Type::Enum:
        enum_data_v.val_ = other.enum_data_v.val_;
        break;
      case Type::Point2d:
        point2d_data_v.val_ = other.point2d_data_v.val_;
        break;
      case Type::Point3d:
        point3d_data_v.val_ = other.point3d_data_v.val_;
        break;
    }
  }

  inline auto operator=(IntermediatePropertyValueImpl const &other) -> IntermediatePropertyValueImpl & {
    auto do_copy = [&]() -> IntermediatePropertyValueImpl<allocator_type> & {
      // if same type try assignment
      if (type_ == other.type_) {
        if (this == &other) return *this;
        switch (other.type_) {
          case Type::Null:
            break;
          case Type::Bool:
            bool_v.val_ = other.bool_v.val_;
            break;
          case Type::Int:
            int_v.val_ = other.int_v.val_;
            break;
          case Type::Double:
            double_v.val_ = other.double_v.val_;
            break;
          case Type::String:
            string_v.val_ = string_t{other.string_v.val_, alloc_};
            break;
          case Type::List:
            list_v.val_ = list_t(other.list_v.val_, alloc_);
            break;
          case Type::Map:
            map_v.val_ = map_t(other.map_v.val_, alloc_);
            break;
          case Type::TemporalData:
            temporal_data_v.val_ = other.temporal_data_v.val_;
            break;
          case Type::ZonedTemporalData:
            zoned_temporal_data_v.val_ = other.zoned_temporal_data_v.val_;
            break;
          case Type::Enum:
            enum_data_v.val_ = other.enum_data_v.val_;
            break;
          case Type::Point2d:
            point2d_data_v.val_ = other.point2d_data_v.val_;
            break;
          case Type::Point3d:
            point3d_data_v.val_ = other.point3d_data_v.val_;
            break;
        }
        return *this;
      } else {
        alloc_trait::destroy(alloc_, this);
        try {
          auto *new_this = std::launder(this);
          alloc_trait::construct(alloc_, new_this, other);
          return *new_this;
        } catch (...) {
          type_ = Type::Null;
          throw;
        }
      }
    };

    if constexpr (alloc_trait::is_always_equal::value) {
      return do_copy();
    } else {
      if (other.alloc_ == alloc_) {
        return do_copy();
      } else {
        if constexpr (alloc_trait::propagate_on_container_copy_assignment::value) {
          auto oldalloc = alloc_;
          try {
            alloc_ = other.alloc_;
            return do_copy();
          } catch (...) {
            alloc_ = oldalloc;
            throw;
          }
        }
        return do_copy();
      }
    }
  }

  inline auto operator=(IntermediatePropertyValueImpl &&other) noexcept(
      alloc_trait::is_always_equal::value || alloc_trait::propagate_on_container_move_assignment::value)
      -> IntermediatePropertyValueImpl<allocator_type> & {
    auto do_move = [&]() -> IntermediatePropertyValueImpl<allocator_type> & {
      if (type_ == other.type_) {
        // maybe the same object, check if no work is required
        if (this == &other) return *this;

        switch (type_) {
          case Type::Null:
            break;
          case Type::Bool:
            bool_v.val_ = other.bool_v.val_;
            break;
          case Type::Int:
            int_v.val_ = other.int_v.val_;
            break;
          case Type::Double:
            double_v.val_ = other.double_v.val_;
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
          case Type::TemporalData:
            temporal_data_v.val_ = other.temporal_data_v.val_;
            break;
          case Type::ZonedTemporalData:
            zoned_temporal_data_v.val_ = other.zoned_temporal_data_v.val_;
            break;
          case Type::Enum:
            enum_data_v.val_ = other.enum_data_v.val_;
            break;
          case Type::Point2d:
            point2d_data_v.val_ = other.point2d_data_v.val_;
            break;
          case Type::Point3d:
            point3d_data_v.val_ = other.point3d_data_v.val_;
            break;
        }
        return *this;
      } else {
        alloc_trait::destroy(alloc_, this);
        try {
          auto *new_this = std::launder(this);
          alloc_trait::construct(alloc_, new_this, std::move(other));
          return *new_this;
        } catch (...) {
          type_ = Type::Null;
          throw;
        }
      }
    };

    if constexpr (alloc_trait::is_always_equal::value) {
      return do_move();
    } else {
      if (other.alloc_ == alloc_) {
        return do_move();
      } else {
        if constexpr (alloc_trait::propagate_on_container_move_assignment::value) {
          std::swap(alloc_, other.alloc_);
          return do_move();  //???
        } else {
          // fall back to copy
          return operator=(other);
        }
      }
    }
  }

  ~IntermediatePropertyValueImpl() {
    switch (type_) {
      // destructor for primitive types does nothing
      case Type::Null:
      case Type::Bool:
      case Type::Int:
      case Type::Double:
      case Type::TemporalData:
      case Type::ZonedTemporalData:
        // Do nothing: std::chrono::time_zone* pointers reference immutable values from the external tz DB
      case Type::Enum:
      case Type::Point2d:
      case Type::Point3d:
        return;
      // destructor for non primitive types since we used placement new
      case Type::String:
        alloc_trait::destroy(alloc_, &string_v.val_);
        return;
      case Type::List:
        alloc_trait::destroy(alloc_, &list_v.val_);
        return;
      case Type::Map:
        alloc_trait::destroy(alloc_, &map_v.val_);
        return;
    }
  }

  Type type() const { return type_; }

  // type checkers
  bool IsNull() const { return type_ == Type::Null; }
  bool IsBool() const { return type_ == Type::Bool; }
  bool IsInt() const { return type_ == Type::Int; }
  bool IsDouble() const { return type_ == Type::Double; }
  bool IsString() const { return type_ == Type::String; }
  bool IsList() const { return type_ == Type::List; }
  bool IsMap() const { return type_ == Type::Map; }
  bool IsEnum() const { return type_ == Type::Enum; }
  bool IsTemporalData() const { return type_ == Type::TemporalData; }
  bool IsZonedTemporalData() const { return type_ == Type::ZonedTemporalData; }
  bool IsPoint2d() const { return type_ == Type::Point2d; }
  bool IsPoint3d() const { return type_ == Type::Point3d; }

  // value getters for primitive types
  /// @throw PropertyValueException if value isn't of correct type.
  bool ValueBool() const {
    if (type_ != Type::Bool) [[unlikely]] {
      throw PropertyValueException("The value isn't a bool!");
    }
    return bool_v.val_;
  }
  /// @throw PropertyValueException if value isn't of correct type.
  auto ValueInt() const -> int64_t {
    if (type_ != Type::Int) [[unlikely]] {
      throw PropertyValueException("The value isn't an int!");
    }
    return int_v.val_;
  }
  /// @throw PropertyValueException if value isn't of correct type.
  auto ValueDouble() const -> double {
    if (type_ != Type::Double) [[unlikely]] {
      throw PropertyValueException("The value isn't a double!");
    }
    return double_v.val_;
  }

  /// @throw PropertyValueException if value isn't of correct type.
  auto ValueTemporalData() const -> TemporalData {
    if (type_ != Type::TemporalData) [[unlikely]] {
      throw PropertyValueException("The value isn't a temporal data!");
    }

    return temporal_data_v.val_;
  }

  auto ValueZonedTemporalData() const -> ZonedTemporalData {
    if (type_ != Type::ZonedTemporalData) [[unlikely]] {
      throw PropertyValueException("The value isn't a zoned temporal datum!");
    }

    return zoned_temporal_data_v.val_;
  }

  auto ValueEnum() const -> Enum {
    if (type_ != Type::Enum) [[unlikely]] {
      throw PropertyValueException("The value isn't an enum!");
    }

    return enum_data_v.val_;
  }

  auto ValuePoint2d() const -> Point2d {
    if (type_ != Type::Point2d) [[unlikely]] {
      throw PropertyValueException("The value isn't a 2d point!");
    }

    return point2d_data_v.val_;
  }

  auto ValuePoint3d() const -> Point3d {
    if (type_ != Type::Point3d) [[unlikely]] {
      throw PropertyValueException("The value isn't a 3d point!");
    }

    return point3d_data_v.val_;
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

  // Conversion to final PropertyValueImpl
  PropertyValueImpl<Alloc> ToFinalValue(NameIdMapper *mapper, allocator_type const &alloc = {},
                                        std::string_view previous_map_key = {}) const {
    switch (type_) {
      case Type::Null:
        return PropertyValueImpl<Alloc>(alloc);
      case Type::Bool:
        return PropertyValueImpl<Alloc>(bool_v.val_, alloc);
      case Type::Int:
        return PropertyValueImpl<Alloc>(int_v.val_, alloc);
      case Type::Double:
        return PropertyValueImpl<Alloc>(double_v.val_, alloc);
      case Type::String:
        return PropertyValueImpl<Alloc>(string_v.val_, alloc);
      case Type::List: {
        typename PropertyValueImpl<Alloc>::list_t list(alloc);
        for (const auto &elem : list_v.val_) {
          list.push_back(elem.ToFinalValue(mapper, alloc));
        }
        return PropertyValueImpl<Alloc>(std::move(list), alloc);
      }
      case Type::Map: {
        typename PropertyValueImpl<Alloc>::map_t map(alloc);
        for (const auto &[key, val] : map_v.val_) {
          std::string_view key_view = key;
          if (!previous_map_key.empty()) {
            key_view = fmt::format("{}.{}", previous_map_key, key);
          }
          auto prop_id = PropertyId::FromUint(mapper->NameToId(key_view));
          map.emplace(prop_id, val.ToFinalValue(mapper, alloc, key_view));
        }
        return PropertyValueImpl<Alloc>(std::move(map), alloc);
      }
      case Type::TemporalData:
        return PropertyValueImpl<Alloc>(temporal_data_v.val_, alloc);
      case Type::ZonedTemporalData:
        return PropertyValueImpl<Alloc>(zoned_temporal_data_v.val_, alloc);
      case Type::Enum:
        return PropertyValueImpl<Alloc>(enum_data_v.val_, alloc);
      case Type::Point2d:
        return PropertyValueImpl<Alloc>(point2d_data_v.val_, alloc);
      case Type::Point3d:
        return PropertyValueImpl<Alloc>(point3d_data_v.val_, alloc);
      default:
        throw PropertyValueException("Unknown type during conversion");
    }
  }

 private:
  allocator_type alloc_;
  union {
    Type type_;
    struct {
      Type type_ = Type::Bool;
      bool val_;
    } bool_v;
    struct {
      Type type_ = Type::Int;
      int64_t val_;
    } int_v;
    struct {
      Type type_ = Type::Double;
      double val_;
    } double_v;
    struct {
      Type type_ = Type::String;
      string_t val_;
    } string_v;
    struct {
      Type type_ = Type::List;
      list_t val_;
    } list_v;
    struct {
      Type type_ = Type::Map;
      map_t val_;
    } map_v;
    struct {
      Type type_ = Type::TemporalData;
      TemporalData val_;
    } temporal_data_v;
    struct {
      Type type_ = Type::ZonedTemporalData;
      ZonedTemporalData val_;
    } zoned_temporal_data_v;  // FYI: current largest member at 40B
    struct {
      Type type_ = Type::Enum;
      Enum val_;
    } enum_data_v;
    struct {
      Type type_ = Type::Point2d;
      Point2d val_;
    } point2d_data_v;
    struct {
      Type type_ = Type::Point3d;
      Point3d val_;
    } point3d_data_v;
  };
};

template <typename T>
concept IsPropertyValueLike = requires(const T &v) {
  { v.type() } -> std::same_as<PropertyValueType>;
  {v.ValueBool()};
  {v.ValueInt()};
  {v.ValueDouble()};
  {v.ValueString()};
  {v.ValueList()};
  {v.ValueMap()};
  {v.ValueTemporalData()};
  {v.ValueZonedTemporalData()};
  {v.ValueEnum()};
  {v.ValuePoint2d()};
  {v.ValuePoint3d()};
};

// stream output
/// @throw anything std::ostream::operator<< may throw.
inline std::ostream &operator<<(std::ostream &os, const PropertyValueType type) {
  switch (type) {
    case PropertyValueType::Null:
      return os << "null";
    case PropertyValueType::Bool:
      return os << "bool";
    case PropertyValueType::Int:
      return os << "int";
    case PropertyValueType::Double:
      return os << "double";
    case PropertyValueType::String:
      return os << "string";
    case PropertyValueType::List:
      return os << "list";
    case PropertyValueType::Map:
      return os << "map";
    case PropertyValueType::TemporalData:
      return os << "temporal data";
    case PropertyValueType::ZonedTemporalData:
      return os << "zoned temporal data";
    case PropertyValueType::Enum:
      return os << "enum";
    case PropertyValueType::Point2d:
      return os << "point";
    case PropertyValueType::Point3d:
      return os << "point";
  }
}

/// @throw anything std::ostream::operator<< may throw.
template <IsPropertyValueLike T>
inline std::ostream &operator<<(std::ostream &os, const T &value) {
  // These can appear in log messages
  switch (value.type()) {
    case PropertyValueType::Null:
      return os << "null";
    case PropertyValueType::Bool:
      return os << (value.ValueBool() ? "true" : "false");
    case PropertyValueType::Int:
      return os << value.ValueInt();
    case PropertyValueType::Double:
      return os << value.ValueDouble();
    case PropertyValueType::String:
      return os << "\"" << value.ValueString() << "\"";
    case PropertyValueType::List:
      os << "[";
      utils::PrintIterable(os, value.ValueList());
      return os << "]";
    case PropertyValueType::Map:
      os << "{";
      utils::PrintIterable(os, value.ValueMap(), ", ",
                           [](auto &stream, const auto &pair) { stream << pair.first << ": " << pair.second; });
      return os << "}";
    case PropertyValueType::TemporalData:
      return os << fmt::format("type: {}, microseconds: {}", TemporalTypeToString(value.ValueTemporalData().type),
                               value.ValueTemporalData().microseconds);
    case PropertyValueType::ZonedTemporalData: {
      auto const &temp_value = value.ValueZonedTemporalData();
      return os << fmt::format("type: {}, microseconds: {}, timezone: {}", ZonedTemporalTypeToString(temp_value.type),
                               temp_value.IntMicroseconds(), temp_value.TimezoneToString());
    }
    case PropertyValueType::Enum: {
      auto const &[e_type, e_value] = value.ValueEnum();

      return os << fmt::format("{{ type: {}, value: {} }}", e_type.value_of(), e_value.value_of());
    }
    case PropertyValueType::Point2d: {
      const auto point = value.ValuePoint2d();
      return os << fmt::format("point({{ x:{}, y:{}, srid:{} }})", point.x(), point.y(),
                               CrsToSrid(point.crs()).value_of());
    }
    case PropertyValueType::Point3d: {
      const auto point = value.ValuePoint3d();
      return os << fmt::format("point({{ x:{}, y:{}, z:{}, srid:{} }})", point.x(), point.y(), point.z(),
                               CrsToSrid(point.crs()).value_of());
    }
  }
}

using IntermediatePropertyValue = IntermediatePropertyValueImpl<std::allocator<std::byte>>;

}  // namespace memgraph::storage
namespace std {

template <>
struct hash<memgraph::storage::ExtendedPropertyType> {
  size_t operator()(const memgraph::storage::ExtendedPropertyType &type) const {
    size_t seed = 0;
    boost::hash_combine(seed, type.type);
    boost::hash_combine(seed, type.temporal_type);
    boost::hash_combine(seed, type.enum_type.value_of());
    return seed;
  }
};

template <>
struct hash<memgraph::storage::PropertyValue> {
  size_t operator()(memgraph::storage::PropertyValue const &value) const noexcept {
    using enum memgraph::storage::PropertyValueType;

    // Hashing here based on the choices made when we hash TypedValues
    switch (value.type()) {
      case Null:
        return 31;
      case Bool:
        return std::hash<bool>{}(value.ValueBool());
      case Int:
        return std::hash<double>{}(static_cast<double>(value.ValueInt()));
      case Double:
        return std::hash<double>{}(value.ValueDouble());
      case String:
        return std::hash<std::string_view>{}(value.ValueString());
      case List: {
        return memgraph::utils::FnvCollection<memgraph::storage::PropertyValue::list_t,
                                              memgraph::storage::PropertyValue>{}(value.ValueList());
      }
      case Map: {
        size_t hash = 6543457;
        for (const auto &kv : value.ValueMap()) {
          hash ^= std::hash<memgraph::storage::PropertyId>{}(kv.first);
          hash ^= this->operator()(kv.second);
        }
        return hash;
      }
      case TemporalData:
        return std::hash<memgraph::storage::TemporalData>{}(value.ValueTemporalData());
      case ZonedTemporalData:
        return std::hash<memgraph::storage::ZonedTemporalData>{}(value.ValueZonedTemporalData());
      case Enum:
        return std::hash<memgraph::storage::Enum>{}(value.ValueEnum());
      case Point2d:
        return std::hash<memgraph::storage::Point2d>{}(value.ValuePoint2d());
      case Point3d:
        return std::hash<memgraph::storage::Point3d>{}(value.ValuePoint3d());
    }
  }
};

}  // namespace std
