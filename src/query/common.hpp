// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file
#pragma once

#include <concepts>
#include <cstdint>
#include <string>
#include <string_view>

#include <range/v3/view/zip.hpp>
#include "query/exceptions.hpp"
#include "query/fmt.hpp"
#include "query/frontend/ast/ordering.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "utils/logging.hpp"

namespace memgraph::query {

namespace {

/// Returns the orderability rank for a TypedValue type.
/// Hierarchy from least to greatest:
/// MAP < NODE < RELATIONSHIP < LIST < PATH < POINT < DATE < LOCAL TIME <
/// LOCAL DATETIME < ZONED DATETIME < DURATION < STRING < BOOLEAN < NUMBER < NULL
/// Note: Temporal types sorted by type (DATE < DATETIME), then by value within type.
constexpr int TypeOrderRank(TypedValue::Type type) {
  switch (type) {
    // MAP (lowest)
    case TypedValue::Type::Map:
      return 0;
    case TypedValue::Type::Vertex:  // NODE
      return 1;
    case TypedValue::Type::Edge:  // RELATIONSHIP
      return 2;
    case TypedValue::Type::List:
      return 3;
    case TypedValue::Type::Path:
      return 4;
    // Non-standard types (not in spec, placed after PATH)
    case TypedValue::Type::Graph:
      return 5;
    case TypedValue::Type::Function:
      return 6;
    // POINT (ordered by SRID: 4326 < 4979 < 7203 < 9157, then coordinates)
    case TypedValue::Type::Point2d:
    case TypedValue::Type::Point3d:
      return 7;
    // Temporal types: DATE < LOCAL TIME < LOCAL DATETIME < ZONED DATETIME < DURATION
    case TypedValue::Type::Date:
      return 8;
    case TypedValue::Type::LocalTime:
      return 9;
    case TypedValue::Type::LocalDateTime:
      return 10;
    case TypedValue::Type::ZonedDateTime:
      return 11;
    case TypedValue::Type::Duration:
      return 12;
    case TypedValue::Type::String:
      return 13;
    case TypedValue::Type::Enum:
      return 14;
    case TypedValue::Type::Bool:
      return 15;
    // Int and Double share rank - compared by value
    case TypedValue::Type::Int:
    case TypedValue::Type::Double:
      return 16;
    // NULL (highest - comes last)
    case TypedValue::Type::Null:
      return 17;
  }
}

std::partial_ordering TypedValueCompare(TypedValue const &a, TypedValue const &b) {
  const auto type_a = a.type();
  const auto type_b = b.type();

  if (type_a == type_b) [[likely]] {
    switch (type_a) {
      case TypedValue::Type::Null:
        return std::partial_ordering::equivalent;
      case TypedValue::Type::Bool:
        return a.UnsafeValueBool() <=> b.UnsafeValueBool();
      case TypedValue::Type::Int:
        return a.UnsafeValueInt() <=> b.UnsafeValueInt();
      case TypedValue::Type::Double:
        return a.UnsafeValueDouble() <=> b.UnsafeValueDouble();
      case TypedValue::Type::String:
        return a.UnsafeValueString() <=> b.UnsafeValueString();
      case TypedValue::Type::Date:
        return a.UnsafeValueDate() <=> b.UnsafeValueDate();
      case TypedValue::Type::LocalTime:
        return a.UnsafeValueLocalTime() <=> b.UnsafeValueLocalTime();
      case TypedValue::Type::LocalDateTime:
        return a.UnsafeValueLocalDateTime() <=> b.UnsafeValueLocalDateTime();
      case TypedValue::Type::ZonedDateTime:
        return a.UnsafeValueZonedDateTime() <=> b.UnsafeValueZonedDateTime();
      case TypedValue::Type::Duration:
        return a.UnsafeValueDuration() <=> b.UnsafeValueDuration();
      case TypedValue::Type::Enum:
        return a.UnsafeValueEnum() <=> b.UnsafeValueEnum();
      case TypedValue::Type::Point2d:
        return a.UnsafeValuePoint2d() <=> b.UnsafeValuePoint2d();
      case TypedValue::Type::Point3d:
        return a.UnsafeValuePoint3d() <=> b.UnsafeValuePoint3d();
        break;
      case TypedValue::Type::List:
        return std::lexicographical_compare_three_way(a.UnsafeValueList().begin(),
                                                      a.UnsafeValueList().end(),
                                                      b.UnsafeValueList().begin(),
                                                      b.UnsafeValueList().end(),
                                                      TypedValueCompare);
      case TypedValue::Type::Map: {
        // Maps ordering: 1) by size, 2) by keys alphabetically, 3) by values
        const auto &map_a = a.UnsafeValueMap();
        const auto &map_b = b.UnsafeValueMap();
        if (map_a.size() != map_b.size()) {
          return map_a.size() <=> map_b.size();
        }
        auto it_a = map_a.begin();
        auto it_b = map_b.begin();
        // maps have same size so this is safe
        while (it_a != map_a.end()) {
          auto key_cmp = it_a->first <=> it_b->first;
          if (key_cmp != std::strong_ordering::equal) {
            return key_cmp;
          }
          ++it_a;
          ++it_b;
        }
        it_a = map_a.begin();
        it_b = map_b.begin();
        while (it_a != map_a.end()) {
          auto val_cmp = TypedValueCompare(it_a->second, it_b->second);
          if (val_cmp != std::partial_ordering::equivalent) {
            return val_cmp;
          }
          ++it_a;
          ++it_b;
        }
        return std::partial_ordering::equivalent;
      }
      case TypedValue::Type::Vertex:
        return a.ValueVertex().Gid() <=> b.ValueVertex().Gid();
      case TypedValue::Type::Edge:
        return a.ValueEdge().Gid() <=> b.ValueEdge().Gid();
      case TypedValue::Type::Path: {
        const auto &path_a = a.ValuePath();
        const auto &path_b = b.ValuePath();
        const auto &verts_a = path_a.vertices();
        const auto &verts_b = path_b.vertices();
        const auto &edges_a = path_a.edges();
        const auto &edges_b = path_b.edges();
        const auto min_edges = std::min(edges_a.size(), edges_b.size());
        for (size_t i = 0; i < min_edges; ++i) {
          // Compare vertex i
          auto v_cmp = verts_a[i].Gid() <=> verts_b[i].Gid();
          if (v_cmp != std::strong_ordering::equal) {
            return v_cmp;
          }
          // Compare edge i
          auto e_cmp = edges_a[i].Gid() <=> edges_b[i].Gid();
          if (e_cmp != std::strong_ordering::equal) {
            return e_cmp;
          }
        }
        // Compare the vertex after the last common edge
        if (min_edges < verts_a.size() && min_edges < verts_b.size()) {
          auto v_cmp = verts_a[min_edges].Gid() <=> verts_b[min_edges].Gid();
          if (v_cmp != std::strong_ordering::equal) {
            return v_cmp;
          }
        }
        return edges_a.size() <=> edges_b.size();
      }
      case TypedValue::Type::Graph:
      case TypedValue::Type::Function:
        return std::partial_ordering::equivalent;
    }
  }

  // Different types: handle common cases before computing ranks
  if (type_a == TypedValue::Type::Int && type_b == TypedValue::Type::Double) {
    return a.UnsafeValueInt() <=> b.UnsafeValueDouble();
  }
  if (type_a == TypedValue::Type::Double && type_b == TypedValue::Type::Int) {
    return a.UnsafeValueDouble() <=> b.UnsafeValueInt();
  }

  // Point2d vs Point3d (same rank, compare by SRID)
  if (type_a == TypedValue::Type::Point2d && type_b == TypedValue::Type::Point3d) {
    return storage::CrsToSrid(a.UnsafeValuePoint2d().crs()) <=> storage::CrsToSrid(b.UnsafeValuePoint3d().crs());
  }
  if (type_a == TypedValue::Type::Point3d && type_b == TypedValue::Type::Point2d) {
    return storage::CrsToSrid(a.UnsafeValuePoint3d().crs()) <=> storage::CrsToSrid(b.UnsafeValuePoint2d().crs());
  }

  // All other different types: compare by rank
  return TypeOrderRank(type_a) <=> TypeOrderRank(type_b);
}

}  // namespace

struct OrderedTypedValueCompare {
  OrderedTypedValueCompare(Ordering ordering) : ordering_{ordering}, ascending{ordering == Ordering::ASC} {}

  auto operator()(const TypedValue &lhs, const TypedValue &rhs) const -> std::partial_ordering {
    return ascending ? TypedValueCompare(lhs, rhs) : TypedValueCompare(rhs, lhs);
  }

  auto ordering() const { return ordering_; }

 private:
  Ordering ordering_;
  bool ascending = true;
};

/// Custom Comparator type for comparing vectors of TypedValues.
///
/// Does lexicographical ordering of elements based on the above
/// defined TypedValueCompare, and also accepts a vector of Orderings
/// the define how respective elements compare.
class TypedValueVectorCompare final {
 public:
  TypedValueVectorCompare() = default;

  explicit TypedValueVectorCompare(std::vector<OrderedTypedValueCompare> orderings)
      : orderings_{std::move(orderings)} {}

  const auto &orderings() const { return orderings_; }

  auto lex_cmp() const {
    return [orderings = &orderings_]<typename TAllocator>(const std::vector<TypedValue, TAllocator> &lhs,
                                                          const std::vector<TypedValue, TAllocator> &rhs) {
      auto rng = ranges::views::zip(*orderings, lhs, rhs);
      for (auto const &[cmp, l, r] : rng) {
        auto res = cmp(l, r);
        if (res == std::partial_ordering::less) return true;
        if (res == std::partial_ordering::greater) return false;
      }
      DMG_ASSERT(orderings->size() == lhs.size() && lhs.size() == rhs.size());
      return false;
    };
  }

 private:
  std::vector<OrderedTypedValueCompare> orderings_;
};

/// Raise QueryRuntimeException if the value for symbol isn't of expected type.
inline void ExpectType(const Symbol &symbol, const TypedValue &value, TypedValue::Type expected) {
  if (value.type() != expected) [[unlikely]] {
    throw QueryRuntimeException("Expected a {} for '{}', but got {}.", expected, symbol.name(), value.type());
  }
}

inline void ProcessError(const storage::Error error) {
  switch (error) {
    case storage::Error::SERIALIZATION_ERROR:
      throw TransactionSerializationException();
    case storage::Error::DELETED_OBJECT:
      throw QueryRuntimeException("Trying to set properties on a deleted object.");
    case storage::Error::PROPERTIES_DISABLED:
      throw QueryRuntimeException("Can't set property because properties on edges are disabled.");
    case storage::Error::VERTEX_HAS_EDGES:
    case storage::Error::NONEXISTENT_OBJECT:
      throw QueryRuntimeException("Unexpected error when setting a property.");
  }
}

template <typename T>
concept AccessorWithSetProperty =
    requires(T accessor, const storage::PropertyId key, const storage::PropertyValue &new_value) {
      { accessor.SetProperty(key, new_value) } -> std::same_as<storage::Result<storage::PropertyValue>>;
    };

/// Set a property `value` mapped with given `key` on a `record`.
///
/// @throw QueryRuntimeException if value cannot be set as a property value
template <AccessorWithSetProperty T>
storage::PropertyValue PropsSetChecked(T *record, const storage::PropertyId &key, const TypedValue &value,
                                       storage::NameIdMapper *name_id_mapper) {
  try {
    auto maybe_old_value = record->SetProperty(key, value.ToPropertyValue(name_id_mapper));
    if (!maybe_old_value) {
      ProcessError(maybe_old_value.error());
    }
    return std::move(*maybe_old_value);
  } catch (const TypedValueException &) {
    throw QueryRuntimeException("'{}' cannot be used as a property value.", value.type());
  }
}

template <typename T>
concept AccessorWithInitProperties =
    requires(T accessor, std::map<storage::PropertyId, storage::PropertyValue> &properties) {
      { accessor.InitProperties(properties) } -> std::same_as<storage::Result<bool>>;
    };

/// Set property `values` mapped with given `key` on a `record`.
///
/// @throw QueryRuntimeException if value cannot be set as a property value
template <AccessorWithInitProperties T>
bool MultiPropsInitChecked(T *record, std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  try {
    auto maybe_values = record->InitProperties(properties);
    if (!maybe_values) {
      ProcessError(maybe_values.error());
    }
    return std::move(*maybe_values);
  } catch (const TypedValueException &) {
    throw QueryRuntimeException("Cannot set properties.");
  }
}

template <typename T>
concept AccessorWithUpdateProperties = requires(T accessor,
                                                std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  {
    accessor.UpdateProperties(properties)
  } -> std::same_as<
      storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>>;
};

/// Set property `values` mapped with given `key` on a `record`.
///
/// @throw QueryRuntimeException if value cannot be set as a property value
template <AccessorWithUpdateProperties T>
auto UpdatePropertiesChecked(T *record, std::map<storage::PropertyId, storage::PropertyValue> &properties)
    -> std::remove_reference_t<decltype(record->UpdateProperties(properties).value())> {
  try {
    auto maybe_values = record->UpdateProperties(properties);
    if (!maybe_values) {
      ProcessError(maybe_values.error());
    }
    return std::move(*maybe_values);
  } catch (const TypedValueException &) {
    throw QueryRuntimeException("Cannot update properties.");
  }
}

int64_t QueryTimestamp();

auto BuildRunTimeS3Config() -> std::map<std::string, std::string, std::less<>>;

}  // namespace memgraph::query
