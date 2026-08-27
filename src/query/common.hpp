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

#include "metrics/metric_handles.hpp"
#include "query/exceptions.hpp"
#include "query/fmt.hpp"
#include "query/frontend/ast/ordering.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/typed_value.hpp"
#include "query/virtual_edge.hpp"
#include "query/virtual_node.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/view.hpp"
#include "utils/logging.hpp"

namespace memgraph::query {

namespace {
/// Reads an ordering from a type that leaves no pair unordered as the total one
/// it already is. Only sound for such a type; the two that are not, a double
/// and a point, are placed by CompareDoublesNaNLast instead.
inline std::weak_ordering AlreadyTotal(std::partial_ordering order) {
  if (std::is_lt(order)) return std::weak_ordering::less;
  if (std::is_gt(order)) return std::weak_ordering::greater;
  DMG_ASSERT(order == std::partial_ordering::equivalent, "A type that orders every pair reported one unordered");
  return std::weak_ordering::equivalent;
}

inline std::weak_ordering TypedValueCompare(TypedValue const &a, TypedValue const &b);

/// Orders two paths as the alternating list of nodes and relationships each
/// runs through, from its start.
inline std::weak_ordering ComparePathsAsAlternating(Path const &lhs, Path const &rhs) {
  auto const length = [](Path const &p) { return p.vertices().size() + p.edges().size(); };
  auto const common = std::min(length(lhs), length(rhs));
  for (size_t i = 0; i != common; ++i) {
    // Even positions hold a node, odd positions the relationship after it.
    auto const order = (i % 2 == 0) ? (lhs.vertices()[i / 2].Gid().AsUint() <=> rhs.vertices()[i / 2].Gid().AsUint())
                                    : (lhs.edges()[i / 2].Gid().AsUint() <=> rhs.edges()[i / 2].Gid().AsUint());
    if (order != 0) return order;
  }
  return length(lhs) <=> length(rhs);
}

/// Where a type sits in the global sort order, which is what lets values of
/// unlike types be ordered against one another at all.
///
/// Cypher fixes this order down to NUMBER and then VOID, and requires only that
/// the types it does not name are not placed after a NaN. The ones it does not
/// name are put where Neo4j puts them, so that a query ordering a mixed column
/// answers alike on both.
///
/// @throw QueryRuntimeException for a type no order is defined over.
inline int SortRank(TypedValue::Type type) {
  switch (type) {
    using enum TypedValue::Type;
    case Map:
      return 0;
    case Vertex:
      return 1;
    case VirtualNode:
      return 2;
    case Edge:
      return 3;
    case VirtualEdge:
      return 4;
    case List:
      return 5;
    case Path:
      return 6;
    case Point2d:
      return 7;
    case Point3d:
      return 8;
    case ZonedDateTime:
      return 9;
    case LocalDateTime:
      return 10;
    case Date:
      return 11;
    case LocalTime:
      return 12;
    case Duration:
      return 13;
    case Enum:
      return 14;
    case String:
      return 15;
    case Bool:
      return 16;
    // The one rank two types share, since an integer and a double are ordered
    // against each other as numbers rather than by their types.
    case Int:
    case Double:
      return 17;
    case Null:
      return 18;

    case Graph:
    case VirtualGraph:
    case Function:
      throw QueryRuntimeException("Comparison is not defined for values of type {}.", type);
  }
}

/// Orders two values under orderability, the total order behind ORDER BY.
///
/// Unlike comparability, which the `<` family reads, this one places every pair
/// somewhere: a Null sorts after everything and alongside another Null, and a
/// list is ordered against another list element by element. What a type's own
/// values order like is not decided here; that is shared with comparability.
///
/// @throw QueryRuntimeException for a type no order is defined over.
std::weak_ordering TypedValueCompare(TypedValue const &a, TypedValue const &b) {
  // First assume typical same type comparisons
  if (a.type() == b.type()) {
    switch (a.type()) {
      using enum TypedValue::Type;
      // Null sorts alongside Null here, where comparability would refuse to say.
      case Null:
        return std::weak_ordering::equivalent;
      case List:
        return std::lexicographical_compare_three_way(a.UnsafeValueList().begin(),
                                                      a.UnsafeValueList().end(),
                                                      b.UnsafeValueList().begin(),
                                                      b.UnsafeValueList().end(),
                                                      TypedValueCompare);
      case Map: {
        // By key and then by value, in the order the keys are held, with a map
        // that runs out first coming before one that continues.
        auto const &lhs = a.UnsafeValueMap();
        auto const &rhs = b.UnsafeValueMap();
        auto lhs_it = lhs.begin();
        auto rhs_it = rhs.begin();
        for (; lhs_it != lhs.end() && rhs_it != rhs.end(); ++lhs_it, ++rhs_it) {
          if (auto const order = lhs_it->first <=> rhs_it->first; order != 0) return order;
          if (auto const order = TypedValueCompare(lhs_it->second, rhs_it->second); order != 0) return order;
        }
        return lhs.size() <=> rhs.size();
      }
      case Vertex:
        return a.UnsafeValueVertex().Gid().AsUint() <=> b.UnsafeValueVertex().Gid().AsUint();
      case Edge:
        return a.UnsafeValueEdge().Gid().AsUint() <=> b.UnsafeValueEdge().Gid().AsUint();
      case VirtualNode:
        return a.UnsafeValueVirtualNode().Gid().AsUint() <=> b.UnsafeValueVirtualNode().Gid().AsUint();
      case VirtualEdge:
        return a.UnsafeValueVirtualEdge().Gid().AsUint() <=> b.UnsafeValueVirtualEdge().Gid().AsUint();
      case Path:
        // As the list of nodes and relationships the path alternates between,
        // read from its start.
        return ComparePathsAsAlternating(a.UnsafeValuePath(), b.UnsafeValuePath());

      case Graph:
      case VirtualGraph:
      case Function:
        throw QueryRuntimeException("Comparison is not defined for values of type {}.", a.type());

      // The two types that can hold a NaN, which has to be given a place here
      // rather than left beside nothing as comparability leaves it.
      case Double:
        return TypedValue::CompareDoublesNaNLast(a.UnsafeValueDouble(), b.UnsafeValueDouble());
      case Point2d: {
        auto const &lhs = a.UnsafeValuePoint2d();
        auto const &rhs = b.UnsafeValuePoint2d();
        if (auto const order = lhs.crs() <=> rhs.crs(); order != 0) return order;
        if (auto const order = TypedValue::CompareDoublesNaNLast(lhs.x(), rhs.x()); order != 0) return order;
        return TypedValue::CompareDoublesNaNLast(lhs.y(), rhs.y());
      }
      case Point3d: {
        auto const &lhs = a.UnsafeValuePoint3d();
        auto const &rhs = b.UnsafeValuePoint3d();
        if (auto const order = lhs.crs() <=> rhs.crs(); order != 0) return order;
        if (auto const order = TypedValue::CompareDoublesNaNLast(lhs.x(), rhs.x()); order != 0) return order;
        if (auto const order = TypedValue::CompareDoublesNaNLast(lhs.y(), rhs.y()); order != 0) return order;
        return TypedValue::CompareDoublesNaNLast(lhs.z(), rhs.z());
      }

      case Bool:
      case Int:
      case String:
      case Date:
      case LocalTime:
      case LocalDateTime:
      case ZonedDateTime:
      case Duration:
      case Enum:
        // Ordered by what they hold, which comparability orders them by too.
        // Between them the two cover every type this case admits, and none of
        // them holds a value that sits outside its own order.
        if (auto const order = TypedValue::ComparePayload(a, b)) return AlreadyTotal(*order);
        return AlreadyTotal(*TypedValue::ComparePayloadOrderOnly(a, b));
    }
  }

  // Unlike types are separated by where their types sit, which puts a null
  // after everything and a map before it.
  if (auto const order = SortRank(a.type()) <=> SortRank(b.type()); order != 0) return order;

  // The one rank two types share: an integer against a double, where the double
  // may again be a NaN.
  if (a.IsInt()) {
    return TypedValue::CompareDoublesNaNLast(static_cast<double>(a.UnsafeValueInt()), b.UnsafeValueDouble());
  }
  return TypedValue::CompareDoublesNaNLast(a.UnsafeValueDouble(), static_cast<double>(b.UnsafeValueInt()));
}

}  // namespace

struct OrderedTypedValueCompare {
  OrderedTypedValueCompare(Ordering ordering) : ordering_{ordering}, ascending{ordering == Ordering::ASC} {}

  auto operator()(const TypedValue &lhs, const TypedValue &rhs) const -> std::weak_ordering {
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
        if (res == std::weak_ordering::less) return true;
        if (res == std::weak_ordering::greater) return false;
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

/// Map `storage::Error` from `VertexAccessor::Labels(...)` failures to `QueryRuntimeException`
[[noreturn]] inline void ThrowVertexLabelsReadFailure(storage::Error error) {
  switch (error) {
    case storage::Error::DELETED_OBJECT:
      throw QueryRuntimeException("Trying to get labels from a deleted node.");
    case storage::Error::NONEXISTENT_OBJECT:
      throw QueryRuntimeException("Trying to get labels from a node that doesn't exist.");
    case storage::Error::SERIALIZATION_ERROR:
    case storage::Error::VERTEX_HAS_EDGES:
    case storage::Error::PROPERTIES_DISABLED:
      throw QueryRuntimeException("Unexpected error when getting labels.");
  }
}

inline void ProcessError(const storage::Error error, metrics::DatabaseMetricHandles &metric_handles) {
  switch (error) {
    case storage::Error::SERIALIZATION_ERROR:
      metric_handles.write_write_conflicts.Increment();
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
                                       storage::NameIdMapper *name_id_mapper,
                                       metrics::DatabaseMetricHandles &metric_handles) {
  try {
    auto maybe_old_value = record->SetProperty(key, value.ToPropertyValue(name_id_mapper));
    if (!maybe_old_value) {
      ProcessError(maybe_old_value.error(), metric_handles);
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
bool MultiPropsInitChecked(T *record, std::map<storage::PropertyId, storage::PropertyValue> &properties,
                           metrics::DatabaseMetricHandles &metric_handles) {
  try {
    auto maybe_values = record->InitProperties(properties);
    if (!maybe_values) {
      ProcessError(maybe_values.error(), metric_handles);
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
auto UpdatePropertiesChecked(T *record, std::map<storage::PropertyId, storage::PropertyValue> &properties,
                             metrics::DatabaseMetricHandles &metric_handles)
    -> std::remove_reference_t<decltype(record->UpdateProperties(properties).value())> {
  try {
    auto maybe_values = record->UpdateProperties(properties);
    if (!maybe_values) {
      ProcessError(maybe_values.error(), metric_handles);
    }
    return std::move(*maybe_values);
  } catch (const TypedValueException &) {
    throw QueryRuntimeException("Cannot update properties.");
  }
}

int64_t QueryTimestamp();

auto BuildRunTimeS3Config() -> std::map<std::string, std::string, std::less<>>;

}  // namespace memgraph::query
