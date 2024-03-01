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

/// @file
#pragma once

#include <concepts>
#include <cstdint>
#include <string>
#include <string_view>

#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/fmt.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/typed_value.hpp"
#include "range/v3/all.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/view.hpp"
#include "utils/logging.hpp"

namespace memgraph::query {

namespace impl {
std::partial_ordering TypedValueCompare(const TypedValue &a, const TypedValue &b);

}  // namespace impl

struct OrderedTypedValueCompare {
  OrderedTypedValueCompare(Ordering ordering) : ordering_{ordering}, ascending{ordering == Ordering::ASC} {}

  auto operator()(const TypedValue &lhs, const TypedValue &rhs) const -> std::partial_ordering {
    return ascending ? impl::TypedValueCompare(lhs, rhs) : impl::TypedValueCompare(rhs, lhs);
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
    return [this]<typename TAllocator>(const std::vector<TypedValue, TAllocator> &lhs,
                                       const std::vector<TypedValue, TAllocator> &rhs) {
      auto rng = ranges::views::zip(this->orderings_, lhs, rhs);
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
concept AccessorWithSetProperty = requires(T accessor, const storage::PropertyId key,
                                           const storage::PropertyValue new_value) {
  { accessor.SetProperty(key, new_value) } -> std::same_as<storage::Result<storage::PropertyValue>>;
};

/// Set a property `value` mapped with given `key` on a `record`.
///
/// @throw QueryRuntimeException if value cannot be set as a property value
template <AccessorWithSetProperty T>
storage::PropertyValue PropsSetChecked(T *record, const storage::PropertyId &key, const TypedValue &value) {
  try {
    auto maybe_old_value = record->SetProperty(key, storage::PropertyValue(value));
    if (maybe_old_value.HasError()) {
      ProcessError(maybe_old_value.GetError());
    }
    return std::move(*maybe_old_value);
  } catch (const TypedValueException &) {
    throw QueryRuntimeException("'{}' cannot be used as a property value.", value.type());
  }
}

template <typename T>
concept AccessorWithInitProperties = requires(T accessor,
                                              const std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  { accessor.InitProperties(properties) } -> std::same_as<storage::Result<bool>>;
};

/// Set property `values` mapped with given `key` on a `record`.
///
/// @throw QueryRuntimeException if value cannot be set as a property value
template <AccessorWithInitProperties T>
bool MultiPropsInitChecked(T *record, std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  try {
    auto maybe_values = record->InitProperties(properties);
    if (maybe_values.HasError()) {
      ProcessError(maybe_values.GetError());
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
    -> std::remove_reference_t<decltype(record->UpdateProperties(properties).GetValue())> {
  try {
    auto maybe_values = record->UpdateProperties(properties);
    if (maybe_values.HasError()) {
      ProcessError(maybe_values.GetError());
    }
    return std::move(*maybe_values);
  } catch (const TypedValueException &) {
    throw QueryRuntimeException("Cannot update properties.");
  }
}

int64_t QueryTimestamp();
}  // namespace memgraph::query
