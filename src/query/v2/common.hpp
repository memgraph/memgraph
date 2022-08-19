// Copyright 2022 Memgraph Ltd.
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
#include <type_traits>

#include "query/v2/db_accessor.hpp"
#include "query/v2/exceptions.hpp"
#include "query/v2/frontend/ast/ast.hpp"
#include "query/v2/frontend/semantic/symbol.hpp"
#include "query/v2/typed_value.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/result.hpp"
#include "storage/v3/schema_validator.hpp"
#include "storage/v3/view.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::query::v2 {

namespace impl {
bool TypedValueCompare(const TypedValue &a, const TypedValue &b);
}  // namespace impl

/// Custom Comparator type for comparing vectors of TypedValues.
///
/// Does lexicographical ordering of elements based on the above
/// defined TypedValueCompare, and also accepts a vector of Orderings
/// the define how respective elements compare.
class TypedValueVectorCompare final {
 public:
  TypedValueVectorCompare() {}
  explicit TypedValueVectorCompare(const std::vector<Ordering> &ordering) : ordering_(ordering) {}

  template <class TAllocator>
  bool operator()(const std::vector<TypedValue, TAllocator> &c1, const std::vector<TypedValue, TAllocator> &c2) const {
    // ordering is invalid if there are more elements in the collections
    // then there are in the ordering_ vector
    MG_ASSERT(c1.size() <= ordering_.size() && c2.size() <= ordering_.size(),
              "Collections contain more elements then there are orderings");

    auto c1_it = c1.begin();
    auto c2_it = c2.begin();
    auto ordering_it = ordering_.begin();
    for (; c1_it != c1.end() && c2_it != c2.end(); c1_it++, c2_it++, ordering_it++) {
      if (impl::TypedValueCompare(*c1_it, *c2_it)) return *ordering_it == Ordering::ASC;
      if (impl::TypedValueCompare(*c2_it, *c1_it)) return *ordering_it == Ordering::DESC;
    }

    // at least one collection is exhausted
    // c1 is less then c2 iff c1 reached the end but c2 didn't
    return (c1_it == c1.end()) && (c2_it != c2.end());
  }

  // TODO: Remove this, member is public
  const auto &ordering() const { return ordering_; }

  std::vector<Ordering> ordering_;
};

/// Raise QueryRuntimeException if the value for symbol isn't of expected type.
inline void ExpectType(const Symbol &symbol, const TypedValue &value, TypedValue::Type expected) {
  if (value.type() != expected)
    throw QueryRuntimeException("Expected a {} for '{}', but got {}.", expected, symbol.name(), value.type());
}

template <typename T>
concept AccessorWithSetProperty = requires(T accessor, const storage::v3::PropertyId key,
                                           const storage::v3::PropertyValue new_value) {
  { accessor.SetProperty(key, new_value) } -> std::same_as<storage::v3::Result<storage::v3::PropertyValue>>;
};

inline void HandleSchemaViolation(const storage::v3::SchemaViolation &schema_violation, const DbAccessor &dba) {
  switch (schema_violation.status) {
    case storage::v3::SchemaViolation::ValidationStatus::VERTEX_HAS_NO_PRIMARY_PROPERTY: {
      throw SchemaViolationException(
          fmt::format("Primary key {} not defined on label :{}",
                      storage::v3::SchemaTypeToString(schema_violation.violated_schema_property->type),
                      dba.LabelToName(schema_violation.label)));
    }
    case storage::v3::SchemaViolation::ValidationStatus::NO_SCHEMA_DEFINED_FOR_LABEL: {
      throw SchemaViolationException(
          fmt::format("Label :{} is not a primary label", dba.LabelToName(schema_violation.label)));
    }
    case storage::v3::SchemaViolation::ValidationStatus::VERTEX_PROPERTY_WRONG_TYPE: {
      throw SchemaViolationException(
          fmt::format("Wrong type of property {} in schema :{}, should be of type {}",
                      *schema_violation.violated_property_value, dba.LabelToName(schema_violation.label),
                      storage::v3::SchemaTypeToString(schema_violation.violated_schema_property->type)));
    }
    case storage::v3::SchemaViolation::ValidationStatus::VERTEX_UPDATE_PRIMARY_KEY: {
      throw SchemaViolationException(fmt::format("Updating of primary key {} on schema :{} not supported",
                                                 *schema_violation.violated_property_value,
                                                 dba.LabelToName(schema_violation.label)));
    }
    case storage::v3::SchemaViolation::ValidationStatus::VERTEX_UPDATE_PRIMARY_LABEL: {
      throw SchemaViolationException(fmt::format(
          "Adding primary label as secondary or removing primary label:", *schema_violation.violated_property_value,
          dba.LabelToName(schema_violation.label)));
    }
    case storage::v3::SchemaViolation::ValidationStatus::VERTEX_SECONDARY_LABEL_IS_PRIMARY: {
      throw SchemaViolationException(fmt::format("Cannot create vertex where primary label is secondary:{}",
                                                 dba.LabelToName(schema_violation.label)));
    }
  }
}

inline void HandleErrorOnPropertyUpdate(const storage::v3::Error error) {
  switch (error) {
    case storage::v3::Error::SERIALIZATION_ERROR:
      throw TransactionSerializationException();
    case storage::v3::Error::DELETED_OBJECT:
      throw QueryRuntimeException("Trying to set properties on a deleted object.");
    case storage::v3::Error::PROPERTIES_DISABLED:
      throw QueryRuntimeException("Can't set property because properties on edges are disabled.");
    case storage::v3::Error::VERTEX_HAS_EDGES:
    case storage::v3::Error::NONEXISTENT_OBJECT:
      throw QueryRuntimeException("Unexpected error when setting a property.");
  }
}

/// Set a property `value` mapped with given `key` on a `record`.
///
/// @throw QueryRuntimeException if value cannot be set as a property value
template <AccessorWithSetProperty T>
storage::v3::PropertyValue PropsSetChecked(T *record, const DbAccessor &dba, const storage::v3::PropertyId &key,
                                           const TypedValue &value) {
  try {
    if constexpr (std::is_same_v<T, VertexAccessor>) {
      const auto maybe_old_value = record->SetPropertyAndValidate(key, storage::v3::PropertyValue(value));
      if (maybe_old_value.HasError()) {
        std::visit(utils::Overloaded{[](const storage::v3::Error error) { HandleErrorOnPropertyUpdate(error); },
                                     [&dba](const storage::v3::SchemaViolation &schema_violation) {
                                       HandleSchemaViolation(schema_violation, dba);
                                     }},
                   maybe_old_value.GetError());
      }
      return std::move(*maybe_old_value);
    } else {
      // No validation on edge properties
      const auto maybe_old_value = record->SetProperty(key, storage::v3::PropertyValue(value));
      if (maybe_old_value.HasError()) {
        HandleErrorOnPropertyUpdate(maybe_old_value.GetError());
      }
      return std::move(*maybe_old_value);
    }
  } catch (const TypedValueException &) {
    throw QueryRuntimeException("'{}' cannot be used as a property value.", value.type());
  }
}

int64_t QueryTimestamp();
}  // namespace memgraph::query::v2
