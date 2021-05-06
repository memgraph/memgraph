/// @file
#pragma once

#include <concepts>
#include <cstdint>
#include <string>

#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "utils/logging.hpp"

namespace query {

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
concept AccessorWithSetProperty = requires(T accessor, const storage::PropertyId key,
                                           const storage::PropertyValue new_value) {
  { accessor.SetProperty(key, new_value) }
  ->std::same_as<storage::Result<storage::PropertyValue>>;
};

/// Set a property `value` mapped with given `key` on a `record`.
///
/// @throw QueryRuntimeException if value cannot be set as a property value
template <AccessorWithSetProperty T>
storage::PropertyValue PropsSetChecked(T *record, const storage::PropertyId &key, const TypedValue &value) {
  try {
    auto maybe_old_value = record->SetProperty(key, storage::PropertyValue(value));
    if (maybe_old_value.HasError()) {
      switch (maybe_old_value.GetError()) {
        case storage::Error::SERIALIZATION_ERROR:
          throw QueryRuntimeException("Can't serialize due to concurrent operations.");
        case storage::Error::DELETED_OBJECT:
          throw QueryRuntimeException("Trying to set properties on a deleted object.");
        case storage::Error::PROPERTIES_DISABLED:
          throw QueryRuntimeException("Can't set property because properties on edges are disabled.");
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::NONEXISTENT_OBJECT:
          throw QueryRuntimeException("Unexpected error when setting a property.");
      }
    }
    return std::move(*maybe_old_value);
  } catch (const TypedValueException &) {
    throw QueryRuntimeException("'{}' cannot be used as a property value.", value.type());
  }
}

}  // namespace query
