/// @file
#pragma once

#include <cstdint>
#include <string>

#include "database/graph_db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/typed_value.hpp"
#include "storage/common/types/types.hpp"

namespace query {

// These are the functions for parsing literals and parameter names from
// opencypher query.
int64_t ParseIntegerLiteral(const std::string &s);
std::string ParseStringLiteral(const std::string &s);
double ParseDoubleLiteral(const std::string &s);
std::string ParseParameter(const std::string &s);

/// Indicates that some part of query execution should see the OLD graph state
/// (the latest state before the current transaction+command), or NEW (state as
/// changed by the current transaction+command).
enum class GraphView { OLD, NEW };

/// Recursively reconstruct all the accessors in the given TypedValue.
///
/// @throw ReconstructionException if any reconstruction failed.
void ReconstructTypedValue(TypedValue &value);

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
  explicit TypedValueVectorCompare(const std::vector<Ordering> &ordering)
      : ordering_(ordering) {}

  template <class TAllocator>
  bool operator()(const std::vector<TypedValue, TAllocator> &c1,
                  const std::vector<TypedValue, TAllocator> &c2) const {
    // ordering is invalid if there are more elements in the collections
    // then there are in the ordering_ vector
    DCHECK(c1.size() <= ordering_.size() && c2.size() <= ordering_.size())
        << "Collections contain more elements then there are orderings";

    auto c1_it = c1.begin();
    auto c2_it = c2.begin();
    auto ordering_it = ordering_.begin();
    for (; c1_it != c1.end() && c2_it != c2.end();
         c1_it++, c2_it++, ordering_it++) {
      if (impl::TypedValueCompare(*c1_it, *c2_it))
        return *ordering_it == Ordering::ASC;
      if (impl::TypedValueCompare(*c2_it, *c1_it))
        return *ordering_it == Ordering::DESC;
    }

    // at least one collection is exhausted
    // c1 is less then c2 iff c1 reached the end but c2 didn't
    return (c1_it == c1.end()) && (c2_it != c2.end());
  }

  // TODO: Remove this, member is public
  const auto &ordering() const { return ordering_; }

  std::vector<Ordering> ordering_;
};

/// Switch the given [Vertex/Edge]Accessor to the desired state.
template <class TAccessor>
void SwitchAccessor(TAccessor &accessor, GraphView graph_view);

/// Raise QueryRuntimeException if the value for symbol isn't of expected type.
inline void ExpectType(const Symbol &symbol, const TypedValue &value,
                       TypedValue::Type expected) {
  if (value.type() != expected)
    throw QueryRuntimeException("Expected a {} for '{}', but got {}.", expected,
                                symbol.name(), value.type());
}

/// Set a property `value` mapped with given `key` on a `record`.
///
/// @throw QueryRuntimeException if value cannot be set as a property value
template <class TRecordAccessor>
void PropsSetChecked(TRecordAccessor *record, const storage::Property &key,
                     const TypedValue &value) {
  try {
    record->PropsSet(key, PropertyValue(value));
  } catch (const TypedValueException &) {
    throw QueryRuntimeException("'{}' cannot be used as a property value.",
                                value.type());
  } catch (const RecordDeletedError &) {
    throw QueryRuntimeException(
        "Trying to set properties on a deleted graph element.");
  } catch (const database::IndexConstraintViolationException &e) {
    throw QueryRuntimeException(e.what());
  }
}

}  // namespace query
