/// @file
#pragma once

#include <cstdint>
#include <string>

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/typed_value.hpp"
#include "storage/types.hpp"

#include "query/common.capnp.h"

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
  bool operator()(const std::vector<TypedValue> &c1,
                  const std::vector<TypedValue> &c2) const;

  const auto &ordering() const { return ordering_; }

  void Save(capnp::TypedValueVectorCompare::Builder *builder) const;
  void Load(const capnp::TypedValueVectorCompare::Reader &reader);

 private:
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
    record->PropsSet(key, value);
  } catch (const TypedValueException &) {
    throw QueryRuntimeException("'{}' cannot be used as a property value.",
                                value.type());
  } catch (const RecordDeletedError &) {
    throw QueryRuntimeException(
        "Trying to set properties on a deleted graph element.");
  }
}

}  // namespace query
