#pragma once

#include <cstdint>
#include <string>

#include "boost/serialization/serialization.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/typed_value.hpp"

namespace query {

// These are the functions for parsing literals and parameter names from
// opencypher query.
int64_t ParseIntegerLiteral(const std::string &s);
std::string ParseStringLiteral(const std::string &s);
double ParseDoubleLiteral(const std::string &s);
std::string ParseParameter(const std::string &s);

/**
 * Indicates that some part of query execution should
 * see the OLD graph state (the latest state before the
 * current transaction+command), or NEW (state as
 * changed by the current transaction+command).
 */
enum class GraphView { OLD, NEW };

/**
 * Helper function for recursively reconstructing all the accessors in the
 * given TypedValue.
 *
 * @returns - If the reconstruction succeeded.
 */
void ReconstructTypedValue(TypedValue &value);

// Custom Comparator type for comparing vectors of TypedValues.
//
// Does lexicographical ordering of elements based on the above
// defined TypedValueCompare, and also accepts a vector of Orderings
// the define how respective elements compare.
class TypedValueVectorCompare {
 public:
  TypedValueVectorCompare() {}
  explicit TypedValueVectorCompare(const std::vector<Ordering> &ordering)
      : ordering_(ordering) {}
  bool operator()(const std::vector<TypedValue> &c1,
                  const std::vector<TypedValue> &c2) const;

  const auto &ordering() const { return ordering_; }

 private:
  std::vector<Ordering> ordering_;

  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, const unsigned int) {
    ar &ordering_;
  }
  // Custom comparison for TypedValue objects.
  //
  // Behaves generally like Neo's ORDER BY comparison operator:
  //  - null is greater than anything else
  //  - primitives compare naturally, only implicit cast is int->double
  //  - (list, map, path, vertex, edge) can't compare to anything
  bool TypedValueCompare(const TypedValue &a, const TypedValue &b) const;
};
}  // namespace query
