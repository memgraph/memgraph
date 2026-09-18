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

// The one law that lets a scan stand in for a sort.
//
// A plan may drop a sort when the scan feeding it already walked the column in
// the order the sort would have put it in. Whether that holds is not a property
// of either order on its own, so neither file that states an order can assert
// it. It is asked here, of drawn values, over the pairs a stored column can
// actually hand a sort.

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <compare>
#include <map>
#include <string>

#include "query/relations/orderability.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "tests/property_based/value_generators.hpp"

using memgraph::query::TypedValue;
using memgraph::storage::PropertyValue;

namespace orderability = memgraph::query::relations::orderability;

namespace {

/// Names whatever key a drawn map holds, so that reading one back does not
/// depend on which identifiers the generator happened to pick.
struct NamesEveryKey : memgraph::storage::NameIdMapper {
  std::string const &IdToName(uint64_t id) override {
    auto [entry, _] = names.try_emplace(id, std::to_string(id));
    return entry->second;
  }

  std::map<uint64_t, std::string> names;
};

/// Reads a stored value as a query one, which is what a scan hands the operator
/// above it.
TypedValue AsRead(PropertyValue const &value) {
  static NamesEveryKey mapper;
  return TypedValue(value, &mapper);
}

/// The three answers an order gives, as one value the two orders can be
/// compared on.
int Sign(std::partial_ordering order) {
  if (std::is_lt(order)) return -1;
  if (std::is_gt(order)) return 1;
  return 0;
}

int Sign(std::weak_ordering order) { return Sign(std::partial_ordering(order)); }

}  // namespace

/// Whether a sort can be handed this value.
///
/// Every stored type but one names a type a query reads back as itself. The
/// identifier a vector index keeps beside a stored vector is read back as a
/// list, so the two layers place it differently on purpose, and it is left out
/// here rather than seated with the lists: two types sharing a stretch compare
/// equivalent, which would let a vector identifier and a list collide in an
/// ordered structure. No column a sort reads holds one.
bool ASortCanBeHandedThis(PropertyValue const &value) {
  if (value.IsVectorIndexId()) return false;
  // A sort refuses a pair of maps rather than placing them, so there is no
  // position to agree with. The two layers could not reach one in any case: a
  // stored map is keyed by an identifier and a read one by a name, and nothing
  // below the query layer can see a name.
  if (value.IsMap()) return false;
  if (value.IsList()) return std::ranges::all_of(value.ValueList(), ASortCanBeHandedThis);
  if (value.IsMap()) {
    return std::ranges::all_of(value.ValueMap(), [](auto const &entry) { return ASortCanBeHandedThis(entry.second); });
  }
  return true;
}

RC_GTEST_PROP(OrderAgreement, AStoredColumnIsWalkedInTheOrderASortReadsIt, ()) {
  auto const first = *memgraph::test::generators::AnyValue();
  auto const second = *memgraph::test::generators::AnyValue();
  RC_PRE(ASortCanBeHandedThis(first) && ASortCanBeHandedThis(second));

  auto const stored = Sign(first <=> second);
  auto const sorted = Sign(orderability::Compare(AsRead(first), AsRead(second)));

  // Where the sort puts one row before another, the walk has to reach them the
  // same way round. Where it puts them in one place it has asked for nothing, so
  // the walk is free to separate two values a read cannot tell apart, and a
  // stored date carrying a part of a day is one such pair.
  if (sorted != 0) RC_ASSERT(stored == sorted);
}
