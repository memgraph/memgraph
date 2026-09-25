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
#include "storage/v2/property_name_order.hpp"
#include "storage/v2/property_value.hpp"
#include "tests/property_based/value_generators.hpp"

using memgraph::query::TypedValue;
using memgraph::storage::PropertyValue;

namespace orderability = memgraph::query::relations::orderability;

namespace {

/// Where the names this hands out sort, which is the order both layers place a
/// pair of maps in: the query layer reads the names and the storage layer reads
/// this.
memgraph::storage::PropertyNameOrder &TheNameOrder() {
  static memgraph::storage::PropertyNameOrder name_order;
  return name_order;
}

/// Names whatever key a drawn map holds, so that reading one back does not
/// depend on which identifiers the generator happened to pick.
///
/// A drawn identifier was never interned, so the name it gets here is the only
/// name it has, and its place in the order is recorded as the name is invented.
/// That is what the stored comparison reads.
struct NamesEveryKey : memgraph::storage::NameIdMapper {
  std::string const &IdToName(uint64_t id) override {
    auto [entry, is_new] = names.try_emplace(id, "key_" + std::to_string(id));
    if (is_new) TheNameOrder().Add(static_cast<uint32_t>(id), entry->second);
    return entry->second;
  }

  std::map<uint64_t, std::string> names;
};

NamesEveryKey &TheMapper() {
  static NamesEveryKey mapper;
  return mapper;
}

/// Reads a stored value as a query one, which is what a scan hands the operator
/// above it. Every key it holds is named here, and so takes its place in the
/// order before either layer is asked to compare anything.
TypedValue AsRead(PropertyValue const &value) { return TypedValue(value, &TheMapper()); }

/// The three answers an order gives, as one value the two orders can be
/// compared on.
int Sign(std::partial_ordering order) {
  if (std::is_lt(order)) return -1;
  if (std::is_gt(order)) return 1;
  return 0;
}

int Sign(std::weak_ordering order) { return Sign(std::partial_ordering(order)); }

/// How many types a value can be stored as, the last enumerator being the
/// highest.
constexpr auto kStoredTypeCount = static_cast<int>(memgraph::storage::PropertyValueType::VectorIndexId) + 1;

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
  if (value.IsList()) return std::ranges::all_of(value.ValueList(), ASortCanBeHandedThis);
  if (value.IsMap())
    return std::ranges::all_of(value.ValueMap(), [](auto const &entry) { return ASortCanBeHandedThis(entry.second); });
  return true;
}

RC_GTEST_PROP(OrderAgreement, AStoredColumnIsWalkedInTheOrderASortReadsIt, ()) {
  memgraph::storage::PointThisThreadAt(TheNameOrder());

  auto const first = *memgraph::test::generators::AnyValue();
  auto const second = *memgraph::test::generators::AnyValue();
  RC_PRE(ASortCanBeHandedThis(first) && ASortCanBeHandedThis(second));

  // Read back first: naming the keys is what puts them in the order, and the
  // stored comparison below reads that order.
  auto const sorted = Sign(orderability::Compare(AsRead(first), AsRead(second)));
  auto const stored = Sign(first <=> second);

  // Where the sort puts one row before another, the walk has to reach them the
  // same way round. Where it puts them in one place it has asked for nothing, so
  // the walk is free to separate two values a read cannot tell apart, and a
  // stored date carrying a part of a day is one such pair.
  if (sorted != 0) RC_ASSERT(stored == sorted);
}

TEST(OrderAgreement, HandsTheLawEnoughPairsToBeWorthAsking) {
  // A discarded pair looks exactly like a pair that passed, so a precondition
  // that grows tighter narrows what the law covers and reports nothing. The
  // share kept is measured here, drawing pairs the way the property draws them,
  // and the spread over types with it: a law reached only by strings is a law
  // about strings.
  constexpr auto kPairs = 20'000;
  constexpr auto kLeastKept = 0.5;
  constexpr auto kLeastShare = 0.02;

  auto kept = 0;
  auto placed = 0;
  auto kept_by_type = std::map<memgraph::storage::PropertyValueType, int>{};

  memgraph::storage::PointThisThreadAt(TheNameOrder());

  auto const generator = memgraph::test::generators::AnyValue();
  for (auto draw = 0; draw < kPairs; ++draw) {
    auto const at = [&](int offset) {
      auto const seed = static_cast<std::uint64_t>(draw) * 2 + static_cast<std::uint64_t>(offset);
      return generator(rc::Random(seed), rc::kNominalSize).value();
    };
    auto const first = at(0);
    auto const second = at(1);

    if (!ASortCanBeHandedThis(first) || !ASortCanBeHandedThis(second)) continue;
    ++kept;
    ++kept_by_type[first.type()];

    // The law says nothing where the sort ties, so a run kept entirely of ties
    // would assert nothing while discarding nothing.
    if (Sign(orderability::Compare(AsRead(first), AsRead(second))) != 0) ++placed;
  }

  auto const kept_share = static_cast<double>(kept) / kPairs;
  EXPECT_GT(kept_share, kLeastKept) << "the law is asked of " << kept_share * 100
                                    << "% of pairs, too few for what it covers to be clear";

  auto const placed_share = static_cast<double>(placed) / kPairs;
  EXPECT_GT(placed_share, kLeastShare) << "the sort places both rows on " << placed_share * 100
                                       << "% of pairs, too few to establish that a walk agrees with it";

  // Every stored type but the one the precondition drops should survive it, so
  // counting the types reached says whether the kept share is spread or is one
  // type standing in for the rest.
  constexpr auto kTypesASortCanBeHanded = kStoredTypeCount - 1;
  EXPECT_GE(std::ssize(kept_by_type), kTypesASortCanBeHanded)
      << "the law was reached by only " << kept_by_type.size() << " of the types a sort can be handed";
}
