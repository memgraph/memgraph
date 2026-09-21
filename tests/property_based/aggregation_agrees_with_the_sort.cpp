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

// The law tying the two ends of a column to the order a sort reads it in.
//
// `max` over a column has to be the value `ORDER BY` puts last, and `min` the
// value it puts first. Stated over drawn columns rather than chosen ones,
// because the pair that breaks it is a pair no example reaches by accident: a
// fold reading a relation that declines to answer keeps whichever value arrived
// first, so the column has to hold such a pair *and* offer it in the losing
// order before an example shows anything.

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <vector>

#include "query/relations/comparability.hpp"
#include "query/relations/extremum.hpp"
#include "query/relations/orderability.hpp"
#include "query/typed_value.hpp"
#include "tests/property_based/typed_value_generators.hpp"

using memgraph::query::TypedValue;

namespace comparability = memgraph::query::relations::comparability;
namespace extremum = memgraph::query::relations::extremum;
namespace orderability = memgraph::query::relations::orderability;

namespace {

/// A column a sort can be handed end to end.
///
/// Orderability places every pair of unlike types, and refuses only a pair of
/// one type with no order of its own. Dropping those types drops the refusal,
/// and leaves the law asking about every column a sort answers for.
bool ASortPlacesEveryPair(TypedValue const &value) {
  switch (value.type()) {
    using enum TypedValue::Type;
    case Map:
    case Vertex:
    case Edge:
    case VirtualEdge:
    case VirtualNode:
    case Path:
    case Graph:
    case VirtualGraph:
    case Function:
      return false;
    case List:
      return std::ranges::all_of(value.ValueList(), ASortPlacesEveryPair);
    default:
      return true;
  }
}

/// A column as an aggregation sees it: `min` and `max` skip a null row, so the
/// law is about the rows that remain.
std::vector<TypedValue> WithoutTheNulls(std::vector<TypedValue> const &column) {
  auto kept = std::vector<TypedValue>{};
  std::ranges::copy_if(column, std::back_inserter(kept), [](auto const &value) { return !value.IsNull(); });
  return kept;
}

/// Folds a column the way the aggregation does, one row at a time, in the order
/// the rows arrive.
TypedValue FoldedToTheMaximum(std::vector<TypedValue> const &column) {
  auto largest = column.front();
  for (auto const &value : column) {
    if (std::is_gt(orderability::Compare(value, largest))) largest = value;
  }
  return largest;
}

TypedValue FoldedToTheMinimum(std::vector<TypedValue> const &column) {
  auto smallest = column.front();
  for (auto const &value : column) {
    if (std::is_lt(orderability::Compare(value, smallest))) smallest = value;
  }
  return smallest;
}

/// The column in the order `ORDER BY` returns it.
std::vector<TypedValue> Sorted(std::vector<TypedValue> column) {
  std::ranges::sort(column,
                    [](TypedValue const &a, TypedValue const &b) { return std::is_lt(orderability::Compare(a, b)); });
  return column;
}

/// Whether two values share a position, which is all a sort promises about the
/// row it hands back: a column holding two of them may report either.
bool ShareAPosition(TypedValue const &a, TypedValue const &b) { return std::is_eq(orderability::Compare(a, b)); }

/// Draws a value a sort places against any other, in the shapes the rest of the
/// suite draws values in.
///
/// The type is chosen from the placeable ones rather than drawn and rejected. A
/// column is only usable when every row in it is, so rejecting whole columns
/// costs a share that falls off with their length, and would leave these laws
/// asking about the short ones.
rc::Gen<TypedValue> APlaceableValue() {
  auto types = memgraph::test::generators::GraphFreeTypes();
  std::erase(types, TypedValue::Type::Map);
  return rc::gen::mapcat(rc::gen::elementOf(types), [](auto const type) {
    return rc::gen::suchThat(memgraph::test::generators::TypedValueOfType(type, 2), ASortPlacesEveryPair);
  });
}

/// Draws a column holding at least one row an aggregation would read.
rc::Gen<std::vector<TypedValue>> AColumn() {
  return rc::gen::suchThat(rc::gen::container<std::vector<TypedValue>>(APlaceableValue()),
                           [](auto const &column) { return !WithoutTheNulls(column).empty(); });
}

/// Draws a value of any type, nothing filtered out.
///
/// The laws above are asked only of columns a sort places end to end, which is
/// what lets them fold without handling a refusal. That leaves the question of
/// which columns those are unasked, and it is the question the aggregation gets
/// wrong when it reads less of a value than the comparison does.
rc::Gen<TypedValue> AnyValueAColumnCanHold() {
  return rc::gen::mapcat(rc::gen::elementOf(memgraph::test::generators::GraphFreeTypes()),
                         [](auto const type) { return memgraph::test::generators::TypedValueOfType(type, 2); });
}

/// Whether the aggregation reads a column rather than refusing it, row by row
/// as it arrives.
bool TheAggregationReads(std::vector<TypedValue> const &rows) {
  return std::ranges::none_of(rows, [](auto const &row) { return extremum::ATypeNoSortOrders(row).has_value(); });
}

}  // namespace

RC_GTEST_PROP(AggregationAgreesWithTheSort, TheMaximumIsTheRowASortPutsLast, ()) {
  auto const rows = WithoutTheNulls(*AColumn());

  auto const folded = FoldedToTheMaximum(rows);
  auto const last = Sorted(rows).back();

  RC_ASSERT(ShareAPosition(folded, last));
}

RC_GTEST_PROP(AggregationAgreesWithTheSort, TheMinimumIsTheRowASortPutsFirst, ()) {
  auto const rows = WithoutTheNulls(*AColumn());

  auto const folded = FoldedToTheMinimum(rows);
  auto const first = Sorted(rows).front();

  RC_ASSERT(ShareAPosition(folded, first));
}

RC_GTEST_PROP(AggregationAgreesWithTheSort, NeitherEndDependsOnTheOrderTheRowsArriveIn, ()) {
  // The shape the defect took: a fold that cannot place a pair keeps the value
  // it met first, so reversing the column changes the answer. A column read
  // twice has to report the same two ends both times.
  auto const rows = WithoutTheNulls(*AColumn());
  auto reversed = rows;
  std::ranges::reverse(reversed);

  RC_ASSERT(ShareAPosition(FoldedToTheMaximum(rows), FoldedToTheMaximum(reversed)));
  RC_ASSERT(ShareAPosition(FoldedToTheMinimum(rows), FoldedToTheMinimum(reversed)));
}

RC_GTEST_PROP(AggregationAgreesWithTheSort, NeitherEndIsOutsideTheColumn, ()) {
  // A fold that reports a value the column never held would satisfy the two
  // laws above wherever the column has a tie to hide it in.
  auto const rows = WithoutTheNulls(*AColumn());

  auto const holds = [&rows](TypedValue const &end) {
    return std::ranges::any_of(rows, [&end](auto const &row) { return ShareAPosition(row, end); });
  };

  RC_ASSERT(holds(FoldedToTheMaximum(rows)));
  RC_ASSERT(holds(FoldedToTheMinimum(rows)));
}

RC_GTEST_PROP(AggregationAgreesWithTheSort, AColumnItAgreesToReadIsOneItCanFinishReading, ()) {
  // What decides whether a column has two ends is asked of each row on its own,
  // so that the answer cannot turn on how many rows arrived or on the order
  // they came in. That only holds while every row it admits is one the fold can
  // place against the others: a row admitted on its own but refused against its
  // neighbour would be read as far as the second row and refused there.
  auto const rows = WithoutTheNulls(*rc::gen::container<std::vector<TypedValue>>(AnyValueAColumnCanHold()));
  RC_PRE(!rows.empty());
  RC_PRE(TheAggregationReads(rows));

  FoldedToTheMaximum(rows);
  FoldedToTheMinimum(rows);
}

TEST(AggregationAgreesWithTheSort, ReachesColumnsOnBothSidesOfTheRefusal) {
  // The law above says nothing about a column it never draws. A generator
  // drawing only columns the aggregation reads would satisfy it while leaving
  // the refusal untested, and one drawing only refused columns would satisfy it
  // having asked nothing at all.
  constexpr auto kDraws = 2'000;
  constexpr auto kLeastShare = 0.05;

  auto usable = 0;
  auto read = 0;
  auto const generator = rc::gen::container<std::vector<TypedValue>>(AnyValueAColumnCanHold());
  for (auto draw = 0; draw < kDraws; ++draw) {
    auto const column = generator(rc::Random(static_cast<std::uint64_t>(draw)), rc::kNominalSize).value();
    auto const rows = WithoutTheNulls(column);
    if (rows.empty()) continue;
    ++usable;
    if (TheAggregationReads(rows)) ++read;
  }

  ASSERT_GT(usable, kDraws / 10) << "too few draws held a row to say anything about the shares below";

  auto const share = static_cast<double>(read) / usable;
  EXPECT_GT(share, kLeastShare) << "only " << share * 100 << "% of drawn columns are read, so the law asks about few";
  EXPECT_LT(share, 1.0 - kLeastShare) << share * 100 << "% of drawn columns are read, so the refusal is barely reached";
}

TEST(AggregationAgreesWithTheSort, ReachesColumnsHoldingAPairNoComparisonPlaces) {
  // Every law here is silent over a column whose rows a comparison already
  // orders, because a fold reading either relation agrees there. The share of
  // drawn columns holding a pair that only orderability places is what decides
  // whether these laws are asking anything, so it is measured rather than
  // assumed.
  constexpr auto kDraws = 2'000;
  constexpr auto kLeastShare = 0.02;

  auto usable = 0;
  auto reached = 0;
  // Drawn unconstrained and filtered here rather than through the generator the
  // laws use: that one retries until a draw satisfies it, which a fixed seed
  // cannot do.
  auto const generator = rc::gen::container<std::vector<TypedValue>>(APlaceableValue());
  for (auto draw = 0; draw < kDraws; ++draw) {
    auto const column = generator(rc::Random(static_cast<std::uint64_t>(draw)), rc::kNominalSize).value();
    auto const rows = WithoutTheNulls(column);
    if (rows.empty()) continue;
    ++usable;

    auto const unplaced_by_a_comparison = [](TypedValue const &a, TypedValue const &b) {
      if (!comparability::Places(a) || !comparability::Places(b)) return true;
      auto const compared = comparability::Compare(a, b);
      return !compared.has_value() || *compared == std::partial_ordering::unordered;
    };
    auto holds_such_a_pair = false;
    for (auto i = 0U; i < rows.size() && !holds_such_a_pair; ++i) {
      for (auto j = i + 1; j < rows.size() && !holds_such_a_pair; ++j) {
        holds_such_a_pair = unplaced_by_a_comparison(rows[i], rows[j]);
      }
    }
    if (holds_such_a_pair) ++reached;
  }

  ASSERT_GT(usable, kDraws / 10) << "too few draws survived the filter for the share below to say anything";

  auto const share = static_cast<double>(reached) / usable;
  EXPECT_GT(share, kLeastShare) << "only " << share * 100
                                << "% of drawn columns hold a pair a comparison leaves unplaced, too few for these "
                                   "laws to be about the case that breaks them";
}
