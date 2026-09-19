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

// The laws the encoding a main sends a replica obeys, asked over generated
// values.
//
// A value written to the stream has to arrive as the value that was written,
// and what is asked here is identity rather than equality: the value together
// with the type it was spelled as.
//
// Equality is the weaker question because it holds across types. A list of
// integers is held four ways, and each is equal to the others element for
// element, so an encoding that sent a packed list and read back a boxed one
// would keep every equality the value had while losing what it was. Equality
// also holds between the two numeric types wherever a double carries the
// integer exactly, which leaves the small integers unasked.

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <variant>

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include "slk_common.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/replication/slk.hpp"
#include "value_generators.hpp"

namespace {

using memgraph::storage::ExternalPropertyValue;
using memgraph::storage::PropertyValue;
using memgraph::storage::PropertyValueType;

/// Answers with a name for every identifier it is asked about, so that a drawn
/// map can be rewritten in the terms the wire uses. The mapper a database owns
/// knows only the identifiers it has issued, and a drawn one has issued none.
struct NamesEveryKey : memgraph::storage::NameIdMapper {
  std::string const &IdToName(uint64_t id) override {
    auto [entry, _] = names.try_emplace(id, std::to_string(id));
    return entry->second;
  }

  std::map<uint64_t, std::string> names;
};

ExternalPropertyValue AsSent(PropertyValue const &value) {
  static NamesEveryKey mapper;
  return ToExternalPropertyValue(value, &mapper);
}

/// Whether two doubles are the same double. Every NaN counts as one, which is
/// the question a round trip is owed: one sent has to arrive as one.
bool SameDouble(double sent, double arrived) { return (std::isnan(sent) && std::isnan(arrived)) || sent == arrived; }

/// Whether two values are the same value, spelled the same way.
///
/// The type is asked first and separately, because equality holds across the
/// two numeric types and across the four ways a list of numbers is held. A
/// round trip that changed which of those a value arrived as would keep every
/// equality it had and still not be a round trip.
///
/// Elements are compared the same way rather than by the container's equality,
/// which would compare them by the equality this is avoiding.
bool Identical(ExternalPropertyValue const &sent, ExternalPropertyValue const &arrived) {
  if (sent.type() != arrived.type()) return false;

  switch (sent.type()) {
    case ExternalPropertyValue::Type::Double:
      return SameDouble(sent.ValueDouble(), arrived.ValueDouble());
    case ExternalPropertyValue::Type::List: {
      auto const &one = sent.ValueList();
      auto const &other = arrived.ValueList();
      return one.size() == other.size() && std::ranges::equal(one, other, Identical);
    }
    case ExternalPropertyValue::Type::Map: {
      auto const &one = sent.ValueMap();
      auto const &other = arrived.ValueMap();
      if (one.size() != other.size()) return false;
      return std::ranges::all_of(one, [&other](auto const &entry) {
        auto const found = other.find(entry.first);
        return found != other.end() && Identical(entry.second, found->second);
      });
    }
    case ExternalPropertyValue::Type::DoubleList: {
      auto const &one = sent.ValueDoubleList();
      auto const &other = arrived.ValueDoubleList();
      return one.size() == other.size() && std::ranges::equal(one, other, SameDouble);
    }
    case ExternalPropertyValue::Type::NumericList: {
      // Each element carries which of the two numeric types it holds, and a
      // round trip owes the element's type as much as the list's.
      auto const &one = sent.ValueNumericList();
      auto const &other = arrived.ValueNumericList();
      return one.size() == other.size() && std::ranges::equal(one, other, [](auto const &first, auto const &second) {
               if (first.index() != second.index()) return false;
               if (auto const *whole = std::get_if<int>(&first)) return *whole == std::get<int>(second);
               return SameDouble(std::get<double>(first), std::get<double>(second));
             });
    }
    default:
      return sent == arrived;
  }
}

ExternalPropertyValue RoundTripped(ExternalPropertyValue const &sent) {
  memgraph::slk::Loopback loopback;
  auto *builder = loopback.GetBuilder();
  memgraph::slk::Save(sent, builder);
  auto *reader = loopback.GetReader();
  auto arrived = ExternalPropertyValue{};
  memgraph::slk::Load(&arrived, reader);
  return arrived;
}

/// Every type a value can be sent as, in the order their numbers run.
constexpr std::array kEveryType{
    PropertyValueType::Null,
    PropertyValueType::Bool,
    PropertyValueType::Int,
    PropertyValueType::Double,
    PropertyValueType::String,
    PropertyValueType::List,
    PropertyValueType::Map,
    PropertyValueType::TemporalData,
    PropertyValueType::ZonedTemporalData,
    PropertyValueType::Enum,
    PropertyValueType::Point2d,
    PropertyValueType::Point3d,
    PropertyValueType::IntList,
    PropertyValueType::DoubleList,
    PropertyValueType::NumericList,
    PropertyValueType::VectorIndexId,
};

}  // namespace

RC_GTEST_PROP(WireRoundTrip, AValueArrivesAsTheValueThatWasSent, ()) {
  auto const drawn = *memgraph::test::generators::AnyValue();
  auto const sent = AsSent(drawn);

  RC_ASSERT(Identical(sent, RoundTripped(sent)));
}

/// A run that reaches only the types somebody listed says nothing about the
/// rest, which is how four of these came to be sent by code no test ran. The
/// draw is per type rather than free, so each is asked whatever the run's
/// luck.
TEST(WireRoundTrip, EveryTypeArrivesAsItself) {
  for (auto const type : kEveryType) {
    rc::check(fmt::format("a {} arrives as itself", std::to_underlying(type)), [type] {
      auto const drawn = *memgraph::test::generators::ValueOfType(type, memgraph::test::generators::kDefaultDepth);
      auto const sent = AsSent(drawn);
      RC_ASSERT(Identical(sent, RoundTripped(sent)));
    });
  }
}

/// What a type is sent as is the number the enumerator holds, written straight
/// onto the stream and looked up again by the reader. Renumbering the type
/// compiles, passes every other suite, and breaks a main against a replica
/// built from another version, which nothing else here pairs.
TEST(WireRoundTrip, ATypeIsSentAsTheNumberItHasAlwaysBeenSentAs) {
  EXPECT_EQ(std::to_underlying(PropertyValueType::Null), 0);
  EXPECT_EQ(std::to_underlying(PropertyValueType::Bool), 1);
  EXPECT_EQ(std::to_underlying(PropertyValueType::Int), 2);
  EXPECT_EQ(std::to_underlying(PropertyValueType::Double), 3);
  EXPECT_EQ(std::to_underlying(PropertyValueType::String), 4);
  EXPECT_EQ(std::to_underlying(PropertyValueType::List), 5);
  EXPECT_EQ(std::to_underlying(PropertyValueType::Map), 6);
  EXPECT_EQ(std::to_underlying(PropertyValueType::TemporalData), 7);
  EXPECT_EQ(std::to_underlying(PropertyValueType::ZonedTemporalData), 8);
  EXPECT_EQ(std::to_underlying(PropertyValueType::Enum), 9);
  EXPECT_EQ(std::to_underlying(PropertyValueType::Point2d), 10);
  EXPECT_EQ(std::to_underlying(PropertyValueType::Point3d), 11);
  EXPECT_EQ(std::to_underlying(PropertyValueType::IntList), 12);
  EXPECT_EQ(std::to_underlying(PropertyValueType::DoubleList), 13);
  EXPECT_EQ(std::to_underlying(PropertyValueType::NumericList), 14);
  EXPECT_EQ(std::to_underlying(PropertyValueType::VectorIndexId), 15);

  EXPECT_EQ(kEveryType.size(), 16) << "a type was added, and the reader has to be told the number it arrives as";
}
