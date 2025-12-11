// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"

using testing::IsNull;
using testing::NotNull;
using testing::UnorderedElementsAre;

using namespace memgraph::storage;
using enum CoordinateReferenceSystem;

namespace {

/** Helper for creating nested maps easily. */

/** Type for  a key-value pair.
 */
using KVPair = std::tuple<PropertyId, PropertyValue>;

/** Creates a map from a (possibly nested) list of `KVPair`s.
 */
template <typename... Ts>
auto MakeMap(Ts &&...values) -> PropertyValue requires(std::is_same_v<std::decay_t<Ts>, KVPair> &&...) {
  return PropertyValue{PropertyValue::map_t{
      {std::get<0>(values),
       std::forward<std::tuple_element_t<1, std::decay_t<Ts>>>(std::get<1>(std::forward<Ts>(values)))}...}};
};

}  // end namespace

ZonedTemporalData GetSampleZonedTemporal() {
  const auto common_duration =
      memgraph::utils::AsSysTime(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::hours{10}).count());
  const auto named_timezone = memgraph::utils::Timezone("America/Los_Angeles");
  return ZonedTemporalData{ZonedTemporalType::ZonedDateTime, common_duration, named_timezone};
}

const PropertyValue kSampleValues[] = {
    PropertyValue(),
    PropertyValue(false),
    PropertyValue(true),
    PropertyValue(0),
    PropertyValue(33),
    PropertyValue(-33),
    PropertyValue(-3137),
    PropertyValue(3137),
    PropertyValue(310000007),
    PropertyValue(-310000007),
    PropertyValue(3100000000007L),
    PropertyValue(-3100000000007L),
    PropertyValue(0.0),
    PropertyValue(33.33),
    PropertyValue(-33.33),
    PropertyValue(3137.3137),
    PropertyValue(-3137.3137),
    PropertyValue("sample"),
    PropertyValue(std::string(404, 'n')),
    PropertyValue(
        std::vector<PropertyValue>{PropertyValue(33), PropertyValue(std::string("sample")), PropertyValue(-33.33)}),
    PropertyValue(std::vector<PropertyValue>{PropertyValue(), PropertyValue(false)}),
    PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue()},
                                       {PropertyId::FromUint(2), PropertyValue(false)}}),
    PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(3), PropertyValue(33)},
                                       {PropertyId::FromUint(4), PropertyValue(std::string("sample"))},
                                       {PropertyId::FromUint(5), PropertyValue(-33.33)}}),
    PropertyValue(TemporalData(TemporalType::Date, 23)),
    PropertyValue(GetSampleZonedTemporal()),
    PropertyValue{Enum{EnumTypeId{2}, EnumValueId{10'000}}},
    PropertyValue{Point2d{Cartesian_2d, 1.0, 2.0}},
    PropertyValue{Point2d{WGS84_2d, 3.0, 4.0}},
    PropertyValue{Point3d{Cartesian_3d, 1.0, 2.0, 3.0}},
    PropertyValue{Point3d{WGS84_3d, 4.0, 5.0, 6.0}},
    PropertyValue(std::vector<int>{33, 0, -33}),
    PropertyValue(std::vector<double>{33.0, 0.0, -33.33}),
    PropertyValue(std::vector<std::variant<int, double>>{33, 0.0, -33.33}),
};

void TestIsPropertyEqual(const PropertyStore &store, PropertyId property, const PropertyValue &value) {
  ASSERT_TRUE(store.IsPropertyEqual(property, value));
  for (const auto &sample : kSampleValues) {
    if (sample == value) {
      ASSERT_TRUE(store.IsPropertyEqual(property, sample));
    } else {
      ASSERT_FALSE(store.IsPropertyEqual(property, sample));
    }
  }
}

TEST(PropertyStore, Simple) {
  PropertyStore props;
  auto prop = PropertyId::FromInt(42);
  auto value = PropertyValue(42);
  ASSERT_TRUE(props.SetProperty(prop, value));
  ASSERT_EQ(props.GetProperty(prop), value);
  ASSERT_TRUE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, value);
  ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));

  ASSERT_FALSE(props.SetProperty(prop, PropertyValue()));
  ASSERT_TRUE(props.GetProperty(prop).IsNull());
  ASSERT_FALSE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, PropertyValue());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST(PropertyStore, SimpleLarge) {
  PropertyStore props;
  auto prop = PropertyId::FromInt(42);
  {
    auto value = PropertyValue(std::string(10000, 'a'));
    ASSERT_TRUE(props.SetProperty(prop, value));
    ASSERT_EQ(props.GetProperty(prop), value);
    ASSERT_TRUE(props.HasProperty(prop));
    TestIsPropertyEqual(props, prop, value);
    ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }
  {
    auto value = PropertyValue(TemporalData(TemporalType::Date, 23));
    ASSERT_FALSE(props.SetProperty(prop, value));
    ASSERT_EQ(props.GetProperty(prop), value);
    ASSERT_TRUE(props.HasProperty(prop));
    TestIsPropertyEqual(props, prop, value);
    ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }

  ASSERT_FALSE(props.SetProperty(prop, PropertyValue()));
  ASSERT_TRUE(props.GetProperty(prop).IsNull());
  ASSERT_FALSE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, PropertyValue());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST(PropertyStore, EmptySetToNull) {
  PropertyStore props;
  auto prop = PropertyId::FromInt(42);
  ASSERT_TRUE(props.SetProperty(prop, PropertyValue()));
  ASSERT_TRUE(props.GetProperty(prop).IsNull());
  ASSERT_FALSE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, PropertyValue());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST(PropertyStore, Clear) {
  PropertyStore props;
  auto prop = PropertyId::FromInt(42);
  auto value = PropertyValue(42);
  ASSERT_TRUE(props.SetProperty(prop, value));
  ASSERT_EQ(props.GetProperty(prop), value);
  ASSERT_TRUE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, value);
  ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  ASSERT_TRUE(props.ClearProperties());
  ASSERT_TRUE(props.GetProperty(prop).IsNull());
  ASSERT_FALSE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, PropertyValue());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST(PropertyStore, EmptyClear) {
  PropertyStore props;
  ASSERT_FALSE(props.ClearProperties());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST(PropertyStore, MoveConstruct) {
  PropertyStore props1;
  auto prop = PropertyId::FromInt(42);
  auto value = PropertyValue(42);
  ASSERT_TRUE(props1.SetProperty(prop, value));
  ASSERT_EQ(props1.GetProperty(prop), value);
  ASSERT_TRUE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, value);
  ASSERT_THAT(props1.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  {
    PropertyStore props2(std::move(props1));
    ASSERT_EQ(props2.GetProperty(prop), value);
    ASSERT_TRUE(props2.HasProperty(prop));
    TestIsPropertyEqual(props2, prop, value);
    ASSERT_THAT(props2.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move,hicpp-invalid-access-moved)
  ASSERT_TRUE(props1.GetProperty(prop).IsNull());
  ASSERT_FALSE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, PropertyValue());
  ASSERT_EQ(props1.Properties().size(), 0);
}

TEST(PropertyStore, MoveConstructLarge) {
  PropertyStore props1;
  auto prop = PropertyId::FromInt(42);
  auto value = PropertyValue(std::string(10000, 'a'));
  ASSERT_TRUE(props1.SetProperty(prop, value));
  ASSERT_EQ(props1.GetProperty(prop), value);
  ASSERT_TRUE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, value);
  ASSERT_THAT(props1.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  {
    PropertyStore props2(std::move(props1));
    ASSERT_EQ(props2.GetProperty(prop), value);
    ASSERT_TRUE(props2.HasProperty(prop));
    TestIsPropertyEqual(props2, prop, value);
    ASSERT_THAT(props2.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move,hicpp-invalid-access-moved)
  ASSERT_TRUE(props1.GetProperty(prop).IsNull());
  ASSERT_FALSE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, PropertyValue());
  ASSERT_EQ(props1.Properties().size(), 0);
}

TEST(PropertyStore, MoveAssign) {
  PropertyStore props1;
  auto prop = PropertyId::FromInt(42);
  auto value = PropertyValue(42);
  ASSERT_TRUE(props1.SetProperty(prop, value));
  ASSERT_EQ(props1.GetProperty(prop), value);
  ASSERT_TRUE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, value);
  ASSERT_THAT(props1.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  {
    auto value2 = PropertyValue(68);
    PropertyStore props2;
    ASSERT_TRUE(props2.SetProperty(prop, value2));
    ASSERT_EQ(props2.GetProperty(prop), value2);
    ASSERT_TRUE(props2.HasProperty(prop));
    TestIsPropertyEqual(props2, prop, value2);
    ASSERT_THAT(props2.Properties(), UnorderedElementsAre(std::pair(prop, value2)));
    props2 = std::move(props1);
    ASSERT_EQ(props2.GetProperty(prop), value);
    ASSERT_TRUE(props2.HasProperty(prop));
    TestIsPropertyEqual(props2, prop, value);
    ASSERT_THAT(props2.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move,hicpp-invalid-access-moved)
  ASSERT_TRUE(props1.GetProperty(prop).IsNull());
  ASSERT_FALSE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, PropertyValue());
  ASSERT_EQ(props1.Properties().size(), 0);
}

TEST(PropertyStore, MoveAssignLarge) {
  PropertyStore props1;
  auto prop = PropertyId::FromInt(42);
  auto value = PropertyValue(std::string(10000, 'a'));
  ASSERT_TRUE(props1.SetProperty(prop, value));
  ASSERT_EQ(props1.GetProperty(prop), value);
  ASSERT_TRUE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, value);
  ASSERT_THAT(props1.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  {
    auto value2 = PropertyValue(std::string(10000, 'b'));
    PropertyStore props2;
    ASSERT_TRUE(props2.SetProperty(prop, value2));
    ASSERT_EQ(props2.GetProperty(prop), value2);
    ASSERT_TRUE(props2.HasProperty(prop));
    TestIsPropertyEqual(props2, prop, value2);
    ASSERT_THAT(props2.Properties(), UnorderedElementsAre(std::pair(prop, value2)));
    props2 = std::move(props1);
    ASSERT_EQ(props2.GetProperty(prop), value);
    ASSERT_TRUE(props2.HasProperty(prop));
    TestIsPropertyEqual(props2, prop, value);
    ASSERT_THAT(props2.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move,hicpp-invalid-access-moved)
  ASSERT_TRUE(props1.GetProperty(prop).IsNull());
  ASSERT_FALSE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, PropertyValue());
  ASSERT_EQ(props1.Properties().size(), 0);
}

TEST(PropertyStore, EmptySet) {
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123), PropertyValue()};
  PropertyValue::map_t map{{PropertyId::FromUint(1), PropertyValue(false)}};
  const TemporalData temporal{TemporalType::LocalDateTime, 23};
  const auto zoned_temporal = GetSampleZonedTemporal();

  std::vector<PropertyValue> data{PropertyValue(map),      PropertyValue(true),          PropertyValue(123),
                                  PropertyValue(123.5),    PropertyValue("nandare"),     PropertyValue(vec),
                                  PropertyValue(temporal), PropertyValue(zoned_temporal)};

  auto prop = PropertyId::FromInt(42);
  for (const auto &value : data) {
    PropertyStore props;

    ASSERT_TRUE(props.SetProperty(prop, value));
    ASSERT_EQ(props.GetProperty(prop), value);
    ASSERT_TRUE(props.HasProperty(prop));
    TestIsPropertyEqual(props, prop, value);
    ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));
    ASSERT_FALSE(props.SetProperty(prop, value));
    ASSERT_EQ(props.GetProperty(prop), value);
    ASSERT_TRUE(props.HasProperty(prop));
    TestIsPropertyEqual(props, prop, value);
    ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));
    ASSERT_FALSE(props.SetProperty(prop, PropertyValue()));
    ASSERT_TRUE(props.GetProperty(prop).IsNull());
    ASSERT_FALSE(props.HasProperty(prop));
    TestIsPropertyEqual(props, prop, PropertyValue());
    ASSERT_EQ(props.Properties().size(), 0);
    ASSERT_TRUE(props.SetProperty(prop, PropertyValue()));
    ASSERT_TRUE(props.GetProperty(prop).IsNull());
    ASSERT_FALSE(props.HasProperty(prop));
    TestIsPropertyEqual(props, prop, PropertyValue());
    ASSERT_EQ(props.Properties().size(), 0);
  }
}

TEST(PropertyStore, FullSet) {
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123), PropertyValue()};
  PropertyValue::map_t map{{PropertyId::FromUint(1), PropertyValue(false)}};
  const TemporalData temporal{TemporalType::LocalDateTime, 23};
  const auto zoned_temporal = GetSampleZonedTemporal();

  std::map<PropertyId, PropertyValue> data{
      {PropertyId::FromInt(1), PropertyValue(map)},
      {PropertyId::FromInt(2), PropertyValue(true)},
      {PropertyId::FromInt(3), PropertyValue(123)},
      {PropertyId::FromInt(4), PropertyValue(123.5)},
      {PropertyId::FromInt(5), PropertyValue("nandare")},
      {PropertyId::FromInt(6), PropertyValue(vec)},
      {PropertyId::FromInt(7), PropertyValue(temporal)},
      {PropertyId::FromInt(8), PropertyValue(zoned_temporal)},
      {PropertyId::FromInt(9), PropertyValue(Enum{EnumTypeId{2}, EnumValueId{42}})},
      {PropertyId::FromInt(10), PropertyValue{Point2d{Cartesian_2d, 1.0, 2.0}}},
      {PropertyId::FromInt(11), PropertyValue{Point2d{WGS84_2d, 3.0, 4.0}}},
      {PropertyId::FromInt(12), PropertyValue{Point3d{Cartesian_3d, 1.0, 2.0, 3.0}}},
      {PropertyId::FromInt(13), PropertyValue{Point3d{WGS84_3d, 4.0, 5.0, 6.0}}},
  };

  std::vector<PropertyValue> alt{PropertyValue(),
                                 PropertyValue(std::string()),
                                 PropertyValue(std::string(10, 'a')),
                                 PropertyValue(std::string(100, 'a')),
                                 PropertyValue(std::string(1000, 'a')),
                                 PropertyValue(std::string(10000, 'a')),
                                 PropertyValue(std::string(100000, 'a'))};

  PropertyStore props;
  for (const auto &target : data) {
    for (const auto &item : data) {
      ASSERT_TRUE(props.SetProperty(item.first, item.second));
    }

    for (size_t i = 0; i < alt.size(); ++i) {
      if (i == 1) {
        ASSERT_TRUE(props.SetProperty(target.first, alt[i]));
      } else {
        ASSERT_FALSE(props.SetProperty(target.first, alt[i]));
      }
      for (const auto &item : data) {
        if (item.first == target.first) {
          ASSERT_EQ(props.GetProperty(item.first), alt[i]);
          if (alt[i].IsNull()) {
            ASSERT_FALSE(props.HasProperty(item.first));
          } else {
            ASSERT_TRUE(props.HasProperty(item.first));
          }
          TestIsPropertyEqual(props, item.first, alt[i]);
        } else {
          ASSERT_EQ(props.GetProperty(item.first), item.second);
          ASSERT_TRUE(props.HasProperty(item.first));
          TestIsPropertyEqual(props, item.first, item.second);
        }
      }
      auto current = data;
      if (alt[i].IsNull()) {
        current.erase(target.first);
      } else {
        current[target.first] = alt[i];
      }
      ASSERT_EQ(props.Properties(), current);
    }

    for (ssize_t i = alt.size() - 1; i >= 0; --i) {
      ASSERT_FALSE(props.SetProperty(target.first, alt[i]));
      for (const auto &item : data) {
        if (item.first == target.first) {
          ASSERT_EQ(props.GetProperty(item.first), alt[i]);
          if (alt[i].IsNull()) {
            ASSERT_FALSE(props.HasProperty(item.first));
          } else {
            ASSERT_TRUE(props.HasProperty(item.first));
          }
          TestIsPropertyEqual(props, item.first, alt[i]);
        } else {
          ASSERT_EQ(props.GetProperty(item.first), item.second);
          ASSERT_TRUE(props.HasProperty(item.first));
          TestIsPropertyEqual(props, item.first, item.second);
        }
      }
      auto current = data;
      if (alt[i].IsNull()) {
        current.erase(target.first);
      } else {
        current[target.first] = alt[i];
      }
      ASSERT_EQ(props.Properties(), current);
    }

    ASSERT_TRUE(props.SetProperty(target.first, target.second));
    ASSERT_EQ(props.GetProperty(target.first), target.second);
    ASSERT_TRUE(props.HasProperty(target.first));
    TestIsPropertyEqual(props, target.first, target.second);

    props.ClearProperties();
    ASSERT_EQ(props.Properties().size(), 0);
    for (const auto &item : data) {
      ASSERT_TRUE(props.GetProperty(item.first).IsNull());
      ASSERT_FALSE(props.HasProperty(item.first));
      TestIsPropertyEqual(props, item.first, PropertyValue());
    }
  }
}

TEST(PropertyStore, IntEncoding) {
  std::map<PropertyId, PropertyValue> data{
      {PropertyId::FromUint(0UL), PropertyValue(std::numeric_limits<int64_t>::min())},
      {PropertyId::FromUint(10UL), PropertyValue(-137438953472L)},
      {PropertyId::FromUint(std::numeric_limits<uint8_t>::max()), PropertyValue(-4294967297L)},
      {PropertyId::FromUint(256UL), PropertyValue(std::numeric_limits<int32_t>::min())},
      {PropertyId::FromUint(1024UL), PropertyValue(-1048576L)},
      {PropertyId::FromUint(1025UL), PropertyValue(-65537L)},
      {PropertyId::FromUint(1026UL), PropertyValue(std::numeric_limits<int16_t>::min())},
      {PropertyId::FromUint(1027UL), PropertyValue(-1024L)},
      {PropertyId::FromUint(2000UL), PropertyValue(-257L)},
      {PropertyId::FromUint(3000UL), PropertyValue(std::numeric_limits<int8_t>::min())},
      {PropertyId::FromUint(4000UL), PropertyValue(-1L)},
      {PropertyId::FromUint(10000UL), PropertyValue(0L)},
      {PropertyId::FromUint(20000UL), PropertyValue(1L)},
      {PropertyId::FromUint(30000UL), PropertyValue(std::numeric_limits<int8_t>::max())},
      {PropertyId::FromUint(40000UL), PropertyValue(256L)},
      {PropertyId::FromUint(50000UL), PropertyValue(1024L)},
      {PropertyId::FromUint(std::numeric_limits<uint16_t>::max()), PropertyValue(std::numeric_limits<int16_t>::max())},
      {PropertyId::FromUint(65536UL), PropertyValue(65536L)},
      {PropertyId::FromUint(1048576UL), PropertyValue(1048576L)},
      {PropertyId::FromUint(std::numeric_limits<uint32_t>::max()), PropertyValue(std::numeric_limits<int32_t>::max())},
      {PropertyId::FromUint(1048577UL), PropertyValue(4294967296L)},
      {PropertyId::FromUint(1048578UL), PropertyValue(137438953472L)},
      {PropertyId::FromUint(std::numeric_limits<uint32_t>::max()), PropertyValue(std::numeric_limits<int64_t>::max())}};

  PropertyStore props;
  for (const auto &item : data) {
    ASSERT_TRUE(props.SetProperty(item.first, item.second));
    ASSERT_EQ(props.GetProperty(item.first), item.second);
    ASSERT_TRUE(props.HasProperty(item.first));
    TestIsPropertyEqual(props, item.first, item.second);
  }
  for (auto it = data.rbegin(); it != data.rend(); ++it) {
    const auto &item = *it;
    ASSERT_FALSE(props.SetProperty(item.first, item.second));
    ASSERT_EQ(props.GetProperty(item.first), item.second);
    ASSERT_TRUE(props.HasProperty(item.first));
    TestIsPropertyEqual(props, item.first, item.second);
  }

  ASSERT_EQ(props.Properties(), data);

  props.ClearProperties();
  ASSERT_EQ(props.Properties().size(), 0);
  for (const auto &item : data) {
    ASSERT_TRUE(props.GetProperty(item.first).IsNull());
    ASSERT_FALSE(props.HasProperty(item.first));
    TestIsPropertyEqual(props, item.first, PropertyValue());
  }
}

TEST(PropertyStore, IsPropertyEqualIntAndDouble) {
  PropertyStore props;
  auto prop = PropertyId::FromInt(42);

  ASSERT_TRUE(props.SetProperty(prop, PropertyValue(42)));

  std::vector<std::pair<PropertyValue, PropertyValue>> tests{
      {PropertyValue(0), PropertyValue(0.0)},
      {PropertyValue(123), PropertyValue(123.0)},
      {PropertyValue(12345), PropertyValue(12345.0)},
      {PropertyValue(12345678), PropertyValue(12345678.0)},
      {PropertyValue(1234567890123L), PropertyValue(1234567890123.0)},
  };

  // Test equality with raw values.
  for (auto test : tests) {
    ASSERT_EQ(test.first, test.second);

    // Test first, second
    ASSERT_FALSE(props.SetProperty(prop, test.first));
    ASSERT_EQ(props.GetProperty(prop), test.first);
    ASSERT_TRUE(props.HasProperty(prop));
    ASSERT_TRUE(props.IsPropertyEqual(prop, test.first));
    ASSERT_TRUE(props.IsPropertyEqual(prop, test.second));

    // Test second, first
    ASSERT_FALSE(props.SetProperty(prop, test.second));
    ASSERT_EQ(props.GetProperty(prop), test.second);
    ASSERT_TRUE(props.HasProperty(prop));
    ASSERT_TRUE(props.IsPropertyEqual(prop, test.second));
    ASSERT_TRUE(props.IsPropertyEqual(prop, test.first));

    // Make both negative
    test.first = PropertyValue(test.first.ValueInt() * -1);
    test.second = PropertyValue(test.second.ValueDouble() * -1.0);
    ASSERT_EQ(test.first, test.second);

    // Test -first, -second
    ASSERT_FALSE(props.SetProperty(prop, test.first));
    ASSERT_EQ(props.GetProperty(prop), test.first);
    ASSERT_TRUE(props.HasProperty(prop));
    ASSERT_TRUE(props.IsPropertyEqual(prop, test.first));
    ASSERT_TRUE(props.IsPropertyEqual(prop, test.second));

    // Test -second, -first
    ASSERT_FALSE(props.SetProperty(prop, test.second));
    ASSERT_EQ(props.GetProperty(prop), test.second);
    ASSERT_TRUE(props.HasProperty(prop));
    ASSERT_TRUE(props.IsPropertyEqual(prop, test.second));
    ASSERT_TRUE(props.IsPropertyEqual(prop, test.first));
  }

  // Test equality with values wrapped in lists.
  for (auto test : tests) {
    test.first = PropertyValue(std::vector<PropertyValue>{PropertyValue(test.first.ValueInt())});
    test.second = PropertyValue(std::vector<PropertyValue>{PropertyValue(test.second.ValueDouble())});
    ASSERT_EQ(test.first, test.second);

    // Test first, second
    ASSERT_FALSE(props.SetProperty(prop, test.first));
    ASSERT_EQ(props.GetProperty(prop), test.first);
    ASSERT_TRUE(props.HasProperty(prop));
    ASSERT_TRUE(props.IsPropertyEqual(prop, test.first));
    ASSERT_TRUE(props.IsPropertyEqual(prop, test.second));

    // Test second, first
    ASSERT_FALSE(props.SetProperty(prop, test.second));
    ASSERT_EQ(props.GetProperty(prop), test.second);
    ASSERT_TRUE(props.HasProperty(prop));
    ASSERT_TRUE(props.IsPropertyEqual(prop, test.second));
    ASSERT_TRUE(props.IsPropertyEqual(prop, test.first));

    // Make both negative
    test.first = PropertyValue(std::vector<PropertyValue>{PropertyValue(test.first.ValueList()[0].ValueInt() * -1)});
    test.second =
        PropertyValue(std::vector<PropertyValue>{PropertyValue(test.second.ValueList()[0].ValueDouble() * -1.0)});
    ASSERT_EQ(test.first, test.second);

    // Test -first, -second
    ASSERT_FALSE(props.SetProperty(prop, test.first));
    ASSERT_EQ(props.GetProperty(prop), test.first);
    ASSERT_TRUE(props.HasProperty(prop));
    ASSERT_TRUE(props.IsPropertyEqual(prop, test.first));
    ASSERT_TRUE(props.IsPropertyEqual(prop, test.second));

    // Test -second, -first
    ASSERT_FALSE(props.SetProperty(prop, test.second));
    ASSERT_EQ(props.GetProperty(prop), test.second);
    ASSERT_TRUE(props.HasProperty(prop));
    ASSERT_TRUE(props.IsPropertyEqual(prop, test.second));
    ASSERT_TRUE(props.IsPropertyEqual(prop, test.first));
  }
}

TEST(PropertyStore, IsPropertyEqualString) {
  PropertyStore props;
  auto prop = PropertyId::FromInt(42);
  ASSERT_TRUE(props.SetProperty(prop, PropertyValue("test")));
  ASSERT_TRUE(props.IsPropertyEqual(prop, PropertyValue("test")));

  // Different length.
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue("helloworld")));

  // Same length, different value.
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue("asdf")));

  // Shortened and extended.
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue("tes")));
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue("testt")));
}

TEST(PropertyStore, IsPropertyEqualList) {
  PropertyStore props;
  auto prop = PropertyId::FromInt(42);
  ASSERT_TRUE(
      props.SetProperty(prop, PropertyValue(std::vector<PropertyValue>{PropertyValue(42), PropertyValue("test")})));
  ASSERT_TRUE(
      props.IsPropertyEqual(prop, PropertyValue(std::vector<PropertyValue>{PropertyValue(42), PropertyValue("test")})));

  // Different length.
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(std::vector<PropertyValue>{PropertyValue(24)})));

  // Same length, different value.
  ASSERT_FALSE(
      props.IsPropertyEqual(prop, PropertyValue(std::vector<PropertyValue>{PropertyValue(42), PropertyValue("asdf")})));

  // Shortened and extended.
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(std::vector<PropertyValue>{PropertyValue(42)})));
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, PropertyValue(std::vector<PropertyValue>{PropertyValue(42), PropertyValue("test"), PropertyValue(true)})));
}

TEST(PropertyStore, IsPropertyEqualSameTypeListsComparison) {
  PropertyStore props;
  auto prop = PropertyId::FromInt(42);

  // Test IntList - same values should be equal
  auto int_list1 = PropertyValue(std::vector<int>{33, 0, -33});
  ASSERT_TRUE(props.SetProperty(prop, int_list1));
  ASSERT_TRUE(props.IsPropertyEqual(prop, int_list1));

  // Test IntList - different values should not be equal
  auto int_list2 = PropertyValue(std::vector<int>{33, 0, -34});
  ASSERT_FALSE(props.IsPropertyEqual(prop, int_list2));

  // Test IntList - different length should not be equal
  auto int_list3 = PropertyValue(std::vector<int>{33, 0});
  ASSERT_FALSE(props.IsPropertyEqual(prop, int_list3));

  // Test DoubleList - same values should be equal
  auto double_list1 = PropertyValue(std::vector<double>{33.0, 0.0, -33.33});
  props.SetProperty(prop, double_list1);
  ASSERT_TRUE(props.IsPropertyEqual(prop, double_list1));

  // Test DoubleList - different values should not be equal
  auto double_list2 = PropertyValue(std::vector<double>{33.0, 0.0, -33.34});
  ASSERT_FALSE(props.IsPropertyEqual(prop, double_list2));

  // Test NumericList - same values should be equal
  auto numeric_list1 = PropertyValue(std::vector<std::variant<int, double>>{33, 0.0, -33.33});
  ASSERT_TRUE(props.IsPropertyEqual(prop, numeric_list1));

  // Test NumericList - different values should not be equal
  auto numeric_list2 = PropertyValue(std::vector<std::variant<int, double>>{33, 0.0, -33.34});
  ASSERT_FALSE(props.IsPropertyEqual(prop, numeric_list2));

  // Test PropertyValue list - should be equal
  auto prop_value_list =
      PropertyValue(std::vector<PropertyValue>{PropertyValue(33), PropertyValue("sample"), PropertyValue(-33.33)});
  props.SetProperty(prop, prop_value_list);
  ASSERT_TRUE(props.IsPropertyEqual(prop, prop_value_list));

  // Test PropertyValue list - different values should not be equal
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(std::vector<PropertyValue>{
                                               PropertyValue(33), PropertyValue("different"), PropertyValue(-33.33)})));
}

TEST(PropertyStore, IsPropertyEqualCrossTypeNumericListsComparison) {
  PropertyStore props;
  auto prop = PropertyId::FromInt(42);

  // ============================================================================
  // 1: IntList cross-type comparisons
  // ============================================================================
  auto int_list_for_cross = PropertyValue(std::vector<int>{42, 100});
  ASSERT_TRUE(props.SetProperty(prop, int_list_for_cross));
  ASSERT_TRUE(props.IsPropertyEqual(prop, int_list_for_cross));
  ASSERT_TRUE(props.IsPropertyEqual(prop, PropertyValue(std::vector<double>{42.0, 100.0})));
  ASSERT_TRUE(props.IsPropertyEqual(prop, PropertyValue(std::vector<std::variant<int, double>>{42, 100.0})));

  // Test IntList - different values should not be equal
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(std::vector<int>{42, 101})));
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(std::vector<double>{42.0, 101.0})));
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(std::vector<std::variant<int, double>>{42, 101.0})));

  // ============================================================================
  // 2: DoubleList cross-type comparisons
  // ============================================================================
  auto double_list_for_cross = PropertyValue(std::vector<double>{42.0, 100.0});
  props.SetProperty(prop, double_list_for_cross);
  ASSERT_TRUE(props.IsPropertyEqual(prop, double_list_for_cross));
  ASSERT_TRUE(props.IsPropertyEqual(prop, PropertyValue(std::vector<int>{42, 100})));
  ASSERT_TRUE(props.IsPropertyEqual(prop, PropertyValue(std::vector<double>{42.0, 100.0})));
  ASSERT_TRUE(props.IsPropertyEqual(prop, PropertyValue(std::vector<std::variant<int, double>>{42, 100.0})));

  // Test DoubleList - different values should not be equal
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(std::vector<int>{42, 101})));
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(std::vector<double>{42.0, 101.0})));
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(std::vector<std::variant<int, double>>{42, 101.0})));

  // ============================================================================
  // 3: NumericList cross-type comparisons
  // ============================================================================
  auto numeric_list_for_cross = PropertyValue(std::vector<std::variant<int, double>>{42, 100.0});
  props.SetProperty(prop, numeric_list_for_cross);
  ASSERT_TRUE(props.IsPropertyEqual(prop, numeric_list_for_cross));
  ASSERT_TRUE(props.IsPropertyEqual(prop, PropertyValue(std::vector<int>{42, 100})));
  ASSERT_TRUE(props.IsPropertyEqual(prop, PropertyValue(std::vector<double>{42.0, 100.0})));
  ASSERT_TRUE(props.IsPropertyEqual(prop, PropertyValue(std::vector<std::variant<int, double>>{42, 100.0})));

  // Test NumericList - different values should not be equal
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(std::vector<int>{42, 101})));
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(std::vector<double>{42.0, 101.0})));
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(std::vector<std::variant<int, double>>{42, 101.0})));

  // ============================================================================
  // 4: PropertyValue lists should not be equal to numeric lists
  // ============================================================================
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(std::vector<PropertyValue>{
                                               PropertyValue(33), PropertyValue("sample"), PropertyValue(-33.33)})));
}

TEST(PropertyStore, IsPropertyEqualMap) {
  PropertyStore props;
  auto prop = PropertyId::FromInt(42);
  ASSERT_TRUE(
      props.SetProperty(prop, PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(42)},
                                                                 {PropertyId::FromUint(2), PropertyValue("test")}})));
  ASSERT_TRUE(props.IsPropertyEqual(
      prop, PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(42)},
                                               {PropertyId::FromUint(2), PropertyValue("test")}})));

  // Different length.
  ASSERT_FALSE(
      props.IsPropertyEqual(prop, PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(42)}})));

  // Same length, different value.
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(42)},
                                               {PropertyId::FromUint(2), PropertyValue("testt")}})));

  // Same length, different key (different length).
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(42)},
                                               {PropertyId::FromUint(3), PropertyValue("test")}})));

  // Shortened and extended.
  ASSERT_FALSE(
      props.IsPropertyEqual(prop, PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(42)}})));
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(42)},
                                               {PropertyId::FromUint(2), PropertyValue(true)},
                                               {PropertyId::FromUint(3), PropertyValue("test")}})));
}

TEST(PropertyStore, IsPropertyEqualTemporalData) {
  PropertyStore props;
  auto prop = PropertyId::FromInt(42);
  const TemporalData temporal{TemporalType::Date, 23};
  ASSERT_TRUE(props.SetProperty(prop, PropertyValue(temporal)));
  ASSERT_TRUE(props.IsPropertyEqual(prop, PropertyValue(temporal)));

  // Different type.
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(TemporalData{TemporalType::Duration, 23})));

  // Same type, different value.
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(TemporalData{TemporalType::Date, 30})));
}

TEST(PropertyStore, IsPropertyEqualZonedTemporalData) {
  const std::array timezone_offset_encoding_cases{
      memgraph::utils::Timezone("America/Los_Angeles"),     memgraph::utils::Timezone(std::chrono::minutes{-360}),
      memgraph::utils::Timezone(std::chrono::minutes{-60}), memgraph::utils::Timezone(std::chrono::minutes{0}),
      memgraph::utils::Timezone(std::chrono::minutes{60}),  memgraph::utils::Timezone(std::chrono::minutes{360}),
  };

  auto check_case = [](const memgraph::utils::Timezone &timezone) {
    using namespace memgraph::storage;

    PropertyStore props;
    const auto common_duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::hours{10}).count();

    const auto zoned_temporal = PropertyValue(
        ZonedTemporalData{ZonedTemporalType::ZonedDateTime, memgraph::utils::AsSysTime(common_duration), timezone});
    const auto unequal_type = PropertyValue(TemporalData{TemporalType::Duration, 23});
    const auto unequal_value = PropertyValue(
        ZonedTemporalData{ZonedTemporalType::ZonedDateTime, memgraph::utils::AsSysTime(common_duration + 1), timezone});

    auto prop = PropertyId::FromInt(42);

    ASSERT_TRUE(props.SetProperty(prop, zoned_temporal));
    ASSERT_TRUE(props.IsPropertyEqual(prop, zoned_temporal));
    // Different type.
    ASSERT_FALSE(props.IsPropertyEqual(prop, unequal_type));
    // Same type, different value.
    ASSERT_FALSE(props.IsPropertyEqual(prop, unequal_value));
  };

  for (const auto &timezone : timezone_offset_encoding_cases) {
    check_case(timezone);
  }
}

TEST(PropertyStore, IsPropertyEqualEnum) {
  PropertyStore props;

  auto const enum_val = Enum{EnumTypeId{2}, EnumValueId{10'000}};
  auto const diff_type = Enum{EnumTypeId{3}, EnumValueId{10'000}};
  auto const diff_value = Enum{EnumTypeId{3}, EnumValueId{10'001}};

  auto const prop = PropertyId::FromInt(42);

  ASSERT_TRUE(props.SetProperty(prop, PropertyValue{enum_val}));
  ASSERT_TRUE(props.IsPropertyEqual(prop, PropertyValue{enum_val}));
  // Different type.
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue{diff_type}));
  // Same type, different value.
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue{diff_value}));
}

TEST(PropertyStore, SetMultipleProperties) {
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123), PropertyValue()};
  PropertyValue::map_t map{{PropertyId::FromUint(1), PropertyValue(false)}};
  const TemporalData temporal{TemporalType::LocalDateTime, 23};
  const auto zoned_temporal = GetSampleZonedTemporal();

  // The order of property ids are purposfully not monotonic to test that PropertyStore orders them properly
  const std::vector<std::pair<PropertyId, PropertyValue>> data{
      {PropertyId::FromInt(1), PropertyValue(true)},     {PropertyId::FromInt(10), PropertyValue(123)},
      {PropertyId::FromInt(3), PropertyValue(123.5)},    {PropertyId::FromInt(4), PropertyValue("nandare")},
      {PropertyId::FromInt(12), PropertyValue(vec)},     {PropertyId::FromInt(6), PropertyValue(map)},
      {PropertyId::FromInt(7), PropertyValue(temporal)}, {PropertyId::FromInt(5), PropertyValue(zoned_temporal)}};

  const std::map<PropertyId, PropertyValue> data_in_map{data.begin(), data.end()};

  auto check_store = [data](const PropertyStore &store) {
    for (const auto &[key, value] : data) {
      ASSERT_TRUE(store.IsPropertyEqual(key, value));
    }
  };
  {
    PropertyStore store;
    EXPECT_TRUE(store.InitProperties(data));
    check_store(store);
    EXPECT_FALSE(store.InitProperties(data));
    EXPECT_FALSE(store.InitProperties(data_in_map));
  }
  {
    PropertyStore store;
    EXPECT_TRUE(store.InitProperties(data_in_map));
    check_store(store);
    EXPECT_FALSE(store.InitProperties(data_in_map));
    EXPECT_FALSE(store.InitProperties(data));
  }
}

TEST(PropertyStore, HasAllProperties) {
  const std::vector<std::pair<PropertyId, PropertyValue>> data{
      {PropertyId::FromInt(1), PropertyValue(true)},
      {PropertyId::FromInt(2), PropertyValue(123)},
      {PropertyId::FromInt(3), PropertyValue("three")},
      {PropertyId::FromInt(5), PropertyValue("0.0")},
      {PropertyId::FromInt(6), PropertyValue(Enum{EnumTypeId{2}, EnumValueId{42}})},
      {PropertyId::FromInt(7), PropertyValue{Point2d{Cartesian_2d, 1.0, 2.0}}},
      {PropertyId::FromInt(8), PropertyValue{Point2d{WGS84_2d, 3.0, 4.0}}},
      {PropertyId::FromInt(9), PropertyValue{Point3d{Cartesian_3d, 1.0, 2.0, 3.0}}},
      {PropertyId::FromInt(10), PropertyValue{Point3d{WGS84_3d, 4.0, 5.0, 6.0}}},
  };

  PropertyStore store;
  EXPECT_TRUE(store.InitProperties(data));
  EXPECT_TRUE(store.HasAllProperties({PropertyId::FromInt(1), PropertyId::FromInt(2), PropertyId::FromInt(3),
                                      PropertyId::FromInt(6), PropertyId::FromInt(9)}));
}

TEST(PropertyStore, HasAllPropertyValues) {
  const std::vector<std::pair<PropertyId, PropertyValue>> data{
      {PropertyId::FromInt(1), PropertyValue(true)},
      {PropertyId::FromInt(2), PropertyValue(123)},
      {PropertyId::FromInt(3), PropertyValue("three")},
      {PropertyId::FromInt(5), PropertyValue(0.0)},
      {PropertyId::FromInt(6), PropertyValue(Enum{EnumTypeId{2}, EnumValueId{42}})},
      {PropertyId::FromInt(7), PropertyValue{Point2d{Cartesian_2d, 1.0, 2.0}}},
      {PropertyId::FromInt(8), PropertyValue{Point2d{WGS84_2d, 3.0, 4.0}}},
      {PropertyId::FromInt(9), PropertyValue{Point3d{Cartesian_3d, 1.0, 2.0, 3.0}}},
      {PropertyId::FromInt(10), PropertyValue{Point3d{WGS84_3d, 4.0, 5.0, 6.0}}},
  };

  PropertyStore store;
  EXPECT_TRUE(store.InitProperties(data));
  EXPECT_TRUE(store.HasAllPropertyValues({
      PropertyValue(0.0),
      PropertyValue(123),
      PropertyValue("three"),
      PropertyValue(Enum{EnumTypeId{2}, EnumValueId{42}}),
      PropertyValue{Point3d{Cartesian_3d, 1.0, 2.0, 3.0}},
  }));
}

TEST(PropertyStore, HasAnyProperties) {
  const std::vector<std::pair<PropertyId, PropertyValue>> data{{PropertyId::FromInt(3), PropertyValue("three")},
                                                               {PropertyId::FromInt(5), PropertyValue("0.0")}};

  PropertyStore store;
  EXPECT_TRUE(store.InitProperties(data));
  EXPECT_FALSE(store.HasAllPropertyValues({PropertyValue(0.0), PropertyValue(123), PropertyValue("three")}));
}

TEST(PropertyStore, ReplaceWithSameSize) {
  // This test is important to catch a case where compression need to be using the correct buffer
  PropertyStore store;
  EXPECT_TRUE(store.SetProperty(PropertyId::FromInt(1), PropertyValue(std::string(100, 'a'))));
  EXPECT_FALSE(store.SetProperty(PropertyId::FromInt(1), PropertyValue(std::string(100, 'b'))));
  EXPECT_EQ(store.GetProperty(PropertyId::FromInt(1)), PropertyValue(std::string(100, 'b')));
}

TEST(PropertyStore, PropertiesOfTypes) {
  const std::vector<std::pair<PropertyId, PropertyValue>> data{
      {PropertyId::FromInt(1), PropertyValue(true)},
      {PropertyId::FromInt(2), PropertyValue(123)},
      {PropertyId::FromInt(3), PropertyValue("three")},
      {PropertyId::FromInt(4), PropertyValue(3.5)},
      {PropertyId::FromInt(5), PropertyValue("0.0")},
      {PropertyId::FromInt(6), PropertyValue(Enum{EnumTypeId{2}, EnumValueId{42}})},
      {PropertyId::FromInt(7), PropertyValue{Point2d{Cartesian_2d, 1.0, 2.0}}},
      {PropertyId::FromInt(8), PropertyValue{Point2d{WGS84_2d, 3.0, 4.0}}},
      {PropertyId::FromInt(9), PropertyValue{Point3d{Cartesian_3d, 1.0, 2.0, 3.0}}},
      {PropertyId::FromInt(10), PropertyValue{Point3d{WGS84_3d, 4.0, 5.0, 6.0}}},
  };

  PropertyStore store;
  store.InitProperties(data);
  constexpr auto types = std::array{PropertyStoreType::BOOL, PropertyStoreType::DOUBLE};
  auto props_of_type = store.PropertiesOfTypes(types);

  ASSERT_EQ(props_of_type.size(), 2);
  ASSERT_EQ(props_of_type[0], data[0].first);
  ASSERT_EQ(props_of_type[1], data[3].first);
}

TEST(PropertyStore, GetPropertyOfTypes) {
  const std::vector<std::pair<PropertyId, PropertyValue>> data1{
      {PropertyId::FromInt(1), PropertyValue(true)},
      {PropertyId::FromInt(2), PropertyValue(123)},
      {PropertyId::FromInt(3), PropertyValue("three")},
  };

  const std::vector<std::pair<PropertyId, PropertyValue>> data2{
      {PropertyId::FromInt(1), PropertyValue(123)},
      {PropertyId::FromInt(2), PropertyValue(true)},
      {PropertyId::FromInt(3), PropertyValue("three")},
  };

  const std::vector<std::pair<PropertyId, PropertyValue>> data3{
      {PropertyId::FromInt(1), PropertyValue(true)},
      {PropertyId::FromInt(2), PropertyValue("three")},
      {PropertyId::FromInt(3), PropertyValue(123)},
  };

  PropertyStore store1;
  store1.InitProperties(data1);

  PropertyStore store2;
  store2.InitProperties(data2);

  PropertyStore store3;
  store3.InitProperties(data3);

  constexpr auto types = std::array{PropertyStoreType::BOOL, PropertyStoreType::INT};

  auto prop_of_type1 = store1.GetPropertyOfTypes(PropertyId::FromInt(2), types);
  ASSERT_EQ(prop_of_type1, data1[1].second);

  auto prop_of_type2 = store2.GetPropertyOfTypes(PropertyId::FromInt(2), types);
  ASSERT_EQ(prop_of_type2, data2[1].second);

  auto prop_of_type3 = store3.GetPropertyOfTypes(PropertyId::FromInt(2), types);
  ASSERT_EQ(prop_of_type3, std::nullopt);
}

TEST(PropertyStore, ExtractPropertyValuesMissingAsNull) {
  auto test = [](std::vector<std::pair<PropertyId, PropertyValue>> const &data, std::span<int const> ids_to_read) {
    PropertyStore store;
    store.InitProperties(data);

    std::vector<PropertyPath> ids;
    ids.reserve(data.size());
    r::transform(ids_to_read, std::back_inserter(ids),
                 [](auto id) -> PropertyPath { return {PropertyId::FromInt(id)}; });

    auto const read_values = store.ExtractPropertyValuesMissingAsNull(ids);
    ASSERT_EQ(ids_to_read.size(), read_values.size());
    for (auto &[prop_id, value] : data) {
      auto id = std::find(ids.cbegin(), ids.cend(), prop_id);
      if (id != ids.cend()) {
        EXPECT_EQ(value, read_values[std::distance(ids.cbegin(), id)]);
      }
    }
  };

  test({{PropertyId::FromInt(1), PropertyValue()},
        {PropertyId::FromInt(2), PropertyValue("bravo")},
        {PropertyId::FromInt(3), PropertyValue("charlie")}},
       std::array{1, 2, 3});

  test({{PropertyId::FromInt(1), PropertyValue("alfa")},
        {PropertyId::FromInt(2), PropertyValue()},
        {PropertyId::FromInt(3), PropertyValue("charlie")}},
       std::array{1, 2, 3});

  test({{PropertyId::FromInt(1), PropertyValue("alfa")},
        {PropertyId::FromInt(2), PropertyValue("bravo")},
        {PropertyId::FromInt(3), PropertyValue()}},
       std::array{1, 2, 3});

  test({{PropertyId::FromInt(1), PropertyValue("alfa")},
        {PropertyId::FromInt(2), PropertyValue()},
        {PropertyId::FromInt(3), PropertyValue()}},
       std::array{1, 2, 3});

  test({{PropertyId::FromInt(1), PropertyValue()},
        {PropertyId::FromInt(2), PropertyValue("bravo")},
        {PropertyId::FromInt(3), PropertyValue()}},
       std::array{1, 2, 3});

  test({{PropertyId::FromInt(1), PropertyValue()},
        {PropertyId::FromInt(2), PropertyValue()},
        {PropertyId::FromInt(3), PropertyValue("charlie")}},
       std::array{1, 2, 3});

  test({{PropertyId::FromInt(1), PropertyValue()},
        {PropertyId::FromInt(2), PropertyValue()},
        {PropertyId::FromInt(3), PropertyValue()}},
       std::array{1, 2, 3});

  test({{PropertyId::FromInt(1), PropertyValue("alfa")},
        {PropertyId::FromInt(2), PropertyValue("bravo")},
        {PropertyId::FromInt(3), PropertyValue("charlie")},
        {PropertyId::FromInt(4), PropertyValue("delta")},
        {PropertyId::FromInt(5), PropertyValue("echo")}},
       std::array{1, 3, 5});
}

TEST(PropertyStore, HasMapsWithPropertyIdKeys) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);
  auto const p5 = PropertyId::FromInt(5);
  auto const p6 = PropertyId::FromInt(6);
  auto const p7 = PropertyId::FromInt(7);
  auto const p8 = PropertyId::FromInt(7);

  PropertyStore store;

  // Property store can have an empty map
  store.SetProperty(p1, PropertyValue{PropertyValue::map_t{}});
  ASSERT_TRUE(store.HasProperty(p1));
  EXPECT_EQ(store.GetProperty(p1).type(), PropertyValue::Type::Map);

  // Property store can have a map with one level
  auto map_p2 = PropertyValue{PropertyValue::map_t{
      {p3, PropertyValue("three")},
      {p4, PropertyValue("four")},
  }};

  store.SetProperty(p2, map_p2);
  ASSERT_TRUE(store.HasProperty(p2));
  ASSERT_EQ(store.GetProperty(p2).type(), PropertyValue::Type::Map);
  ASSERT_EQ(store.GetProperty(p2).ValueMap().size(), 2u);
  EXPECT_EQ(store.GetProperty(p2).ValueMap()[p3], PropertyValue("three"));
  EXPECT_EQ(store.GetProperty(p2).ValueMap()[p4], PropertyValue("four"));

  // Property store can have a map with multiple levels
  auto map_p5 = PropertyValue{PropertyValue::map_t{
      {p6,
       PropertyValue{PropertyValue::map_t{{p7, PropertyValue{PropertyValue::map_t{{p8, PropertyValue{"eight"}}}}}}}}}};

  store.SetProperty(p5, map_p5);
  ASSERT_TRUE(store.HasProperty(p5));
  ASSERT_EQ(store.GetProperty(p5).type(), PropertyValue::Type::Map);
  ASSERT_EQ(store.GetProperty(p5).ValueMap().size(), 1u);
  auto val6 = store.GetProperty(p5).ValueMap()[p6];
  ASSERT_EQ(val6.type(), PropertyValue::Type::Map);
  ASSERT_EQ(val6.ValueMap().size(), 1u);
  auto val7 = val6.ValueMap()[p7];
  ASSERT_EQ(val7.type(), PropertyValue::Type::Map);
  ASSERT_EQ(val7.ValueMap().size(), 1u);
  EXPECT_EQ(val7.ValueMap()[p8], PropertyValue("eight"));
}

TEST(PropertyStore, ArePropertiesEqual_ComparesOneNestedValue) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);
  auto const p5 = PropertyId::FromInt(5);

  const std::vector<std::pair<PropertyId, PropertyValue>> data{
      {p1, MakeMap(KVPair{p2, MakeMap(KVPair{p3, MakeMap(KVPair{p4, PropertyValue{"expected"}})})})}};

  struct Test {
    PropertyPath path;
    PropertyValue value;
    bool result;
  };

  for (auto &&test : {
           // clang-format off
    // Success, where nested property exists and value matches
    Test{.path = {p1, p2, p3, p4}, .value = PropertyValue{"expected"}, .result = true},
    // Fails because nested property is a different value
    Test{.path = {p1, p2, p3, p4}, .value = PropertyValue{"unexpected"}, .result = false},
    // Fails because nested property is a different type
    Test{.path = {p1, p2, p3, p4}, .value = PropertyValue{23}, .result = false},
    // Fails because final part of nested property path doens't exist
    Test{.path = {p1, p2, p3, p5}, .value = PropertyValue{"expected"}, .result = false},
    // Fails because intermediate parts of nested property path doens't exist
    Test{.path = {p1, p2, p5}, .value = PropertyValue{"expected"}, .result = false},
    Test{.path = {p1, p5}, .value = PropertyValue{"expected"}, .result = false},
    Test{.path = {p5}, .value = PropertyValue{"expected"}, .result = false}
           // clang-format on
       }) {
    PropertyStore store;
    store.InitProperties(data);
    EXPECT_EQ(store.ArePropertiesEqual(std::array{test.path}, std::array{test.value}, std::array<std::size_t, 1>{0}),
              std::vector{test.result});
  }
}

TEST(PropertyStore, ArePropertiesEqual_ComparesMultipleNestedValues) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);

  const std::vector<std::pair<PropertyId, PropertyValue>> data = {
      {p1, MakeMap(KVPair{p2, PropertyValue("apple")}, KVPair{p4, PropertyValue("banana")})}};

  PropertyStore store;
  store.InitProperties(data);

  struct Test {
    std::vector<PropertyPath> paths;
    std::vector<PropertyValue> values;
    std::vector<std::size_t> lookup;
    std::vector<bool> result;
  };

  using PP = PropertyPath;
  using PV = PropertyValue;

  for (auto &&test : {
           // clang-format off
    Test{.paths = {PP{p1, p2}}, .values = {PV{"apple"}}, .lookup = {0}, .result = {true}},
    Test{.paths = {PP{p1, p2}}, .values = {PV{"banana"}}, .lookup = {0}, .result = {false}},
    Test{.paths = {PP{p1, p2}, PP{p1, p4}}, .values = {PV{"apple"}, PV{"banana"}}, .lookup = {0, 1}, .result = {true, true}},
    Test{.paths = {PP{p1, p2}, PP{p1, p4}}, .values = {PV{"banana"}, PV{"apple"}}, .lookup = {1, 0}, .result = {true, true}},
    Test{.paths = {PP{p1, p2}, PP{p1, p4}}, .values = {PV{"xapple"}, PV{"xbanana"}}, .lookup = {0, 1}, .result = {false, false}},
    Test{.paths = {PP{p1, p4}}, .values = {PV{"banana"}}, .lookup = {0}, .result = {true}},
    Test{.paths = {PP{p1, p4}}, .values = {PV{"xbanana"}}, .lookup = {0}, .result = {false}},
    Test{.paths = {PP{p1, p2}, PP{p4}}, .values = {PV{"apple"}, PV{}}, .lookup = {0, 1}, .result = {true, true}},
    Test{.paths = {PP{p1, p2}, PP{p4}}, .values = {PV{"applex"}, PV{}}, .lookup = {0, 1}, .result = {false, true}},
    Test{.paths = {PP{p1, p2}, PP{p4}}, .values = {PV{}, PV{"applex"}}, .lookup = {1, 0}, .result = {false, true}},
    Test{.paths = {PP{p1, p3}, PP{p1, p4}}, .values = {PV{}, PV{"banana"}}, .lookup = {0, 1}, .result = {true, true}},
    Test{.paths = {PP{p1, p3}, PP{p1, p4}}, .values = {PV{}, PV{"xbanana"}}, .lookup = {0, 1}, .result = {true, false}},
    Test{.paths = {PP{p1, p2}, PP{p1, p3}}, .values = {PV{"apple"}, PV{"banana"}}, .lookup = {0, 1}, .result = {true, false}},
    Test{.paths = {PP{p1, p2}, PP{p1, p3}, PP{p1, p4}}, .values = {PV{"apple"}, PV{"banana"}, PV{"banana"}}, .lookup = {0, 1, 2}, .result = {true, false, true}},
    Test{.paths = {PP{p1, p3}, PP{p4}}, .values = {PV{}, PV{}}, .lookup = {0, 1}, .result = {true, true}},
    Test{.paths = {PP{p3}}, .values = {PV{}}, .lookup = {0}, .result = {true}},
    Test{.paths = {PP{p4}}, .values = {PV{}}, .lookup = {0}, .result = {true}},
           // clang-format on
       }) {
    EXPECT_EQ(store.ArePropertiesEqual(test.paths, test.values, test.lookup), test.result);
  }
}

TEST(PropertyStore, ArePropertiesEqual_ComparesMultipleNestedMaps) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);
  auto const p5 = PropertyId::FromInt(5);
  auto const p6 = PropertyId::FromInt(6);
  auto const p7 = PropertyId::FromInt(7);

  auto map_prop_value_1 = MakeMap(KVPair{p3, MakeMap(KVPair{p4, PropertyValue{"apple"}})});
  auto map_prop_value_2 = MakeMap(KVPair{p6, MakeMap(KVPair{p7, PropertyValue{"banana"}})});

  const std::vector<std::pair<PropertyId, PropertyValue>> data = {
      {p1, MakeMap(KVPair{p2, map_prop_value_1}, KVPair{p5, map_prop_value_2})}};

  PropertyStore store;
  store.InitProperties(data);

  EXPECT_EQ(store.ArePropertiesEqual(std::array{PropertyPath{p1, p2}, PropertyPath{p1, p5}},
                                     std::array{map_prop_value_1, map_prop_value_2}, std::array<std::size_t, 2>{0, 1}),
            (std::vector{true, true}));
}

TEST(PropertyStore, ExtractPropertyValuesMissingAsNull_ReturnsNullsForAllItemsWithAnEmptyStore) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);
  auto const p5 = PropertyId::FromInt(5);
  auto const p6 = PropertyId::FromInt(6);

  PropertyStore store;

  EXPECT_EQ(store.ExtractPropertyValuesMissingAsNull(
                std::vector{PropertyPath{p1}, PropertyPath{p2, p3}, PropertyPath{p4, p5, p6}}),
            (std::vector{
                PropertyValue(),
                PropertyValue(),
                PropertyValue(),
            }));
}

TEST(PropertyStore, ExtractPropertyValuesMissingAsNull_CanReadNestedValuesOnSameBranch) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);
  auto const p5 = PropertyId::FromInt(5);

  const std::vector<std::pair<PropertyId, PropertyValue>> data = {
      {p1, MakeMap(KVPair{p2, MakeMap(KVPair{p3, PropertyValue("apple")}, KVPair{p4, PropertyValue("banana")})},
                   KVPair(p5, PropertyValue("cherry")))}};

  PropertyStore store;
  store.InitProperties(data);

  EXPECT_EQ(store.ExtractPropertyValuesMissingAsNull(
                std::vector{PropertyPath{p1, p2, p3}, PropertyPath{p1, p2, p4}, PropertyPath{p1, p5}}),
            (std::vector{
                PropertyValue("apple"),
                PropertyValue("banana"),
                PropertyValue("cherry"),
            }));
}

TEST(PropertyStore, ExtractPropertyValuesMissingAsNull_DoesNotReadPropertiesFromWrongDepth) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);

  const std::vector<std::pair<PropertyId, PropertyValue>> data = {
      {p1, MakeMap(KVPair{p2, PropertyValue("apple")}, KVPair{p4, PropertyValue("banana")})}};

  PropertyStore store;
  store.InitProperties(data);

  struct Test {
    std::vector<PropertyPath> paths;
    std::vector<PropertyValue> values;
  };

  std::vector<Test> tests = {
      // clang format off
      {std::vector{PropertyPath{p1, p2}}, std::vector{PropertyValue("apple")}},
      {std::vector{PropertyPath{p1, p2}, PropertyPath{p1, p4}},
       std::vector{PropertyValue("apple"), PropertyValue("banana")}},
      {std::vector{PropertyPath{p1, p4}}, std::vector{PropertyValue("banana")}},
      {std::vector{PropertyPath{p1, p2}, PropertyPath{p4}}, std::vector{PropertyValue("apple"), PropertyValue{}}},
      {std::vector{PropertyPath{p1, p3}, PropertyPath{p1, p4}}, std::vector{PropertyValue{}, PropertyValue("banana")}},
      {std::vector{PropertyPath{p1, p2}, PropertyPath{p1, p3}}, std::vector{PropertyValue("apple"), PropertyValue{}}},
      {std::vector{PropertyPath{p1, p3}, PropertyPath{p4}}, std::vector{PropertyValue{}, PropertyValue{}}},
      {std::vector{PropertyPath{p3}}, std::vector{PropertyValue{}}},
      {std::vector{PropertyPath{p4}}, std::vector{PropertyValue{}}},
      // clang format on
  };

  for (auto &&[paths, values] : tests) {
    EXPECT_EQ(store.ExtractPropertyValuesMissingAsNull(paths), values);
  }
}

//==============================================================================

TEST(PropertiesPermutationHelper, CanReadOneValueFromStore) {
  auto const p1 = PropertyId::FromInt(1);
  const std::vector<std::pair<PropertyId, PropertyValue>> data{
      {p1, PropertyValue("test-value")},
  };

  PropertyStore store;
  store.InitProperties(data);

  PropertiesPermutationHelper prop_reader{std::array{PropertyPath{p1}}};
  auto values = prop_reader.ApplyPermutation(prop_reader.Extract(store)).values_;
  ASSERT_EQ(1u, values.size());
  EXPECT_EQ(values[0], data[0].second);
}

TEST(PropertiesPermutationHelper, CanReadTwoValuesInOrderFromStore) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  const std::vector<std::pair<PropertyId, PropertyValue>> data{
      {p1, PropertyValue("test-value")},
      {p2, PropertyValue(42)},
  };

  PropertyStore store;
  store.InitProperties(data);

  PropertiesPermutationHelper prop_reader(std::array{PropertyPath{p1}, PropertyPath{p2}});
  auto values = prop_reader.ApplyPermutation(prop_reader.Extract(store)).values_;
  ASSERT_EQ(2u, values.size());
  EXPECT_EQ(values[0], data[0].second);
  EXPECT_EQ(values[1], data[1].second);
}

TEST(PropertiesPermutationHelper, CanReadTwoValuesOutOfOrderFromStore) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  const std::vector<std::pair<PropertyId, PropertyValue>> data{
      {p1, PropertyValue("test-value")},
      {p2, PropertyValue(42)},
  };

  PropertyStore store;
  store.InitProperties(data);

  PropertiesPermutationHelper prop_reader{std::array{PropertyPath{p2}, PropertyPath{p1}}};
  auto values = prop_reader.ApplyPermutation(prop_reader.Extract(store)).values_;
  ASSERT_EQ(2u, values.size());
  EXPECT_EQ(values[0], data[1].second);
  EXPECT_EQ(values[1], data[0].second);
}

TEST(PropertiesPermutationHelper, CanReadMultipleValuesOutOfOrderFromStore) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);
  const std::vector<std::pair<PropertyId, PropertyValue>> data{
      {p1, PropertyValue("test-value")},
      {p2, PropertyValue(42)},
      {p3, PropertyValue(true)},
      {p4, PropertyValue(3.141592f)},
  };

  PropertyStore store;
  store.InitProperties(data);

  PropertiesPermutationHelper prop_reader{
      std::vector{PropertyPath{p3}, PropertyPath{p1}, PropertyPath{p4}, PropertyPath{p2}}};
  auto values = prop_reader.ApplyPermutation(prop_reader.Extract(store)).values_;
  ASSERT_EQ(4u, values.size());

  EXPECT_EQ(values[0], data[2].second);
  EXPECT_EQ(values[1], data[0].second);
  EXPECT_EQ(values[2], data[3].second);
  EXPECT_EQ(values[3], data[1].second);
}

TEST(PropertiesPermutationHelper, CanExtractSinglyNestedValuesFromMap) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);

  const std::vector<std::pair<PropertyId, PropertyValue>> data{
      {p1, PropertyValue{PropertyValue::map_t{{p2, PropertyValue{"test-value"}}}}}};

  PropertyStore store;
  store.InitProperties(data);

  PropertiesPermutationHelper prop_reader{std::vector<PropertyPath>{PropertyPath{p1, p2}}};
  auto values = prop_reader.ApplyPermutation(prop_reader.Extract(store)).values_;
  ASSERT_EQ(1u, values.size());
  EXPECT_EQ(values[0], PropertyValue{"test-value"});
}

TEST(PropertiesPermutationHelper, ExtractWillReturnNullForMissingNestedValues) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);

  const std::vector<std::pair<PropertyId, PropertyValue>> data{
      {p1, PropertyValue{PropertyValue::map_t{{p2, PropertyValue{"test-value"}}}}}, {p2, PropertyValue{"two"}}};

  PropertyStore store;
  store.InitProperties(data);

  for (auto &&path : {PropertyPath{p1, p3}, PropertyPath{p2, p3}, PropertyPath{p1, p2, p3}}) {
    PropertiesPermutationHelper prop_reader{std::vector<PropertyPath>{path}};
    auto values = prop_reader.ApplyPermutation(prop_reader.Extract(store)).values_;
    ASSERT_EQ(1u, values.size());
    EXPECT_EQ(values[0], PropertyValue{});
  };
}

TEST(PropertiesPermutationHelper, CanExtractDeeplyNestedValuesFromMap) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);

  const std::vector<std::pair<PropertyId, PropertyValue>> data{
      {p3, MakeMap(KVPair{p1, MakeMap(KVPair{p4, MakeMap(KVPair{p2, PropertyValue{"test-value"}})})})}};

  PropertyStore store;
  store.InitProperties(data);

  PropertiesPermutationHelper prop_reader{std::vector<PropertyPath>{PropertyPath{p3, p1, p4, p2}}};
  auto values = prop_reader.ApplyPermutation(prop_reader.Extract(store)).values_;
  ASSERT_EQ(1u, values.size());
  EXPECT_EQ(values[0], PropertyValue{"test-value"});
}

TEST(PropertiesPermutationHelper, CanExtractPermutedNestedValues) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);
  auto const p5 = PropertyId::FromInt(5);
  auto const p6 = PropertyId::FromInt(6);
  auto const p7 = PropertyId::FromInt(7);
  auto const p8 = PropertyId::FromInt(8);

  {
    const std::vector<std::pair<PropertyId, PropertyValue>> data{
        {p1, MakeMap(KVPair{p2, MakeMap(KVPair{p3, PropertyValue{"apple"}})})},
        {p4, MakeMap(KVPair{p5, PropertyValue{"banana"}})},
        {p6, PropertyValue{"cherry"}},
        {p7, MakeMap(KVPair{p8, PropertyValue{"date"}})}};

    PropertyStore store;
    store.InitProperties(data);

    PropertiesPermutationHelper prop_reader{std::vector<PropertyPath>{PropertyPath{p7, p8}, PropertyPath{p4, p5},
                                                                      PropertyPath{p1, p2, p3}, PropertyPath{p6}}};
    auto values = prop_reader.ApplyPermutation(prop_reader.Extract(store)).values_;
    ASSERT_EQ(4u, values.size());
    EXPECT_EQ(values[0], PropertyValue{"date"});
    EXPECT_EQ(values[1], PropertyValue{"banana"});
    EXPECT_EQ(values[2], PropertyValue{"apple"});
    EXPECT_EQ(values[3], PropertyValue{"cherry"});
  }

  {
    const std::vector<std::pair<PropertyId, PropertyValue>> data = {
        {p1, MakeMap(KVPair{p1, PropertyValue("apple")}, KVPair{p2, PropertyValue("banana")},
                     KVPair{p3, PropertyValue("cherry")})}};

    PropertyStore store;
    store.InitProperties(data);

    PropertiesPermutationHelper prop_reader{
        std::vector<PropertyPath>{PropertyPath{p1, p2}, PropertyPath{p1, p3}, PropertyPath{p1, p1}}};
    auto values = prop_reader.ApplyPermutation(prop_reader.Extract(store)).values_;
    ASSERT_EQ(3u, values.size());
    EXPECT_EQ(values[0], PropertyValue{"banana"});
    EXPECT_EQ(values[1], PropertyValue{"cherry"});
    EXPECT_EQ(values[2], PropertyValue{"apple"});
  }
}

TEST(PropertiesPermutationHelper, CanExtractMultipleValuesFromSameTopMostProperty) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);
  auto const p5 = PropertyId::FromInt(5);

  const std::vector<std::pair<PropertyId, PropertyValue>> data = {
      {p1, MakeMap(KVPair{p2, MakeMap(KVPair{p3, PropertyValue("apple")}, KVPair{p4, PropertyValue("banana")},
                                      KVPair{p5, PropertyValue("cherry")})})}};

  PropertyStore store;
  store.InitProperties(data);

  PropertiesPermutationHelper prop_reader{
      std::vector<PropertyPath>{PropertyPath{p1, p2, p3}, PropertyPath{p1, p2, p4}, PropertyPath{p1, p2, p5}}};

  auto values = prop_reader.ApplyPermutation(prop_reader.Extract(store)).values_;
  ASSERT_EQ(3u, values.size());
  EXPECT_EQ(values[0], PropertyValue{"apple"});
  EXPECT_EQ(values[1], PropertyValue{"banana"});
  EXPECT_EQ(values[2], PropertyValue{"cherry"});
}

TEST(PropertiesPermutationHelper, MatchesValue_ProducesVectorOfPositionsAndComparisons) {
  using Match = std::pair<std::ptrdiff_t, bool>;

  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);
  auto const p5 = PropertyId::FromInt(5);
  auto const p6 = PropertyId::FromInt(6);
  auto const p7 = PropertyId::FromInt(7);

  PropertiesPermutationHelper prop_reader{std::array{PropertyPath{p1, p2}, PropertyPath{p1, p3}, PropertyPath{p1, p4},
                                                     PropertyPath{p5, p6}, PropertyPath{p7}}};

  IndexOrderedPropertyValues const baseline{{
      PropertyValue("apple"),
      PropertyValue("banana"),
      PropertyValue("cherry"),
      PropertyValue("date"),
      PropertyValue("eggplant"),
  }};

  // No root properties for `p6`
  EXPECT_THAT(prop_reader.MatchesValue(p6, PropertyValue("eggplant"), baseline), UnorderedElementsAre());

  // Three root properties for `p1`, match all values
  EXPECT_THAT(prop_reader.MatchesValue(p1,
                                       PropertyValue(PropertyValue::map_t{
                                           {p2, PropertyValue("apple")},
                                           {p3, PropertyValue("banana")},
                                           {p4, PropertyValue("cherry")},
                                       }),
                                       baseline),
              UnorderedElementsAre(Match(0, true), Match(1, true), Match(2, true)));

  // Three root properties for `p1`, fails to match values because value is not a map
  EXPECT_THAT(prop_reader.MatchesValue(p1, PropertyValue("grapefruit"), baseline),
              UnorderedElementsAre(Match(0, false), Match(1, false), Match(2, false)));

  // Three root properties for `p1`, fails to match values because nested values are missing
  EXPECT_THAT(prop_reader.MatchesValue(p1,
                                       PropertyValue(PropertyValue::map_t{
                                           {p5, PropertyValue("grapefruit")},
                                           {p6, PropertyValue("honeydew melon")},
                                       }),
                                       baseline),
              UnorderedElementsAre(Match(0, false), Match(1, false), Match(2, false)));

  // Three root properties for `p1`, match no values because they are different
  EXPECT_THAT(prop_reader.MatchesValue(p1,
                                       PropertyValue(PropertyValue::map_t{
                                           {p2, PropertyValue("banana")},
                                           {p3, PropertyValue("apple")},
                                           {p4, PropertyValue("apple")},
                                       }),
                                       baseline),
              UnorderedElementsAre(Match(0, false), Match(1, false), Match(2, false)));

  // Three root properties for `p1`, match just one value as others missing
  EXPECT_THAT(prop_reader.MatchesValue(p1,
                                       PropertyValue(PropertyValue::map_t{
                                           {p3, PropertyValue("banana")},
                                       }),
                                       baseline),
              UnorderedElementsAre(Match(0, false), Match(1, true), Match(2, false)));

  // Three root properties for `p1`, match just one value as others different
  EXPECT_THAT(prop_reader.MatchesValue(p1,
                                       PropertyValue(PropertyValue::map_t{
                                           {p2, PropertyValue("apple")},
                                           {p3, PropertyValue("grapefruit")},
                                           {p4, PropertyValue("honeydew melon")},
                                       }),
                                       baseline),
              UnorderedElementsAre(Match(0, true), Match(1, false), Match(2, false)));

  // Test positively against non-nested property p7
  EXPECT_THAT(prop_reader.MatchesValue(p7, PropertyValue("eggplant"), baseline), UnorderedElementsAre(Match(4, true)));

  // Test negatively against non-nested property p7
  EXPECT_THAT(prop_reader.MatchesValue(p7, PropertyValue("grapefruit"), baseline),
              UnorderedElementsAre(Match(4, false)));
}

TEST(PropertiesPermutationHelper, MatchesValue_ComparesOutOfOrderProperties) {
  using Match = std::pair<std::ptrdiff_t, bool>;

  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);

  PropertiesPermutationHelper prop_reader{std::array{
      PropertyPath{p3, p4},
      PropertyPath{p1, p2},
  }};

  IndexOrderedPropertyValues const baseline{{
      PropertyValue("apple"),   // corresponds to p3.p4; ordered-index[1]
      PropertyValue("banana"),  // corresponds to p1.p2; ordered-index[0]
  }};

  EXPECT_THAT(
      prop_reader.MatchesValue(p1, PropertyValue(PropertyValue::map_t{{p2, PropertyValue("cherry")}}), baseline),
      UnorderedElementsAre(Match(0, false)));

  EXPECT_THAT(
      prop_reader.MatchesValue(p1, PropertyValue(PropertyValue::map_t{{p2, PropertyValue("banana")}}), baseline),
      UnorderedElementsAre(Match(0, true)));

  EXPECT_THAT(
      prop_reader.MatchesValue(p3, PropertyValue(PropertyValue::map_t{{p2, PropertyValue("cherry")}}), baseline),
      UnorderedElementsAre(Match(1, false)));

  EXPECT_THAT(prop_reader.MatchesValue(p3, PropertyValue(PropertyValue::map_t{{p4, PropertyValue("apple")}}), baseline),
              UnorderedElementsAre(Match(1, true)));
}

TEST(PropertiesPermutationHelper, MatchesValue_ComparesOutOfOrderPropertiesWhenRootPropertiesAreDuplicated) {
  using Match = std::pair<std::ptrdiff_t, bool>;

  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);
  auto const p5 = PropertyId::FromInt(5);
  auto const p6 = PropertyId::FromInt(6);

  PropertiesPermutationHelper prop_reader{
      std::array{PropertyPath{p3, p4}, PropertyPath{p1, p6}, PropertyPath{p3, p5}, PropertyPath{p1, p2}}};

  IndexOrderedPropertyValues const baseline{{
      PropertyValue("apple"),   // corresponds to p3.p4; ordered-index[2]
      PropertyValue("banana"),  // corresponds to p1.p6; ordered-index[1]
      PropertyValue("cherry"),  // corresponds to p3.p5; ordered-index[3]
      PropertyValue("date"),    // corresponds to p1.p2; ordered-index[0]
  }};

  EXPECT_THAT(
      prop_reader.MatchesValue(p1, PropertyValue(PropertyValue::map_t{{p2, PropertyValue("eggplant")}}), baseline),
      UnorderedElementsAre(Match(0, false), (Match(1, false))));

  EXPECT_THAT(
      prop_reader.MatchesValue(p1, PropertyValue(PropertyValue::map_t{{p5, PropertyValue("eggplant")}}), baseline),
      UnorderedElementsAre(Match(0, false), (Match(1, false))));

  EXPECT_THAT(
      prop_reader.MatchesValue(p1, PropertyValue(PropertyValue::map_t{{p6, PropertyValue("eggplant")}}), baseline),
      UnorderedElementsAre(Match(0, false), (Match(1, false))));

  EXPECT_THAT(prop_reader.MatchesValue(p1, PropertyValue(PropertyValue::map_t{{p2, PropertyValue("date")}}), baseline),
              UnorderedElementsAre(Match(0, true), (Match(1, false))));

  EXPECT_THAT(
      prop_reader.MatchesValue(p1, PropertyValue(PropertyValue::map_t{{p6, PropertyValue("banana")}}), baseline),
      UnorderedElementsAre(Match(0, false), (Match(1, true))));

  EXPECT_THAT(prop_reader.MatchesValue(
                  p1, PropertyValue(PropertyValue::map_t{{p2, PropertyValue("date")}, {p6, PropertyValue("banana")}}),
                  baseline),
              UnorderedElementsAre(Match(0, true), (Match(1, true))));

  EXPECT_THAT(
      prop_reader.MatchesValue(p3, PropertyValue(PropertyValue::map_t{{p4, PropertyValue("eggplant")}}), baseline),
      UnorderedElementsAre(Match(2, false), (Match(3, false))));

  EXPECT_THAT(
      prop_reader.MatchesValue(p3, PropertyValue(PropertyValue::map_t{{p3, PropertyValue("eggplant")}}), baseline),
      UnorderedElementsAre(Match(2, false), (Match(3, false))));

  EXPECT_THAT(
      prop_reader.MatchesValue(p3, PropertyValue(PropertyValue::map_t{{p6, PropertyValue("eggplant")}}), baseline),
      UnorderedElementsAre(Match(2, false), (Match(3, false))));

  EXPECT_THAT(prop_reader.MatchesValue(p3, PropertyValue(PropertyValue::map_t{{p4, PropertyValue("apple")}}), baseline),
              UnorderedElementsAre(Match(2, true), (Match(3, false))));

  EXPECT_THAT(
      prop_reader.MatchesValue(p3, PropertyValue(PropertyValue::map_t{{p5, PropertyValue("cherry")}}), baseline),
      UnorderedElementsAre(Match(2, false), (Match(3, true))));

  EXPECT_THAT(prop_reader.MatchesValue(
                  p3, PropertyValue(PropertyValue::map_t{{p4, PropertyValue("apple")}, {p5, PropertyValue("cherry")}}),
                  baseline),
              UnorderedElementsAre(Match(2, true), (Match(3, true))));
}

TEST(PropertiesPermutationHelper, ExtractContinuesReadsIfNestedValueIsNull) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);

  const std::vector<std::pair<PropertyId, PropertyValue>> data{
      {p1, MakeMap(KVPair{p4, PropertyValue(0)})}, {p2, PropertyValue()}, {p3, MakeMap(KVPair{p4, PropertyValue(20)})}};

  PropertyStore store;
  store.InitProperties(data);

  PropertiesPermutationHelper prop_reader{std::vector<PropertyPath>{
      PropertyPath{p2, p4},
      PropertyPath{p1, p4},
      PropertyPath{p3, p4},
  }};

  // Read values back unpermuted and unnested
  EXPECT_EQ(store.GetProperty(p2), PropertyValue());
  EXPECT_EQ(store.GetProperty(p1), MakeMap(KVPair{p4, PropertyValue(0)}));
  EXPECT_EQ(store.GetProperty(p3), MakeMap(KVPair{p4, PropertyValue(20)}));

  // Read leaf nested values back in a single pass
  auto values = prop_reader.ApplyPermutation(prop_reader.Extract(store)).values_;
  ASSERT_EQ(3u, values.size());
  EXPECT_EQ(values[0], PropertyValue());
  EXPECT_EQ(values[1], PropertyValue(0));
  EXPECT_EQ(values[2], PropertyValue(20));
}

//==============================================================================

TEST(PropertiesPermutationHelper, MatchesValues_ReturnsABooleanMaskOfMatches) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);

  PropertiesPermutationHelper prop_reader{
      std::array{PropertyPath{p1}, PropertyPath{p2}, PropertyPath{p3}, PropertyPath{p4}}};

  const std::vector<std::pair<PropertyId, PropertyValue>> data = {{p1, PropertyValue{"apple"}},
                                                                  {p2, PropertyValue{"banana"}},
                                                                  {p3, PropertyValue{"cherry"}},
                                                                  {p4, PropertyValue{"date"}}};

  PropertyStore store;
  store.InitProperties(data);

  EXPECT_EQ(prop_reader.MatchesValues(store, std::vector{PropertyValue{"apple"}, PropertyValue{"banana"},
                                                         PropertyValue{"cherry"}, PropertyValue{"date"}}),
            (std::vector{true, true, true, true}));

  EXPECT_EQ(prop_reader.MatchesValues(store, std::vector{PropertyValue{"applex"}, PropertyValue{"bananax"},
                                                         PropertyValue{"cherryx"}, PropertyValue{"datex"}}),
            (std::vector{false, false, false, false}));

  EXPECT_EQ(prop_reader.MatchesValues(store, std::vector{PropertyValue{"apple"}, PropertyValue{"bananax"},
                                                         PropertyValue{"cherry"}, PropertyValue{"datex"}}),
            (std::vector{true, false, true, false}));
}

TEST(PropertiesPermutationHelper, MatchesValues_WorksWithOutOfOrderProperties) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);

  PropertiesPermutationHelper prop_reader{
      std::array{PropertyPath{p3}, PropertyPath{p1}, PropertyPath{p2}, PropertyPath{p4}}};

  const std::vector<std::pair<PropertyId, PropertyValue>> data = {{p1, PropertyValue{"apple"}},
                                                                  {p2, PropertyValue{"banana"}},
                                                                  {p3, PropertyValue{"cherry"}},
                                                                  {p4, PropertyValue{"date"}}};

  PropertyStore store;
  store.InitProperties(data);

  EXPECT_EQ(prop_reader.MatchesValues(store, std::vector{PropertyValue{"cherry"}, PropertyValue{"apple"},
                                                         PropertyValue{"banana"}, PropertyValue{"date"}}),
            (std::vector{true, true, true, true}));

  EXPECT_EQ(prop_reader.MatchesValues(store, std::vector{PropertyValue{"apple"}, PropertyValue{"banana"},
                                                         PropertyValue{"cherry"}, PropertyValue{"date"}}),
            (std::vector{false, false, false, true}));
}

TEST(PropertiesPermutationHelper, MatchesValues_WorksWithNestedProperties) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);

  PropertiesPermutationHelper prop_reader{
      std::array{PropertyPath{p1, p2}, PropertyPath{p1, p3}, PropertyPath{p1, p1}, PropertyPath{p4}}};

  const std::vector<std::pair<PropertyId, PropertyValue>> data{
      {p1, MakeMap(KVPair{p1, PropertyValue{"apple"}}, KVPair{p2, PropertyValue{"banana"}},
                   KVPair{p3, PropertyValue{"cherry"}})},
      {p4, PropertyValue{"date"}}};

  PropertyStore store;
  store.InitProperties(data);

  EXPECT_EQ(prop_reader.MatchesValues(store, std::vector{PropertyValue{"banana"}, PropertyValue{"cherry"},
                                                         PropertyValue{"apple"}, PropertyValue{"date"}}),
            (std::vector{true, true, true, true}));

  EXPECT_EQ(prop_reader.MatchesValues(store, std::vector{PropertyValue{"apple"}, PropertyValue{"cherry"},
                                                         PropertyValue{"banana"}, PropertyValue{"date"}}),
            (std::vector{false, false, true, true}));
}

//==============================================================================

TEST(ReadNestedPropertyValue, RetrievesPositionalPointerToNestedPropertyValue) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);

  auto const value = MakeMap(KVPair{p1, MakeMap(KVPair{p2, MakeMap(KVPair{p3, PropertyValue("apple")})})});
  ASSERT_THAT(ReadNestedPropertyValue(value, std::array{p1, p2, p3}), NotNull());
  EXPECT_EQ(*ReadNestedPropertyValue(value, std::array{p1, p2, p3}), PropertyValue("apple"));
  EXPECT_THAT(ReadNestedPropertyValue(value, std::array{p1, p2, p4}), IsNull());
  EXPECT_THAT(ReadNestedPropertyValue(value, std::array{p1, p3}), IsNull());
  EXPECT_THAT(ReadNestedPropertyValue(value, std::array{p3}), IsNull());
  EXPECT_THAT(ReadNestedPropertyValue(value, std::array{p4}), IsNull());
}

//==============================================================================

TEST(PropertyStore, DecodeExpectedPropertyType) {
  auto const prop1 = PropertyId::FromInt(1);
  auto const prop2 = PropertyId::FromInt(2);
  auto const prop3 = PropertyId::FromInt(3);
  auto const prop4 = PropertyId::FromInt(4);
  auto const prop5 = PropertyId::FromInt(5);
  auto const prop6 = PropertyId::FromInt(6);
  auto const prop7 = PropertyId::FromInt(7);
  auto const prop8 = PropertyId::FromInt(8);
  auto const prop9 = PropertyId::FromInt(9);
  auto const prop10 = PropertyId::FromInt(10);
  auto const prop11 = PropertyId::FromInt(11);
  auto const prop12 = PropertyId::FromInt(12);
  auto const prop13 = PropertyId::FromInt(13);
  auto const prop14 = PropertyId::FromInt(14);

  {
    PropertyStore store;
    std::vector<std::pair<PropertyId, PropertyValue>> data{
        {prop1, PropertyValue()},
        {prop2, PropertyValue(true)},
        {prop3, PropertyValue(42)},
        {prop4, PropertyValue(3.14)},
        {prop5, PropertyValue("test")},
        {prop6, PropertyValue(std::vector<PropertyValue>{PropertyValue(1), PropertyValue(2)})},
        {prop7, PropertyValue(std::vector<int>{1, 2, 3})},
        {prop8, PropertyValue(std::vector<double>{1.0, 2.0, 3.0})},
        {prop9, PropertyValue(std::vector<std::variant<int, double>>{1, 2.0, 3})},
        {prop10, PropertyValue(PropertyValue::map_t{{PropertyId::FromUint(1), PropertyValue(1)}})},
        {prop11, PropertyValue(TemporalData(TemporalType::Date, 23))},
        {prop12, PropertyValue(GetSampleZonedTemporal())},
        {prop13, PropertyValue(Enum{EnumTypeId{2}, EnumValueId{42}})},
        {prop14, PropertyValue{Point2d{Cartesian_2d, 1.0, 2.0}}},
    };
    EXPECT_TRUE(store.InitProperties(data));
    EXPECT_EQ(store.GetExtendedPropertyType(prop1), ExtendedPropertyType{PropertyValue::Type::Null});
    EXPECT_EQ(store.GetExtendedPropertyType(prop2), ExtendedPropertyType{PropertyValue::Type::Bool});
    EXPECT_EQ(store.GetExtendedPropertyType(prop3), ExtendedPropertyType{PropertyValue::Type::Int});
    EXPECT_EQ(store.GetExtendedPropertyType(prop4), ExtendedPropertyType{PropertyValue::Type::Double});
    EXPECT_EQ(store.GetExtendedPropertyType(prop5), ExtendedPropertyType{PropertyValue::Type::String});
    EXPECT_EQ(store.GetExtendedPropertyType(prop6), ExtendedPropertyType{PropertyValue::Type::List});
    EXPECT_EQ(store.GetExtendedPropertyType(prop7), ExtendedPropertyType{PropertyValue::Type::List});
    EXPECT_EQ(store.GetExtendedPropertyType(prop8), ExtendedPropertyType{PropertyValue::Type::List});
    EXPECT_EQ(store.GetExtendedPropertyType(prop9), ExtendedPropertyType{PropertyValue::Type::List});
    EXPECT_EQ(store.GetExtendedPropertyType(prop10), ExtendedPropertyType{PropertyValue::Type::Map});
    EXPECT_EQ(store.GetExtendedPropertyType(prop11), ExtendedPropertyType{TemporalType::Date});
    EXPECT_EQ(store.GetExtendedPropertyType(prop12), ExtendedPropertyType{PropertyValue::Type::ZonedTemporalData});
    EXPECT_EQ(store.GetExtendedPropertyType(prop13), ExtendedPropertyType{EnumTypeId{2}});
    EXPECT_EQ(store.GetExtendedPropertyType(prop14), ExtendedPropertyType{PropertyValue::Type::Point2d});
  }

  {
    PropertyStore store;
    std::vector<std::pair<PropertyId, PropertyValue>> data{
        {prop1, PropertyValue(TemporalData(TemporalType::Date, 23))},
        {prop2, PropertyValue(TemporalData(TemporalType::LocalDateTime, 2000))},
    };
    EXPECT_TRUE(store.InitProperties(data));
    auto type1 = store.GetExtendedPropertyType(prop1);
    auto type2 = store.GetExtendedPropertyType(prop2);
    EXPECT_EQ(type1.type, PropertyValue::Type::TemporalData);
    EXPECT_EQ(type1.temporal_type, TemporalType::Date);
    EXPECT_EQ(type2.type, PropertyValue::Type::TemporalData);
    EXPECT_EQ(type2.temporal_type, TemporalType::LocalDateTime);
  }

  {
    PropertyStore store;
    std::vector<std::pair<PropertyId, PropertyValue>> data{
        {prop1, PropertyValue(Enum{EnumTypeId{1}, EnumValueId{10}})},
        {prop2, PropertyValue(Enum{EnumTypeId{5}, EnumValueId{20}})},
    };
    EXPECT_TRUE(store.InitProperties(data));
    auto type1 = store.GetExtendedPropertyType(prop1);
    auto type2 = store.GetExtendedPropertyType(prop2);
    EXPECT_EQ(type1.type, PropertyValue::Type::Enum);
    EXPECT_EQ(type1.enum_type, EnumTypeId{1});
    EXPECT_EQ(type2.type, PropertyValue::Type::Enum);
    EXPECT_EQ(type2.enum_type, EnumTypeId{5});
  }
}

//==============================================================================

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  int result = RUN_ALL_TESTS();

  // now run with compression on
  FLAGS_storage_property_store_compression_enabled = true;
  result &= RUN_ALL_TESTS();
  return result;
}
