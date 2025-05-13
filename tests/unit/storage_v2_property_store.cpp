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
#include "storage/v2/indices/label_property_index.hpp"  // @TODO temp for `PropertiesPermutationHelper`
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"

using testing::IsNull;
using testing::NotNull;
using testing::UnorderedElementsAre;

using namespace memgraph::storage;
using enum CoordinateReferenceSystem;

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
/*
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
}*/

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

/* @TODO put back
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
*/
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

/* @TODO put back
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
*/
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

    std::vector<PropertyId> ids;
    ids.reserve(data.size());
    std::ranges::transform(ids_to_read, std::back_inserter(ids), [](auto id) { return PropertyId::FromInt(id); });

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

  auto const make_map = [](PropertyId key, PropertyValue value) {
    return PropertyValue{PropertyValue::map_t{{key, std::move(value)}}};
  };

  const std::vector<std::pair<PropertyId, PropertyValue>> data{
      {p1, make_map(p2, make_map(p3, make_map(p4, PropertyValue{"expected"})))}};

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

// @TODO test reader leaves read cursor in correct position for reading
// successive properties.

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

  auto const make_map = [](PropertyId key, PropertyValue value) {
    return PropertyValue{PropertyValue::map_t{{key, std::move(value)}}};
  };

  const std::vector<std::pair<PropertyId, PropertyValue>> data{
      {p3, make_map(p1, make_map(p4, make_map(p2, PropertyValue{"test-value"})))}};

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

  auto const make_map = [](PropertyId key, PropertyValue value) {
    return PropertyValue{PropertyValue::map_t{{key, std::move(value)}}};
  };

  const std::vector<std::pair<PropertyId, PropertyValue>> data{{p1, make_map(p2, make_map(p3, PropertyValue{"apple"}))},
                                                               {p4, make_map(p5, PropertyValue{"banana"})},
                                                               {p6, PropertyValue{"cherry"}},
                                                               {p7, make_map(p8, PropertyValue{"date"})}};

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
      PropertyValue("apple"),
      PropertyValue("banana"),
  }};

  EXPECT_THAT(
      prop_reader.MatchesValue(p1, PropertyValue(PropertyValue::map_t{{p2, PropertyValue("cherry")}}), baseline),
      UnorderedElementsAre(Match(1, false)));

  EXPECT_THAT(prop_reader.MatchesValue(p1, PropertyValue(PropertyValue::map_t{{p2, PropertyValue("apple")}}), baseline),
              UnorderedElementsAre(Match(1, true)));

  EXPECT_THAT(
      prop_reader.MatchesValue(p3, PropertyValue(PropertyValue::map_t{{p2, PropertyValue("cherry")}}), baseline),
      UnorderedElementsAre(Match(0, false)));

  EXPECT_THAT(
      prop_reader.MatchesValue(p3, PropertyValue(PropertyValue::map_t{{p4, PropertyValue("banana")}}), baseline),
      UnorderedElementsAre(Match(0, true)));
}

TEST(ReadNestedPropertyValue, RetrievesPositionalPointerToNestedPropertyValue) {
  auto const p1 = PropertyId::FromInt(1);
  auto const p2 = PropertyId::FromInt(2);
  auto const p3 = PropertyId::FromInt(3);
  auto const p4 = PropertyId::FromInt(4);

  auto const make_map = [](PropertyId key, PropertyValue value) {
    return PropertyValue{PropertyValue::map_t{{key, std::move(value)}}};
  };

  auto const value = make_map(p1, make_map(p2, make_map(p3, PropertyValue("apple"))));
  ASSERT_THAT(ReadNestedPropertyValue(value, std::array{p1, p2, p3}), NotNull());
  EXPECT_EQ(*ReadNestedPropertyValue(value, std::array{p1, p2, p3}), PropertyValue("apple"));
  EXPECT_THAT(ReadNestedPropertyValue(value, std::array{p1, p2, p4}), IsNull());
  EXPECT_THAT(ReadNestedPropertyValue(value, std::array{p1, p3}), IsNull());
  EXPECT_THAT(ReadNestedPropertyValue(value, std::array{p3}), IsNull());
  EXPECT_THAT(ReadNestedPropertyValue(value, std::array{p4}), IsNull());
}

// @TODO add test for multiple properties in same map (e.g, a.b.c, a.b.d).
// Currently, this will not work because we can only read from the `PropertyStore`
// using monotonically increasing `PropertyId`s. No going back to read the same
// value (`a` in this case) twice.

// @TODO add tests for all methods in `PropertiesPermutationHelper`, such as
// MatchValue, Matches value, etc

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  int result = RUN_ALL_TESTS();

  // now run with compression on
  FLAGS_storage_property_store_compression_enabled = true;
  result &= RUN_ALL_TESTS();
  return result;
}
