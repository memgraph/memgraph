// Copyright 2024 Memgraph Ltd.
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
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"

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
    PropertyValue(PropertyValue::map_t{{"sample", PropertyValue()}, {"key", PropertyValue(false)}}),
    PropertyValue(PropertyValue::map_t{
        {"test", PropertyValue(33)}, {"map", PropertyValue(std::string("sample"))}, {"item", PropertyValue(-33.33)}}),
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

TEST(PropertyStore, EmptySet) {
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123), PropertyValue()};
  PropertyValue::map_t map{{"nandare", PropertyValue(false)}};
  const TemporalData temporal{TemporalType::LocalDateTime, 23};
  const auto zoned_temporal = GetSampleZonedTemporal();

  std::vector<PropertyValue> data{PropertyValue(true),      PropertyValue(123),           PropertyValue(123.5),
                                  PropertyValue("nandare"), PropertyValue(vec),           PropertyValue(map),
                                  PropertyValue(temporal),  PropertyValue(zoned_temporal)};

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
  PropertyValue::map_t map{{"nandare", PropertyValue(false)}};
  const TemporalData temporal{TemporalType::LocalDateTime, 23};
  const auto zoned_temporal = GetSampleZonedTemporal();

  std::map<PropertyId, PropertyValue> data{
      {PropertyId::FromInt(1), PropertyValue(true)},
      {PropertyId::FromInt(2), PropertyValue(123)},
      {PropertyId::FromInt(3), PropertyValue(123.5)},
      {PropertyId::FromInt(4), PropertyValue("nandare")},
      {PropertyId::FromInt(5), PropertyValue(vec)},
      {PropertyId::FromInt(6), PropertyValue(map)},
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

TEST(PropertyStore, IsPropertyEqualMap) {
  PropertyStore props;
  auto prop = PropertyId::FromInt(42);
  ASSERT_TRUE(props.SetProperty(
      prop, PropertyValue(PropertyValue::map_t{{"abc", PropertyValue(42)}, {"zyx", PropertyValue("test")}})));
  ASSERT_TRUE(props.IsPropertyEqual(
      prop, PropertyValue(PropertyValue::map_t{{"abc", PropertyValue(42)}, {"zyx", PropertyValue("test")}})));

  // Different length.
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(PropertyValue::map_t{{"fgh", PropertyValue(24)}})));

  // Same length, different value.
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, PropertyValue(PropertyValue::map_t{{"abc", PropertyValue(42)}, {"zyx", PropertyValue("testt")}})));

  // Same length, different key (different length).
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, PropertyValue(PropertyValue::map_t{{"abc", PropertyValue(42)}, {"zyxw", PropertyValue("test")}})));

  // Same length, different key (same length).
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, PropertyValue(PropertyValue::map_t{{"abc", PropertyValue(42)}, {"zyw", PropertyValue("test")}})));

  // Shortened and extended.
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(PropertyValue::map_t{{"abc", PropertyValue(42)}})));
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, PropertyValue(PropertyValue::map_t{
                {"abc", PropertyValue(42)}, {"sdf", PropertyValue(true)}, {"zyx", PropertyValue("test")}})));
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
  PropertyValue::map_t map{{"nandare", PropertyValue(false)}};
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

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  int result = RUN_ALL_TESTS();

  // now run with compression on
  FLAGS_storage_property_store_compression_enabled = true;
  result &= RUN_ALL_TESTS();
  return result;
}
