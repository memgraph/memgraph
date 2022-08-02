// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <array>
#include <limits>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "storage/v3/property_store.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/temporal.hpp"

namespace memgraph::storage::v3::tests {

class StorageV3PropertyStore : public ::testing::Test {
 protected:
  PropertyStore props;

  const std::array<PropertyValue, 24> kSampleValues = {
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
      PropertyValue(std::map<std::string, PropertyValue>{{"sample", PropertyValue()}, {"key", PropertyValue(false)}}),
      PropertyValue(std::map<std::string, PropertyValue>{
          {"test", PropertyValue(33)}, {"map", PropertyValue(std::string("sample"))}, {"item", PropertyValue(-33.33)}}),
      PropertyValue(TemporalData(TemporalType::Date, 23)),
  };

  void AssertPropertyIsEqual(const PropertyStore &store, PropertyId property, const PropertyValue &value) {
    ASSERT_TRUE(store.IsPropertyEqual(property, value));
    for (const auto &sample : kSampleValues) {
      if (sample == value) {
        ASSERT_TRUE(store.IsPropertyEqual(property, sample));
      } else {
        ASSERT_FALSE(store.IsPropertyEqual(property, sample));
      }
    }
  }
};

using testing::UnorderedElementsAre;

TEST_F(StorageV3PropertyStore, StoreTwoProperties) {
  const auto make_prop = [](int64_t prop_id_and_value) {
    auto prop = PropertyId::FromInt(prop_id_and_value);
    auto value = PropertyValue(prop_id_and_value);
    return std::make_pair(prop, value);
  };

  const auto first_prop_and_value = make_prop(42);
  const auto second_prop_and_value = make_prop(43);
  ASSERT_TRUE(props.SetProperty(first_prop_and_value.first, first_prop_and_value.second));
  ASSERT_TRUE(props.SetProperty(second_prop_and_value.first, second_prop_and_value.second));
  ASSERT_THAT(props.Properties(), UnorderedElementsAre(first_prop_and_value, second_prop_and_value));
}

TEST_F(StorageV3PropertyStore, Simple) {
  auto prop = PropertyId::FromInt(42);
  auto value = PropertyValue(42);
  ASSERT_TRUE(props.SetProperty(prop, value));
  ASSERT_EQ(props.GetProperty(prop), value);
  ASSERT_TRUE(props.HasProperty(prop));
  AssertPropertyIsEqual(props, prop, value);
  ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));

  ASSERT_FALSE(props.SetProperty(prop, PropertyValue()));
  ASSERT_TRUE(props.GetProperty(prop).IsNull());
  ASSERT_FALSE(props.HasProperty(prop));
  AssertPropertyIsEqual(props, prop, PropertyValue());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST_F(StorageV3PropertyStore, SimpleLarge) {
  auto prop = PropertyId::FromInt(42);
  {
    auto value = PropertyValue(std::string(10000, 'a'));
    ASSERT_TRUE(props.SetProperty(prop, value));
    ASSERT_EQ(props.GetProperty(prop), value);
    ASSERT_TRUE(props.HasProperty(prop));
    AssertPropertyIsEqual(props, prop, value);
    ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }
  {
    auto value = PropertyValue(TemporalData(TemporalType::Date, 23));
    ASSERT_FALSE(props.SetProperty(prop, value));
    ASSERT_EQ(props.GetProperty(prop), value);
    ASSERT_TRUE(props.HasProperty(prop));
    AssertPropertyIsEqual(props, prop, value);
    ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }

  ASSERT_FALSE(props.SetProperty(prop, PropertyValue()));
  ASSERT_TRUE(props.GetProperty(prop).IsNull());
  ASSERT_FALSE(props.HasProperty(prop));
  AssertPropertyIsEqual(props, prop, PropertyValue());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST_F(StorageV3PropertyStore, EmptySetToNull) {
  auto prop = PropertyId::FromInt(42);
  ASSERT_TRUE(props.SetProperty(prop, PropertyValue()));
  ASSERT_TRUE(props.GetProperty(prop).IsNull());
  ASSERT_FALSE(props.HasProperty(prop));
  AssertPropertyIsEqual(props, prop, PropertyValue());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST_F(StorageV3PropertyStore, Clear) {
  auto prop = PropertyId::FromInt(42);
  auto value = PropertyValue(42);
  ASSERT_TRUE(props.SetProperty(prop, value));
  ASSERT_EQ(props.GetProperty(prop), value);
  ASSERT_TRUE(props.HasProperty(prop));
  AssertPropertyIsEqual(props, prop, value);
  ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  ASSERT_TRUE(props.ClearProperties());
  ASSERT_TRUE(props.GetProperty(prop).IsNull());
  ASSERT_FALSE(props.HasProperty(prop));
  AssertPropertyIsEqual(props, prop, PropertyValue());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST_F(StorageV3PropertyStore, EmptyClear) {
  ASSERT_FALSE(props.ClearProperties());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST_F(StorageV3PropertyStore, MoveConstruct) {
  PropertyStore props1;
  auto prop = PropertyId::FromInt(42);
  auto value = PropertyValue(42);
  ASSERT_TRUE(props1.SetProperty(prop, value));
  ASSERT_EQ(props1.GetProperty(prop), value);
  ASSERT_TRUE(props1.HasProperty(prop));
  AssertPropertyIsEqual(props1, prop, value);
  ASSERT_THAT(props1.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  {
    PropertyStore props2(std::move(props1));
    ASSERT_EQ(props2.GetProperty(prop), value);
    ASSERT_TRUE(props2.HasProperty(prop));
    AssertPropertyIsEqual(props2, prop, value);
    ASSERT_THAT(props2.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move,hicpp-invalid-access-moved)
  ASSERT_TRUE(props1.GetProperty(prop).IsNull());
  ASSERT_FALSE(props1.HasProperty(prop));
  AssertPropertyIsEqual(props1, prop, PropertyValue());
  ASSERT_EQ(props1.Properties().size(), 0);
}

TEST_F(StorageV3PropertyStore, MoveConstructLarge) {
  PropertyStore props1;
  auto prop = PropertyId::FromInt(42);
  auto value = PropertyValue(std::string(10000, 'a'));
  ASSERT_TRUE(props1.SetProperty(prop, value));
  ASSERT_EQ(props1.GetProperty(prop), value);
  ASSERT_TRUE(props1.HasProperty(prop));
  AssertPropertyIsEqual(props1, prop, value);
  ASSERT_THAT(props1.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  {
    PropertyStore props2(std::move(props1));
    ASSERT_EQ(props2.GetProperty(prop), value);
    ASSERT_TRUE(props2.HasProperty(prop));
    AssertPropertyIsEqual(props2, prop, value);
    ASSERT_THAT(props2.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move,hicpp-invalid-access-moved)
  ASSERT_TRUE(props1.GetProperty(prop).IsNull());
  ASSERT_FALSE(props1.HasProperty(prop));
  AssertPropertyIsEqual(props1, prop, PropertyValue());
  ASSERT_EQ(props1.Properties().size(), 0);
}

TEST_F(StorageV3PropertyStore, MoveAssign) {
  PropertyStore props1;
  auto prop = PropertyId::FromInt(42);
  auto value = PropertyValue(42);
  ASSERT_TRUE(props1.SetProperty(prop, value));
  ASSERT_EQ(props1.GetProperty(prop), value);
  ASSERT_TRUE(props1.HasProperty(prop));
  AssertPropertyIsEqual(props1, prop, value);
  ASSERT_THAT(props1.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  {
    auto value2 = PropertyValue(68);
    PropertyStore props2;
    ASSERT_TRUE(props2.SetProperty(prop, value2));
    ASSERT_EQ(props2.GetProperty(prop), value2);
    ASSERT_TRUE(props2.HasProperty(prop));
    AssertPropertyIsEqual(props2, prop, value2);
    ASSERT_THAT(props2.Properties(), UnorderedElementsAre(std::pair(prop, value2)));
    props2 = std::move(props1);
    ASSERT_EQ(props2.GetProperty(prop), value);
    ASSERT_TRUE(props2.HasProperty(prop));
    AssertPropertyIsEqual(props2, prop, value);
    ASSERT_THAT(props2.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move,hicpp-invalid-access-moved)
  ASSERT_TRUE(props1.GetProperty(prop).IsNull());
  ASSERT_FALSE(props1.HasProperty(prop));
  AssertPropertyIsEqual(props1, prop, PropertyValue());
  ASSERT_EQ(props1.Properties().size(), 0);
}

TEST_F(StorageV3PropertyStore, MoveAssignLarge) {
  PropertyStore props1;
  auto prop = PropertyId::FromInt(42);
  auto value = PropertyValue(std::string(10000, 'a'));
  ASSERT_TRUE(props1.SetProperty(prop, value));
  ASSERT_EQ(props1.GetProperty(prop), value);
  ASSERT_TRUE(props1.HasProperty(prop));
  AssertPropertyIsEqual(props1, prop, value);
  ASSERT_THAT(props1.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  {
    auto value2 = PropertyValue(std::string(10000, 'b'));
    PropertyStore props2;
    ASSERT_TRUE(props2.SetProperty(prop, value2));
    ASSERT_EQ(props2.GetProperty(prop), value2);
    ASSERT_TRUE(props2.HasProperty(prop));
    AssertPropertyIsEqual(props2, prop, value2);
    ASSERT_THAT(props2.Properties(), UnorderedElementsAre(std::pair(prop, value2)));
    props2 = std::move(props1);
    ASSERT_EQ(props2.GetProperty(prop), value);
    ASSERT_TRUE(props2.HasProperty(prop));
    AssertPropertyIsEqual(props2, prop, value);
    ASSERT_THAT(props2.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move,hicpp-invalid-access-moved)
  ASSERT_TRUE(props1.GetProperty(prop).IsNull());
  ASSERT_FALSE(props1.HasProperty(prop));
  AssertPropertyIsEqual(props1, prop, PropertyValue());
  ASSERT_EQ(props1.Properties().size(), 0);
}

TEST_F(StorageV3PropertyStore, EmptySet) {
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123), PropertyValue()};
  std::map<std::string, PropertyValue> map{{"nandare", PropertyValue(false)}};
  const TemporalData temporal{TemporalType::LocalDateTime, 23};
  std::vector<PropertyValue> data{PropertyValue(true),      PropertyValue(123), PropertyValue(123.5),
                                  PropertyValue("nandare"), PropertyValue(vec), PropertyValue(map),
                                  PropertyValue(temporal)};

  auto prop = PropertyId::FromInt(42);
  for (const auto &value : data) {
    PropertyStore local_props;

    ASSERT_TRUE(local_props.SetProperty(prop, value));
    ASSERT_EQ(local_props.GetProperty(prop), value);
    ASSERT_TRUE(local_props.HasProperty(prop));
    AssertPropertyIsEqual(local_props, prop, value);
    ASSERT_THAT(local_props.Properties(), UnorderedElementsAre(std::pair(prop, value)));
    ASSERT_FALSE(local_props.SetProperty(prop, value));
    ASSERT_EQ(local_props.GetProperty(prop), value);
    ASSERT_TRUE(local_props.HasProperty(prop));
    AssertPropertyIsEqual(local_props, prop, value);
    ASSERT_THAT(local_props.Properties(), UnorderedElementsAre(std::pair(prop, value)));
    ASSERT_FALSE(local_props.SetProperty(prop, PropertyValue()));
    ASSERT_TRUE(local_props.GetProperty(prop).IsNull());
    ASSERT_FALSE(local_props.HasProperty(prop));
    AssertPropertyIsEqual(local_props, prop, PropertyValue());
    ASSERT_EQ(local_props.Properties().size(), 0);
    ASSERT_TRUE(local_props.SetProperty(prop, PropertyValue()));
    ASSERT_TRUE(local_props.GetProperty(prop).IsNull());
    ASSERT_FALSE(local_props.HasProperty(prop));
    AssertPropertyIsEqual(local_props, prop, PropertyValue());
    ASSERT_EQ(local_props.Properties().size(), 0);
  }
}

TEST_F(StorageV3PropertyStore, FullSet) {
  std::vector<PropertyValue> vec{PropertyValue(true), PropertyValue(123), PropertyValue()};
  std::map<std::string, PropertyValue> map{{"nandare", PropertyValue(false)}};
  const TemporalData temporal{TemporalType::LocalDateTime, 23};
  std::map<PropertyId, PropertyValue> data{
      {PropertyId::FromInt(1), PropertyValue(true)},    {PropertyId::FromInt(2), PropertyValue(123)},
      {PropertyId::FromInt(3), PropertyValue(123.5)},   {PropertyId::FromInt(4), PropertyValue("nandare")},
      {PropertyId::FromInt(5), PropertyValue(vec)},     {PropertyId::FromInt(6), PropertyValue(map)},
      {PropertyId::FromInt(7), PropertyValue(temporal)}};

  std::vector<PropertyValue> alt{PropertyValue(),
                                 PropertyValue(std::string()),
                                 PropertyValue(std::string(10, 'a')),
                                 PropertyValue(std::string(100, 'a')),
                                 PropertyValue(std::string(1000, 'a')),
                                 PropertyValue(std::string(10000, 'a')),
                                 PropertyValue(std::string(100000, 'a'))};

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
          AssertPropertyIsEqual(props, item.first, alt[i]);
        } else {
          ASSERT_EQ(props.GetProperty(item.first), item.second);
          ASSERT_TRUE(props.HasProperty(item.first));
          AssertPropertyIsEqual(props, item.first, item.second);
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
          AssertPropertyIsEqual(props, item.first, alt[i]);
        } else {
          ASSERT_EQ(props.GetProperty(item.first), item.second);
          ASSERT_TRUE(props.HasProperty(item.first));
          AssertPropertyIsEqual(props, item.first, item.second);
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
    AssertPropertyIsEqual(props, target.first, target.second);

    props.ClearProperties();
    ASSERT_EQ(props.Properties().size(), 0);
    for (const auto &item : data) {
      ASSERT_TRUE(props.GetProperty(item.first).IsNull());
      ASSERT_FALSE(props.HasProperty(item.first));
      AssertPropertyIsEqual(props, item.first, PropertyValue());
    }
  }
}

TEST_F(StorageV3PropertyStore, IntEncoding) {
  std::map<PropertyId, PropertyValue> data{
      // {PropertyId::FromUint(0UL),
      //  PropertyValue(std::numeric_limits<int64_t>::min())},
      // {PropertyId::FromUint(10UL), PropertyValue(-137438953472L)},
      // {PropertyId::FromUint(std::numeric_limits<uint8_t>::max()),
      //  PropertyValue(-4294967297L)},
      // {PropertyId::FromUint(256UL),
      //  PropertyValue(std::numeric_limits<int32_t>::min())},
      // {PropertyId::FromUint(1024UL), PropertyValue(-1048576L)},
      // {PropertyId::FromUint(1025UL), PropertyValue(-65537L)},
      // {PropertyId::FromUint(1026UL),
      //  PropertyValue(std::numeric_limits<int16_t>::min())},
      // {PropertyId::FromUint(1027UL), PropertyValue(-1024L)},
      // {PropertyId::FromUint(2000UL), PropertyValue(-257L)},
      // {PropertyId::FromUint(3000UL),
      //  PropertyValue(std::numeric_limits<int8_t>::min())},
      // {PropertyId::FromUint(4000UL), PropertyValue(-1L)},
      // {PropertyId::FromUint(10000UL), PropertyValue(0L)},
      // {PropertyId::FromUint(20000UL), PropertyValue(1L)},
      // {PropertyId::FromUint(30000UL),
      //  PropertyValue(std::numeric_limits<int8_t>::max())},
      // {PropertyId::FromUint(40000UL), PropertyValue(256L)},
      // {PropertyId::FromUint(50000UL), PropertyValue(1024L)},
      // {PropertyId::FromUint(std::numeric_limits<uint16_t>::max()),
      //  PropertyValue(std::numeric_limits<int16_t>::max())},
      // {PropertyId::FromUint(65536UL), PropertyValue(65536L)},
      // {PropertyId::FromUint(1048576UL), PropertyValue(1048576L)},
      // {PropertyId::FromUint(std::numeric_limits<uint32_t>::max()),
      //  PropertyValue(std::numeric_limits<int32_t>::max())},
      {PropertyId::FromUint(4294967296UL), PropertyValue(4294967296L)},
      {PropertyId::FromUint(137438953472UL), PropertyValue(137438953472L)},
      {PropertyId::FromUint(std::numeric_limits<uint64_t>::max()), PropertyValue(std::numeric_limits<int64_t>::max())}};

  for (const auto &item : data) {
    ASSERT_TRUE(props.SetProperty(item.first, item.second));
    ASSERT_EQ(props.GetProperty(item.first), item.second);
    ASSERT_TRUE(props.HasProperty(item.first));
    AssertPropertyIsEqual(props, item.first, item.second);
  }
  for (auto it = data.rbegin(); it != data.rend(); ++it) {
    const auto &item = *it;
    ASSERT_FALSE(props.SetProperty(item.first, item.second)) << item.first.AsInt();
    ASSERT_EQ(props.GetProperty(item.first), item.second);
    ASSERT_TRUE(props.HasProperty(item.first));
    AssertPropertyIsEqual(props, item.first, item.second);
  }

  ASSERT_EQ(props.Properties(), data);

  props.ClearProperties();
  ASSERT_EQ(props.Properties().size(), 0);
  for (const auto &item : data) {
    ASSERT_TRUE(props.GetProperty(item.first).IsNull());
    ASSERT_FALSE(props.HasProperty(item.first));
    AssertPropertyIsEqual(props, item.first, PropertyValue());
  }
}

TEST_F(StorageV3PropertyStore, IsPropertyEqualIntAndDouble) {
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

TEST_F(StorageV3PropertyStore, IsPropertyEqualString) {
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

TEST_F(StorageV3PropertyStore, IsPropertyEqualList) {
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

TEST_F(StorageV3PropertyStore, IsPropertyEqualMap) {
  auto prop = PropertyId::FromInt(42);
  ASSERT_TRUE(props.SetProperty(prop, PropertyValue(std::map<std::string, PropertyValue>{
                                          {"abc", PropertyValue(42)}, {"zyx", PropertyValue("test")}})));
  ASSERT_TRUE(props.IsPropertyEqual(prop, PropertyValue(std::map<std::string, PropertyValue>{
                                              {"abc", PropertyValue(42)}, {"zyx", PropertyValue("test")}})));

  // Different length.
  ASSERT_FALSE(
      props.IsPropertyEqual(prop, PropertyValue(std::map<std::string, PropertyValue>{{"fgh", PropertyValue(24)}})));

  // Same length, different value.
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(std::map<std::string, PropertyValue>{
                                               {"abc", PropertyValue(42)}, {"zyx", PropertyValue("testt")}})));

  // Same length, different key (different length).
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(std::map<std::string, PropertyValue>{
                                               {"abc", PropertyValue(42)}, {"zyxw", PropertyValue("test")}})));

  // Same length, different key (same length).
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(std::map<std::string, PropertyValue>{
                                               {"abc", PropertyValue(42)}, {"zyw", PropertyValue("test")}})));

  // Shortened and extended.
  ASSERT_FALSE(
      props.IsPropertyEqual(prop, PropertyValue(std::map<std::string, PropertyValue>{{"abc", PropertyValue(42)}})));
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, PropertyValue(std::map<std::string, PropertyValue>{
                {"abc", PropertyValue(42)}, {"sdf", PropertyValue(true)}, {"zyx", PropertyValue("test")}})));
}

TEST_F(StorageV3PropertyStore, IsPropertyEqualTemporalData) {
  auto prop = PropertyId::FromInt(42);
  const TemporalData temporal{TemporalType::Date, 23};
  ASSERT_TRUE(props.SetProperty(prop, PropertyValue(temporal)));
  ASSERT_TRUE(props.IsPropertyEqual(prop, PropertyValue(temporal)));

  // Different type.
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(TemporalData{TemporalType::Duration, 23})));

  // Same type, different value.
  ASSERT_FALSE(props.IsPropertyEqual(prop, PropertyValue(TemporalData{TemporalType::Date, 30})));
}
}  // namespace memgraph::storage::v3::tests
