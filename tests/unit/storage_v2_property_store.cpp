// Copyright 2023 Memgraph Ltd.
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

const memgraph::storage::PropertyValue kSampleValues[] = {
    memgraph::storage::PropertyValue(),
    memgraph::storage::PropertyValue(false),
    memgraph::storage::PropertyValue(true),
    memgraph::storage::PropertyValue(0),
    memgraph::storage::PropertyValue(33),
    memgraph::storage::PropertyValue(-33),
    memgraph::storage::PropertyValue(-3137),
    memgraph::storage::PropertyValue(3137),
    memgraph::storage::PropertyValue(310000007),
    memgraph::storage::PropertyValue(-310000007),
    memgraph::storage::PropertyValue(3100000000007L),
    memgraph::storage::PropertyValue(-3100000000007L),
    memgraph::storage::PropertyValue(0.0),
    memgraph::storage::PropertyValue(33.33),
    memgraph::storage::PropertyValue(-33.33),
    memgraph::storage::PropertyValue(3137.3137),
    memgraph::storage::PropertyValue(-3137.3137),
    memgraph::storage::PropertyValue("sample"),
    memgraph::storage::PropertyValue(std::string(404, 'n')),
    memgraph::storage::PropertyValue(std::vector<memgraph::storage::PropertyValue>{
        memgraph::storage::PropertyValue(33), memgraph::storage::PropertyValue(std::string("sample")),
        memgraph::storage::PropertyValue(-33.33)}),
    memgraph::storage::PropertyValue(std::vector<memgraph::storage::PropertyValue>{
        memgraph::storage::PropertyValue(), memgraph::storage::PropertyValue(false)}),
    memgraph::storage::PropertyValue(std::map<std::string, memgraph::storage::PropertyValue>{
        {"sample", memgraph::storage::PropertyValue()}, {"key", memgraph::storage::PropertyValue(false)}}),
    memgraph::storage::PropertyValue(std::map<std::string, memgraph::storage::PropertyValue>{
        {"test", memgraph::storage::PropertyValue(33)},
        {"map", memgraph::storage::PropertyValue(std::string("sample"))},
        {"item", memgraph::storage::PropertyValue(-33.33)}}),
    memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23)),
};

void TestIsPropertyEqual(const memgraph::storage::PropertyStore &store, memgraph::storage::PropertyId property,
                         const memgraph::storage::PropertyValue &value) {
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
  memgraph::storage::PropertyStore props;
  auto prop = memgraph::storage::PropertyId::FromInt(42);
  auto value = memgraph::storage::PropertyValue(42);
  ASSERT_TRUE(props.SetProperty(prop, value));
  ASSERT_EQ(props.GetProperty(prop), value);
  ASSERT_TRUE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, value);
  ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));

  ASSERT_FALSE(props.SetProperty(prop, memgraph::storage::PropertyValue()));
  ASSERT_TRUE(props.GetProperty(prop).IsNull());
  ASSERT_FALSE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, memgraph::storage::PropertyValue());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST(PropertyStore, SimpleLarge) {
  memgraph::storage::PropertyStore props;
  auto prop = memgraph::storage::PropertyId::FromInt(42);
  {
    auto value = memgraph::storage::PropertyValue(std::string(10000, 'a'));
    ASSERT_TRUE(props.SetProperty(prop, value));
    ASSERT_EQ(props.GetProperty(prop), value);
    ASSERT_TRUE(props.HasProperty(prop));
    TestIsPropertyEqual(props, prop, value);
    ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }
  {
    auto value =
        memgraph::storage::PropertyValue(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Date, 23));
    ASSERT_FALSE(props.SetProperty(prop, value));
    ASSERT_EQ(props.GetProperty(prop), value);
    ASSERT_TRUE(props.HasProperty(prop));
    TestIsPropertyEqual(props, prop, value);
    ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }

  ASSERT_FALSE(props.SetProperty(prop, memgraph::storage::PropertyValue()));
  ASSERT_TRUE(props.GetProperty(prop).IsNull());
  ASSERT_FALSE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, memgraph::storage::PropertyValue());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST(PropertyStore, EmptySetToNull) {
  memgraph::storage::PropertyStore props;
  auto prop = memgraph::storage::PropertyId::FromInt(42);
  ASSERT_TRUE(props.SetProperty(prop, memgraph::storage::PropertyValue()));
  ASSERT_TRUE(props.GetProperty(prop).IsNull());
  ASSERT_FALSE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, memgraph::storage::PropertyValue());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST(PropertyStore, Clear) {
  memgraph::storage::PropertyStore props;
  auto prop = memgraph::storage::PropertyId::FromInt(42);
  auto value = memgraph::storage::PropertyValue(42);
  ASSERT_TRUE(props.SetProperty(prop, value));
  ASSERT_EQ(props.GetProperty(prop), value);
  ASSERT_TRUE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, value);
  ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  ASSERT_TRUE(props.ClearProperties());
  ASSERT_TRUE(props.GetProperty(prop).IsNull());
  ASSERT_FALSE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, memgraph::storage::PropertyValue());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST(PropertyStore, EmptyClear) {
  memgraph::storage::PropertyStore props;
  ASSERT_FALSE(props.ClearProperties());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST(PropertyStore, MoveConstruct) {
  memgraph::storage::PropertyStore props1;
  auto prop = memgraph::storage::PropertyId::FromInt(42);
  auto value = memgraph::storage::PropertyValue(42);
  ASSERT_TRUE(props1.SetProperty(prop, value));
  ASSERT_EQ(props1.GetProperty(prop), value);
  ASSERT_TRUE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, value);
  ASSERT_THAT(props1.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  {
    memgraph::storage::PropertyStore props2(std::move(props1));
    ASSERT_EQ(props2.GetProperty(prop), value);
    ASSERT_TRUE(props2.HasProperty(prop));
    TestIsPropertyEqual(props2, prop, value);
    ASSERT_THAT(props2.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move,hicpp-invalid-access-moved)
  ASSERT_TRUE(props1.GetProperty(prop).IsNull());
  ASSERT_FALSE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, memgraph::storage::PropertyValue());
  ASSERT_EQ(props1.Properties().size(), 0);
}

TEST(PropertyStore, MoveConstructLarge) {
  memgraph::storage::PropertyStore props1;
  auto prop = memgraph::storage::PropertyId::FromInt(42);
  auto value = memgraph::storage::PropertyValue(std::string(10000, 'a'));
  ASSERT_TRUE(props1.SetProperty(prop, value));
  ASSERT_EQ(props1.GetProperty(prop), value);
  ASSERT_TRUE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, value);
  ASSERT_THAT(props1.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  {
    memgraph::storage::PropertyStore props2(std::move(props1));
    ASSERT_EQ(props2.GetProperty(prop), value);
    ASSERT_TRUE(props2.HasProperty(prop));
    TestIsPropertyEqual(props2, prop, value);
    ASSERT_THAT(props2.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move,hicpp-invalid-access-moved)
  ASSERT_TRUE(props1.GetProperty(prop).IsNull());
  ASSERT_FALSE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, memgraph::storage::PropertyValue());
  ASSERT_EQ(props1.Properties().size(), 0);
}

TEST(PropertyStore, MoveAssign) {
  memgraph::storage::PropertyStore props1;
  auto prop = memgraph::storage::PropertyId::FromInt(42);
  auto value = memgraph::storage::PropertyValue(42);
  ASSERT_TRUE(props1.SetProperty(prop, value));
  ASSERT_EQ(props1.GetProperty(prop), value);
  ASSERT_TRUE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, value);
  ASSERT_THAT(props1.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  {
    auto value2 = memgraph::storage::PropertyValue(68);
    memgraph::storage::PropertyStore props2;
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
  TestIsPropertyEqual(props1, prop, memgraph::storage::PropertyValue());
  ASSERT_EQ(props1.Properties().size(), 0);
}

TEST(PropertyStore, MoveAssignLarge) {
  memgraph::storage::PropertyStore props1;
  auto prop = memgraph::storage::PropertyId::FromInt(42);
  auto value = memgraph::storage::PropertyValue(std::string(10000, 'a'));
  ASSERT_TRUE(props1.SetProperty(prop, value));
  ASSERT_EQ(props1.GetProperty(prop), value);
  ASSERT_TRUE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, value);
  ASSERT_THAT(props1.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  {
    auto value2 = memgraph::storage::PropertyValue(std::string(10000, 'b'));
    memgraph::storage::PropertyStore props2;
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
  TestIsPropertyEqual(props1, prop, memgraph::storage::PropertyValue());
  ASSERT_EQ(props1.Properties().size(), 0);
}

TEST(PropertyStore, EmptySet) {
  std::vector<memgraph::storage::PropertyValue> vec{memgraph::storage::PropertyValue(true),
                                                    memgraph::storage::PropertyValue(123),
                                                    memgraph::storage::PropertyValue()};
  std::map<std::string, memgraph::storage::PropertyValue> map{{"nandare", memgraph::storage::PropertyValue(false)}};
  const memgraph::storage::TemporalData temporal{memgraph::storage::TemporalType::LocalDateTime, 23};
  std::vector<memgraph::storage::PropertyValue> data{
      memgraph::storage::PropertyValue(true),    memgraph::storage::PropertyValue(123),
      memgraph::storage::PropertyValue(123.5),   memgraph::storage::PropertyValue("nandare"),
      memgraph::storage::PropertyValue(vec),     memgraph::storage::PropertyValue(map),
      memgraph::storage::PropertyValue(temporal)};

  auto prop = memgraph::storage::PropertyId::FromInt(42);
  for (const auto &value : data) {
    memgraph::storage::PropertyStore props;

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
    ASSERT_FALSE(props.SetProperty(prop, memgraph::storage::PropertyValue()));
    ASSERT_TRUE(props.GetProperty(prop).IsNull());
    ASSERT_FALSE(props.HasProperty(prop));
    TestIsPropertyEqual(props, prop, memgraph::storage::PropertyValue());
    ASSERT_EQ(props.Properties().size(), 0);
    ASSERT_TRUE(props.SetProperty(prop, memgraph::storage::PropertyValue()));
    ASSERT_TRUE(props.GetProperty(prop).IsNull());
    ASSERT_FALSE(props.HasProperty(prop));
    TestIsPropertyEqual(props, prop, memgraph::storage::PropertyValue());
    ASSERT_EQ(props.Properties().size(), 0);
  }
}

TEST(PropertyStore, FullSet) {
  std::vector<memgraph::storage::PropertyValue> vec{memgraph::storage::PropertyValue(true),
                                                    memgraph::storage::PropertyValue(123),
                                                    memgraph::storage::PropertyValue()};
  std::map<std::string, memgraph::storage::PropertyValue> map{{"nandare", memgraph::storage::PropertyValue(false)}};
  const memgraph::storage::TemporalData temporal{memgraph::storage::TemporalType::LocalDateTime, 23};
  std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> data{
      {memgraph::storage::PropertyId::FromInt(1), memgraph::storage::PropertyValue(true)},
      {memgraph::storage::PropertyId::FromInt(2), memgraph::storage::PropertyValue(123)},
      {memgraph::storage::PropertyId::FromInt(3), memgraph::storage::PropertyValue(123.5)},
      {memgraph::storage::PropertyId::FromInt(4), memgraph::storage::PropertyValue("nandare")},
      {memgraph::storage::PropertyId::FromInt(5), memgraph::storage::PropertyValue(vec)},
      {memgraph::storage::PropertyId::FromInt(6), memgraph::storage::PropertyValue(map)},
      {memgraph::storage::PropertyId::FromInt(7), memgraph::storage::PropertyValue(temporal)}};

  std::vector<memgraph::storage::PropertyValue> alt{memgraph::storage::PropertyValue(),
                                                    memgraph::storage::PropertyValue(std::string()),
                                                    memgraph::storage::PropertyValue(std::string(10, 'a')),
                                                    memgraph::storage::PropertyValue(std::string(100, 'a')),
                                                    memgraph::storage::PropertyValue(std::string(1000, 'a')),
                                                    memgraph::storage::PropertyValue(std::string(10000, 'a')),
                                                    memgraph::storage::PropertyValue(std::string(100000, 'a'))};

  memgraph::storage::PropertyStore props;
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
      TestIsPropertyEqual(props, item.first, memgraph::storage::PropertyValue());
    }
  }
}

TEST(PropertyStore, IntEncoding) {
  std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> data{
      {memgraph::storage::PropertyId::FromUint(0UL),
       memgraph::storage::PropertyValue(std::numeric_limits<int64_t>::min())},
      {memgraph::storage::PropertyId::FromUint(10UL), memgraph::storage::PropertyValue(-137438953472L)},
      {memgraph::storage::PropertyId::FromUint(std::numeric_limits<uint8_t>::max()),
       memgraph::storage::PropertyValue(-4294967297L)},
      {memgraph::storage::PropertyId::FromUint(256UL),
       memgraph::storage::PropertyValue(std::numeric_limits<int32_t>::min())},
      {memgraph::storage::PropertyId::FromUint(1024UL), memgraph::storage::PropertyValue(-1048576L)},
      {memgraph::storage::PropertyId::FromUint(1025UL), memgraph::storage::PropertyValue(-65537L)},
      {memgraph::storage::PropertyId::FromUint(1026UL),
       memgraph::storage::PropertyValue(std::numeric_limits<int16_t>::min())},
      {memgraph::storage::PropertyId::FromUint(1027UL), memgraph::storage::PropertyValue(-1024L)},
      {memgraph::storage::PropertyId::FromUint(2000UL), memgraph::storage::PropertyValue(-257L)},
      {memgraph::storage::PropertyId::FromUint(3000UL),
       memgraph::storage::PropertyValue(std::numeric_limits<int8_t>::min())},
      {memgraph::storage::PropertyId::FromUint(4000UL), memgraph::storage::PropertyValue(-1L)},
      {memgraph::storage::PropertyId::FromUint(10000UL), memgraph::storage::PropertyValue(0L)},
      {memgraph::storage::PropertyId::FromUint(20000UL), memgraph::storage::PropertyValue(1L)},
      {memgraph::storage::PropertyId::FromUint(30000UL),
       memgraph::storage::PropertyValue(std::numeric_limits<int8_t>::max())},
      {memgraph::storage::PropertyId::FromUint(40000UL), memgraph::storage::PropertyValue(256L)},
      {memgraph::storage::PropertyId::FromUint(50000UL), memgraph::storage::PropertyValue(1024L)},
      {memgraph::storage::PropertyId::FromUint(std::numeric_limits<uint16_t>::max()),
       memgraph::storage::PropertyValue(std::numeric_limits<int16_t>::max())},
      {memgraph::storage::PropertyId::FromUint(65536UL), memgraph::storage::PropertyValue(65536L)},
      {memgraph::storage::PropertyId::FromUint(1048576UL), memgraph::storage::PropertyValue(1048576L)},
      {memgraph::storage::PropertyId::FromUint(std::numeric_limits<uint32_t>::max()),
       memgraph::storage::PropertyValue(std::numeric_limits<int32_t>::max())},
      {memgraph::storage::PropertyId::FromUint(4294967296UL), memgraph::storage::PropertyValue(4294967296L)},
      {memgraph::storage::PropertyId::FromUint(137438953472UL), memgraph::storage::PropertyValue(137438953472L)},
      {memgraph::storage::PropertyId::FromUint(std::numeric_limits<uint64_t>::max()),
       memgraph::storage::PropertyValue(std::numeric_limits<int64_t>::max())}};

  memgraph::storage::PropertyStore props;
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
    TestIsPropertyEqual(props, item.first, memgraph::storage::PropertyValue());
  }
}

TEST(PropertyStore, IsPropertyEqualIntAndDouble) {
  memgraph::storage::PropertyStore props;
  auto prop = memgraph::storage::PropertyId::FromInt(42);

  ASSERT_TRUE(props.SetProperty(prop, memgraph::storage::PropertyValue(42)));

  std::vector<std::pair<memgraph::storage::PropertyValue, memgraph::storage::PropertyValue>> tests{
      {memgraph::storage::PropertyValue(0), memgraph::storage::PropertyValue(0.0)},
      {memgraph::storage::PropertyValue(123), memgraph::storage::PropertyValue(123.0)},
      {memgraph::storage::PropertyValue(12345), memgraph::storage::PropertyValue(12345.0)},
      {memgraph::storage::PropertyValue(12345678), memgraph::storage::PropertyValue(12345678.0)},
      {memgraph::storage::PropertyValue(1234567890123L), memgraph::storage::PropertyValue(1234567890123.0)},
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
    test.first = memgraph::storage::PropertyValue(test.first.ValueInt() * -1);
    test.second = memgraph::storage::PropertyValue(test.second.ValueDouble() * -1.0);
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
    test.first = memgraph::storage::PropertyValue(
        std::vector<memgraph::storage::PropertyValue>{memgraph::storage::PropertyValue(test.first.ValueInt())});
    test.second = memgraph::storage::PropertyValue(
        std::vector<memgraph::storage::PropertyValue>{memgraph::storage::PropertyValue(test.second.ValueDouble())});
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
    test.first = memgraph::storage::PropertyValue(std::vector<memgraph::storage::PropertyValue>{
        memgraph::storage::PropertyValue(test.first.ValueList()[0].ValueInt() * -1)});
    test.second = memgraph::storage::PropertyValue(std::vector<memgraph::storage::PropertyValue>{
        memgraph::storage::PropertyValue(test.second.ValueList()[0].ValueDouble() * -1.0)});
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
  memgraph::storage::PropertyStore props;
  auto prop = memgraph::storage::PropertyId::FromInt(42);
  ASSERT_TRUE(props.SetProperty(prop, memgraph::storage::PropertyValue("test")));
  ASSERT_TRUE(props.IsPropertyEqual(prop, memgraph::storage::PropertyValue("test")));

  // Different length.
  ASSERT_FALSE(props.IsPropertyEqual(prop, memgraph::storage::PropertyValue("helloworld")));

  // Same length, different value.
  ASSERT_FALSE(props.IsPropertyEqual(prop, memgraph::storage::PropertyValue("asdf")));

  // Shortened and extended.
  ASSERT_FALSE(props.IsPropertyEqual(prop, memgraph::storage::PropertyValue("tes")));
  ASSERT_FALSE(props.IsPropertyEqual(prop, memgraph::storage::PropertyValue("testt")));
}

TEST(PropertyStore, IsPropertyEqualList) {
  memgraph::storage::PropertyStore props;
  auto prop = memgraph::storage::PropertyId::FromInt(42);
  ASSERT_TRUE(
      props.SetProperty(prop, memgraph::storage::PropertyValue(std::vector<memgraph::storage::PropertyValue>{
                                  memgraph::storage::PropertyValue(42), memgraph::storage::PropertyValue("test")})));
  ASSERT_TRUE(props.IsPropertyEqual(
      prop, memgraph::storage::PropertyValue(std::vector<memgraph::storage::PropertyValue>{
                memgraph::storage::PropertyValue(42), memgraph::storage::PropertyValue("test")})));

  // Different length.
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, memgraph::storage::PropertyValue(
                std::vector<memgraph::storage::PropertyValue>{memgraph::storage::PropertyValue(24)})));

  // Same length, different value.
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, memgraph::storage::PropertyValue(std::vector<memgraph::storage::PropertyValue>{
                memgraph::storage::PropertyValue(42), memgraph::storage::PropertyValue("asdf")})));

  // Shortened and extended.
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, memgraph::storage::PropertyValue(
                std::vector<memgraph::storage::PropertyValue>{memgraph::storage::PropertyValue(42)})));
  ASSERT_FALSE(
      props.IsPropertyEqual(prop, memgraph::storage::PropertyValue(std::vector<memgraph::storage::PropertyValue>{
                                      memgraph::storage::PropertyValue(42), memgraph::storage::PropertyValue("test"),
                                      memgraph::storage::PropertyValue(true)})));
}

TEST(PropertyStore, IsPropertyEqualMap) {
  memgraph::storage::PropertyStore props;
  auto prop = memgraph::storage::PropertyId::FromInt(42);
  ASSERT_TRUE(props.SetProperty(
      prop, memgraph::storage::PropertyValue(std::map<std::string, memgraph::storage::PropertyValue>{
                {"abc", memgraph::storage::PropertyValue(42)}, {"zyx", memgraph::storage::PropertyValue("test")}})));
  ASSERT_TRUE(props.IsPropertyEqual(
      prop, memgraph::storage::PropertyValue(std::map<std::string, memgraph::storage::PropertyValue>{
                {"abc", memgraph::storage::PropertyValue(42)}, {"zyx", memgraph::storage::PropertyValue("test")}})));

  // Different length.
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, memgraph::storage::PropertyValue(std::map<std::string, memgraph::storage::PropertyValue>{
                {"fgh", memgraph::storage::PropertyValue(24)}})));

  // Same length, different value.
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, memgraph::storage::PropertyValue(std::map<std::string, memgraph::storage::PropertyValue>{
                {"abc", memgraph::storage::PropertyValue(42)}, {"zyx", memgraph::storage::PropertyValue("testt")}})));

  // Same length, different key (different length).
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, memgraph::storage::PropertyValue(std::map<std::string, memgraph::storage::PropertyValue>{
                {"abc", memgraph::storage::PropertyValue(42)}, {"zyxw", memgraph::storage::PropertyValue("test")}})));

  // Same length, different key (same length).
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, memgraph::storage::PropertyValue(std::map<std::string, memgraph::storage::PropertyValue>{
                {"abc", memgraph::storage::PropertyValue(42)}, {"zyw", memgraph::storage::PropertyValue("test")}})));

  // Shortened and extended.
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, memgraph::storage::PropertyValue(std::map<std::string, memgraph::storage::PropertyValue>{
                {"abc", memgraph::storage::PropertyValue(42)}})));
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, memgraph::storage::PropertyValue(std::map<std::string, memgraph::storage::PropertyValue>{
                {"abc", memgraph::storage::PropertyValue(42)},
                {"sdf", memgraph::storage::PropertyValue(true)},
                {"zyx", memgraph::storage::PropertyValue("test")}})));
}

TEST(PropertyStore, IsPropertyEqualTemporalData) {
  memgraph::storage::PropertyStore props;
  auto prop = memgraph::storage::PropertyId::FromInt(42);
  const memgraph::storage::TemporalData temporal{memgraph::storage::TemporalType::Date, 23};
  ASSERT_TRUE(props.SetProperty(prop, memgraph::storage::PropertyValue(temporal)));
  ASSERT_TRUE(props.IsPropertyEqual(prop, memgraph::storage::PropertyValue(temporal)));

  // Different type.
  ASSERT_FALSE(props.IsPropertyEqual(prop, memgraph::storage::PropertyValue(memgraph::storage::TemporalData{
                                               memgraph::storage::TemporalType::Duration, 23})));

  // Same type, different value.
  ASSERT_FALSE(props.IsPropertyEqual(prop, memgraph::storage::PropertyValue(memgraph::storage::TemporalData{
                                               memgraph::storage::TemporalType::Date, 30})));
}

TEST(PropertyStore, SetMultipleProperties) {
  std::vector<memgraph::storage::PropertyValue> vec{memgraph::storage::PropertyValue(true),
                                                    memgraph::storage::PropertyValue(123),
                                                    memgraph::storage::PropertyValue()};
  std::map<std::string, memgraph::storage::PropertyValue> map{{"nandare", memgraph::storage::PropertyValue(false)}};
  const memgraph::storage::TemporalData temporal{memgraph::storage::TemporalType::LocalDateTime, 23};
  // The order of property ids are purposefully not monotonic to test that PropertyStore orders them properly
  const std::vector<std::pair<memgraph::storage::PropertyId, memgraph::storage::PropertyValue>> data{
      {memgraph::storage::PropertyId::FromInt(1), memgraph::storage::PropertyValue(true)},
      {memgraph::storage::PropertyId::FromInt(10), memgraph::storage::PropertyValue(123)},
      {memgraph::storage::PropertyId::FromInt(3), memgraph::storage::PropertyValue(123.5)},
      {memgraph::storage::PropertyId::FromInt(4), memgraph::storage::PropertyValue("nandare")},
      {memgraph::storage::PropertyId::FromInt(12), memgraph::storage::PropertyValue(vec)},
      {memgraph::storage::PropertyId::FromInt(6), memgraph::storage::PropertyValue(map)},
      {memgraph::storage::PropertyId::FromInt(7), memgraph::storage::PropertyValue(temporal)}};

  const std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> data_in_map{data.begin(), data.end()};

  auto check_store = [data](const memgraph::storage::PropertyStore &store) {
    for (auto &[key, value] : data) {
      ASSERT_TRUE(store.IsPropertyEqual(key, value));
    }
  };
  {
    memgraph::storage::PropertyStore store;
    EXPECT_TRUE(store.InitProperties(data));
    check_store(store);
    EXPECT_FALSE(store.InitProperties(data));
    EXPECT_FALSE(store.InitProperties(data_in_map));
  }
  {
    memgraph::storage::PropertyStore store;
    EXPECT_TRUE(store.InitProperties(data_in_map));
    check_store(store);
    EXPECT_FALSE(store.InitProperties(data_in_map));
    EXPECT_FALSE(store.InitProperties(data));
  }
}
