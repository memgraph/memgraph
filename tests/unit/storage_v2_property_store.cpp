#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>

#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"

using testing::UnorderedElementsAre;

const storage::PropertyValue kSampleValues[] = {
    storage::PropertyValue(),
    storage::PropertyValue(false),
    storage::PropertyValue(true),
    storage::PropertyValue(0),
    storage::PropertyValue(33),
    storage::PropertyValue(-33),
    storage::PropertyValue(-3137),
    storage::PropertyValue(3137),
    storage::PropertyValue(310000007),
    storage::PropertyValue(-310000007),
    storage::PropertyValue(3100000000007L),
    storage::PropertyValue(-3100000000007L),
    storage::PropertyValue(0.0),
    storage::PropertyValue(33.33),
    storage::PropertyValue(-33.33),
    storage::PropertyValue(3137.3137),
    storage::PropertyValue(-3137.3137),
    storage::PropertyValue("sample"),
    storage::PropertyValue(std::string(404, 'n')),
    storage::PropertyValue(std::vector<storage::PropertyValue>{
        storage::PropertyValue(33), storage::PropertyValue(std::string("sample")), storage::PropertyValue(-33.33)}),
    storage::PropertyValue(
        std::vector<storage::PropertyValue>{storage::PropertyValue(), storage::PropertyValue(false)}),
    storage::PropertyValue(std::map<std::string, storage::PropertyValue>{{"sample", storage::PropertyValue()},
                                                                         {"key", storage::PropertyValue(false)}}),
    storage::PropertyValue(
        std::map<std::string, storage::PropertyValue>{{"test", storage::PropertyValue(33)},
                                                      {"map", storage::PropertyValue(std::string("sample"))},
                                                      {"item", storage::PropertyValue(-33.33)}}),
    storage::PropertyValue(storage::TemporalData(storage::TemporalType::Date, 23)),
};

void TestIsPropertyEqual(const storage::PropertyStore &store, storage::PropertyId property,
                         const storage::PropertyValue &value) {
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
  storage::PropertyStore props;
  auto prop = storage::PropertyId::FromInt(42);
  auto value = storage::PropertyValue(42);
  ASSERT_TRUE(props.SetProperty(prop, value));
  ASSERT_EQ(props.GetProperty(prop), value);
  ASSERT_TRUE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, value);
  ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));

  ASSERT_FALSE(props.SetProperty(prop, storage::PropertyValue()));
  ASSERT_TRUE(props.GetProperty(prop).IsNull());
  ASSERT_FALSE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, storage::PropertyValue());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST(PropertyStore, SimpleLarge) {
  storage::PropertyStore props;
  auto prop = storage::PropertyId::FromInt(42);
  {
    auto value = storage::PropertyValue(std::string(10000, 'a'));
    ASSERT_TRUE(props.SetProperty(prop, value));
    ASSERT_EQ(props.GetProperty(prop), value);
    ASSERT_TRUE(props.HasProperty(prop));
    TestIsPropertyEqual(props, prop, value);
    ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }
  {
    auto value = storage::PropertyValue(storage::TemporalData(storage::TemporalType::Date, 23));
    ASSERT_FALSE(props.SetProperty(prop, value));
    ASSERT_EQ(props.GetProperty(prop), value);
    ASSERT_TRUE(props.HasProperty(prop));
    TestIsPropertyEqual(props, prop, value);
    ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }

  ASSERT_FALSE(props.SetProperty(prop, storage::PropertyValue()));
  ASSERT_TRUE(props.GetProperty(prop).IsNull());
  ASSERT_FALSE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, storage::PropertyValue());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST(PropertyStore, EmptySetToNull) {
  storage::PropertyStore props;
  auto prop = storage::PropertyId::FromInt(42);
  ASSERT_TRUE(props.SetProperty(prop, storage::PropertyValue()));
  ASSERT_TRUE(props.GetProperty(prop).IsNull());
  ASSERT_FALSE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, storage::PropertyValue());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST(PropertyStore, Clear) {
  storage::PropertyStore props;
  auto prop = storage::PropertyId::FromInt(42);
  auto value = storage::PropertyValue(42);
  ASSERT_TRUE(props.SetProperty(prop, value));
  ASSERT_EQ(props.GetProperty(prop), value);
  ASSERT_TRUE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, value);
  ASSERT_THAT(props.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  ASSERT_TRUE(props.ClearProperties());
  ASSERT_TRUE(props.GetProperty(prop).IsNull());
  ASSERT_FALSE(props.HasProperty(prop));
  TestIsPropertyEqual(props, prop, storage::PropertyValue());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST(PropertyStore, EmptyClear) {
  storage::PropertyStore props;
  ASSERT_FALSE(props.ClearProperties());
  ASSERT_EQ(props.Properties().size(), 0);
}

TEST(PropertyStore, MoveConstruct) {
  storage::PropertyStore props1;
  auto prop = storage::PropertyId::FromInt(42);
  auto value = storage::PropertyValue(42);
  ASSERT_TRUE(props1.SetProperty(prop, value));
  ASSERT_EQ(props1.GetProperty(prop), value);
  ASSERT_TRUE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, value);
  ASSERT_THAT(props1.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  {
    storage::PropertyStore props2(std::move(props1));
    ASSERT_EQ(props2.GetProperty(prop), value);
    ASSERT_TRUE(props2.HasProperty(prop));
    TestIsPropertyEqual(props2, prop, value);
    ASSERT_THAT(props2.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move,hicpp-invalid-access-moved)
  ASSERT_TRUE(props1.GetProperty(prop).IsNull());
  ASSERT_FALSE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, storage::PropertyValue());
  ASSERT_EQ(props1.Properties().size(), 0);
}

TEST(PropertyStore, MoveConstructLarge) {
  storage::PropertyStore props1;
  auto prop = storage::PropertyId::FromInt(42);
  auto value = storage::PropertyValue(std::string(10000, 'a'));
  ASSERT_TRUE(props1.SetProperty(prop, value));
  ASSERT_EQ(props1.GetProperty(prop), value);
  ASSERT_TRUE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, value);
  ASSERT_THAT(props1.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  {
    storage::PropertyStore props2(std::move(props1));
    ASSERT_EQ(props2.GetProperty(prop), value);
    ASSERT_TRUE(props2.HasProperty(prop));
    TestIsPropertyEqual(props2, prop, value);
    ASSERT_THAT(props2.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  }
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move,hicpp-invalid-access-moved)
  ASSERT_TRUE(props1.GetProperty(prop).IsNull());
  ASSERT_FALSE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, storage::PropertyValue());
  ASSERT_EQ(props1.Properties().size(), 0);
}

TEST(PropertyStore, MoveAssign) {
  storage::PropertyStore props1;
  auto prop = storage::PropertyId::FromInt(42);
  auto value = storage::PropertyValue(42);
  ASSERT_TRUE(props1.SetProperty(prop, value));
  ASSERT_EQ(props1.GetProperty(prop), value);
  ASSERT_TRUE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, value);
  ASSERT_THAT(props1.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  {
    auto value2 = storage::PropertyValue(68);
    storage::PropertyStore props2;
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
  TestIsPropertyEqual(props1, prop, storage::PropertyValue());
  ASSERT_EQ(props1.Properties().size(), 0);
}

TEST(PropertyStore, MoveAssignLarge) {
  storage::PropertyStore props1;
  auto prop = storage::PropertyId::FromInt(42);
  auto value = storage::PropertyValue(std::string(10000, 'a'));
  ASSERT_TRUE(props1.SetProperty(prop, value));
  ASSERT_EQ(props1.GetProperty(prop), value);
  ASSERT_TRUE(props1.HasProperty(prop));
  TestIsPropertyEqual(props1, prop, value);
  ASSERT_THAT(props1.Properties(), UnorderedElementsAre(std::pair(prop, value)));
  {
    auto value2 = storage::PropertyValue(std::string(10000, 'b'));
    storage::PropertyStore props2;
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
  TestIsPropertyEqual(props1, prop, storage::PropertyValue());
  ASSERT_EQ(props1.Properties().size(), 0);
}

TEST(PropertyStore, EmptySet) {
  std::vector<storage::PropertyValue> vec{storage::PropertyValue(true), storage::PropertyValue(123),
                                          storage::PropertyValue()};
  std::map<std::string, storage::PropertyValue> map{{"nandare", storage::PropertyValue(false)}};
  const storage::TemporalData temporal{storage::TemporalType::LocalDateTime, 23};
  std::vector<storage::PropertyValue> data{storage::PropertyValue(true),    storage::PropertyValue(123),
                                           storage::PropertyValue(123.5),   storage::PropertyValue("nandare"),
                                           storage::PropertyValue(vec),     storage::PropertyValue(map),
                                           storage::PropertyValue(temporal)};

  auto prop = storage::PropertyId::FromInt(42);
  for (const auto &value : data) {
    storage::PropertyStore props;

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
    ASSERT_FALSE(props.SetProperty(prop, storage::PropertyValue()));
    ASSERT_TRUE(props.GetProperty(prop).IsNull());
    ASSERT_FALSE(props.HasProperty(prop));
    TestIsPropertyEqual(props, prop, storage::PropertyValue());
    ASSERT_EQ(props.Properties().size(), 0);
    ASSERT_TRUE(props.SetProperty(prop, storage::PropertyValue()));
    ASSERT_TRUE(props.GetProperty(prop).IsNull());
    ASSERT_FALSE(props.HasProperty(prop));
    TestIsPropertyEqual(props, prop, storage::PropertyValue());
    ASSERT_EQ(props.Properties().size(), 0);
  }
}

TEST(PropertyStore, FullSet) {
  std::vector<storage::PropertyValue> vec{storage::PropertyValue(true), storage::PropertyValue(123),
                                          storage::PropertyValue()};
  std::map<std::string, storage::PropertyValue> map{{"nandare", storage::PropertyValue(false)}};
  const storage::TemporalData temporal{storage::TemporalType::LocalDateTime, 23};
  std::map<storage::PropertyId, storage::PropertyValue> data{
      {storage::PropertyId::FromInt(1), storage::PropertyValue(true)},
      {storage::PropertyId::FromInt(2), storage::PropertyValue(123)},
      {storage::PropertyId::FromInt(3), storage::PropertyValue(123.5)},
      {storage::PropertyId::FromInt(4), storage::PropertyValue("nandare")},
      {storage::PropertyId::FromInt(5), storage::PropertyValue(vec)},
      {storage::PropertyId::FromInt(6), storage::PropertyValue(map)},
      {storage::PropertyId::FromInt(7), storage::PropertyValue(temporal)}};

  std::vector<storage::PropertyValue> alt{storage::PropertyValue(),
                                          storage::PropertyValue(std::string()),
                                          storage::PropertyValue(std::string(10, 'a')),
                                          storage::PropertyValue(std::string(100, 'a')),
                                          storage::PropertyValue(std::string(1000, 'a')),
                                          storage::PropertyValue(std::string(10000, 'a')),
                                          storage::PropertyValue(std::string(100000, 'a'))};

  storage::PropertyStore props;
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
      TestIsPropertyEqual(props, item.first, storage::PropertyValue());
    }
  }
}

TEST(PropertyStore, IntEncoding) {
  std::map<storage::PropertyId, storage::PropertyValue> data{
      {storage::PropertyId::FromUint(0UL), storage::PropertyValue(std::numeric_limits<int64_t>::min())},
      {storage::PropertyId::FromUint(10UL), storage::PropertyValue(-137438953472L)},
      {storage::PropertyId::FromUint(std::numeric_limits<uint8_t>::max()), storage::PropertyValue(-4294967297L)},
      {storage::PropertyId::FromUint(256UL), storage::PropertyValue(std::numeric_limits<int32_t>::min())},
      {storage::PropertyId::FromUint(1024UL), storage::PropertyValue(-1048576L)},
      {storage::PropertyId::FromUint(1025UL), storage::PropertyValue(-65537L)},
      {storage::PropertyId::FromUint(1026UL), storage::PropertyValue(std::numeric_limits<int16_t>::min())},
      {storage::PropertyId::FromUint(1027UL), storage::PropertyValue(-1024L)},
      {storage::PropertyId::FromUint(2000UL), storage::PropertyValue(-257L)},
      {storage::PropertyId::FromUint(3000UL), storage::PropertyValue(std::numeric_limits<int8_t>::min())},
      {storage::PropertyId::FromUint(4000UL), storage::PropertyValue(-1L)},
      {storage::PropertyId::FromUint(10000UL), storage::PropertyValue(0L)},
      {storage::PropertyId::FromUint(20000UL), storage::PropertyValue(1L)},
      {storage::PropertyId::FromUint(30000UL), storage::PropertyValue(std::numeric_limits<int8_t>::max())},
      {storage::PropertyId::FromUint(40000UL), storage::PropertyValue(256L)},
      {storage::PropertyId::FromUint(50000UL), storage::PropertyValue(1024L)},
      {storage::PropertyId::FromUint(std::numeric_limits<uint16_t>::max()),
       storage::PropertyValue(std::numeric_limits<int16_t>::max())},
      {storage::PropertyId::FromUint(65536UL), storage::PropertyValue(65536L)},
      {storage::PropertyId::FromUint(1048576UL), storage::PropertyValue(1048576L)},
      {storage::PropertyId::FromUint(std::numeric_limits<uint32_t>::max()),
       storage::PropertyValue(std::numeric_limits<int32_t>::max())},
      {storage::PropertyId::FromUint(4294967296UL), storage::PropertyValue(4294967296L)},
      {storage::PropertyId::FromUint(137438953472UL), storage::PropertyValue(137438953472L)},
      {storage::PropertyId::FromUint(std::numeric_limits<uint64_t>::max()),
       storage::PropertyValue(std::numeric_limits<int64_t>::max())}};

  storage::PropertyStore props;
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
    TestIsPropertyEqual(props, item.first, storage::PropertyValue());
  }
}

TEST(PropertyStore, IsPropertyEqualIntAndDouble) {
  storage::PropertyStore props;
  auto prop = storage::PropertyId::FromInt(42);

  ASSERT_TRUE(props.SetProperty(prop, storage::PropertyValue(42)));

  std::vector<std::pair<storage::PropertyValue, storage::PropertyValue>> tests{
      {storage::PropertyValue(0), storage::PropertyValue(0.0)},
      {storage::PropertyValue(123), storage::PropertyValue(123.0)},
      {storage::PropertyValue(12345), storage::PropertyValue(12345.0)},
      {storage::PropertyValue(12345678), storage::PropertyValue(12345678.0)},
      {storage::PropertyValue(1234567890123L), storage::PropertyValue(1234567890123.0)},
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
    test.first = storage::PropertyValue(test.first.ValueInt() * -1);
    test.second = storage::PropertyValue(test.second.ValueDouble() * -1.0);
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
    test.first =
        storage::PropertyValue(std::vector<storage::PropertyValue>{storage::PropertyValue(test.first.ValueInt())});
    test.second =
        storage::PropertyValue(std::vector<storage::PropertyValue>{storage::PropertyValue(test.second.ValueDouble())});
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
    test.first = storage::PropertyValue(
        std::vector<storage::PropertyValue>{storage::PropertyValue(test.first.ValueList()[0].ValueInt() * -1)});
    test.second = storage::PropertyValue(
        std::vector<storage::PropertyValue>{storage::PropertyValue(test.second.ValueList()[0].ValueDouble() * -1.0)});
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
  storage::PropertyStore props;
  auto prop = storage::PropertyId::FromInt(42);
  ASSERT_TRUE(props.SetProperty(prop, storage::PropertyValue("test")));
  ASSERT_TRUE(props.IsPropertyEqual(prop, storage::PropertyValue("test")));

  // Different length.
  ASSERT_FALSE(props.IsPropertyEqual(prop, storage::PropertyValue("helloworld")));

  // Same length, different value.
  ASSERT_FALSE(props.IsPropertyEqual(prop, storage::PropertyValue("asdf")));

  // Shortened and extended.
  ASSERT_FALSE(props.IsPropertyEqual(prop, storage::PropertyValue("tes")));
  ASSERT_FALSE(props.IsPropertyEqual(prop, storage::PropertyValue("testt")));
}

TEST(PropertyStore, IsPropertyEqualList) {
  storage::PropertyStore props;
  auto prop = storage::PropertyId::FromInt(42);
  ASSERT_TRUE(props.SetProperty(prop, storage::PropertyValue(std::vector<storage::PropertyValue>{
                                          storage::PropertyValue(42), storage::PropertyValue("test")})));
  ASSERT_TRUE(props.IsPropertyEqual(prop, storage::PropertyValue(std::vector<storage::PropertyValue>{
                                              storage::PropertyValue(42), storage::PropertyValue("test")})));

  // Different length.
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, storage::PropertyValue(std::vector<storage::PropertyValue>{storage::PropertyValue(24)})));

  // Same length, different value.
  ASSERT_FALSE(props.IsPropertyEqual(prop, storage::PropertyValue(std::vector<storage::PropertyValue>{
                                               storage::PropertyValue(42), storage::PropertyValue("asdf")})));

  // Shortened and extended.
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, storage::PropertyValue(std::vector<storage::PropertyValue>{storage::PropertyValue(42)})));
  ASSERT_FALSE(props.IsPropertyEqual(
      prop, storage::PropertyValue(std::vector<storage::PropertyValue>{
                storage::PropertyValue(42), storage::PropertyValue("test"), storage::PropertyValue(true)})));
}

TEST(PropertyStore, IsPropertyEqualMap) {
  storage::PropertyStore props;
  auto prop = storage::PropertyId::FromInt(42);
  ASSERT_TRUE(
      props.SetProperty(prop, storage::PropertyValue(std::map<std::string, storage::PropertyValue>{
                                  {"abc", storage::PropertyValue(42)}, {"zyx", storage::PropertyValue("test")}})));
  ASSERT_TRUE(
      props.IsPropertyEqual(prop, storage::PropertyValue(std::map<std::string, storage::PropertyValue>{
                                      {"abc", storage::PropertyValue(42)}, {"zyx", storage::PropertyValue("test")}})));

  // Different length.
  ASSERT_FALSE(props.IsPropertyEqual(prop, storage::PropertyValue(std::map<std::string, storage::PropertyValue>{
                                               {"fgh", storage::PropertyValue(24)}})));

  // Same length, different value.
  ASSERT_FALSE(
      props.IsPropertyEqual(prop, storage::PropertyValue(std::map<std::string, storage::PropertyValue>{
                                      {"abc", storage::PropertyValue(42)}, {"zyx", storage::PropertyValue("testt")}})));

  // Same length, different key (different length).
  ASSERT_FALSE(
      props.IsPropertyEqual(prop, storage::PropertyValue(std::map<std::string, storage::PropertyValue>{
                                      {"abc", storage::PropertyValue(42)}, {"zyxw", storage::PropertyValue("test")}})));

  // Same length, different key (same length).
  ASSERT_FALSE(
      props.IsPropertyEqual(prop, storage::PropertyValue(std::map<std::string, storage::PropertyValue>{
                                      {"abc", storage::PropertyValue(42)}, {"zyw", storage::PropertyValue("test")}})));

  // Shortened and extended.
  ASSERT_FALSE(props.IsPropertyEqual(prop, storage::PropertyValue(std::map<std::string, storage::PropertyValue>{
                                               {"abc", storage::PropertyValue(42)}})));
  ASSERT_FALSE(props.IsPropertyEqual(prop, storage::PropertyValue(std::map<std::string, storage::PropertyValue>{
                                               {"abc", storage::PropertyValue(42)},
                                               {"sdf", storage::PropertyValue(true)},
                                               {"zyx", storage::PropertyValue("test")}})));
}

TEST(PropertyStore, IsPropertyEqualTemporalData) {
  storage::PropertyStore props;
  auto prop = storage::PropertyId::FromInt(42);
  const storage::TemporalData temporal{storage::TemporalType::Date, 23};
  ASSERT_TRUE(props.SetProperty(prop, storage::PropertyValue(temporal)));
  ASSERT_TRUE(props.IsPropertyEqual(prop, storage::PropertyValue(temporal)));

  // Different type.
  ASSERT_FALSE(
      props.IsPropertyEqual(prop, storage::PropertyValue(storage::TemporalData{storage::TemporalType::Duration, 23})));

  // Same type, different value.
  ASSERT_FALSE(
      props.IsPropertyEqual(prop, storage::PropertyValue(storage::TemporalData{storage::TemporalType::Date, 30})));
}
