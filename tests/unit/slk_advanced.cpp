#include <gtest/gtest.h>

#include "storage/common/types/slk.hpp"

#include "slk_common.hpp"

TEST(SlkAdvanced, PropertyValueList) {
  std::vector<PropertyValue> original{PropertyValue("hello world!"),
                                      PropertyValue(5), PropertyValue(1.123423),
                                      PropertyValue(true), PropertyValue()};
  ASSERT_EQ(original[0].type(), PropertyValue::Type::String);
  ASSERT_EQ(original[1].type(), PropertyValue::Type::Int);
  ASSERT_EQ(original[2].type(), PropertyValue::Type::Double);
  ASSERT_EQ(original[3].type(), PropertyValue::Type::Bool);
  ASSERT_EQ(original[4].type(), PropertyValue::Type::Null);

  slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  slk::Save(original, builder);

  std::vector<PropertyValue> decoded;
  auto reader = loopback.GetReader();
  slk::Load(&decoded, reader);

  ASSERT_EQ(original, decoded);
}

TEST(SlkAdvanced, PropertyValueMap) {
  std::map<std::string, PropertyValue> original{
      {"hello", PropertyValue("world")},
      {"number", PropertyValue(5)},
      {"real", PropertyValue(1.123423)},
      {"truth", PropertyValue(true)},
      {"nothing", PropertyValue()}};
  ASSERT_EQ(original["hello"].type(), PropertyValue::Type::String);
  ASSERT_EQ(original["number"].type(), PropertyValue::Type::Int);
  ASSERT_EQ(original["real"].type(), PropertyValue::Type::Double);
  ASSERT_EQ(original["truth"].type(), PropertyValue::Type::Bool);
  ASSERT_EQ(original["nothing"].type(), PropertyValue::Type::Null);

  slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  slk::Save(original, builder);

  std::map<std::string, PropertyValue> decoded;
  auto reader = loopback.GetReader();
  slk::Load(&decoded, reader);

  ASSERT_EQ(original, decoded);
}

TEST(SlkAdvanced, PropertyValueComplex) {
  std::vector<PropertyValue> vec_v{PropertyValue("hello world!"),
                                   PropertyValue(5), PropertyValue(1.123423),
                                   PropertyValue(true), PropertyValue()};
  ASSERT_EQ(vec_v[0].type(), PropertyValue::Type::String);
  ASSERT_EQ(vec_v[1].type(), PropertyValue::Type::Int);
  ASSERT_EQ(vec_v[2].type(), PropertyValue::Type::Double);
  ASSERT_EQ(vec_v[3].type(), PropertyValue::Type::Bool);
  ASSERT_EQ(vec_v[4].type(), PropertyValue::Type::Null);

  std::map<std::string, PropertyValue> map_v{{"hello", PropertyValue("world")},
                                             {"number", PropertyValue(5)},
                                             {"real", PropertyValue(1.123423)},
                                             {"truth", PropertyValue(true)},
                                             {"nothing", PropertyValue()}};
  ASSERT_EQ(map_v["hello"].type(), PropertyValue::Type::String);
  ASSERT_EQ(map_v["number"].type(), PropertyValue::Type::Int);
  ASSERT_EQ(map_v["real"].type(), PropertyValue::Type::Double);
  ASSERT_EQ(map_v["truth"].type(), PropertyValue::Type::Bool);
  ASSERT_EQ(map_v["nothing"].type(), PropertyValue::Type::Null);

  PropertyValue original(
      std::vector<PropertyValue>{PropertyValue(vec_v), PropertyValue(map_v)});
  ASSERT_EQ(original.type(), PropertyValue::Type::List);

  slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  slk::Save(original, builder);

  PropertyValue decoded;
  auto reader = loopback.GetReader();
  slk::Load(&decoded, reader);

  ASSERT_EQ(original, decoded);
}
