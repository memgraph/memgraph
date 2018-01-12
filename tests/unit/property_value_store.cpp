#include <vector>

#include "gtest/gtest.h"

#include "storage/property_value.hpp"
#include "storage/property_value_store.hpp"

using std::string;

class PropertyValueStoreTest : public ::testing::Test {
 protected:
  PropertyValueStore props_;

  void Set(int key, PropertyValue value) {
    props_.set(database::Property(key), value);
  }

  PropertyValue At(int key) { return props_.at(database::Property(key)); }

  auto Erase(int key) { return props_.erase(database::Property(key)); }
};

TEST_F(PropertyValueStoreTest, At) {
  std::string some_string = "something";

  EXPECT_EQ(PropertyValue(At(0)).type(), PropertyValue::Type::Null);
  Set(0, some_string);
  EXPECT_EQ(PropertyValue(At(0)).Value<string>(), some_string);
  Set(120, 42);
  EXPECT_EQ(PropertyValue(At(120)).Value<int64_t>(), 42);
}

TEST_F(PropertyValueStoreTest, AtNull) {
  EXPECT_EQ(At(0).type(), PropertyValue::Type::Null);
  EXPECT_EQ(At(100).type(), PropertyValue::Type::Null);

  // set one prop and test it's not null
  Set(0, true);
  EXPECT_NE(At(0).type(), PropertyValue::Type::Null);
  EXPECT_EQ(At(100).type(), PropertyValue::Type::Null);
}

TEST_F(PropertyValueStoreTest, SetNull) {
  Set(11, PropertyValue::Null);
  EXPECT_EQ(0, props_.size());
}

TEST_F(PropertyValueStoreTest, Remove) {
  // set some props
  Set(11, "a");
  Set(30, "b");
  EXPECT_NE(At(11).type(), PropertyValue::Type::Null);
  EXPECT_NE(At(30).type(), PropertyValue::Type::Null);
  EXPECT_EQ(props_.size(), 2);

  Erase(11);
  EXPECT_EQ(props_.size(), 1);
  EXPECT_EQ(At(11).type(), PropertyValue::Type::Null);

  EXPECT_EQ(Erase(30), 1);
  EXPECT_EQ(props_.size(), 0);
  EXPECT_EQ(At(30).type(), PropertyValue::Type::Null);

  EXPECT_EQ(Erase(1000), 0);
}

TEST_F(PropertyValueStoreTest, Clear) {
  // set some props
  EXPECT_EQ(props_.size(), 0);
  props_.clear();

  EXPECT_EQ(props_.size(), 0);
  Set(11, "a");
  Set(30, "b");
  EXPECT_EQ(props_.size(), 2);
  props_.clear();
  EXPECT_EQ(props_.size(), 0);
}

TEST_F(PropertyValueStoreTest, Replace) {
  Set(10, 42);
  EXPECT_EQ(At(10).Value<int64_t>(), 42);
  Set(10, 0.25f);
  EXPECT_EQ(At(10).type(), PropertyValue::Type::Double);
  EXPECT_FLOAT_EQ(At(10).Value<double>(), 0.25);
}

TEST_F(PropertyValueStoreTest, Size) {
  EXPECT_EQ(props_.size(), 0);

  Set(0, "something");
  EXPECT_EQ(props_.size(), 1);
  Set(0, true);
  EXPECT_EQ(props_.size(), 1);
  Set(1, true);
  EXPECT_EQ(props_.size(), 2);

  for (int i = 0; i < 100; ++i) Set(i + 20, true);
  EXPECT_EQ(props_.size(), 102);

  Erase(0);
  EXPECT_EQ(props_.size(), 101);
  Erase(0);
  EXPECT_EQ(props_.size(), 101);
  Erase(1);
  EXPECT_EQ(props_.size(), 100);
}

TEST_F(PropertyValueStoreTest, InsertRetrieveList) {
  Set(0, std::vector<PropertyValue>{1, true, 2.5, "something",
                                    PropertyValue::Null});
  auto p = At(0);

  EXPECT_EQ(p.type(), PropertyValue::Type::List);
  auto l = p.Value<std::vector<PropertyValue>>();
  EXPECT_EQ(l.size(), 5);
  EXPECT_EQ(l[0].type(), PropertyValue::Type::Int);
  EXPECT_EQ(l[0].Value<int64_t>(), 1);
  EXPECT_EQ(l[1].type(), PropertyValue::Type::Bool);
  EXPECT_EQ(l[1].Value<bool>(), true);
  EXPECT_EQ(l[2].type(), PropertyValue::Type::Double);
  EXPECT_EQ(l[2].Value<double>(), 2.5);
  EXPECT_EQ(l[3].type(), PropertyValue::Type::String);
  EXPECT_EQ(l[3].Value<std::string>(), "something");
  EXPECT_EQ(l[4].type(), PropertyValue::Type::Null);
}

TEST_F(PropertyValueStoreTest, InsertRetrieveMap) {
  Set(0, std::map<std::string, PropertyValue>{
             {"a", 1}, {"b", true}, {"c", "something"}});

  auto p = At(0);
  EXPECT_EQ(p.type(), PropertyValue::Type::Map);
  auto m = p.Value<std::map<std::string, PropertyValue>>();
  EXPECT_EQ(m.size(), 3);
  auto get = [&m](const std::string &prop_name) {
    return m.find(prop_name)->second;
  };
  EXPECT_EQ(get("a").type(), PropertyValue::Type::Int);
  EXPECT_EQ(get("a").Value<int64_t>(), 1);
  EXPECT_EQ(get("b").type(), PropertyValue::Type::Bool);
  EXPECT_EQ(get("b").Value<bool>(), true);
  EXPECT_EQ(get("c").type(), PropertyValue::Type::String);
  EXPECT_EQ(get("c").Value<std::string>(), "something");
}
