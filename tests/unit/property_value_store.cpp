#include <vector>

#include "gtest/gtest.h"

#include "storage/property_value.hpp"
#include "storage/property_value_store.hpp"

using std::string;
using Location = storage::Location;

class PropertyValueStoreTest : public ::testing::Test {
 protected:
  PropertyValueStore props_;

  void Set(int key, Location location, PropertyValue value) {
    props_.set(storage::Property(key, location), value);
  }

  PropertyValue At(int key, Location location) {
    return props_.at(storage::Property(key, location));
  }

  auto Erase(int key, Location location) {
    return props_.erase(storage::Property(key, location));
  }

  void TearDown() override { props_.clear(); }
};

TEST_F(PropertyValueStoreTest, At) {
  std::string some_string = "something";
  std::string other_string = "something completely different";

  EXPECT_EQ(PropertyValue(At(0, Location::Memory)).type(),
            PropertyValue::Type::Null);
  Set(0, Location::Memory, some_string);
  EXPECT_EQ(PropertyValue(At(0, Location::Memory)).Value<string>(),
            some_string);
  Set(120, Location::Memory, 42);
  EXPECT_EQ(PropertyValue(At(120, Location::Memory)).Value<int64_t>(), 42);

  Set(100, Location::Disk, other_string);
  EXPECT_EQ(PropertyValue(At(100, Location::Disk)).Value<string>(),
            other_string);
}

TEST_F(PropertyValueStoreTest, AtNull) {
  EXPECT_EQ(At(0, Location::Memory).type(), PropertyValue::Type::Null);
  EXPECT_EQ(At(100, Location::Memory).type(), PropertyValue::Type::Null);

  EXPECT_EQ(At(0, Location::Disk).type(), PropertyValue::Type::Null);
  EXPECT_EQ(At(100, Location::Disk).type(), PropertyValue::Type::Null);

  // set one prop and test it's not null
  Set(0, Location::Memory, true);
  EXPECT_NE(At(0, Location::Memory).type(), PropertyValue::Type::Null);
  EXPECT_EQ(At(100, Location::Memory).type(), PropertyValue::Type::Null);

  Set(0, Location::Disk, true);
  EXPECT_NE(At(0, Location::Disk).type(), PropertyValue::Type::Null);
  EXPECT_EQ(At(100, Location::Disk).type(), PropertyValue::Type::Null);
}

TEST_F(PropertyValueStoreTest, SetNull) {
  Set(11, Location::Memory, PropertyValue::Null);
  EXPECT_EQ(0, props_.size());

  Set(100, Location::Disk, PropertyValue::Null);
  EXPECT_EQ(0, props_.size());
}

TEST_F(PropertyValueStoreTest, Remove) {
  // set some props
  Set(11, Location::Memory, "a");
  Set(30, Location::Memory, "b");
  EXPECT_NE(At(11, Location::Memory).type(), PropertyValue::Type::Null);
  EXPECT_NE(At(30, Location::Memory).type(), PropertyValue::Type::Null);
  EXPECT_EQ(props_.size(), 2);

  Erase(11, Location::Memory);
  EXPECT_EQ(props_.size(), 1);
  EXPECT_EQ(At(11, Location::Memory).type(), PropertyValue::Type::Null);

  EXPECT_EQ(Erase(30, Location::Memory), 1);
  EXPECT_EQ(props_.size(), 0);
  EXPECT_EQ(At(30, Location::Memory).type(), PropertyValue::Type::Null);

  EXPECT_EQ(Erase(1000, Location::Memory), 0);

  props_.clear();

  Set(110, Location::Disk, "a");
  EXPECT_NE(At(110, Location::Disk).type(), PropertyValue::Type::Null);
  EXPECT_EQ(props_.size(), 1);

  Erase(110, Location::Disk);
  EXPECT_EQ(props_.size(), 0);
  EXPECT_EQ(At(110, Location::Disk).type(), PropertyValue::Type::Null);
  EXPECT_EQ(Erase(1000, Location::Disk), 0);
}

TEST_F(PropertyValueStoreTest, Clear) {
  EXPECT_EQ(props_.size(), 0);
  Set(11, Location::Memory, "a");
  Set(30, Location::Memory, "b");
  EXPECT_EQ(props_.size(), 2);
  props_.clear();
  EXPECT_EQ(props_.size(), 0);

  Set(11, Location::Disk, "a");
  EXPECT_EQ(props_.size(), 1);
  props_.clear();
  EXPECT_EQ(props_.size(), 0);
}

TEST_F(PropertyValueStoreTest, Replace) {
  Set(10, Location::Memory, 42);
  EXPECT_EQ(At(10, Location::Memory).Value<int64_t>(), 42);
  Set(10, Location::Memory, 0.25f);
  EXPECT_EQ(At(10, Location::Memory).type(), PropertyValue::Type::Double);
  EXPECT_FLOAT_EQ(At(10, Location::Memory).Value<double>(), 0.25);

  Set(100, Location::Disk, "some text");
  EXPECT_EQ(At(100, Location::Disk).Value<string>(), "some text");
  Set(100, Location::Disk, "some other text");
  EXPECT_EQ(At(100, Location::Disk).Value<string>(), "some other text");
}

TEST_F(PropertyValueStoreTest, Size) {
  EXPECT_EQ(props_.size(), 0);

  Set(0, Location::Memory, "something");
  EXPECT_EQ(props_.size(), 1);
  Set(0, Location::Memory, true);
  EXPECT_EQ(props_.size(), 1);
  Set(1, Location::Memory, true);
  EXPECT_EQ(props_.size(), 2);

  for (int i = 0; i < 100; ++i) Set(i + 20, Location::Memory, true);
  EXPECT_EQ(props_.size(), 102);

  Erase(0, Location::Memory);
  EXPECT_EQ(props_.size(), 101);
  Erase(0, Location::Memory);
  EXPECT_EQ(props_.size(), 101);
  Erase(1, Location::Memory);
  EXPECT_EQ(props_.size(), 100);

  Set(101, Location::Disk, "dalmatians");
  EXPECT_EQ(props_.size(), 101);
  Erase(101, Location::Disk);
  EXPECT_EQ(props_.size(), 100);
}

TEST_F(PropertyValueStoreTest, InsertRetrieveList) {
  Set(0, Location::Memory, std::vector<PropertyValue>{1, true, 2.5, "something",
                                                      PropertyValue::Null});
  auto p = At(0, Location::Memory);

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
  Set(0, Location::Memory, std::map<std::string, PropertyValue>{
                               {"a", 1}, {"b", true}, {"c", "something"}});

  auto p = At(0, Location::Memory);
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
