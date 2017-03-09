//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 24.01.17..
//
#include <vector>

#include "gtest/gtest.h"

#include "storage/property_value_store.hpp"
#include "storage/property_value.hpp"

using std::string;

TEST(PropertyValueStore, At) {
  std::string some_string = "something";

  PropertyValueStore<> props;
  EXPECT_EQ(PropertyValue(props.at(0)).type(), PropertyValue::Type::Null);
  props.set(0, some_string);
  EXPECT_EQ(PropertyValue(props.at(0)).Value<string>(), some_string);
  props.set(120, 42);
  EXPECT_EQ(PropertyValue(props.at(120)).Value<int64_t>(), 42);
}

TEST(PropertyValueStore, AtNull) {
  PropertyValueStore<> props;
  EXPECT_EQ(props.at(0).type(), PropertyValue::Type::Null);
  EXPECT_EQ(props.at(100).type(), PropertyValue::Type::Null);

  // set one prop and test it's not null
  props.set(0, true);
  EXPECT_NE(props.at(0).type(), PropertyValue::Type::Null);
  EXPECT_EQ(props.at(100).type(), PropertyValue::Type::Null);
}

TEST(PropertyValueStore, Remove) {
  // set some props
  PropertyValueStore<> props;
  props.set(11, "a");
  props.set(30, "b");
  EXPECT_NE(props.at(11).type(), PropertyValue::Type::Null);
  EXPECT_NE(props.at(30).type(), PropertyValue::Type::Null);
  EXPECT_EQ(props.size(), 2);

  props.erase(11);
  EXPECT_EQ(props.size(), 1);
  EXPECT_EQ(props.at(11).type(), PropertyValue::Type::Null);

  EXPECT_EQ(props.erase(30), 1);
  EXPECT_EQ(props.size(), 0);
  EXPECT_EQ(props.at(30).type(), PropertyValue::Type::Null);

  EXPECT_EQ(props.erase(1000), 0);
}

TEST(PropertyValueStore, Replace) {
  PropertyValueStore<> props;
  props.set(10, 42);
  EXPECT_EQ(props.at(10).Value<int64_t>(), 42);
  props.set(10, 0.25f);
  EXPECT_EQ(props.at(10).type(), PropertyValue::Type::Double);
  EXPECT_FLOAT_EQ(props.at(10).Value<double>(), 0.25);
}

TEST(PropertyValueStore, Size) {
  PropertyValueStore<> props;
  EXPECT_EQ(props.size(), 0);

  props.set(0, "something");
  EXPECT_EQ(props.size(), 1);
  props.set(0, true);
  EXPECT_EQ(props.size(), 1);
  props.set(1, true);
  EXPECT_EQ(props.size(), 2);

  for (int i = 0; i < 100; ++i) props.set(i + 20, true);
  EXPECT_EQ(props.size(), 102);

  props.erase(0);
  EXPECT_EQ(props.size(), 101);
  props.erase(0);
  EXPECT_EQ(props.size(), 101);
  props.erase(1);
  EXPECT_EQ(props.size(), 100);
}

TEST(PropertyValueStore, Accept) {
  int count_props = 0;
  int count_finish = 0;

  auto handler =
      [&](const uint32_t, const PropertyValue&) { count_props += 1; };
  auto finish = [&]() { count_finish += 1; };

  PropertyValueStore<uint32_t> props;
  props.Accept(handler, finish);
  EXPECT_EQ(count_props, 0);
  EXPECT_EQ(count_finish, 1);

  props.Accept(handler);
  EXPECT_EQ(count_props, 0);
  EXPECT_EQ(count_finish, 1);

  props.set(0, 20);
  props.set(1, "bla");

  props.Accept(handler, finish);
  EXPECT_EQ(count_props, 2);
  EXPECT_EQ(count_finish, 2);
}

TEST(PropertyValueStore, InsertRetrieveList) {
  PropertyValueStore<> props;
  props.set(0, std::vector<PropertyValue>{1, true, 2.5, "something",
                                          PropertyValue::Null});
  auto p = props.at(0);

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
