//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 24.01.17..
//
#include <vector>

#include "gtest/gtest.h"

#include "storage/model/typed_value.hpp"
#include "storage/model/typed_value_store.hpp"

using std::string;

TEST(TypedValueStore, At) {

  std::string some_string = "something";

  TypedValueStore props;
  EXPECT_EQ(TypedValue(props.at(0)).type_, TypedValue::Type::Null);
  props.set(0, some_string);
  EXPECT_EQ(TypedValue(props.at(0)).Value<string>(), some_string);
  props.set(120, 42);
  EXPECT_EQ(TypedValue(props.at(120)).Value<int>(), 42);
}

TEST(TypedValueStore, AtNull) {
  TypedValueStore props;
  EXPECT_EQ(props.at(0).type_, TypedValue::Type::Null);
  EXPECT_EQ(props.at(100).type_, TypedValue::Type::Null);

  // set one prop and test it's not null
  props.set(0, true);
  EXPECT_NE(props.at(0).type_, TypedValue::Type::Null);
  EXPECT_EQ(props.at(100).type_, TypedValue::Type::Null);
}

TEST(TypedValueStore, Remove) {
  // set some props
  TypedValueStore props;
  props.set(11, "a");
  props.set(30, "b");
  EXPECT_NE(props.at(11).type_, TypedValue::Type::Null);
  EXPECT_NE(props.at(30).type_, TypedValue::Type::Null);
  EXPECT_EQ(props.size(), 2);

  props.erase(11);
  EXPECT_EQ(props.size(), 1);
  EXPECT_EQ(props.at(11).type_, TypedValue::Type::Null);

  EXPECT_EQ(props.erase(30), 1);
  EXPECT_EQ(props.size(), 0);
  EXPECT_EQ(props.at(30).type_, TypedValue::Type::Null);

  EXPECT_EQ(props.erase(1000), 0);
}

TEST(TypedValueStore, Replace) {
  TypedValueStore props;
  props.set(10, 42);
  EXPECT_EQ(props.at(10).Value<int>(), 42);
  props.set(10, 0.25f);
  EXPECT_EQ(props.at(10).type_, TypedValue::Type::Float);
  EXPECT_FLOAT_EQ(props.at(10).Value<float>(), 0.25);
}

TEST(TypedValueStore, Size) {

  TypedValueStore props;
  EXPECT_EQ(props.size(), 0);

  props.set(0, "something");
  EXPECT_EQ(props.size(), 1);
  props.set(0, true);
  EXPECT_EQ(props.size(), 1);
  props.set(1, true);
  EXPECT_EQ(props.size(), 2);

  for (int i = 0; i < 100; ++i)
    props.set(i + 20, true);
  EXPECT_EQ(props.size(), 102);

  props.erase(0);
  EXPECT_EQ(props.size(), 101);
  props.erase(0);
  EXPECT_EQ(props.size(), 101);
  props.erase(1);
  EXPECT_EQ(props.size(), 100);
}

TEST(TypedValueStore, Accept) {

  int count_props = 0;
  int count_finish = 0;

  auto handler = [&](const TypedValueStore::TKey key, const TypedValue& prop) {
    count_props += 1;
  };
  auto finish = [&]() {
    count_finish += 1;
  };

  TypedValueStore props;
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
