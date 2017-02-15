//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 24.01.17..
//
#include <vector>
#include <functional>

#include "gtest/gtest.h"

#include "storage/typed_value.hpp"
#include "storage/typed_value_store.hpp"

TypedValueStore<> MakePropsAllTypes() {
  TypedValueStore<> props;
  props.set(0, true);
  props.set(1, "something");
  props.set(2, 42);
  props.set(3, 0.5f);
  return props;
}

void EXPECT_PROP_FALSE(const TypedValue& a) {
  EXPECT_TRUE(a.type_ == TypedValue::Type::Bool && !a.Value<bool>());
}

void EXPECT_PROP_TRUE(const TypedValue& a) {
  EXPECT_TRUE(a.type_ == TypedValue::Type::Bool && a.Value<bool>());
}

void EXPECT_PROP_EQ(const TypedValue& a, const TypedValue& b)  {
  EXPECT_PROP_TRUE(a == b);
}

void EXPECT_PROP_ISNULL(const TypedValue& a)  {
  EXPECT_TRUE(a.type_ == TypedValue::Type::Null);
}

void EXPECT_PROP_NE(const TypedValue& a, const TypedValue& b)  {
  EXPECT_PROP_TRUE(a != b);
}

TEST(TypedValue, CreationTypes) {
  EXPECT_TRUE(TypedValue::Null.type_ == TypedValue::Type::Null);

  EXPECT_TRUE(TypedValue(true).type_ == TypedValue::Type::Bool);
  EXPECT_TRUE(TypedValue(false).type_ == TypedValue::Type::Bool);

  EXPECT_TRUE(TypedValue(std::string("form string class")).type_ == TypedValue::Type::String);
  EXPECT_TRUE(TypedValue("form c-string").type_ == TypedValue::Type::String);

  EXPECT_TRUE(TypedValue(0).type_ == TypedValue::Type::Int);
  EXPECT_TRUE(TypedValue(42).type_ == TypedValue::Type::Int);

  EXPECT_TRUE(TypedValue(0.0f).type_ == TypedValue::Type::Float);
  EXPECT_TRUE(TypedValue(42.5f).type_ == TypedValue::Type::Float);
}

TEST(TypedValue, CreationValues) {
  EXPECT_EQ(TypedValue(true).Value<bool>(), true);
  EXPECT_EQ(TypedValue(false).Value<bool>(), false);

  EXPECT_EQ(TypedValue(std::string("bla")).Value<std::string>(), "bla");
  EXPECT_EQ(TypedValue("bla2").Value<std::string>(), "bla2");

  EXPECT_EQ(TypedValue(55).Value<int>(), 55);

  EXPECT_FLOAT_EQ(TypedValue(66.6f).Value<float>(), 66.6f);
}

TEST(TypedValue, Equals) {
  EXPECT_PROP_EQ(TypedValue(true), TypedValue(true));
  EXPECT_PROP_NE(TypedValue(true), TypedValue(false));

  EXPECT_PROP_EQ(TypedValue(42), TypedValue(42));
  EXPECT_PROP_NE(TypedValue(0), TypedValue(1));

  EXPECT_PROP_NE(TypedValue(0.5f), TypedValue(0.12f));
  EXPECT_PROP_EQ(TypedValue(0.123f), TypedValue(0.123f));

  EXPECT_PROP_EQ(TypedValue(2), TypedValue(2.0f));
  EXPECT_PROP_NE(TypedValue(2), TypedValue(2.1f));

  EXPECT_PROP_NE(TypedValue("str1"), TypedValue("str2"));
  EXPECT_PROP_EQ(TypedValue("str3"), TypedValue("str3"));
  EXPECT_PROP_EQ(TypedValue(std::string("str3")), TypedValue("str3"));
}

TEST(TypedValue, Less) {
  // not_bool_type < bool -> exception
  TypedValueStore<> props = MakePropsAllTypes();
  for (int i = 0; i < props.size() + 1; ++i) {
    if (props.at(i).type_ == TypedValue::Type::Bool)
      continue;
    // the comparison should raise an exception
    // cast to (void) so the compiler does not complain about unused comparison result
    EXPECT_THROW((void)(props.at(i) < TypedValue(true)), TypedValueException);
  }

  // not_bool_type < Null = Null
  props = MakePropsAllTypes();
  for (int i = 0; i < props.size() + 1; ++i) {
    if (props.at(i).type_ == TypedValue::Type::Bool)
      continue;
    EXPECT_PROP_ISNULL(props.at(i) < TypedValue::Null);
  }

  // int tests
  EXPECT_PROP_TRUE(TypedValue(2) < TypedValue(3));
  EXPECT_PROP_FALSE(TypedValue(2) < TypedValue(2));
  EXPECT_PROP_FALSE(TypedValue(3) < TypedValue(2));

  // float tests
  EXPECT_PROP_TRUE(TypedValue(2.1f) < TypedValue(2.5f));
  EXPECT_PROP_FALSE(TypedValue(2.0f) < TypedValue(2.0f));
  EXPECT_PROP_FALSE(TypedValue(2.5f) < TypedValue(2.4f));

  // implicit casting int->float
  EXPECT_PROP_TRUE(TypedValue(2) < TypedValue(2.1f));
  EXPECT_PROP_FALSE(TypedValue(2.1f) < TypedValue(2));
  EXPECT_PROP_FALSE(TypedValue(2) < TypedValue(1.5f));
  EXPECT_PROP_TRUE(TypedValue(1.5f) < TypedValue(2));

  // string tests
  EXPECT_PROP_TRUE(TypedValue("a") < TypedValue("b"));
  EXPECT_PROP_TRUE(TypedValue("aaaaa") < TypedValue("b"));
  EXPECT_PROP_TRUE(TypedValue("A") < TypedValue("a"));
}

TEST(TypedValue, LogicalNot) {
  EXPECT_PROP_EQ(!TypedValue(true), TypedValue(false));
  EXPECT_PROP_ISNULL(!TypedValue::Null);
  EXPECT_THROW(!TypedValue(0), TypedValueException);
  EXPECT_THROW(!TypedValue(0.2f), TypedValueException);
  EXPECT_THROW(!TypedValue("something"), TypedValueException);
}

TEST(TypedValue, UnaryMinus) {
  EXPECT_TRUE((-TypedValue::Null).type_ == TypedValue::Type::Null);

  EXPECT_PROP_EQ((-TypedValue(2).Value<int>()), -2);
  EXPECT_FLOAT_EQ((-TypedValue(2.0f).Value<float>()), -2.0f);

  EXPECT_THROW(-TypedValue(true), TypedValueException);
  EXPECT_THROW(-TypedValue("something"), TypedValueException);
}


/**
 * Performs a series of tests on properties of all types. The tests
 * evaluate how arithmetic operators behave w.r.t. exception throwing
 * in case of invalid operands and null handling.
 *
 * @param string_ok Indicates if or not the operation tested works
 *  with String values (does not throw).
 * @param op  The operation lambda. Takes two values and resturns
 *  the results.
 */
void ExpectArithmeticThrowsAndNull(bool string_ok, std::function<TypedValue(const TypedValue&, const TypedValue&)> op) {
  // arithmetic ops that raise
  TypedValueStore<> props = MakePropsAllTypes();
  for (int i = 0; i < props.size() + 1; ++i) {
    EXPECT_THROW(op(TypedValue(true), props.at(i)), TypedValueException);
    EXPECT_THROW(op(props.at(i), TypedValue(true)), TypedValueException);
    if (!string_ok) {
      EXPECT_THROW(op(TypedValue("some"), props.at(i)), TypedValueException);
      EXPECT_THROW(op(props.at(i), TypedValue("some")), TypedValueException);
    }
  }

  // null resulting ops
  props = MakePropsAllTypes();
  for (int i = 0; i < props.size() + 1; ++i) {
    if (props.at(i).type_ == TypedValue::Type::Bool)
      continue;
    if (!string_ok && props.at(i).type_ == TypedValue::Type::String)
      continue;

    EXPECT_PROP_ISNULL(op(props.at(i), TypedValue::Null));
    EXPECT_PROP_ISNULL(op(TypedValue::Null, props.at(i)));
  }
}

TEST(TypedValue, Sum) {
  ExpectArithmeticThrowsAndNull(true, [](const TypedValue &a, const TypedValue& b) { return a + b; });

  // sum of props of the same type
  EXPECT_EQ((TypedValue(2) + TypedValue(3)).Value<int>(), 5);
  EXPECT_FLOAT_EQ((TypedValue(2.5f) + TypedValue(1.25f)).Value<float>(), 3.75);
  EXPECT_EQ((TypedValue("one") + TypedValue("two")).Value<std::string>(), "onetwo");

  // sum of string and numbers
  EXPECT_EQ((TypedValue("one") + TypedValue(1)).Value<std::string>(), "one1");
  EXPECT_EQ((TypedValue(1) + TypedValue("one")).Value<std::string>(), "1one");
  EXPECT_EQ((TypedValue("one") + TypedValue(3.2f)).Value<std::string>(), "one3.2");
  EXPECT_EQ((TypedValue(3.2f) + TypedValue("one")).Value<std::string>(), "3.2one");
}

TEST(TypedValue, Difference) {
  ExpectArithmeticThrowsAndNull(false, [](const TypedValue &a, const TypedValue& b) { return a - b; });

  // difference of props of the same type
  EXPECT_EQ((TypedValue(2) - TypedValue(3)).Value<int>(), -1);
  EXPECT_FLOAT_EQ((TypedValue(2.5f) - TypedValue(2.25f)).Value<float>(), 0.25);

  // implicit casting
  EXPECT_FLOAT_EQ((TypedValue(2) - TypedValue(0.5f)).Value<float>(), 1.5f);
  EXPECT_FLOAT_EQ((TypedValue(2.5f) - TypedValue(2)).Value<float>(), 0.5f);
}

TEST(TypedValue, Divison) {
  ExpectArithmeticThrowsAndNull(false, [](const TypedValue &a, const TypedValue& b) { return a / b; });

  EXPECT_PROP_EQ(TypedValue(10) / TypedValue(2), TypedValue(5));
  EXPECT_PROP_EQ(TypedValue(10) / TypedValue(4), TypedValue(2));

  EXPECT_PROP_EQ(TypedValue(10.0f) / TypedValue(2.0f), TypedValue(5.0f));
  EXPECT_FLOAT_EQ((TypedValue(10.0f) / TypedValue(4.0f)).Value<float>(), 2.5f);

  EXPECT_FLOAT_EQ((TypedValue(10) / TypedValue(4.0f)).Value<float>(), 2.5f);
  EXPECT_FLOAT_EQ((TypedValue(10.0f) / TypedValue(4)).Value<float>(), 2.5f);
}

TEST(TypedValue, Multiplication) {
  ExpectArithmeticThrowsAndNull(false, [](const TypedValue &a, const TypedValue& b) { return a * b; });

  EXPECT_PROP_EQ(TypedValue(10) * TypedValue(2), TypedValue(20));
  EXPECT_FLOAT_EQ((TypedValue(12.5f) * TypedValue(6.6f)).Value<float>(), 12.5f * 6.6f);
  EXPECT_FLOAT_EQ((TypedValue(10) * TypedValue(4.5f)).Value<float>(), 10 * 4.5f);
  EXPECT_FLOAT_EQ((TypedValue(10.2f) * TypedValue(4)).Value<float>(), 10.2f * 4);
}

TEST(TypedValue, Modulo) {
  ExpectArithmeticThrowsAndNull(false, [](const TypedValue &a, const TypedValue& b) { return a % b; });

  EXPECT_PROP_EQ(TypedValue(10) % TypedValue(2), TypedValue(0));
  EXPECT_PROP_EQ(TypedValue(10) % TypedValue(4), TypedValue(2));

  EXPECT_PROP_EQ(TypedValue(10.0f) % TypedValue(2.0f), TypedValue(0.0f));
  EXPECT_FLOAT_EQ((TypedValue(10.0f) % TypedValue(3.25f)).Value<float>(), 0.25f);

  EXPECT_FLOAT_EQ((TypedValue(10) % TypedValue(4.0f)).Value<float>(), 2.0f);
  EXPECT_FLOAT_EQ((TypedValue(10.0f) % TypedValue(4)).Value<float>(), 2.0f);
}

TEST(TypedValue, TypeIncompatibility) {
  TypedValueStore<> props = MakePropsAllTypes();

  // iterate over all the props, plus one, what will return
  // the Null property, which must be incompatible with all
  // the others
  for (int i = 0; i < props.size() + 1; ++i)
    for (int j = 0; j < props.size() + 1; ++j)
      EXPECT_EQ(props.at(i).type_== props.at(j).type_, i == j);
}

/**
 * Logical operations (logical and, or) are only legal on bools
 * and nulls. This function ensures that the given
 * logical operation throws exceptions when either operand
 * is not bool or null.
 *
 * @param op The logical operation to test.
 */
void TestLogicalThrows(std::function<TypedValue(const TypedValue&, const TypedValue&)> op) {
  TypedValueStore<> props = MakePropsAllTypes();
  for (int i = 0; i < props.size() + 1; ++i) {
    auto p1 = props.at(i);
    for (int j = 0; j < props.size() + 1; ++j) {
      auto p2 = props.at(j);
      // skip situations when both p1 and p2 are either bool or null
      auto p1_ok = p1.type_ == TypedValue::Type::Bool || p1.type_ == TypedValue::Type::Null;
      auto p2_ok = p2.type_ == TypedValue::Type::Bool || p2.type_ == TypedValue::Type::Null;
      if (p1_ok && p2_ok)
        continue;

      EXPECT_THROW(op(p1, p2), TypedValueException);
    }
  }
}

TEST(TypedValue, LogicalAnd) {
  TestLogicalThrows([](const TypedValue& p1, const TypedValue& p2) {return p1 && p2;});
  EXPECT_PROP_ISNULL(TypedValue::Null && TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(true) && TypedValue(true), TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(false) && TypedValue(true), TypedValue(false));
}

TEST(TypedValue, LogicalOr) {
  TestLogicalThrows([](const TypedValue& p1, const TypedValue& p2) {return p1 || p2;});
  EXPECT_PROP_ISNULL(TypedValue::Null && TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(true) || TypedValue(true), TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(false) || TypedValue(true), TypedValue(true));
}
