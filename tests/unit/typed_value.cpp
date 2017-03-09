//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 24.01.17..
//
#include <functional>
#include <vector>

#include "gtest/gtest.h"
#include "query/backend/cpp/typed_value.hpp"

namespace {

std::vector<TypedValue> MakePropsAllTypes() {
  return {
      true, "something", 42, 0.5, TypedValue::Null,
      std::vector<TypedValue>{true, "something", 42, 0.5, TypedValue::Null}};
}
}

void EXPECT_PROP_FALSE(const TypedValue& a) {
  EXPECT_TRUE(a.type() == TypedValue::Type::Bool && !a.Value<bool>());
}

void EXPECT_PROP_TRUE(const TypedValue& a) {
  EXPECT_TRUE(a.type() == TypedValue::Type::Bool && a.Value<bool>());
}

void EXPECT_PROP_EQ(const TypedValue& a, const TypedValue& b) {
  EXPECT_PROP_TRUE(a == b);
}

void EXPECT_PROP_ISNULL(const TypedValue& a) {
  EXPECT_TRUE(a.type() == TypedValue::Type::Null);
}

void EXPECT_PROP_NE(const TypedValue& a, const TypedValue& b) {
  EXPECT_PROP_TRUE(a != b);
}

TEST(TypedValue, CreationTypes) {
  EXPECT_TRUE(TypedValue::Null.type() == TypedValue::Type::Null);

  EXPECT_TRUE(TypedValue(true).type() == TypedValue::Type::Bool);
  EXPECT_TRUE(TypedValue(false).type() == TypedValue::Type::Bool);

  EXPECT_TRUE(TypedValue(std::string("form string class")).type() ==
              TypedValue::Type::String);
  EXPECT_TRUE(TypedValue("form c-string").type() == TypedValue::Type::String);

  EXPECT_TRUE(TypedValue(0).type() == TypedValue::Type::Int);
  EXPECT_TRUE(TypedValue(42).type() == TypedValue::Type::Int);

  EXPECT_TRUE(TypedValue(0.0).type() == TypedValue::Type::Double);
  EXPECT_TRUE(TypedValue(42.5).type() == TypedValue::Type::Double);
}

TEST(TypedValue, CreationValues) {
  EXPECT_EQ(TypedValue(true).Value<bool>(), true);
  EXPECT_EQ(TypedValue(false).Value<bool>(), false);

  EXPECT_EQ(TypedValue(std::string("bla")).Value<std::string>(), "bla");
  EXPECT_EQ(TypedValue("bla2").Value<std::string>(), "bla2");

  EXPECT_EQ(TypedValue(55).Value<int64_t>(), 55);

  EXPECT_FLOAT_EQ(TypedValue(66.6).Value<double>(), 66.6);
}

TEST(TypedValue, Equals) {
  EXPECT_PROP_EQ(TypedValue(true), TypedValue(true));
  EXPECT_PROP_NE(TypedValue(true), TypedValue(false));

  EXPECT_PROP_EQ(TypedValue(42), TypedValue(42));
  EXPECT_PROP_NE(TypedValue(0), TypedValue(1));

  EXPECT_PROP_NE(TypedValue(0.5), TypedValue(0.12));
  EXPECT_PROP_EQ(TypedValue(0.123), TypedValue(0.123));

  EXPECT_PROP_EQ(TypedValue(2), TypedValue(2.0));
  EXPECT_PROP_NE(TypedValue(2), TypedValue(2.1));

  EXPECT_PROP_NE(TypedValue("str1"), TypedValue("str2"));
  EXPECT_PROP_EQ(TypedValue("str3"), TypedValue("str3"));
  EXPECT_PROP_EQ(TypedValue(std::string("str3")), TypedValue("str3"));

  EXPECT_PROP_NE(TypedValue(std::vector<TypedValue>{1}), TypedValue(1));
  EXPECT_PROP_EQ(TypedValue(std::vector<TypedValue>{1, true, "a"}),
                 TypedValue(std::vector<TypedValue>{1, true, "a"}));
}

TEST(TypedValue, Less) {
  // not_bool_type < bool -> exception
  auto props = MakePropsAllTypes();
  for (int i = 0; i < (int)props.size(); ++i) {
    if (props.at(i).type() == TypedValue::Type::Bool) continue;
    // the comparison should raise an exception
    // cast to (void) so the compiler does not complain about unused comparison
    // result
    EXPECT_THROW((void)(props.at(i) < TypedValue(true)), TypedValueException);
  }

  // not_bool_type < Null = Null
  props = MakePropsAllTypes();
  for (int i = 0; i < (int)props.size(); ++i) {
    if (props.at(i).type() == TypedValue::Type::Bool) continue;
    EXPECT_PROP_ISNULL(props.at(i) < TypedValue::Null);
  }

  // int tests
  EXPECT_PROP_TRUE(TypedValue(2) < TypedValue(3));
  EXPECT_PROP_FALSE(TypedValue(2) < TypedValue(2));
  EXPECT_PROP_FALSE(TypedValue(3) < TypedValue(2));

  // double tests
  EXPECT_PROP_TRUE(TypedValue(2.1) < TypedValue(2.5));
  EXPECT_PROP_FALSE(TypedValue(2.0) < TypedValue(2.0));
  EXPECT_PROP_FALSE(TypedValue(2.5) < TypedValue(2.4));

  // implicit casting int->double
  EXPECT_PROP_TRUE(TypedValue(2) < TypedValue(2.1));
  EXPECT_PROP_FALSE(TypedValue(2.1) < TypedValue(2));
  EXPECT_PROP_FALSE(TypedValue(2) < TypedValue(1.5));
  EXPECT_PROP_TRUE(TypedValue(1.5) < TypedValue(2));

  // string tests
  EXPECT_PROP_TRUE(TypedValue("a") < TypedValue("b"));
  EXPECT_PROP_TRUE(TypedValue("aaaaa") < TypedValue("b"));
  EXPECT_PROP_TRUE(TypedValue("A") < TypedValue("a"));
}

TEST(TypedValue, LogicalNot) {
  EXPECT_PROP_EQ(!TypedValue(true), TypedValue(false));
  EXPECT_PROP_ISNULL(!TypedValue::Null);
  EXPECT_THROW(!TypedValue(0), TypedValueException);
  EXPECT_THROW(!TypedValue(0.2), TypedValueException);
  EXPECT_THROW(!TypedValue("something"), TypedValueException);
}

TEST(TypedValue, UnaryMinus) {
  EXPECT_TRUE((-TypedValue::Null).type() == TypedValue::Type::Null);

  EXPECT_PROP_EQ((-TypedValue(2).Value<int64_t>()), -2);
  EXPECT_FLOAT_EQ((-TypedValue(2.0).Value<double>()), -2.0);

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
void ExpectArithmeticThrowsAndNull(
    bool string_list_ok,
    std::function<TypedValue(const TypedValue&, const TypedValue&)> op) {
  // arithmetic ops that raise
  auto props = MakePropsAllTypes();
  for (int i = 0; i < (int)props.size(); ++i) {
    if (!string_list_ok || props.at(i).type() == TypedValue::Type::String) {
      EXPECT_THROW(op(TypedValue(true), props.at(i)), TypedValueException);
      EXPECT_THROW(op(props.at(i), TypedValue(true)), TypedValueException);
    }
    if (!string_list_ok) {
      EXPECT_THROW(op(TypedValue("some"), props.at(i)), TypedValueException);
      EXPECT_THROW(op(props.at(i), TypedValue("some")), TypedValueException);
      EXPECT_THROW(op(TypedValue("[1]"), props.at(i)), TypedValueException);
      EXPECT_THROW(op(props.at(i), TypedValue("[]")), TypedValueException);
    }
  }

  // null resulting ops
  props = MakePropsAllTypes();
  for (int i = 0; i < (int)props.size(); ++i) {
    if (props.at(i).type() == TypedValue::Type::Bool) continue;
    if (!string_list_ok && (props.at(i).type() == TypedValue::Type::String ||
                            props.at(i).type() == TypedValue::Type::List))
      continue;

    EXPECT_PROP_ISNULL(op(props.at(i), TypedValue::Null));
    EXPECT_PROP_ISNULL(op(TypedValue::Null, props.at(i)));
  }
}

TEST(TypedValue, Sum) {
  ExpectArithmeticThrowsAndNull(
      true, [](const TypedValue& a, const TypedValue& b) { return a + b; });

  // sum of props of the same type
  EXPECT_EQ((TypedValue(2) + TypedValue(3)).Value<int64_t>(), 5);
  EXPECT_FLOAT_EQ((TypedValue(2.5) + TypedValue(1.25)).Value<double>(), 3.75);
  EXPECT_EQ((TypedValue("one") + TypedValue("two")).Value<std::string>(),
            "onetwo");

  // sum of string and numbers
  EXPECT_EQ((TypedValue("one") + TypedValue(1)).Value<std::string>(), "one1");
  EXPECT_EQ((TypedValue(1) + TypedValue("one")).Value<std::string>(), "1one");
  EXPECT_EQ((TypedValue("one") + TypedValue(3.2)).Value<std::string>(),
            "one3.2");
  EXPECT_EQ((TypedValue(3.2) + TypedValue("one")).Value<std::string>(),
            "3.2one");
  std::vector<TypedValue> in = {1, 2, true, "a"};
  std::vector<TypedValue> out1 = {2, 1, 2, true, "a"};
  std::vector<TypedValue> out2 = {1, 2, true, "a", 2};
  std::vector<TypedValue> out3 = {1, 2, true, "a", 1, 2, true, "a"};
  EXPECT_PROP_EQ(
      (TypedValue(2) + TypedValue(in)).Value<std::vector<TypedValue>>(), out1);
  EXPECT_PROP_EQ(
      (TypedValue(in) + TypedValue(2)).Value<std::vector<TypedValue>>(), out2);
  EXPECT_PROP_EQ(
      (TypedValue(in) + TypedValue(in)).Value<std::vector<TypedValue>>(), out3);
}

TEST(TypedValue, Difference) {
  ExpectArithmeticThrowsAndNull(
      false, [](const TypedValue& a, const TypedValue& b) { return a - b; });

  // difference of props of the same type
  EXPECT_EQ((TypedValue(2) - TypedValue(3)).Value<int64_t>(), -1);
  EXPECT_FLOAT_EQ((TypedValue(2.5) - TypedValue(2.25)).Value<double>(), 0.25);

  // implicit casting
  EXPECT_FLOAT_EQ((TypedValue(2) - TypedValue(0.5)).Value<double>(), 1.5);
  EXPECT_FLOAT_EQ((TypedValue(2.5) - TypedValue(2)).Value<double>(), 0.5);
}

TEST(TypedValue, Divison) {
  ExpectArithmeticThrowsAndNull(
      false, [](const TypedValue& a, const TypedValue& b) { return a / b; });

  EXPECT_PROP_EQ(TypedValue(10) / TypedValue(2), TypedValue(5));
  EXPECT_PROP_EQ(TypedValue(10) / TypedValue(4), TypedValue(2));

  EXPECT_PROP_EQ(TypedValue(10.0) / TypedValue(2.0), TypedValue(5.0));
  EXPECT_FLOAT_EQ((TypedValue(10.0) / TypedValue(4.0)).Value<double>(), 2.5);

  EXPECT_FLOAT_EQ((TypedValue(10) / TypedValue(4.0)).Value<double>(), 2.5);
  EXPECT_FLOAT_EQ((TypedValue(10.0) / TypedValue(4)).Value<double>(), 2.5);
}

TEST(TypedValue, Multiplication) {
  ExpectArithmeticThrowsAndNull(
      false, [](const TypedValue& a, const TypedValue& b) { return a * b; });

  EXPECT_PROP_EQ(TypedValue(10) * TypedValue(2), TypedValue(20));
  EXPECT_FLOAT_EQ((TypedValue(12.5) * TypedValue(6.6)).Value<double>(),
                  12.5 * 6.6);
  EXPECT_FLOAT_EQ((TypedValue(10) * TypedValue(4.5)).Value<double>(), 10 * 4.5);
  EXPECT_FLOAT_EQ((TypedValue(10.2) * TypedValue(4)).Value<double>(), 10.2 * 4);
}

TEST(TypedValue, Modulo) {
  ExpectArithmeticThrowsAndNull(
      false, [](const TypedValue& a, const TypedValue& b) { return a % b; });

  EXPECT_PROP_EQ(TypedValue(10) % TypedValue(2), TypedValue(0));
  EXPECT_PROP_EQ(TypedValue(10) % TypedValue(4), TypedValue(2));

  EXPECT_PROP_EQ(TypedValue(10.0) % TypedValue(2.0), TypedValue(0.0));
  EXPECT_FLOAT_EQ((TypedValue(10.0) % TypedValue(3.25)).Value<double>(), 0.25);

  EXPECT_FLOAT_EQ((TypedValue(10) % TypedValue(4.0)).Value<double>(), 2.0);
  EXPECT_FLOAT_EQ((TypedValue(10.0) % TypedValue(4)).Value<double>(), 2.0);
}

TEST(TypedValue, TypeIncompatibility) {
  auto props = MakePropsAllTypes();

  // iterate over all the props, plus one, what will return
  // the Null property, which must be incompatible with all
  // the others
  for (int i = 0; i < (int)props.size(); ++i)
    for (int j = 0; j < (int)props.size(); ++j)
      EXPECT_EQ(props.at(i).type() == props.at(j).type(), i == j);
}

/**
 * Logical operations (logical and, or) are only legal on bools
 * and nulls. This function ensures that the given
 * logical operation throws exceptions when either operand
 * is not bool or null.
 *
 * @param op The logical operation to test.
 */
void TestLogicalThrows(
    std::function<TypedValue(const TypedValue&, const TypedValue&)> op) {
  auto props = MakePropsAllTypes();
  for (int i = 0; i < (int)props.size(); ++i) {
    auto p1 = props.at(i);
    for (int j = 0; j < (int)props.size(); ++j) {
      auto p2 = props.at(j);
      // skip situations when both p1 and p2 are either bool or null
      auto p1_ok = p1.type() == TypedValue::Type::Bool ||
                   p1.type() == TypedValue::Type::Null;
      auto p2_ok = p2.type() == TypedValue::Type::Bool ||
                   p2.type() == TypedValue::Type::Null;
      if (p1_ok && p2_ok) continue;

      EXPECT_THROW(op(p1, p2), TypedValueException);
    }
  }
}

TEST(TypedValue, LogicalAnd) {
  TestLogicalThrows(
      [](const TypedValue& p1, const TypedValue& p2) { return p1 && p2; });
  EXPECT_PROP_ISNULL(TypedValue::Null && TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(true) && TypedValue(true), TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(false) && TypedValue(true), TypedValue(false));
}

TEST(TypedValue, LogicalOr) {
  TestLogicalThrows(
      [](const TypedValue& p1, const TypedValue& p2) { return p1 || p2; });
  EXPECT_PROP_ISNULL(TypedValue::Null && TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(true) || TypedValue(true), TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(false) || TypedValue(true), TypedValue(true));
}

TEST(TypedValue, LogicalXor) {
  TestLogicalThrows(
      [](const TypedValue& p1, const TypedValue& p2) { return p1 ^ p2; });
  EXPECT_PROP_ISNULL(TypedValue::Null && TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(true) ^ TypedValue(true), TypedValue(false));
  EXPECT_PROP_EQ(TypedValue(false) ^ TypedValue(true), TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(true) ^ TypedValue(false), TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(false) ^ TypedValue(false), TypedValue(false));
}
