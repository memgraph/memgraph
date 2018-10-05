//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 24.01.17..
//
#include <functional>
#include <map>
#include <set>
#include <vector>

#include "gtest/gtest.h"

#include "database/single_node/graph_db_accessor.hpp"
#include "query/typed_value.hpp"

using query::TypedValue;
using query::TypedValueException;

class AllTypesFixture : public testing::Test {
 protected:
  std::vector<TypedValue> values_;
  database::SingleNode db_;
  std::unique_ptr<database::GraphDbAccessor> dba_{db_.Access()};

  void SetUp() override {
    values_.emplace_back(TypedValue::Null);
    values_.emplace_back(true);
    values_.emplace_back(42);
    values_.emplace_back(3.14);
    values_.emplace_back("something");
    values_.emplace_back(
        std::vector<TypedValue>{true, "something", 42, 0.5, TypedValue::Null});
    values_.emplace_back(
        std::map<std::string, TypedValue>{{"a", true},
                                          {"b", "something"},
                                          {"c", 42},
                                          {"d", 0.5},
                                          {"e", TypedValue::Null}});
    auto vertex = dba_->InsertVertex();
    values_.emplace_back(vertex);
    values_.emplace_back(
        dba_->InsertEdge(vertex, vertex, dba_->EdgeType("et")));
    values_.emplace_back(query::Path(dba_->InsertVertex()));
  }
};

void EXPECT_PROP_FALSE(const TypedValue &a) {
  EXPECT_TRUE(a.type() == TypedValue::Type::Bool && !a.Value<bool>());
}

void EXPECT_PROP_TRUE(const TypedValue &a) {
  EXPECT_TRUE(a.type() == TypedValue::Type::Bool && a.Value<bool>());
}

void EXPECT_PROP_EQ(const TypedValue &a, const TypedValue &b) {
  EXPECT_PROP_TRUE(a == b);
}

void EXPECT_PROP_ISNULL(const TypedValue &a) { EXPECT_TRUE(a.IsNull()); }

void EXPECT_PROP_NE(const TypedValue &a, const TypedValue &b) {
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

  // compare two ints close to 2 ^ 62
  // this will fail if they are converted to float at any point
  EXPECT_PROP_NE(TypedValue(4611686018427387905),
                 TypedValue(4611686018427387900));

  EXPECT_PROP_NE(TypedValue(0.5), TypedValue(0.12));
  EXPECT_PROP_EQ(TypedValue(0.123), TypedValue(0.123));

  EXPECT_PROP_EQ(TypedValue(2), TypedValue(2.0));
  EXPECT_PROP_NE(TypedValue(2), TypedValue(2.1));

  EXPECT_PROP_NE(TypedValue("str1"), TypedValue("str2"));
  EXPECT_PROP_EQ(TypedValue("str3"), TypedValue("str3"));
  EXPECT_PROP_EQ(TypedValue(std::string("str3")), TypedValue("str3"));

  EXPECT_PROP_NE(TypedValue(std::vector<TypedValue>{1}), TypedValue(1));
  EXPECT_PROP_NE(TypedValue(std::vector<TypedValue>{1, true, "a"}),
                 TypedValue(std::vector<TypedValue>{1, true, "b"}));
  EXPECT_PROP_EQ(TypedValue(std::vector<TypedValue>{1, true, "a"}),
                 TypedValue(std::vector<TypedValue>{1, true, "a"}));

  EXPECT_PROP_EQ(TypedValue(std::map<std::string, TypedValue>{{"a", 1}}),
                 TypedValue(std::map<std::string, TypedValue>{{"a", 1}}));
  EXPECT_PROP_NE(TypedValue(std::map<std::string, TypedValue>{{"a", 1}}),
                 TypedValue(1));
  EXPECT_PROP_NE(TypedValue(std::map<std::string, TypedValue>{{"a", 1}}),
                 TypedValue(std::map<std::string, TypedValue>{{"b", 1}}));
  EXPECT_PROP_NE(TypedValue(std::map<std::string, TypedValue>{{"a", 1}}),
                 TypedValue(std::map<std::string, TypedValue>{{"a", 2}}));
  EXPECT_PROP_NE(
      TypedValue(std::map<std::string, TypedValue>{{"a", 1}}),
      TypedValue(std::map<std::string, TypedValue>{{"a", 1}, {"b", 1}}));
}

TEST(TypedValue, BoolEquals) {
  auto eq = TypedValue::BoolEqual{};
  EXPECT_TRUE(eq(TypedValue(1), TypedValue(1)));
  EXPECT_FALSE(eq(TypedValue(1), TypedValue(2)));
  EXPECT_FALSE(eq(TypedValue(1), TypedValue("asd")));
  EXPECT_FALSE(eq(TypedValue(1), TypedValue::Null));
  EXPECT_TRUE(eq(TypedValue::Null, TypedValue::Null));
}

TEST(TypedValue, Hash) {
  auto hash = TypedValue::Hash{};

  EXPECT_EQ(hash(TypedValue(1)), hash(TypedValue(1)));
  EXPECT_EQ(hash(TypedValue(1)), hash(TypedValue(1.0)));
  EXPECT_EQ(hash(TypedValue(1.5)), hash(TypedValue(1.5)));
  EXPECT_EQ(hash(TypedValue::Null), hash(TypedValue::Null));
  EXPECT_EQ(hash(TypedValue("bla")), hash(TypedValue("bla")));
  EXPECT_EQ(hash(TypedValue(std::vector<TypedValue>{1, 2})),
            hash(TypedValue(std::vector<TypedValue>{1, 2})));
  EXPECT_EQ(hash(TypedValue(std::map<std::string, TypedValue>{{"a", 1}})),
            hash(TypedValue(std::map<std::string, TypedValue>{{"a", 1}})));

  // these tests are not really true since they expect
  // hashes to differ, but it's the thought that counts
  EXPECT_NE(hash(TypedValue(1)), hash(TypedValue(42)));
  EXPECT_NE(hash(TypedValue(1.5)), hash(TypedValue(2.5)));
  EXPECT_NE(hash(TypedValue("bla")), hash(TypedValue("johnny")));
  EXPECT_NE(hash(TypedValue(std::vector<TypedValue>{1, 1})),
            hash(TypedValue(std::vector<TypedValue>{1, 2})));
  EXPECT_NE(hash(TypedValue(std::map<std::string, TypedValue>{{"b", 1}})),
            hash(TypedValue(std::map<std::string, TypedValue>{{"a", 1}})));
}

TEST_F(AllTypesFixture, Less) {
  // 'Less' is legal only between numerics, Null and strings.
  auto is_string_compatible = [](const TypedValue &v) {
    return v.IsNull() || v.type() == TypedValue::Type::String;
  };
  auto is_numeric_compatible = [](const TypedValue &v) {
    return v.IsNull() || v.IsNumeric();
  };
  for (TypedValue &a : values_) {
    for (TypedValue &b : values_) {
      if (is_numeric_compatible(a) && is_numeric_compatible(b)) continue;
      if (is_string_compatible(a) && is_string_compatible(b)) continue;
      // Comparison should raise an exception. Cast to (void) so the compiler
      // does not complain about unused comparison result.
      EXPECT_THROW((void)(a < b), TypedValueException);
    }
  }

  // legal_type < Null = Null
  for (TypedValue &value : values_) {
    if (!(value.IsNumeric() || value.type() == TypedValue::Type::String))
      continue;
    EXPECT_PROP_ISNULL(value < TypedValue::Null);
    EXPECT_PROP_ISNULL(TypedValue::Null < value);
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

TEST(TypedValue, UnaryPlus) {
  EXPECT_TRUE((+TypedValue::Null).type() == TypedValue::Type::Null);

  EXPECT_PROP_EQ((+TypedValue(2).Value<int64_t>()), 2);
  EXPECT_FLOAT_EQ((+TypedValue(2.0).Value<double>()), 2.0);

  EXPECT_THROW(+TypedValue(true), TypedValueException);
  EXPECT_THROW(+TypedValue("something"), TypedValueException);
}

class TypedValueArithmeticTest : public AllTypesFixture {
 protected:
  /**
   * Performs a series of tests on properties of all types. The tests
   * evaluate how arithmetic operators behave w.r.t. exception throwing
   * in case of invalid operands and null handling.
   *
   * @param string_list_ok Indicates if or not the operation tested works
   *  with String and List values (does not throw).
   * @param op  The operation lambda. Takes two values and resturns
   *  the results.
   */
  void ExpectArithmeticThrowsAndNull(
      bool string_list_ok,
      std::function<TypedValue(const TypedValue &, const TypedValue &)> op) {
    // If one operand is always valid, the other can be of any type.
    auto always_valid = [string_list_ok](const TypedValue &value) {
      return value.IsNull() ||
             (string_list_ok && value.type() == TypedValue::Type::List);
    };

    // If we don't have an always_valid operand, they both must be plain valid.
    auto valid = [string_list_ok](const TypedValue &value) {
      switch (value.type()) {
        case TypedValue::Type::Int:
        case TypedValue::Type::Double:
          return true;
        case TypedValue::Type::String:
          return string_list_ok;
        default:
          return false;
      }
    };

    for (const TypedValue &a : values_) {
      for (const TypedValue &b : values_) {
        if (always_valid(a) || always_valid(b)) continue;
        if (valid(a) && valid(b)) continue;
        EXPECT_THROW(op(a, b), TypedValueException);
        EXPECT_THROW(op(b, a), TypedValueException);
      }
    }

    // null resulting ops
    for (const TypedValue &value : values_) {
      EXPECT_PROP_ISNULL(op(value, TypedValue::Null));
      EXPECT_PROP_ISNULL(op(TypedValue::Null, value));
    }
  }
};

TEST_F(TypedValueArithmeticTest, Sum) {
  ExpectArithmeticThrowsAndNull(
      true, [](const TypedValue &a, const TypedValue &b) { return a + b; });

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

TEST_F(TypedValueArithmeticTest, Difference) {
  ExpectArithmeticThrowsAndNull(
      false, [](const TypedValue &a, const TypedValue &b) { return a - b; });

  // difference of props of the same type
  EXPECT_EQ((TypedValue(2) - TypedValue(3)).Value<int64_t>(), -1);
  EXPECT_FLOAT_EQ((TypedValue(2.5) - TypedValue(2.25)).Value<double>(), 0.25);

  // implicit casting
  EXPECT_FLOAT_EQ((TypedValue(2) - TypedValue(0.5)).Value<double>(), 1.5);
  EXPECT_FLOAT_EQ((TypedValue(2.5) - TypedValue(2)).Value<double>(), 0.5);
}

TEST_F(TypedValueArithmeticTest, Divison) {
  ExpectArithmeticThrowsAndNull(
      false, [](const TypedValue &a, const TypedValue &b) { return a / b; });
  EXPECT_THROW(TypedValue(1) / TypedValue(0), TypedValueException);

  EXPECT_PROP_EQ(TypedValue(10) / TypedValue(2), TypedValue(5));
  EXPECT_PROP_EQ(TypedValue(10) / TypedValue(4), TypedValue(2));

  EXPECT_PROP_EQ(TypedValue(10.0) / TypedValue(2.0), TypedValue(5.0));
  EXPECT_FLOAT_EQ((TypedValue(10.0) / TypedValue(4.0)).Value<double>(), 2.5);

  EXPECT_FLOAT_EQ((TypedValue(10) / TypedValue(4.0)).Value<double>(), 2.5);
  EXPECT_FLOAT_EQ((TypedValue(10.0) / TypedValue(4)).Value<double>(), 2.5);
}

TEST_F(TypedValueArithmeticTest, Multiplication) {
  ExpectArithmeticThrowsAndNull(
      false, [](const TypedValue &a, const TypedValue &b) { return a * b; });

  EXPECT_PROP_EQ(TypedValue(10) * TypedValue(2), TypedValue(20));
  EXPECT_FLOAT_EQ((TypedValue(12.5) * TypedValue(6.6)).Value<double>(),
                  12.5 * 6.6);
  EXPECT_FLOAT_EQ((TypedValue(10) * TypedValue(4.5)).Value<double>(), 10 * 4.5);
  EXPECT_FLOAT_EQ((TypedValue(10.2) * TypedValue(4)).Value<double>(), 10.2 * 4);
}

TEST_F(TypedValueArithmeticTest, Modulo) {
  ExpectArithmeticThrowsAndNull(
      false, [](const TypedValue &a, const TypedValue &b) { return a % b; });
  EXPECT_THROW(TypedValue(1) % TypedValue(0), TypedValueException);

  EXPECT_PROP_EQ(TypedValue(10) % TypedValue(2), TypedValue(0));
  EXPECT_PROP_EQ(TypedValue(10) % TypedValue(4), TypedValue(2));

  EXPECT_PROP_EQ(TypedValue(10.0) % TypedValue(2.0), TypedValue(0.0));
  EXPECT_FLOAT_EQ((TypedValue(10.0) % TypedValue(3.25)).Value<double>(), 0.25);

  EXPECT_FLOAT_EQ((TypedValue(10) % TypedValue(4.0)).Value<double>(), 2.0);
  EXPECT_FLOAT_EQ((TypedValue(10.0) % TypedValue(4)).Value<double>(), 2.0);
}

class TypedValueLogicTest : public AllTypesFixture {
 protected:
  /**
   * Logical operations (logical and, or) are only legal on bools
   * and nulls. This function ensures that the given
   * logical operation throws exceptions when either operand
   * is not bool or null.
   *
   * @param op The logical operation to test.
   */
  void TestLogicalThrows(
      std::function<TypedValue(const TypedValue &, const TypedValue &)> op) {
    for (const auto &p1 : values_) {
      for (const auto &p2 : values_) {
        // skip situations when both p1 and p2 are either bool or null
        auto p1_ok = p1.type() == TypedValue::Type::Bool || p1.IsNull();
        auto p2_ok = p2.type() == TypedValue::Type::Bool || p2.IsNull();
        if (p1_ok && p2_ok) continue;

        EXPECT_THROW(op(p1, p2), TypedValueException);
      }
    }
  }
};

TEST_F(TypedValueLogicTest, LogicalAnd) {
  TestLogicalThrows(
      [](const TypedValue &p1, const TypedValue &p2) { return p1 && p2; });

  EXPECT_PROP_ISNULL(TypedValue::Null && TypedValue(true));
  EXPECT_PROP_EQ(TypedValue::Null && TypedValue(false), TypedValue(false));
  EXPECT_PROP_EQ(TypedValue(true) && TypedValue(true), TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(false) && TypedValue(true), TypedValue(false));
}

TEST_F(TypedValueLogicTest, LogicalOr) {
  TestLogicalThrows(
      [](const TypedValue &p1, const TypedValue &p2) { return p1 || p2; });

  EXPECT_PROP_ISNULL(TypedValue::Null || TypedValue(false));
  EXPECT_PROP_EQ(TypedValue::Null || TypedValue(true), TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(true) || TypedValue(true), TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(false) || TypedValue(true), TypedValue(true));
}

TEST_F(TypedValueLogicTest, LogicalXor) {
  TestLogicalThrows(
      [](const TypedValue &p1, const TypedValue &p2) { return p1 ^ p2; });

  EXPECT_PROP_ISNULL(TypedValue::Null && TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(true) ^ TypedValue(true), TypedValue(false));
  EXPECT_PROP_EQ(TypedValue(false) ^ TypedValue(true), TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(true) ^ TypedValue(false), TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(false) ^ TypedValue(false), TypedValue(false));
}
