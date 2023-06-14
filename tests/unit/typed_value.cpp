// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 24.01.17..
//
#include <functional>
#include <map>
#include <set>
#include <vector>

#include "gtest/gtest.h"

#include "query/graph.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/storage.hpp"

using memgraph::query::TypedValue;
using memgraph::query::TypedValueException;

class AllTypesFixture : public testing::Test {
 protected:
  std::vector<TypedValue> values_;
  memgraph::storage::Storage db;
  memgraph::storage::Storage::Accessor storage_dba{db.Access()};
  memgraph::query::DbAccessor dba{&storage_dba};

  void SetUp() override {
    values_.emplace_back(TypedValue());
    values_.emplace_back(true);
    values_.emplace_back(42);
    values_.emplace_back(3.14);
    values_.emplace_back("something");
    values_.emplace_back(std::vector<TypedValue>{TypedValue(true), TypedValue("something"), TypedValue(42),
                                                 TypedValue(0.5), TypedValue()});
    values_.emplace_back(std::map<std::string, TypedValue>{{"a", TypedValue(true)},
                                                           {"b", TypedValue("something")},
                                                           {"c", TypedValue(42)},
                                                           {"d", TypedValue(0.5)},
                                                           {"e", TypedValue()}});
    auto vertex = dba.InsertVertex();
    values_.emplace_back(vertex);
    auto edge = *dba.InsertEdge(&vertex, &vertex, dba.NameToEdgeType("et"));
    values_.emplace_back(edge);
    values_.emplace_back(memgraph::query::Path(dba.InsertVertex()));
    memgraph::query::Graph graph{memgraph::utils::NewDeleteResource()};
    graph.InsertVertex(vertex);
    graph.InsertEdge(edge);
    values_.emplace_back(std::move(graph));
  }
};

void EXPECT_PROP_FALSE(const TypedValue &a) { EXPECT_TRUE(a.type() == TypedValue::Type::Bool && !a.ValueBool()); }

void EXPECT_PROP_TRUE(const TypedValue &a) { EXPECT_TRUE(a.type() == TypedValue::Type::Bool && a.ValueBool()); }

void EXPECT_PROP_EQ(const TypedValue &a, const TypedValue &b) { EXPECT_PROP_TRUE(a == b); }

void EXPECT_PROP_ISNULL(const TypedValue &a) { EXPECT_TRUE(a.IsNull()); }

void EXPECT_PROP_NE(const TypedValue &a, const TypedValue &b) { EXPECT_PROP_TRUE(a != b); }

TEST(TypedValue, CreationTypes) {
  EXPECT_TRUE(TypedValue().type() == TypedValue::Type::Null);

  EXPECT_TRUE(TypedValue(true).type() == TypedValue::Type::Bool);
  EXPECT_TRUE(TypedValue(false).type() == TypedValue::Type::Bool);

  EXPECT_TRUE(TypedValue(std::string("form string class")).type() == TypedValue::Type::String);
  EXPECT_TRUE(TypedValue("form c-string").type() == TypedValue::Type::String);

  EXPECT_TRUE(TypedValue(0).type() == TypedValue::Type::Int);
  EXPECT_TRUE(TypedValue(42).type() == TypedValue::Type::Int);

  EXPECT_TRUE(TypedValue(0.0).type() == TypedValue::Type::Double);
  EXPECT_TRUE(TypedValue(42.5).type() == TypedValue::Type::Double);
}

TEST(TypedValue, CreationValues) {
  EXPECT_EQ(TypedValue(true).ValueBool(), true);
  EXPECT_EQ(TypedValue(false).ValueBool(), false);

  EXPECT_EQ(TypedValue(std::string("bla")).ValueString(), "bla");
  EXPECT_EQ(TypedValue("bla2").ValueString(), "bla2");

  EXPECT_EQ(TypedValue(55).ValueInt(), 55);

  EXPECT_FLOAT_EQ(TypedValue(66.6).ValueDouble(), 66.6);
}

TEST(TypedValue, Equals) {
  EXPECT_PROP_EQ(TypedValue(true), TypedValue(true));
  EXPECT_PROP_NE(TypedValue(true), TypedValue(false));

  EXPECT_PROP_EQ(TypedValue(42), TypedValue(42));
  EXPECT_PROP_NE(TypedValue(0), TypedValue(1));

  // compare two ints close to 2 ^ 62
  // this will fail if they are converted to float at any point
  EXPECT_PROP_NE(TypedValue(4611686018427387905), TypedValue(4611686018427387900));

  EXPECT_PROP_NE(TypedValue(0.5), TypedValue(0.12));
  EXPECT_PROP_EQ(TypedValue(0.123), TypedValue(0.123));

  EXPECT_PROP_EQ(TypedValue(2), TypedValue(2.0));
  EXPECT_PROP_NE(TypedValue(2), TypedValue(2.1));

  EXPECT_PROP_NE(TypedValue("str1"), TypedValue("str2"));
  EXPECT_PROP_EQ(TypedValue("str3"), TypedValue("str3"));
  EXPECT_PROP_EQ(TypedValue(std::string("str3")), TypedValue("str3"));

  EXPECT_PROP_NE(TypedValue(std::vector<TypedValue>{TypedValue(1)}), TypedValue(1));
  EXPECT_PROP_NE(TypedValue(std::vector<TypedValue>{TypedValue(1), TypedValue(true), TypedValue("a")}),
                 TypedValue(std::vector<TypedValue>{TypedValue(1), TypedValue(true), TypedValue("b")}));
  EXPECT_PROP_EQ(TypedValue(std::vector<TypedValue>{TypedValue(1), TypedValue(true), TypedValue("a")}),
                 TypedValue(std::vector<TypedValue>{TypedValue(1), TypedValue(true), TypedValue("a")}));

  EXPECT_PROP_EQ(TypedValue(std::map<std::string, TypedValue>{{"a", TypedValue(1)}}),
                 TypedValue(std::map<std::string, TypedValue>{{"a", TypedValue(1)}}));
  EXPECT_PROP_NE(TypedValue(std::map<std::string, TypedValue>{{"a", TypedValue(1)}}), TypedValue(1));
  EXPECT_PROP_NE(TypedValue(std::map<std::string, TypedValue>{{"a", TypedValue(1)}}),
                 TypedValue(std::map<std::string, TypedValue>{{"b", TypedValue(1)}}));
  EXPECT_PROP_NE(TypedValue(std::map<std::string, TypedValue>{{"a", TypedValue(1)}}),
                 TypedValue(std::map<std::string, TypedValue>{{"a", TypedValue(2)}}));
  EXPECT_PROP_NE(TypedValue(std::map<std::string, TypedValue>{{"a", TypedValue(1)}}),
                 TypedValue(std::map<std::string, TypedValue>{{"a", TypedValue(1)}, {"b", TypedValue(1)}}));
}

TEST(TypedValue, BoolEquals) {
  auto eq = TypedValue::BoolEqual{};
  EXPECT_TRUE(eq(TypedValue(1), TypedValue(1)));
  EXPECT_FALSE(eq(TypedValue(1), TypedValue(2)));
  EXPECT_FALSE(eq(TypedValue(1), TypedValue("asd")));
  EXPECT_FALSE(eq(TypedValue(1), TypedValue()));
  EXPECT_TRUE(eq(TypedValue(), TypedValue()));
}

TEST(TypedValue, Hash) {
  auto hash = TypedValue::Hash{};

  EXPECT_EQ(hash(TypedValue(1)), hash(TypedValue(1)));
  EXPECT_EQ(hash(TypedValue(1)), hash(TypedValue(1.0)));
  EXPECT_EQ(hash(TypedValue(1.5)), hash(TypedValue(1.5)));
  EXPECT_EQ(hash(TypedValue()), hash(TypedValue()));
  EXPECT_EQ(hash(TypedValue("bla")), hash(TypedValue("bla")));
  EXPECT_EQ(hash(TypedValue(std::vector<TypedValue>{TypedValue(1), TypedValue(2)})),
            hash(TypedValue(std::vector<TypedValue>{TypedValue(1), TypedValue(2)})));
  EXPECT_EQ(hash(TypedValue(std::map<std::string, TypedValue>{{"a", TypedValue(1)}})),
            hash(TypedValue(std::map<std::string, TypedValue>{{"a", TypedValue(1)}})));

  // these tests are not really true since they expect
  // hashes to differ, but it's the thought that counts
  EXPECT_NE(hash(TypedValue(1)), hash(TypedValue(42)));
  EXPECT_NE(hash(TypedValue(1.5)), hash(TypedValue(2.5)));
  EXPECT_NE(hash(TypedValue("bla")), hash(TypedValue("johnny")));
  EXPECT_NE(hash(TypedValue(std::vector<TypedValue>{TypedValue(1), TypedValue(1)})),
            hash(TypedValue(std::vector<TypedValue>{TypedValue(1), TypedValue(2)})));
  EXPECT_NE(hash(TypedValue(std::map<std::string, TypedValue>{{"b", TypedValue(1)}})),
            hash(TypedValue(std::map<std::string, TypedValue>{{"a", TypedValue(1)}})));
}

TEST_F(AllTypesFixture, Less) {
  // 'Less' is legal only between numerics, Null and strings.
  auto is_string_compatible = [](const TypedValue &v) { return v.IsNull() || v.type() == TypedValue::Type::String; };
  auto is_numeric_compatible = [](const TypedValue &v) { return v.IsNull() || v.IsNumeric(); };
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
    if (!(value.IsNumeric() || value.type() == TypedValue::Type::String)) continue;
    EXPECT_PROP_ISNULL(value < TypedValue());
    EXPECT_PROP_ISNULL(TypedValue() < value);
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
  EXPECT_PROP_ISNULL(!TypedValue());
  EXPECT_THROW(!TypedValue(0), TypedValueException);
  EXPECT_THROW(!TypedValue(0.2), TypedValueException);
  EXPECT_THROW(!TypedValue("something"), TypedValueException);
}

TEST(TypedValue, UnaryMinus) {
  EXPECT_TRUE((-TypedValue()).type() == TypedValue::Type::Null);

  EXPECT_PROP_EQ(TypedValue(-TypedValue(2).ValueInt()), TypedValue(-2));
  EXPECT_FLOAT_EQ((-TypedValue(2.0).ValueDouble()), -2.0);

  EXPECT_THROW(-TypedValue(true), TypedValueException);
  EXPECT_THROW(-TypedValue("something"), TypedValueException);
}

TEST(TypedValue, UnaryPlus) {
  EXPECT_TRUE((+TypedValue()).type() == TypedValue::Type::Null);

  EXPECT_PROP_EQ(TypedValue(+TypedValue(2).ValueInt()), TypedValue(2));
  EXPECT_FLOAT_EQ((+TypedValue(2.0).ValueDouble()), 2.0);

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
   * @param op  The operation lambda. Takes two values and returns
   *  the results.
   */
  void ExpectArithmeticThrowsAndNull(bool string_list_ok,
                                     std::function<TypedValue(const TypedValue &, const TypedValue &)> op) {
    // If one operand is always valid, the other can be of any type.
    auto always_valid = [string_list_ok](const TypedValue &value) {
      return value.IsNull() || (string_list_ok && value.type() == TypedValue::Type::List);
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
      EXPECT_PROP_ISNULL(op(value, TypedValue()));
      EXPECT_PROP_ISNULL(op(TypedValue(), value));
    }
  }
};

TEST_F(TypedValueArithmeticTest, Sum) {
  ExpectArithmeticThrowsAndNull(true, [](const TypedValue &a, const TypedValue &b) { return a + b; });

  // sum of props of the same type
  EXPECT_EQ((TypedValue(2) + TypedValue(3)).ValueInt(), 5);
  EXPECT_FLOAT_EQ((TypedValue(2.5) + TypedValue(1.25)).ValueDouble(), 3.75);
  EXPECT_EQ((TypedValue("one") + TypedValue("two")).ValueString(), "onetwo");

  // sum of string and numbers
  EXPECT_EQ((TypedValue("one") + TypedValue(1)).ValueString(), "one1");
  EXPECT_EQ((TypedValue(1) + TypedValue("one")).ValueString(), "1one");
  EXPECT_EQ((TypedValue("one") + TypedValue(3.2)).ValueString(), "one3.2");
  EXPECT_EQ((TypedValue(3.2) + TypedValue("one")).ValueString(), "3.2one");
  std::vector<TypedValue> in{TypedValue(1), TypedValue(2), TypedValue(true), TypedValue("a")};
  std::vector<TypedValue> out1{TypedValue(2), TypedValue(1), TypedValue(2), TypedValue(true), TypedValue("a")};
  std::vector<TypedValue> out2{TypedValue(1), TypedValue(2), TypedValue(true), TypedValue("a"), TypedValue(2)};
  std::vector<TypedValue> out3{TypedValue(1), TypedValue(2), TypedValue(true), TypedValue("a"),
                               TypedValue(1), TypedValue(2), TypedValue(true), TypedValue("a")};
  EXPECT_PROP_EQ(TypedValue(2) + TypedValue(in), TypedValue(out1));
  EXPECT_PROP_EQ(TypedValue(in) + TypedValue(2), TypedValue(out2));
  EXPECT_PROP_EQ(TypedValue(in) + TypedValue(in), TypedValue(out3));

  // Temporal Types
  // Duration
  EXPECT_NO_THROW(TypedValue(memgraph::utils::Duration(1)) + TypedValue(memgraph::utils::Duration(1)));
  // Date
  EXPECT_NO_THROW(TypedValue(memgraph::utils::Date(1)) + TypedValue(memgraph::utils::Duration(1)));
  EXPECT_NO_THROW(TypedValue(memgraph::utils::Duration(1)) + TypedValue(memgraph::utils::Date(1)));
  EXPECT_THROW(TypedValue(memgraph::utils::Date(1)) + TypedValue(memgraph::utils::Date(1)), TypedValueException);
  // LocalTime
  EXPECT_NO_THROW(TypedValue(memgraph::utils::LocalTime(1)) + TypedValue(memgraph::utils::Duration(1)));
  EXPECT_NO_THROW(TypedValue(memgraph::utils::Duration(1)) + TypedValue(memgraph::utils::LocalTime(1)));
  EXPECT_THROW(TypedValue(memgraph::utils::LocalTime(1)) + TypedValue(memgraph::utils::LocalTime(1)),
               TypedValueException);
  // LocalDateTime
  EXPECT_NO_THROW(TypedValue(memgraph::utils::LocalDateTime(1)) + TypedValue(memgraph::utils::Duration(1)));
  EXPECT_NO_THROW(TypedValue(memgraph::utils::Duration(1)) + TypedValue(memgraph::utils::LocalDateTime(1)));
  EXPECT_THROW(TypedValue(memgraph::utils::LocalDateTime(1)) + TypedValue(memgraph::utils::LocalDateTime(1)),
               TypedValueException);
}

TEST_F(TypedValueArithmeticTest, Difference) {
  ExpectArithmeticThrowsAndNull(false, [](const TypedValue &a, const TypedValue &b) { return a - b; });

  // difference of props of the same type
  EXPECT_EQ((TypedValue(2) - TypedValue(3)).ValueInt(), -1);
  EXPECT_FLOAT_EQ((TypedValue(2.5) - TypedValue(2.25)).ValueDouble(), 0.25);

  // implicit casting
  EXPECT_FLOAT_EQ((TypedValue(2) - TypedValue(0.5)).ValueDouble(), 1.5);
  EXPECT_FLOAT_EQ((TypedValue(2.5) - TypedValue(2)).ValueDouble(), 0.5);
  // Temporal Types
  // Duration
  EXPECT_NO_THROW(TypedValue(memgraph::utils::Duration(1)) - TypedValue(memgraph::utils::Duration(1)));
  // Date
  EXPECT_NO_THROW(TypedValue(memgraph::utils::Date(1)) - TypedValue(memgraph::utils::Duration(1)));
  EXPECT_NO_THROW(TypedValue(memgraph::utils::Date(1)) - TypedValue(memgraph::utils::Date(1)));
  EXPECT_THROW(TypedValue(memgraph::utils::Duration(1)) - TypedValue(memgraph::utils::Date(1)), TypedValueException);
  // LocalTime
  EXPECT_NO_THROW(TypedValue(memgraph::utils::LocalTime(1)) - TypedValue(memgraph::utils::Duration(1)));
  EXPECT_NO_THROW(TypedValue(memgraph::utils::LocalTime(1)) - TypedValue(memgraph::utils::LocalTime(1)));
  EXPECT_THROW(TypedValue(memgraph::utils::Duration(1)) - TypedValue(memgraph::utils::LocalTime(1)),
               TypedValueException);
  // LocalDateTime
  EXPECT_NO_THROW(TypedValue(memgraph::utils::LocalDateTime(1)) - TypedValue(memgraph::utils::Duration(1)));
  EXPECT_NO_THROW(TypedValue(memgraph::utils::LocalDateTime(1)) - TypedValue(memgraph::utils::LocalDateTime(1)));
  EXPECT_THROW(TypedValue(memgraph::utils::Duration(1)) - TypedValue(memgraph::utils::LocalDateTime(1)),
               TypedValueException);
}

TEST_F(TypedValueArithmeticTest, Negate) { EXPECT_NO_THROW(-TypedValue(memgraph::utils::Duration(1))); }

TEST_F(TypedValueArithmeticTest, Division) {
  ExpectArithmeticThrowsAndNull(false, [](const TypedValue &a, const TypedValue &b) { return a / b; });
  EXPECT_THROW(TypedValue(1) / TypedValue(0), TypedValueException);

  EXPECT_PROP_EQ(TypedValue(10) / TypedValue(2), TypedValue(5));
  EXPECT_PROP_EQ(TypedValue(10) / TypedValue(4), TypedValue(2));

  EXPECT_PROP_EQ(TypedValue(10.0) / TypedValue(2.0), TypedValue(5.0));
  EXPECT_FLOAT_EQ((TypedValue(10.0) / TypedValue(4.0)).ValueDouble(), 2.5);

  EXPECT_FLOAT_EQ((TypedValue(10) / TypedValue(4.0)).ValueDouble(), 2.5);
  EXPECT_FLOAT_EQ((TypedValue(10.0) / TypedValue(4)).ValueDouble(), 2.5);
}

TEST_F(TypedValueArithmeticTest, Multiplication) {
  ExpectArithmeticThrowsAndNull(false, [](const TypedValue &a, const TypedValue &b) { return a * b; });

  EXPECT_PROP_EQ(TypedValue(10) * TypedValue(2), TypedValue(20));
  EXPECT_FLOAT_EQ((TypedValue(12.5) * TypedValue(6.6)).ValueDouble(), 12.5 * 6.6);
  EXPECT_FLOAT_EQ((TypedValue(10) * TypedValue(4.5)).ValueDouble(), 10 * 4.5);
  EXPECT_FLOAT_EQ((TypedValue(10.2) * TypedValue(4)).ValueDouble(), 10.2 * 4);
}

TEST_F(TypedValueArithmeticTest, Modulo) {
  ExpectArithmeticThrowsAndNull(false, [](const TypedValue &a, const TypedValue &b) { return a % b; });
  EXPECT_THROW(TypedValue(1) % TypedValue(0), TypedValueException);

  EXPECT_PROP_EQ(TypedValue(10) % TypedValue(2), TypedValue(0));
  EXPECT_PROP_EQ(TypedValue(10) % TypedValue(4), TypedValue(2));

  EXPECT_PROP_EQ(TypedValue(10.0) % TypedValue(2.0), TypedValue(0.0));
  EXPECT_FLOAT_EQ((TypedValue(10.0) % TypedValue(3.25)).ValueDouble(), 0.25);

  EXPECT_FLOAT_EQ((TypedValue(10) % TypedValue(4.0)).ValueDouble(), 2.0);
  EXPECT_FLOAT_EQ((TypedValue(10.0) % TypedValue(4)).ValueDouble(), 2.0);
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
  void TestLogicalThrows(std::function<TypedValue(const TypedValue &, const TypedValue &)> op) {
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
  TestLogicalThrows([](const TypedValue &p1, const TypedValue &p2) { return p1 && p2; });

  EXPECT_PROP_ISNULL(TypedValue() && TypedValue(true));
  EXPECT_PROP_EQ(TypedValue() && TypedValue(false), TypedValue(false));
  EXPECT_PROP_EQ(TypedValue(true) && TypedValue(true), TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(false) && TypedValue(true), TypedValue(false));
}

TEST_F(TypedValueLogicTest, LogicalOr) {
  TestLogicalThrows([](const TypedValue &p1, const TypedValue &p2) { return p1 || p2; });

  EXPECT_PROP_ISNULL(TypedValue() || TypedValue(false));
  EXPECT_PROP_EQ(TypedValue() || TypedValue(true), TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(true) || TypedValue(true), TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(false) || TypedValue(true), TypedValue(true));
}

TEST_F(TypedValueLogicTest, LogicalXor) {
  TestLogicalThrows([](const TypedValue &p1, const TypedValue &p2) { return p1 ^ p2; });

  EXPECT_PROP_ISNULL(TypedValue() && TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(true) ^ TypedValue(true), TypedValue(false));
  EXPECT_PROP_EQ(TypedValue(false) ^ TypedValue(true), TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(true) ^ TypedValue(false), TypedValue(true));
  EXPECT_PROP_EQ(TypedValue(false) ^ TypedValue(false), TypedValue(false));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(AllTypesFixture, ConstructionWithMemoryResource) {
  memgraph::utils::MonotonicBufferResource monotonic_memory(1024);
  std::vector<TypedValue> values_with_custom_memory;
  for (const auto &value : values_) {
    EXPECT_EQ(value.GetMemoryResource(), memgraph::utils::NewDeleteResource());
    TypedValue copy_constructed_value(value, &monotonic_memory);
    EXPECT_EQ(copy_constructed_value.GetMemoryResource(), &monotonic_memory);
    values_with_custom_memory.emplace_back(std::move(copy_constructed_value));
    const auto &move_constructed_value = values_with_custom_memory.back();
    EXPECT_EQ(move_constructed_value.GetMemoryResource(), &monotonic_memory);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(AllTypesFixture, AssignmentWithMemoryResource) {
  std::vector<TypedValue> values_with_default_memory;
  memgraph::utils::MonotonicBufferResource monotonic_memory(1024);
  for (const auto &value : values_) {
    EXPECT_EQ(value.GetMemoryResource(), memgraph::utils::NewDeleteResource());
    TypedValue copy_assigned_value(&monotonic_memory);
    copy_assigned_value = value;
    EXPECT_EQ(copy_assigned_value.GetMemoryResource(), &monotonic_memory);
    values_with_default_memory.emplace_back(memgraph::utils::NewDeleteResource());
    auto &move_assigned_value = values_with_default_memory.back();
    move_assigned_value = std::move(copy_assigned_value);
    EXPECT_EQ(move_assigned_value.GetMemoryResource(), memgraph::utils::NewDeleteResource());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(AllTypesFixture, PropagationOfMemoryOnConstruction) {
  memgraph::utils::MonotonicBufferResource monotonic_memory(1024);
  std::vector<TypedValue, memgraph::utils::Allocator<TypedValue>> values_with_custom_memory(&monotonic_memory);
  for (const auto &value : values_) {
    EXPECT_EQ(value.GetMemoryResource(), memgraph::utils::NewDeleteResource());
    values_with_custom_memory.emplace_back(value);
    const auto &copy_constructed_value = values_with_custom_memory.back();
    EXPECT_EQ(copy_constructed_value.GetMemoryResource(), &monotonic_memory);
    TypedValue copy(values_with_custom_memory.back());
    EXPECT_EQ(copy.GetMemoryResource(), memgraph::utils::NewDeleteResource());
    values_with_custom_memory.emplace_back(std::move(copy));
    const auto &move_constructed_value = values_with_custom_memory.back();
    EXPECT_EQ(move_constructed_value.GetMemoryResource(), &monotonic_memory);
    if (value.type() == TypedValue::Type::List) {
      ASSERT_EQ(move_constructed_value.type(), value.type());
      const auto &original = value.ValueList();
      const auto &moved = move_constructed_value.ValueList();
      const auto &copied = copy_constructed_value.ValueList();
      ASSERT_EQ(moved.size(), original.size());
      ASSERT_EQ(copied.size(), original.size());
      for (size_t i = 0; i < value.ValueList().size(); ++i) {
        EXPECT_EQ(original[i].GetMemoryResource(), memgraph::utils::NewDeleteResource());
        EXPECT_EQ(moved[i].GetMemoryResource(), &monotonic_memory);
        EXPECT_EQ(copied[i].GetMemoryResource(), &monotonic_memory);
        EXPECT_TRUE(TypedValue::BoolEqual{}(original[i], moved[i]));
        EXPECT_TRUE(TypedValue::BoolEqual{}(original[i], copied[i]));
      }
    } else if (value.type() == TypedValue::Type::Map) {
      ASSERT_EQ(move_constructed_value.type(), value.type());
      const auto &original = value.ValueMap();
      const auto &moved = move_constructed_value.ValueMap();
      const auto &copied = copy_constructed_value.ValueMap();
      auto expect_allocator = [](const auto &kv, auto *memory_resource) {
        EXPECT_EQ(*kv.first.get_allocator().GetMemoryResource(), *memory_resource);
        EXPECT_EQ(*kv.second.GetMemoryResource(), *memory_resource);
      };
      for (const auto &kv : original) {
        expect_allocator(kv, memgraph::utils::NewDeleteResource());
        auto moved_it = moved.find(kv.first);
        ASSERT_NE(moved_it, moved.end());
        expect_allocator(*moved_it, &monotonic_memory);
        auto copied_it = copied.find(kv.first);
        ASSERT_NE(copied_it, copied.end());
        expect_allocator(*copied_it, &monotonic_memory);
        EXPECT_TRUE(TypedValue::BoolEqual{}(kv.second, moved_it->second));
        EXPECT_TRUE(TypedValue::BoolEqual{}(kv.second, copied_it->second));
      }
    } else if (value.type() == TypedValue::Type::Path) {
      ASSERT_EQ(move_constructed_value.type(), value.type());
      const auto &original = value.ValuePath();
      const auto &moved = move_constructed_value.ValuePath();
      const auto &copied = copy_constructed_value.ValuePath();
      EXPECT_EQ(original.GetMemoryResource(), memgraph::utils::NewDeleteResource());
      EXPECT_EQ(moved.vertices(), original.vertices());
      EXPECT_EQ(moved.edges(), original.edges());
      EXPECT_EQ(moved.GetMemoryResource(), &monotonic_memory);
      EXPECT_EQ(copied.vertices(), original.vertices());
      EXPECT_EQ(copied.edges(), original.edges());
      EXPECT_EQ(copied.GetMemoryResource(), &monotonic_memory);
    } else if (value.type() == TypedValue::Type::Graph) {
      ASSERT_EQ(move_constructed_value.type(), value.type());
      const auto &original = value.ValueGraph();
      const auto &moved = move_constructed_value.ValueGraph();
      const auto &copied = copy_constructed_value.ValueGraph();
      EXPECT_EQ(original.GetMemoryResource(), memgraph::utils::NewDeleteResource());
      EXPECT_EQ(moved.vertices(), original.vertices());
      EXPECT_EQ(moved.edges(), original.edges());
      EXPECT_EQ(moved.GetMemoryResource(), &monotonic_memory);
      EXPECT_EQ(copied.vertices(), original.vertices());
      EXPECT_EQ(copied.edges(), original.edges());
      EXPECT_EQ(copied.GetMemoryResource(), &monotonic_memory);
    }
  }
}
