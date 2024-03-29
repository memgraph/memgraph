// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <iostream>
#include <string_view>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include <utils/java_string_formatter.hpp>
#include <utils/pmr/string.hpp>

using TString = memgraph::utils::pmr::string;

namespace {

class DummyTypedValue {
 public:
  template <typename T>
  DummyTypedValue(T &&val) : data_(std::forward<decltype(val)>(val)) {}

  int ValueInt() const noexcept {
    auto *value = std::get_if<int>(&data_);
    MG_ASSERT(value);
    return *value;
  }
  double ValueDouble() const noexcept {
    auto *value = std::get_if<double>(&data_);
    MG_ASSERT(value);
    return *value;
  }
  TString ValueString() const noexcept {
    auto *value = std::get_if<TString>(&data_);
    MG_ASSERT(value);
    return *value;
  }

 private:
  std::variant<int, double, TString> data_;
};

auto GetVector() { return memgraph::utils::pmr::vector<DummyTypedValue>(memgraph::utils::NewDeleteResource()); }
auto GetString() { return TString(memgraph::utils::NewDeleteResource()); }

memgraph::utils::JStringFormatter<TString, DummyTypedValue> gFormatter;

}  // namespace

TEST(JavaStringFormatter, FormatesOneIntegerCharacter) {
  auto args = GetVector();
  auto str = GetString();
  args.emplace_back(1);
  str = "\%d";

  auto result = gFormatter.FormatString(str, args);

  ASSERT_EQ(result, "1");
}

TEST(JavaStringFormatter, FormatesOneDoubleCharacter) {
  auto args = GetVector();
  auto str = GetString();
  args.emplace_back(1.0);
  str = "\%f";

  auto result = gFormatter.FormatString(str, args);

  ASSERT_EQ(result, "1.000000");
}

TEST(JavaStringFormatter, FormatesOneSimpleStringCharacter) {
  auto args = GetVector();
  auto str = GetString();
  auto replacement_str = GetString();
  replacement_str = "x";
  args.emplace_back(replacement_str);
  str = "\%s";

  auto result = gFormatter.FormatString(str, args);

  ASSERT_EQ(result, "x");
}

TEST(JavaStringFormatter, FormatesOneComplexStringCharacter) {
  auto args = GetVector();
  auto str = GetString();
  auto replacement_str = GetString();
  replacement_str = "moja najdraža boja je zelena";
  args.emplace_back(replacement_str);
  str = "\%s";

  auto result = gFormatter.FormatString(str, args);

  ASSERT_EQ(result, "moja najdraža boja je zelena");
}

TEST(JavaStringFormatter, FormatesMoreCharacters) {
  auto args = GetVector();
  auto str = GetString();
  args.emplace_back(1);
  args.emplace_back(2);
  args.emplace_back(3);
  str = "\%d\%d\%d";

  auto result = gFormatter.FormatString(str, args);

  ASSERT_EQ(result, "123");
}

TEST(JavaStringFormatter, FormatesMoreCharactersInSentence) {
  auto args = GetVector();
  auto str = GetString();
  args.emplace_back(1);
  args.emplace_back(10);
  args.emplace_back(10.0);
  str = "The chances of picking \%d out of \%d matches is \%f%";

  auto result = gFormatter.FormatString(str, args);

  ASSERT_EQ(result, "The chances of picking 1 out of 10 matches is 10.000000%");
}

TEST(JavaStringFormatter, FormateSinglePecrentile) {
  auto args = GetVector();
  auto str = GetString();
  str = "%";

  auto result = gFormatter.FormatString(str, args);

  ASSERT_EQ(result, "%");
}

TEST(JavaStringFormatter, FormatPercineleBeforeFormatString) {
  auto args = GetVector();
  auto str = GetString();
  args.emplace_back(1);
  str = "%\%d";

  auto result = gFormatter.FormatString(str, args);

  ASSERT_EQ(result, "\%1");
}

TEST(JavaStringFormatter, FormatPercineleAfterFormatString) {
  auto args = GetVector();
  auto str = GetString();
  args.emplace_back(1);
  str = "\%d%";

  auto result = gFormatter.FormatString(str, args);

  ASSERT_EQ(result, "1\%");
}

TEST(JavaStringFormatter, FormatPercineleInBetweenFormatStrings) {
  auto args = GetVector();
  auto str = GetString();
  args.emplace_back(1);
  str = "%\%d%";

  auto result = gFormatter.FormatString(str, args);

  ASSERT_EQ(result, "\%1\%");
}

TEST(JavaStringFormatter, FormatManyPercentilesOneFormatString) {
  auto args = GetVector();
  auto str = GetString();
  args.emplace_back(1);
  str = "Some% random% strings here% and there wit% h \%da bunch of percent% iles and one format%-string.";

  auto result = gFormatter.FormatString(str, args);

  ASSERT_EQ(result, "Some% random% strings here% and there wit% h 1a bunch of percent% iles and one format%-string.");
}

TEST(JavaStringFormatter, ThrowOnNonExistentFormatSpecifier) {
  auto args = GetVector();
  auto str = GetString();
  args.emplace_back(1);
  str = "\%x";

  ASSERT_THROW(gFormatter.FormatString(str, args), memgraph::utils::JStringFormatException);
}

TEST(JavaStringFormatter, ThrowOnLessFormatSpecifiersThanSlots) {
  auto args = GetVector();
  auto str = GetString();
  auto replacement_str = GetString();
  replacement_str = "format specifiers";
  args.emplace_back(3);
  args.emplace_back(replacement_str);
  str = "There is \%d \%s in this sentence but only \%d specified in the argument list";

  ASSERT_THROW(gFormatter.FormatString(str, args), memgraph::utils::JStringFormatException);
}

TEST(JavaStringFormatter, DoNotThrowOnMoreFormatSpecifiersThanSlots) {
  auto args = GetVector();
  auto str = GetString();
  auto replacement_str = GetString();
  replacement_str = "format specifiers";
  args.emplace_back(3);
  args.emplace_back(replacement_str);
  args.emplace_back(123);
  args.emplace_back(123);
  str = "There is \%d \%s in this sentence but only \%d specified in the argument list";

  auto result = gFormatter.FormatString(str, args);

  ASSERT_EQ(result, "There is 3 format specifiers in this sentence but only 123 specified in the argument list");
}
