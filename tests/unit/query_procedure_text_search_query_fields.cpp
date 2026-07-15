// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <string_view>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "query/procedure/mg_procedure_helpers.hpp"

using memgraph::query::procedure::ReferencedTextQueryFields;
using testing::ElementsAre;
using testing::IsEmpty;

TEST(ReferencedTextQueryFields, SingleField) {
  EXPECT_THAT(ReferencedTextQueryFields("data.title:Rules2024"), ElementsAre("title"));
}

TEST(ReferencedTextQueryFields, IdentifierWithDigitsAndUnderscore) {
  EXPECT_THAT(ReferencedTextQueryFields("data.my_field2:x"), ElementsAre("my_field2"));
}

TEST(ReferencedTextQueryFields, BooleanAndCollectsBoth) {
  EXPECT_THAT(ReferencedTextQueryFields("data.title:X AND data.body:Y"), ElementsAre("title", "body"));
}

TEST(ReferencedTextQueryFields, BooleanOrWithParensAndNegationCollectsAllInOrder) {
  EXPECT_THAT(ReferencedTextQueryFields("(data.title:A OR data.title:B) AND NOT data.fulltext:words"),
              ElementsAre("title", "title", "fulltext"));
}

TEST(ReferencedTextQueryFields, BareTermIsFieldless) { EXPECT_THAT(ReferencedTextQueryFields("Rules2024"), IsEmpty()); }

TEST(ReferencedTextQueryFields, EmptyQuery) { EXPECT_THAT(ReferencedTextQueryFields(""), IsEmpty()); }

TEST(ReferencedTextQueryFields, ValueContainingDataPrefixIsNotAField) {
  EXPECT_THAT(ReferencedTextQueryFields("data.title:data.bar"), ElementsAre("title"));
}

TEST(ReferencedTextQueryFields, FieldWithoutColonIsIgnored) {
  EXPECT_THAT(ReferencedTextQueryFields("data.title"), IsEmpty());
}

TEST(ReferencedTextQueryFields, PrefixWithoutIdentifierIsIgnored) {
  EXPECT_THAT(ReferencedTextQueryFields("data.:x"), IsEmpty());
  EXPECT_THAT(ReferencedTextQueryFields("data. title:x"), IsEmpty());
}

TEST(ReferencedTextQueryFields, ColonBearingValueFragmentIsOverExtracted) {
  EXPECT_THAT(ReferencedTextQueryFields("data.title:data.bar:baz"), ElementsAre("title", "bar"));
}
