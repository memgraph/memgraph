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

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "query/db_accessor.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/unit/typed_value_shapes.hpp"
#include "utils/memory.hpp"

using memgraph::query::TypedValue;
using memgraph::query::TypedValueException;

namespace shapes = memgraph::test::shapes;

namespace {

// What an operator did: the value it produced, or the fact that it threw. Both
// halves have to match for the in-place form to stand in for the operator,
// since a caller can see either.
struct Outcome {
  bool threw{false};
  TypedValue value;
};

template <typename Fn>
Outcome Attempt(Fn fn) {
  try {
    Outcome outcome;
    outcome.value = fn();
    return outcome;
  } catch (TypedValueException const &) {
    return Outcome{.threw = true, .value = TypedValue()};
  }
}

testing::AssertionResult Agrees(Outcome const &expected, Outcome const &actual) {
  if (expected.threw != actual.threw) {
    return testing::AssertionFailure() << "one threw and the other did not: operator threw=" << expected.threw
                                       << " in-place threw=" << actual.threw;
  }
  if (expected.threw) return testing::AssertionSuccess();
  if (!TypedValue::BoolEqual{}(expected.value, actual.value)) {
    return testing::AssertionFailure() << "values differ: operator gave type "
                                       << static_cast<int>(expected.value.type()) << ", in-place gave type "
                                       << static_cast<int>(actual.value.type());
  }
  return testing::AssertionSuccess();
}

class TypedValueInPlace : public ::testing::Test {
 protected:
  std::unique_ptr<memgraph::storage::Storage> db_ =
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{});
  std::unique_ptr<memgraph::storage::Storage::Accessor> accessor_ = db_->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba_{accessor_.get()};
};

}  // namespace

// O1 and O2 in the plan, for equality. Every ordered pair of shapes, so every
// pair of types the operator can be handed, including the ones it refuses.
TEST_F(TypedValueInPlace, EqualityMatchesTheOperatorOnEveryPairOfShapes) {
  auto const values = shapes::EveryTypedValueShape(&dba_);
  ASSERT_FALSE(values.empty());

  for (auto const &lhs : values) {
    for (auto const &rhs : values) {
      auto const expected = Attempt([&] { return lhs == rhs; });
      auto const actual = Attempt([&] {
        TypedValue out;
        memgraph::query::EqualInto(out, lhs, rhs);
        return out;
      });
      EXPECT_TRUE(Agrees(expected, actual))
          << "for lhs type " << static_cast<int>(lhs.type()) << " and rhs type " << static_cast<int>(rhs.type());
    }
  }
}

// O3 in the plan. An evaluator writing into a scratch slot can hand that same
// slot in as an operand, so the in-place form has to read both operands before
// it writes, on every pair of shapes rather than on a case someone thought of.
TEST_F(TypedValueInPlace, EqualityIsCorrectWhenTheDestinationIsAlsoAnOperand) {
  auto const values = shapes::EveryTypedValueShape(&dba_);
  ASSERT_FALSE(values.empty());

  for (auto const &lhs : values) {
    for (auto const &rhs : values) {
      auto const expected = Attempt([&] { return lhs == rhs; });

      auto const as_left = Attempt([&] {
        TypedValue out = lhs;
        memgraph::query::EqualInto(out, out, rhs);
        return out;
      });
      EXPECT_TRUE(Agrees(expected, as_left))
          << "destination aliased the left operand, lhs type " << static_cast<int>(lhs.type());

      auto const as_right = Attempt([&] {
        TypedValue out = rhs;
        memgraph::query::EqualInto(out, lhs, out);
        return out;
      });
      EXPECT_TRUE(Agrees(expected, as_right))
          << "destination aliased the right operand, rhs type " << static_cast<int>(rhs.type());
    }
  }

  for (auto const &value : values) {
    auto const expected = Attempt([&] { return value == value; });
    auto const all_three = Attempt([&] {
      TypedValue out = value;
      memgraph::query::EqualInto(out, out, out);
      return out;
    });
    EXPECT_TRUE(Agrees(expected, all_three))
        << "destination aliased both operands, type " << static_cast<int>(value.type());
  }
}

// O4 in the plan. A query evaluates on its own memory, so a destination built
// from it has to stay on it. Taking an operand's allocator would quietly move
// the value onto memory with a different lifetime.
TEST_F(TypedValueInPlace, TheDestinationKeepsItsOwnAllocator) {
  memgraph::utils::MonotonicBufferResource monotonic{4UL * 1024UL};
  memgraph::utils::PoolResource<> pool{64, &monotonic};
  auto const query_alloc = TypedValue::allocator_type{&pool};

  auto const values = shapes::EveryTypedValueShape(&dba_);
  for (auto const &lhs : values) {
    for (auto const &rhs : values) {
      TypedValue out{query_alloc};
      try {
        memgraph::query::EqualInto(out, lhs, rhs);
      } catch (TypedValueException const &) {
        continue;
      }
      EXPECT_EQ(out.get_allocator(), query_alloc)
          << "the destination took an operand's allocator, lhs type " << static_cast<int>(lhs.type());
    }
  }
}
