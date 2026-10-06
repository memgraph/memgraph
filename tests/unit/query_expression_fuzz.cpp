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

// Builds random expressions and checks that evaluating one into its slot
// leaves what evaluating it the old way returns. The operands are drawn from
// every TypedValue type, so the pairs an operator refuses come up as often as
// the ones it accepts, and refusing is part of what has to match.
//
// A failure prints the seed. Re-running with MG_FUZZ_SEED set to it rebuilds
// the same expressions.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/pretty_print.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpret/frame.hpp"
#include "query/interpret/typed_program.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/unit/typed_value_shapes.hpp"

using memgraph::query::AstStorage;
using memgraph::query::Expression;
using memgraph::query::TypedValue;

namespace shapes = memgraph::test::shapes;

namespace {

// What evaluating did: the value, or the complaint. Both have to match, since
// a caller sees either.
struct Outcome {
  bool threw{false};
  std::string complaint;
  TypedValue value;
};

template <typename Fn>
Outcome Attempt(Fn fn) {
  try {
    Outcome outcome;
    outcome.value = fn();
    return outcome;
  } catch (std::exception const &e) {
    return Outcome{.threw = true, .complaint = e.what(), .value = TypedValue()};
  } catch (...) {
    return Outcome{.threw = true, .complaint = "<non-standard exception>", .value = TypedValue()};
  }
}

// Some types have no comparison at all, and asking for one throws. Those are
// settled on their type, which is as far as the check can go for them.
bool SameValue(TypedValue const &left, TypedValue const &right) {
  try {
    return TypedValue::BoolEqual{}(left, right);
  } catch (...) {
    return left.type() == right.type();
  }
}

class ExpressionFuzz : public ::testing::Test {
 protected:
  std::unique_ptr<memgraph::storage::Storage> db_ =
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{});
  std::unique_ptr<memgraph::storage::Storage::Accessor> accessor_ = db_->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba_{accessor_.get()};

  AstStorage storage_;
  std::vector<TypedValue> operands_ = shapes::EveryTypedValueShape(&dba_);
  memgraph::query::Frame frame_{static_cast<int64_t>(operands_.size())};
  memgraph::query::SymbolTable symbol_table_;
  memgraph::query::ExecutionContext context_;

  void SetUp() override {
    auto writer = frame_.GetFrameWriter(nullptr, memgraph::utils::NewDeleteResource());
    for (size_t i = 0; i < operands_.size(); ++i) {
      memgraph::query::Symbol const symbol{"v" + std::to_string(i), static_cast<int>(i), false};
      writer.Modify(symbol, [&](TypedValue &slot) { slot = operands_[i]; });
    }
    context_.db_accessor = &dba_;
    context_.symbol_table = symbol_table_;
  }

  memgraph::query::ExpressionEvaluator MakeEvaluator() {
    return memgraph::query::ExpressionEvaluator{&frame_, context_, memgraph::storage::View::OLD};
  }

  // A leaf reads one of the operands off the frame, so every type reaches the
  // operators rather than only the ones a literal can spell.
  Expression *Leaf(std::mt19937 &rng) {
    auto *identifier = storage_.Create<memgraph::query::Identifier>("v");
    identifier->symbol_pos_ = static_cast<int32_t>(rng() % operands_.size());
    return identifier;
  }

  Expression *Build(std::mt19937 &rng, int depth) {
    if (depth <= 0) return Leaf(rng);
    switch (rng() % 14) {
      case 0:
        return storage_.Create<memgraph::query::AndOperator>(Build(rng, depth - 1), Build(rng, depth - 1));
      case 1:
        return storage_.Create<memgraph::query::OrOperator>(Build(rng, depth - 1), Build(rng, depth - 1));
      case 2:
        return storage_.Create<memgraph::query::XorOperator>(Build(rng, depth - 1), Build(rng, depth - 1));
      case 3:
        return storage_.Create<memgraph::query::EqualOperator>(Build(rng, depth - 1), Build(rng, depth - 1));
      case 4:
        return storage_.Create<memgraph::query::NotEqualOperator>(Build(rng, depth - 1), Build(rng, depth - 1));
      case 5:
        return storage_.Create<memgraph::query::LessOperator>(Build(rng, depth - 1), Build(rng, depth - 1));
      case 6:
        return storage_.Create<memgraph::query::GreaterOperator>(Build(rng, depth - 1), Build(rng, depth - 1));
      case 7:
        return storage_.Create<memgraph::query::AdditionOperator>(Build(rng, depth - 1), Build(rng, depth - 1));
      case 8:
        return storage_.Create<memgraph::query::SubtractionOperator>(Build(rng, depth - 1), Build(rng, depth - 1));
      case 9:
        return storage_.Create<memgraph::query::MultiplicationOperator>(Build(rng, depth - 1), Build(rng, depth - 1));
      case 10:
        return storage_.Create<memgraph::query::DivisionOperator>(Build(rng, depth - 1), Build(rng, depth - 1));
      case 11:
        return storage_.Create<memgraph::query::NotOperator>(Build(rng, depth - 1));
      case 12:
        return storage_.Create<memgraph::query::IsNullOperator>(Build(rng, depth - 1));
      default:
        return storage_.Create<memgraph::query::UnaryMinusOperator>(Build(rng, depth - 1));
    }
  }

  std::string Describe(Expression *expr) {
    std::ostringstream out;
    try {
      memgraph::query::PrintExpression(expr, &out, dba_);
    } catch (...) {
      return "<could not print>";
    }
    return out.str();
  }
};

uint32_t ChosenSeed() {
  if (char const *given = std::getenv("MG_FUZZ_SEED"); given != nullptr) {
    return static_cast<uint32_t>(std::strtoul(given, nullptr, 10));
  }
  return 0x5EEDU;
}

}  // namespace

TEST_F(ExpressionFuzz, TheSlotPathMatchesAcceptOnRandomExpressions) {
  auto const seed = ChosenSeed();
  std::mt19937 rng{seed};
  auto evaluator = MakeEvaluator();

  constexpr int kExpressions = 4000;
  for (int i = 0; i < kExpressions; ++i) {
    auto *expr = Build(rng, 1 + static_cast<int>(rng() % 4));

    auto const by_accept = Attempt([&] { return expr->Accept(evaluator); });
    auto const by_slot = Attempt([&] { return TypedValue{evaluator.EvalIntoSlot(expr)}; });

    ASSERT_EQ(by_accept.threw, by_slot.threw)
        << "one path refused and the other did not, seed " << seed << ", expression " << i << ": " << Describe(expr)
        << "\n  accept: " << by_accept.complaint << "\n  slot:   " << by_slot.complaint;
    if (by_accept.threw) {
      ASSERT_EQ(by_accept.complaint, by_slot.complaint)
          << "the two refused differently, seed " << seed << ", expression " << i << ": " << Describe(expr);
      continue;
    }
    ASSERT_TRUE(SameValue(by_accept.value, by_slot.value))
        << "the two gave different values, seed " << seed << ", expression " << i << ": " << Describe(expr);
  }
}

// The other half of standing in for the old path: it must not be slower. This
// reports rather than asserts, because a threshold on wall time in a unit test
// fails on a loaded machine for reasons that have nothing to do with the code.
// The number to act on is the end-to-end one; this says where it comes from.
TEST_F(ExpressionFuzz, ReportsWhatTheSlotPathCostsAgainstAccept) {
  std::mt19937 rng{ChosenSeed()};
  auto evaluator = MakeEvaluator();

  std::vector<Expression *> corpus;
  corpus.reserve(2000);
  for (int i = 0; i < 2000; ++i) corpus.push_back(Build(rng, 1 + static_cast<int>(rng() % 4)));

  auto time_it = [&](auto evaluate) {
    // One pass first, so neither is charged for warming the caches.
    for (auto *expr : corpus) Attempt([&] { return evaluate(expr); });
    auto const start = std::chrono::steady_clock::now();
    for (int pass = 0; pass < 20; ++pass) {
      for (auto *expr : corpus) Attempt([&] { return evaluate(expr); });
    }
    return std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
  };

  auto const accept_seconds = time_it([&](Expression *expr) { return expr->Accept(evaluator); });
  auto const slot_seconds = time_it([&](Expression *expr) { return TypedValue{evaluator.EvalIntoSlot(expr)}; });

  std::cerr << "accept " << accept_seconds << "s, slot " << slot_seconds << "s, ratio "
            << (slot_seconds / accept_seconds) << "\n";
  SUCCEED();
}

namespace {

// What the compiled program said, lined up against what the evaluator says.
// Refusing is not a disagreement: it means the guess about a type was wrong and
// the row belongs to the ordinary evaluator.
testing::AssertionResult CompiledAgrees(memgraph::query::TypedProgram const &program,
                                        memgraph::query::Frame const &frame, Outcome const &boxed) {
  using Answer = memgraph::query::TypedProgram::Answer;
  auto const answer = program.Run(frame);
  if (answer == Answer::Refused) return testing::AssertionSuccess();

  if (boxed.threw) {
    return testing::AssertionFailure() << "the compiled program answered where the evaluator refused: "
                                       << boxed.complaint;
  }
  if (answer == Answer::Null) {
    return boxed.value.IsNull() ? testing::AssertionSuccess()
                                : testing::AssertionFailure() << "compiled said null, evaluator did not";
  }
  if (!boxed.value.IsBool()) {
    return testing::AssertionFailure() << "compiled said a bool, evaluator gave type "
                                       << static_cast<int>(boxed.value.type());
  }
  const bool want = boxed.value.ValueBool();
  const bool got = answer == Answer::True;
  return got == want ? testing::AssertionSuccess()
                     : testing::AssertionFailure() << "compiled said " << got << ", evaluator said " << want;
}

}  // namespace

// C1 to C4 in the plan, for the compiled path. Operands of every type, so the
// guard is exercised as hard as the arithmetic.
TEST_F(ExpressionFuzz, TheCompiledProgramMatchesAcceptOrRefuses) {
  auto const seed = ChosenSeed();
  std::mt19937 rng{seed};
  auto evaluator = MakeEvaluator();

  int compiled = 0;
  int answered = 0;
  for (int i = 0; i < 4000; ++i) {
    auto *expr = Build(rng, 1 + static_cast<int>(rng() % 4));
    auto program = memgraph::query::TypedProgram::Compile(expr);
    if (!program) continue;
    ++compiled;

    auto const boxed = Attempt([&] { return expr->Accept(evaluator); });
    if (program->Run(frame_) != memgraph::query::TypedProgram::Answer::Refused) ++answered;
    EXPECT_TRUE(CompiledAgrees(*program, frame_, boxed))
        << "seed " << seed << ", expression " << i << ": " << Describe(expr);
  }
  std::cerr << "compiled " << compiled << " of 4000, answered " << answered << "\n";
}

// The same, over a frame of integers, so the compiled path actually runs
// instead of refusing every row for want of an integer.
TEST_F(ExpressionFuzz, TheCompiledProgramMatchesAcceptOnIntegers) {
  auto const seed = ChosenSeed();
  std::mt19937 rng{seed};

  {
    auto writer = frame_.GetFrameWriter(nullptr, memgraph::utils::NewDeleteResource());
    for (size_t i = 0; i < operands_.size(); ++i) {
      memgraph::query::Symbol const symbol{"v" + std::to_string(i), static_cast<int>(i), false};
      // Every fourth one null, so the three-valued cases come up too.
      writer.Modify(symbol, [&](TypedValue &slot) {
        slot = (i % 4 == 3) ? TypedValue() : TypedValue(static_cast<int64_t>(i) - 4);
      });
    }
  }
  auto evaluator = MakeEvaluator();

  int answered = 0;
  for (int i = 0; i < 4000; ++i) {
    auto *expr = Build(rng, 1 + static_cast<int>(rng() % 4));
    auto program = memgraph::query::TypedProgram::Compile(expr);
    if (!program) continue;

    auto const boxed = Attempt([&] { return expr->Accept(evaluator); });
    if (program->Run(frame_) != memgraph::query::TypedProgram::Answer::Refused) ++answered;
    EXPECT_TRUE(CompiledAgrees(*program, frame_, boxed))
        << "seed " << seed << ", expression " << i << ": " << Describe(expr);
  }
  std::cerr << "answered " << answered << " of 4000 on an integer frame\n";
  EXPECT_GT(answered, 0) << "nothing ran, so nothing was really compared";
}
