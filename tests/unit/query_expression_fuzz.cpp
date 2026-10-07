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

// Builds random expressions and checks that a compiled program answers what
// the evaluator answers, or refuses. The operands are drawn from every
// TypedValue type, so the pairs an operator refuses come up as often as the
// ones it accepts, and refusing is part of what has to match.
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
#include "utils/temporal.hpp"

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
  memgraph::query::Frame frame_{static_cast<int64_t>(operands_.size()) + 3};
  memgraph::query::SymbolTable symbol_table_;
  memgraph::query::ExecutionContext context_;

  void SetUp() override {
    auto vertex = dba_.InsertVertex();
    auto const put = [&](char const *name, memgraph::storage::PropertyValue value) {
      auto const id = dba_.NameToProperty(name);
      [[maybe_unused]] auto const ok = vertex.SetProperty(id, std::move(value));
    };
    put("whole", memgraph::storage::PropertyValue(int64_t{7}));
    put("fraction", memgraph::storage::PropertyValue(2.5));
    put("word", memgraph::storage::PropertyValue(std::string{"seven"}));
    put("truth", memgraph::storage::PropertyValue(true));
    {
      auto const [day, time] = memgraph::utils::ParseLocalDateTimeParameters("2020-06-01T12:00:00");
      auto const when = memgraph::utils::LocalDateTime(day, time);
      put("moment",
          memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
              memgraph::storage::TemporalType::LocalDateTime, when.SysMicrosecondsSinceEpoch())));
    }
    [[maybe_unused]] auto const labelled = vertex.AddLabel(dba_.NameToLabel("Present"));
    dba_.AdvanceCommand();
    record_ = TypedValue(vertex);

    auto doomed = dba_.InsertVertex();
    [[maybe_unused]] auto const set =
        doomed.SetProperty(dba_.NameToProperty("whole"), memgraph::storage::PropertyValue(int64_t{7}));
    dba_.AdvanceCommand();
    [[maybe_unused]] auto const removed = dba_.RemoveVertex(&doomed);
    dba_.AdvanceCommand();
    gone_ = TypedValue(doomed);
    property_names_ = {"whole", "fraction", "word", "truth", "absent", "moment"};

    auto writer = frame_.GetFrameWriter(nullptr, memgraph::utils::NewDeleteResource());
    for (size_t i = 0; i < operands_.size(); ++i) {
      memgraph::query::Symbol const symbol{"v" + std::to_string(i), static_cast<int>(i), false};
      writer.Modify(symbol, [&](TypedValue &slot) { slot = operands_[i]; });
    }
    // The record sits one past the operands, so a lookup has somewhere to read
    // from without displacing them.
    record_position_ = static_cast<int>(operands_.size());
    memgraph::query::Symbol const record_symbol{"record", record_position_, false};
    writer.Modify(record_symbol, [&](TypedValue &slot) { slot = record_; });

    // A record that is gone throws when read, which is how a path that
    // evaluates something the other path skipped gives itself away.
    gone_position_ = record_position_ + 1;
    memgraph::query::Symbol const gone_symbol{"gone", gone_position_, false};
    writer.Modify(gone_symbol, [&](TypedValue &slot) { slot = gone_; });

    // Every name a lookup might use is registered before the mapping is built,
    // since the mapping is indexed by the order they were registered in and a
    // name first seen while generating would sit past the end of it.
    for (auto const &name : property_names_) storage_.GetPropertyIx(name);

    // A stripped query reaches the evaluator with its literals bound here, so
    // a parameter of every type a literal can spell is bound, null included.
    parameters_.Add(0, memgraph::storage::ExternalPropertyValue(int64_t{7}));
    parameters_.Add(1, memgraph::storage::ExternalPropertyValue(2.5));
    parameters_.Add(2, memgraph::storage::ExternalPropertyValue(std::string{"seven"}));
    parameters_.Add(3, memgraph::storage::ExternalPropertyValue(true));
    parameters_.Add(4, memgraph::storage::ExternalPropertyValue());

    context_.db_accessor = &dba_;
    context_.symbol_table = symbol_table_;
    for (auto const &name : label_names_) storage_.GetLabelIx(name);
    context_.evaluation_context.properties = memgraph::query::NamesToProperties(storage_.properties_, &dba_);
    context_.evaluation_context.labels = memgraph::query::NamesToLabels(storage_.labels_, &dba_);
    context_.evaluation_context.parameters = parameters_;
  }

  memgraph::query::Parameters parameters_;
  // Only a bound position may be generated: reading an unbound one aborts the
  // evaluator rather than throwing, so there would be nothing to compare.
  static constexpr int kBoundParameters = 5;

  TypedValue record_;
  int record_position_{0};
  TypedValue gone_;
  int gone_position_{0};
  std::vector<std::string> property_names_;
  std::vector<std::string> label_names_{"Present", "Absent"};

  memgraph::query::ExpressionEvaluator MakeEvaluator() {
    return memgraph::query::ExpressionEvaluator{&frame_, context_, memgraph::storage::View::OLD};
  }

  // A leaf reads one of the operands off the frame, so every type reaches the
  // operators rather than only the ones a literal can spell.
  Expression *Leaf(std::mt19937 &rng) {
    if (rng() % 11 == 0) {
      // A time the query names, which is the one call a program takes, and the
      // only thing that makes it read anything beside it as a time.
      auto *when = storage_.Create<memgraph::query::PrimitiveLiteral>(
          memgraph::storage::ExternalPropertyValue(std::string{"2020-06-01T12:00:00"}));
      return storage_.Create<memgraph::query::Function>("LOCALDATETIME", std::vector<Expression *>{when});
    }
    if (rng() % 7 == 0) {
      // A label test over whatever the frame holds, so the operand that is not
      // a node comes up as often as the one that is.
      auto *subject = storage_.Create<memgraph::query::Identifier>("v");
      subject->symbol_pos_ = static_cast<int32_t>(rng() % (operands_.size() + 2));
      auto const &name = label_names_[rng() % label_names_.size()];
      return storage_.Create<memgraph::query::LabelsTest>(
          subject, std::vector<memgraph::query::LabelIx>{storage_.GetLabelIx(name)});
    }
    if (rng() % 5 == 0) {
      return storage_.Create<memgraph::query::ParameterLookup>(static_cast<int>(rng() % kBoundParameters));
    }
    if (rng() % 4 == 0) {
      // Reading a property brings in what a lookup has to get right: a value
      // of the wrong type, a property that is not there at all, and a record
      // that cannot be read without throwing.
      bool const from_a_deleted_record = rng() % 4 == 0;
      auto *record = storage_.Create<memgraph::query::Identifier>(from_a_deleted_record ? "gone" : "record");
      record->symbol_pos_ = from_a_deleted_record ? gone_position_ : record_position_;
      auto const &name = property_names_[rng() % property_names_.size()];
      return storage_.Create<memgraph::query::PropertyLookup>(record, storage_.GetPropertyIx(name));
    }
    auto *identifier = storage_.Create<memgraph::query::Identifier>("v");
    identifier->symbol_pos_ = static_cast<int32_t>(rng() % operands_.size());
    return identifier;
  }

  Expression *Build(std::mt19937 &rng, int depth) {
    if (depth <= 0) return Leaf(rng);
    switch (rng() % 17) {
      case 16:
        return storage_.Create<memgraph::query::DivisionOperator>(Build(rng, depth - 1), Build(rng, depth - 1));
      case 15:
        return storage_.Create<memgraph::query::IsNullOperator>(Build(rng, depth - 1));
      case 14: {
        auto *range = storage_.Create<memgraph::query::RangeOperator>();
        range->expression1_ = Build(rng, depth - 1);
        range->expression2_ = Build(rng, depth - 1);
        return range;
      }
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

namespace {

// What the compiled program said, lined up against what the evaluator says.
// Refusing is not a disagreement: it means the guess about a type was wrong and
// the row belongs to the ordinary evaluator.
// Running a compiled program can throw what the evaluator throws, for a record
// that is gone, so it is attempted the same way and the complaint compared.
struct CompiledOutcome {
  bool refused{false};
  bool threw{false};
  std::string complaint;
  memgraph::query::TypedProgram::Answer answer{};
};

CompiledOutcome RunCompiled(memgraph::query::TypedProgram const &program, memgraph::query::Frame const &frame,
                            memgraph::query::RecordReader *reader, memgraph::query::Parameters const *parameters) {
  using Answer = memgraph::query::TypedProgram::Answer;
  try {
    auto const answer = program.Run(frame, reader, parameters);
    return CompiledOutcome{.refused = answer == Answer::Refused, .answer = answer};
  } catch (std::exception const &e) {
    return CompiledOutcome{.threw = true, .complaint = e.what()};
  } catch (...) {
    return CompiledOutcome{.threw = true, .complaint = "<non-standard exception>"};
  }
}

// Refusing is not a disagreement: it means the guess about a type was wrong and
// the row belongs to the ordinary evaluator.
testing::AssertionResult CompiledAgrees(CompiledOutcome const &typed, Outcome const &boxed) {
  using Answer = memgraph::query::TypedProgram::Answer;
  if (typed.refused) return testing::AssertionSuccess();

  if (typed.threw || boxed.threw) {
    if (typed.threw && boxed.threw) {
      return typed.complaint == boxed.complaint
                 ? testing::AssertionSuccess()
                 : testing::AssertionFailure() << "the two refused differently: compiled said '" << typed.complaint
                                               << "', evaluator said '" << boxed.complaint << "'";
    }
    return testing::AssertionFailure() << "only one threw: compiled " << typed.threw << " (" << typed.complaint
                                       << "), evaluator " << boxed.threw << " (" << boxed.complaint << ")";
  }

  if (typed.answer == Answer::Null) {
    return boxed.value.IsNull() ? testing::AssertionSuccess()
                                : testing::AssertionFailure() << "compiled said null, evaluator did not";
  }
  if (!boxed.value.IsBool()) {
    return testing::AssertionFailure() << "compiled said a bool, evaluator gave type "
                                       << static_cast<int>(boxed.value.type());
  }
  const bool want = boxed.value.ValueBool();
  const bool got = typed.answer == Answer::True;
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
    auto const typed = RunCompiled(*program, frame_, &evaluator, &parameters_);
    if (!typed.refused) ++answered;
    EXPECT_TRUE(CompiledAgrees(typed, boxed)) << "seed " << seed << ", expression " << i << ": " << Describe(expr);
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
    auto const typed = RunCompiled(*program, frame_, &evaluator, &parameters_);
    if (!typed.refused) ++answered;
    EXPECT_TRUE(CompiledAgrees(typed, boxed)) << "seed " << seed << ", expression " << i << ": " << Describe(expr);
  }
  std::cerr << "answered " << answered << " of 4000 on an integer frame\n";
  EXPECT_GT(answered, 0) << "nothing ran, so nothing was really compared";
}
