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

/// @file
#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <vector>

#include "query/frontend/ast/ast.hpp"
#include "query/parameters.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::query {

class Frame;

/// Three-valued, because a null operand makes a predicate null, with a fourth
/// state for a value that is no truth value at all. The fourth is what a caller
/// cannot act on, so it hands the row back.
enum class Truth : int8_t { False = 0, True = 1, Null = 2, Refused = 3 };

/// One expression compiled to work on values that are not boxed. Which type
/// each operand will hold is guessed when the program is built and checked
/// every time it is run, so a wrong guess costs a refusal rather than a wrong
/// answer.
///
/// A program is built once for a plan and read by every execution of it, so it
/// holds nothing that changes while it runs. The working values live on the
/// frame, which belongs to one execution.
/// What a typed program needs beyond the frame: what a vertex or an edge says.
/// Answering involves a view, a permission check, and the handling of a record
/// that has been deleted, all of which already exist on the evaluator, so a
/// program asks rather than repeats it. What the program saves is the value
/// built around the answer, not the work of finding it.
class RecordReader {
 public:
  RecordReader() = default;
  RecordReader(RecordReader const &) = default;
  RecordReader(RecordReader &&) = default;
  RecordReader &operator=(RecordReader const &) = default;
  RecordReader &operator=(RecordReader &&) = default;
  virtual ~RecordReader() = default;

  /// Null when the record has no such property, or when it may not be read.
  /// Throws what the ordinary evaluator throws for a record that is gone.
  /// The property is named by its place in the query's table of them, which is
  /// all a read needs: an instruction that carried the name too would be half
  /// a string wide, and every row walks every instruction.
  virtual storage::PropertyValue ReadProperty(TypedValue const &record, int64_t property_ix) = 0;

  /// Reads an integer property without a value being built around it. Nothing
  /// when the property is missing, which makes a comparison against it null;
  /// `refused` when it is there and is not an integer, which is a guess the
  /// program got wrong.
  virtual std::optional<int64_t> ReadIntProperty(TypedValue const &record, int64_t property_ix, bool &refused) = 0;

  /// Nothing when the record is null, which makes the test null. Throws what
  /// the ordinary evaluator throws when the record is not a node.
  virtual std::optional<bool> TestLabels(TypedValue const &record, LabelsTest &test) = 0;

  /// Evaluates an expression a program does not cover and reads it as a truth
  /// value. Refused when it is no truth value, which the evaluator would have
  /// complained about in words the caller still has to produce.
  virtual Truth EvaluateTruth(Expression &expression) = 0;

  /// Evaluates an expression a program does not cover and reads it as a local
  /// date time, in the microseconds its ordering is defined on. Nothing when it
  /// is null or is no local date time at all; the caller tells those apart by
  /// asking first whether the expression was null.
  virtual std::optional<int64_t> EvaluateLocalDateTime(Expression &expression, bool &was_null) = 0;
};

class TypedProgram {
 public:
  /// A guess that turned out wrong refuses the row, the same as a value that
  /// is no truth value.
  using Answer = Truth;

  /// Compiles the expression, or gives nothing back when it holds something
  /// this does not cover. Giving nothing back is always safe: it means the
  /// caller evaluates the expression the ordinary way.
  /// `refused_on`, when given, is left pointing at the node that was not
  /// covered, which is what says which expression to teach it next.
  static std::optional<TypedProgram> Compile(Expression *expression, Expression **refused_on = nullptr);

  /// Compiles an expression that leaves a value rather than an answer, which is
  /// what the places that write a row rather than keep or drop one need.
  static std::optional<TypedProgram> CompileValue(Expression *expression, Expression **refused_on = nullptr);

  /// Answers for one row, or refuses it when a value was not the type the
  /// guess settled on.
  /// `source` and `parameters` may be null when no instruction needs them; a
  /// program that reads one without it refuses the row rather than guessing.
  Answer Run(Frame const &frame, RecordReader *reader = nullptr, Parameters const *parameters = nullptr) const;

  /// Writes what the program computes into `out`. False means a guard refused,
  /// and `out` is left as it was, so the caller evaluates the expression the
  /// ordinary way. A missing operand is written as null rather than refused.
  bool RunInto(Frame const &frame, TypedValue &out, RecordReader *reader = nullptr,
               Parameters const *parameters = nullptr) const;

  /// How many of the instructions hand an expression back to the evaluator.
  size_t DelegatedOps() const;

  /// Whether any instruction works on a value that is not boxed. A program
  /// whose every leaf is handed back to the evaluator saves nothing and costs
  /// a walk over itself, so a caller is better off without it.
  bool WorthRunning() const;

  /// How many integer and three-valued working slots a run needs.
  size_t IntSlots() const { return int_slots_; }

  size_t TriSlots() const { return tri_slots_; }

 private:
  enum class Op : uint8_t {
    TestLabels,    // on a record from the frame, answered by the reader
    LoadInt,       // from the frame, checking it really is one
    LoadPropInt,   // from a record on the frame, checking the same
    LoadParamInt,  // from the query's parameters, bound once per execution
    // A local date time is held as the microseconds its ordering is defined on,
    // so once loaded it is compared exactly as an integer is.
    LoadTime,
    LoadPropTime,
    EvalTime,
    ConstInt,  // from the expression itself, so never in doubt
    // A property compared with something the query names. This is what almost
    // every filter is, and running it as one instruction keeps the loop that
    // walks a program from being most of the cost of a short one.
    PropCmpConst,
    PropCmpParam,
    AddInt,
    SubInt,
    MulInt,
    DivInt,
    EqInt,
    NeInt,
    LtInt,
    GtInt,
    LeInt,
    GeInt,
    AndTri,
    OrTri,
    NotTri,
    // Whether a value is missing. This always answers, where a comparison
    // against a missing value does not.
    IsNullInt,
    IsNullTri,
    CopyTri,
    /// An expression this does not cover, read as a truth value by the
    /// evaluator. One conjunct it cannot take no longer refuses the rest.
    EvalTri,
    // The right side of a conjunction is not evaluated when the left side
    // settles it, which is what keeps a reader that would throw out of reach.
    JumpIfFalseTri,
    JumpIfTrueTri,
  };

  struct Instr {
    Op op;
    int32_t dst;
    int32_t a;
    int32_t b;
    int64_t literal;
    int64_t property_ix;
    LabelsTest *labels{nullptr};
    Expression *delegated{nullptr};
  };

  /// Whether the program's result is an answer or an integer. Which one a
  /// caller wants is settled when it compiles, not when it runs.
  enum class Shape : uint8_t { Predicate, Integer };

  /// Runs the code and leaves the slots behind for whichever result is wanted.
  /// Only what says whether a slot holds anything starts out cleared: every
  /// value is written by the instruction that produces it before any
  /// instruction reads it, and clearing all of them costs a row more than the
  /// work the row came to do.
  struct Slots {
    Slots() { int_known.fill(0); }

    std::array<int64_t, 64> ints;
    std::array<char, 64> int_known;
    std::array<Answer, 64> tris;
  };

  bool Execute(Frame const &frame, RecordReader *reader, Parameters const *parameters, Slots &slots) const;

  /// The instructions that come up rarely, kept out of the loop that runs the
  /// common ones. Every row walks the loop, so what sits in it is what decides
  /// how much of the instruction cache the loop needs.
  bool RareOp(Instr const &in, Frame const &frame, RecordReader *reader, Slots &slots) const;

  std::vector<Instr> code_;
  Shape shape_{Shape::Predicate};
  size_t int_slots_{0};
  size_t tri_slots_{0};
  int32_t result_{0};

  friend class TypedProgramBuilder;
};

}  // namespace memgraph::query
