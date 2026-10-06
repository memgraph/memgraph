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

#include <cstdint>
#include <optional>
#include <vector>

#include "query/frontend/ast/ast.hpp"
#include "query/parameters.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::query {

class Frame;

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
  virtual storage::PropertyValue ReadProperty(TypedValue const &record, PropertyIx const &property) = 0;

  /// Nothing when the record is null, which makes the test null. Throws what
  /// the ordinary evaluator throws when the record is not a node.
  virtual std::optional<bool> TestLabels(TypedValue const &record, LabelsTest &test) = 0;
};

class TypedProgram {
 public:
  /// Three-valued, because a null operand makes a predicate null, with a
  /// fourth state for a guess that turned out wrong.
  enum class Answer : int8_t { False = 0, True = 1, Null = 2, Refused = 3 };

  /// Compiles the expression, or gives nothing back when it holds something
  /// this does not cover. Giving nothing back is always safe: it means the
  /// caller evaluates the expression the ordinary way.
  /// `refused_on`, when given, is left pointing at the node that was not
  /// covered, which is what says which expression to teach it next.
  static std::optional<TypedProgram> Compile(Expression *expression, Expression **refused_on = nullptr);

  /// Answers for one row, or refuses it when a value was not the type the
  /// guess settled on.
  /// `source` and `parameters` may be null when no instruction needs them; a
  /// program that reads one without it refuses the row rather than guessing.
  Answer Run(Frame const &frame, RecordReader *reader = nullptr, Parameters const *parameters = nullptr) const;

  /// How many integer and three-valued working slots a run needs.
  size_t IntSlots() const { return int_slots_; }

  size_t TriSlots() const { return tri_slots_; }

 private:
  enum class Op : uint8_t {
    TestLabels,    // on a record from the frame, answered by the reader
    LoadInt,       // from the frame, checking it really is one
    LoadPropInt,   // from a record on the frame, checking the same
    LoadParamInt,  // from the query's parameters, bound once per execution
    ConstInt,      // from the expression itself, so never in doubt
    AddInt,
    SubInt,
    MulInt,
    EqInt,
    NeInt,
    LtInt,
    GtInt,
    LeInt,
    GeInt,
    AndTri,
    OrTri,
    NotTri,
    CopyTri,
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
    PropertyIx property;
    LabelsTest *labels{nullptr};
  };

  std::vector<Instr> code_;
  size_t int_slots_{0};
  size_t tri_slots_{0};
  int32_t result_{0};

  friend class TypedProgramBuilder;
};

}  // namespace memgraph::query
