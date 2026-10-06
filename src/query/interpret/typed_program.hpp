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
#include "query/interpret/frame.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::query {

/// One expression compiled to work on values that are not boxed. Which type
/// each operand will hold is guessed when the program is built and checked
/// every time it is run, so a wrong guess costs a refusal rather than a wrong
/// answer.
///
/// A program is built once for a plan and read by every execution of it, so it
/// holds nothing that changes while it runs. The working values live on the
/// frame, which belongs to one execution.
/// What a typed program needs beyond the frame: the value of a property on a
/// vertex or an edge. Reading one involves a view, a permission check, and the
/// handling of a record that has been deleted, all of which already exist on
/// the evaluator, so a program asks rather than repeats it.
class PropertySource {
 public:
  PropertySource() = default;
  PropertySource(PropertySource const &) = default;
  PropertySource(PropertySource &&) = default;
  PropertySource &operator=(PropertySource const &) = default;
  PropertySource &operator=(PropertySource &&) = default;
  virtual ~PropertySource() = default;

  /// Null when the record has no such property, or when it may not be read.
  /// Throws what the ordinary evaluator throws for a record that is gone.
  virtual storage::PropertyValue ReadProperty(TypedValue const &record, PropertyIx const &property) = 0;
};

class TypedProgram {
 public:
  /// Three-valued, because a null operand makes a predicate null, with a
  /// fourth state for a guess that turned out wrong.
  enum class Answer : int8_t { False = 0, True = 1, Null = 2, Refused = 3 };

  /// Compiles the expression, or gives nothing back when it holds something
  /// this does not cover. Giving nothing back is always safe: it means the
  /// caller evaluates the expression the ordinary way.
  static std::optional<TypedProgram> Compile(Expression *expression);

  /// Answers for one row, or refuses it when a value was not the type the
  /// guess settled on.
  /// `source` may be null when no instruction needs one; a program that reads
  /// a property without one refuses the row rather than guessing.
  Answer Run(Frame const &frame, PropertySource *source = nullptr) const;

  /// How many integer and three-valued working slots a run needs.
  size_t IntSlots() const { return int_slots_; }

  size_t TriSlots() const { return tri_slots_; }

 private:
  enum class Op : uint8_t {
    LoadInt,      // from the frame, checking it really is one
    LoadPropInt,  // from a record on the frame, checking the same
    ConstInt,     // from the expression itself, so never in doubt
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
  };

  struct Instr {
    Op op;
    int32_t dst;
    int32_t a;
    int32_t b;
    int64_t literal;
    PropertyIx property;
  };

  std::vector<Instr> code_;
  size_t int_slots_{0};
  size_t tri_slots_{0};
  int32_t result_{0};

  friend class TypedProgramBuilder;
};

}  // namespace memgraph::query
