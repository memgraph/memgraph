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

#include "query/interpret/typed_program.hpp"

#include "query/interpret/eval.hpp"
#include "query/interpret/frame.hpp"

#include <algorithm>
#include <bit>
#include <compare>
#include <limits>

#include "utils/typeinfo.hpp"
#include "value_order/numbers.hpp"

namespace memgraph::query {

namespace {

/// Where a compiled operand left its answer, and in what form. An integer and
/// a truth value are kept apart because they are held differently and the
/// operators that take them do not overlap.
struct Operand {
  bool is_tri;
  int32_t slot;
};

/// Where an instruction's property path sits among all of them.
struct PathRef {
  int32_t at{0};
  int32_t len{0};
};

/// What an integer slot is holding. Both are int64, and a comparison of two of
/// them is the same instruction either way; the kind decides only what a load
/// will accept and what it takes out of it. Mixing the two is refused, since
/// comparing a number with a time is an error rather than an answer.
enum class Kind : uint8_t { Number, Time };

/// Whether an expression says, by itself, that it is a time. A constructor does;
/// a property or a parameter could be anything, and is read as whatever the
/// other side of the comparison turned out to be.
bool NamesATime(Expression *expression) {
  if (expression == nullptr) return false;
  if (expression->GetTypeInfo().id != utils::TypeId::AST_FUNCTION) return false;
  return static_cast<Function *>(expression)->function_name_ == "LOCALDATETIME";
}

}  // namespace

/// Walks the expression once, handing out working slots and emitting the
/// instructions that fill them. Refuses anything it does not cover, which
/// leaves that expression to the ordinary evaluator.
class TypedProgramBuilder {
 public:
  std::optional<Operand> Build(Expression *expression, Kind kind = Kind::Number) {
    auto const refuse = [&] { return Refuse(expression); };
    switch (expression->GetTypeInfo().id) {
      case utils::TypeId::AST_PRIMITIVE_LITERAL: {
        auto const &value = static_cast<PrimitiveLiteral *>(expression)->value_;
        if (value.IsInt()) {
          auto const slot = NextInt();
          Emit(TypedProgram::Op::ConstInt, slot, 0, 0, value.ValueInt());
          return Operand{.is_tri = false, .slot = slot};
        }
        if (value.IsDouble()) {
          auto const slot = NextInt();
          Emit(TypedProgram::Op::ConstDouble, slot, 0, 0, std::bit_cast<int64_t>(value.ValueDouble()));
          return Operand{.is_tri = false, .slot = slot};
        }
        return refuse();
      }
      case utils::TypeId::AST_IDENTIFIER: {
        auto const position = static_cast<Identifier *>(expression)->symbol_pos_;
        if (position < 0) return refuse();
        auto const slot = NextInt();
        Emit(kind == Kind::Time ? TypedProgram::Op::LoadTime : TypedProgram::Op::LoadInt, slot, position, 0, 0);
        return Operand{.is_tri = false, .slot = slot};
      }
      case utils::TypeId::AST_PARAMETER_LOOKUP: {
        // A parameter bound to a time is read through the evaluator, which is
        // the one place that knows how a bound value becomes one.
        if (kind == Kind::Time) {
          auto const slot = NextInt();
          Emit(TypedProgram::Op::EvalTime, slot, 0, 0, 0, {}, nullptr, expression);
          return Operand{.is_tri = false, .slot = slot};
        }
        auto const position = static_cast<ParameterLookup *>(expression)->token_position_;
        auto const slot = NextInt();
        Emit(TypedProgram::Op::LoadParamInt, slot, position, 0, 0);
        return Operand{.is_tri = false, .slot = slot};
      }
      case utils::TypeId::AST_FUNCTION: {
        // The one call taken, and only where a time is what is wanted. Its
        // arguments are left to the evaluator, which is what builds the time.
        if (kind != Kind::Time || !NamesATime(expression)) return refuse();
        auto const slot = NextInt();
        Emit(TypedProgram::Op::EvalTime, slot, 0, 0, 0, {}, nullptr, expression);
        return Operand{.is_tri = false, .slot = slot};
      }
      case utils::TypeId::AST_LABELS_TEST: {
        auto *test = static_cast<LabelsTest *>(expression);
        // Only a test on a record read straight off the frame. Anything else
        // would have to evaluate its subject first, which is the work this
        // avoids.
        if (test->expression_ == nullptr) return refuse();
        if (test->expression_->GetTypeInfo().id != utils::TypeId::AST_IDENTIFIER) return refuse();
        auto const position = static_cast<Identifier *>(test->expression_)->symbol_pos_;
        if (position < 0) return refuse();
        auto const slot = NextTri();
        Emit(TypedProgram::Op::TestLabels, slot, position, 0, 0, {}, test);
        return Operand{.is_tri = true, .slot = slot};
      }
      case utils::TypeId::AST_PROPERTY_LOOKUP: {
        std::vector<int32_t> path;
        auto const position = PropertyChain(expression, path);
        if (!position) return refuse();
        auto const slot = NextInt();
        Emit(kind == Kind::Time ? TypedProgram::Op::LoadPropTime : TypedProgram::Op::LoadPropInt,
             slot,
             *position,
             0,
             0,
             AddPath(path));
        return Operand{.is_tri = false, .slot = slot};
      }
      case utils::TypeId::AST_ADDITION_OPERATOR:
        return Arithmetic(expression, TypedProgram::Op::AddInt);
      case utils::TypeId::AST_SUBTRACTION_OPERATOR:
        return Arithmetic(expression, TypedProgram::Op::SubInt);
      case utils::TypeId::AST_MULTIPLICATION_OPERATOR:
        return Arithmetic(expression, TypedProgram::Op::MulInt);
      case utils::TypeId::AST_DIVISION_OPERATOR:
        return Arithmetic(expression, TypedProgram::Op::DivInt);
      case utils::TypeId::AST_EQUAL_OPERATOR:
        return Comparison(expression, TypedProgram::Op::EqInt);
      case utils::TypeId::AST_NOT_EQUAL_OPERATOR:
        return Comparison(expression, TypedProgram::Op::NeInt);
      case utils::TypeId::AST_LESS_OPERATOR:
        return Comparison(expression, TypedProgram::Op::LtInt);
      case utils::TypeId::AST_GREATER_OPERATOR:
        return Comparison(expression, TypedProgram::Op::GtInt);
      case utils::TypeId::AST_LESS_EQUAL_OPERATOR:
        return Comparison(expression, TypedProgram::Op::LeInt);
      case utils::TypeId::AST_GREATER_EQUAL_OPERATOR:
        return Comparison(expression, TypedProgram::Op::GeInt);
      case utils::TypeId::AST_AND_OPERATOR:
        return Logical(expression, TypedProgram::Op::AndTri);
      case utils::TypeId::AST_RANGE_OPERATOR:
        // A chained comparison evaluates both sides whatever the first says,
        // so it takes no jump over the second.
        return Conjoin(expression,
                       static_cast<RangeOperator *>(expression)->expression1_,
                       static_cast<RangeOperator *>(expression)->expression2_);
      case utils::TypeId::AST_OR_OPERATOR:
        return Logical(expression, TypedProgram::Op::OrTri);
      case utils::TypeId::AST_IS_NULL_OPERATOR: {
        auto *op = static_cast<IsNullOperator *>(expression);
        auto const operand = Build(op->expression_);
        if (!operand) return Refuse(expression);
        auto const slot = NextTri();
        Emit(operand->is_tri ? TypedProgram::Op::IsNullTri : TypedProgram::Op::IsNullInt, slot, operand->slot, 0, 0);
        return Operand{.is_tri = true, .slot = slot};
      }
      case utils::TypeId::AST_NOT_OPERATOR: {
        auto *op = static_cast<NotOperator *>(expression);
        auto const operand = BuildTri(op->expression_);
        if (!operand) return refuse();
        auto const slot = NextTri();
        Emit(TypedProgram::Op::NotTri, slot, operand->slot, 0, 0);
        return Operand{.is_tri = true, .slot = slot};
      }
      default:
        return refuse();
    }
  }

  TypedProgram Finish(Operand root) {
    TypedProgram program;
    program.shape_ = root.is_tri ? TypedProgram::Shape::Predicate : TypedProgram::Shape::Integer;
    program.code_ = std::move(code_);
    program.paths_ = std::move(paths_);
    program.int_slots_ = int_slots_;
    program.tri_slots_ = tri_slots_;
    program.result_ = root.slot;
    return program;
  }

 private:
  std::optional<Operand> Arithmetic(Expression *expression, TypedProgram::Op op) {
    auto *binary = static_cast<BinaryOperator *>(expression);
    auto const lhs = Build(binary->expression1_);
    if (!lhs || lhs->is_tri) return Refuse(expression);
    auto const rhs = Build(binary->expression2_);
    if (!rhs || rhs->is_tri) return Refuse(expression);
    auto const slot = NextInt();
    Emit(op, slot, lhs->slot, rhs->slot, 0);
    return Operand{.is_tri = false, .slot = slot};
  }

  /// Reads a lookup, or a chain of them, back to the record it starts from.
  /// `a.b.c` parses as a lookup of `c` on a lookup of `b` on `a`, so walking
  /// down to the identifier and collecting each property on the way back up
  /// gives the path from the record to the value.
  ///
  /// Returns the record's place on the frame, and leaves the path in `path`.
  /// Nothing when the chain does not start at a record, or reaches further in
  /// than a read will go.
  static std::optional<int32_t> PropertyChain(Expression *expression, std::vector<int32_t> &path) {
    if (expression == nullptr) return std::nullopt;
    if (expression->GetTypeInfo().id != utils::TypeId::AST_PROPERTY_LOOKUP) return std::nullopt;
    auto *lookup = static_cast<PropertyLookup *>(expression);
    // A lookup that takes every property is a map rather than a value, and a
    // path of more than one here is a target of SET or REMOVE rather than
    // something read. Both are left to the evaluator.
    if (lookup->evaluation_mode_ != PropertyLookup::EvaluationMode::GET_OWN_PROPERTY) return std::nullopt;
    if (lookup->property_path_.size() != 1) return std::nullopt;
    if (lookup->expression_ == nullptr) return std::nullopt;

    if (lookup->expression_->GetTypeInfo().id == utils::TypeId::AST_IDENTIFIER) {
      auto const position = static_cast<Identifier *>(lookup->expression_)->symbol_pos_;
      if (position < 0) return std::nullopt;
      path.push_back(static_cast<int32_t>(lookup->property_.ix));
      return position;
    }

    auto const position = PropertyChain(lookup->expression_, path);
    if (!position) return std::nullopt;
    if (path.size() >= kMaxPathDepth) return std::nullopt;
    path.push_back(static_cast<int32_t>(lookup->property_.ix));
    return position;
  }

  /// Emits the whole of a property compared with a literal or a parameter as
  /// one instruction, which is the shape most filters have.
  std::optional<Operand> FusedComparison(Expression *left, Expression *right, TypedProgram::Op op) {
    std::vector<int32_t> path;
    auto const position = PropertyChain(left, path);
    if (!position) return std::nullopt;

    auto const kind = static_cast<int32_t>(op);
    if (right->GetTypeInfo().id == utils::TypeId::AST_PRIMITIVE_LITERAL) {
      auto const &value = static_cast<PrimitiveLiteral *>(right)->value_;
      if (!value.IsInt()) return std::nullopt;
      auto const slot = NextTri();
      Emit(TypedProgram::Op::PropCmpConst, slot, *position, kind, value.ValueInt(), AddPath(path));
      return Operand{.is_tri = true, .slot = slot};
    }
    if (right->GetTypeInfo().id == utils::TypeId::AST_PARAMETER_LOOKUP) {
      auto const token = static_cast<ParameterLookup *>(right)->token_position_;
      auto const slot = NextTri();
      Emit(TypedProgram::Op::PropCmpParam, slot, *position, kind, token, AddPath(path));
      return Operand{.is_tri = true, .slot = slot};
    }
    return std::nullopt;
  }

  std::optional<Operand> Comparison(Expression *expression, TypedProgram::Op op) {
    auto *binary = static_cast<BinaryOperator *>(expression);
    auto const kind = NamesATime(binary->expression1_) || NamesATime(binary->expression2_) ? Kind::Time : Kind::Number;
    if (kind != Kind::Time) {
      if (auto const fused = FusedComparison(binary->expression1_, binary->expression2_, op)) return fused;
    }
    auto const lhs = Build(binary->expression1_, kind);
    if (!lhs || lhs->is_tri) return Refuse(expression);
    auto const rhs = Build(binary->expression2_, kind);
    if (!rhs || rhs->is_tri) return Refuse(expression);
    auto const slot = NextTri();
    Emit(op, slot, lhs->slot, rhs->slot, 0);
    return Operand{.is_tri = true, .slot = slot};
  }

  /// Both sides into one answer, with nothing skipped.
  std::optional<Operand> Conjoin(Expression *expression, Expression *left, Expression *right) {
    auto const lhs = BuildTri(left);
    if (!lhs) return Refuse(expression);
    auto const rhs = BuildTri(right);
    if (!rhs) return Refuse(expression);
    auto const slot = NextTri();
    Emit(TypedProgram::Op::AndTri, slot, lhs->slot, rhs->slot, 0);
    return Operand{.is_tri = true, .slot = slot};
  }

  /// Emits the left side, then a jump over the right side for the value that
  /// settles the answer on its own, so the right side is only reached when the
  /// evaluator would reach it too.
  std::optional<Operand> Logical(Expression *expression, TypedProgram::Op op) {
    auto *binary = static_cast<BinaryOperator *>(expression);
    auto const lhs = BuildTri(binary->expression1_);
    if (!lhs) return Refuse(expression);

    auto const slot = NextTri();
    Emit(TypedProgram::Op::CopyTri, slot, lhs->slot, 0, 0);
    auto const jump = code_.size();
    Emit(op == TypedProgram::Op::AndTri ? TypedProgram::Op::JumpIfFalseTri : TypedProgram::Op::JumpIfTrueTri,
         0,
         lhs->slot,
         0,
         0);

    auto const rhs = BuildTri(binary->expression2_);
    if (!rhs) return Refuse(expression);
    Emit(op, slot, lhs->slot, rhs->slot, 0);
    code_[jump].b = static_cast<int32_t>(code_.size());
    return Operand{.is_tri = true, .slot = slot};
  }

  /// The first node to stop the walk is the one to report: the ones above it
  /// only refused because it did.
  std::nullopt_t Refuse(Expression *expression) {
    if (refused_on_ == nullptr) refused_on_ = expression;
    return std::nullopt;
  }

  /// Builds the operand, and where that fails takes it as a truth value the
  /// evaluator will supply. Only sound where a truth value is what the
  /// operator wants, which is why it is not the default everywhere.
  std::optional<Operand> BuildTri(Expression *expression) {
    if (auto const operand = Build(expression); operand && operand->is_tri) return operand;
    // The walk may have recorded why it stopped; it no longer stops here.
    refused_on_ = nullptr;
    auto const slot = NextTri();
    Emit(TypedProgram::Op::EvalTri, slot, 0, 0, 0, {}, nullptr, expression);
    return Operand{.is_tri = true, .slot = slot};
  }

  int32_t NextInt() { return static_cast<int32_t>(int_slots_++); }

  int32_t NextTri() { return static_cast<int32_t>(tri_slots_++); }

  void Emit(TypedProgram::Op op, int32_t dst, int32_t a, int32_t b, int64_t literal, PathRef path = {},
            LabelsTest *labels = nullptr, Expression *delegated = nullptr) {
    code_.push_back(TypedProgram::Instr{.op = op,
                                        .dst = dst,
                                        .a = a,
                                        .b = b,
                                        .literal = literal,
                                        .path_at = path.at,
                                        .path_len = path.len,
                                        .labels = labels,
                                        .delegated = delegated});
  }

  /// Lays a path down end to end with the others and says where it went.
  PathRef AddPath(std::vector<int32_t> const &path) {
    auto const at = static_cast<int32_t>(paths_.size());
    paths_.insert(paths_.end(), path.begin(), path.end());
    return PathRef{.at = at, .len = static_cast<int32_t>(path.size())};
  }

  std::vector<TypedProgram::Instr> code_;
  std::vector<int32_t> paths_;
  size_t int_slots_{0};
  size_t tri_slots_{0};

 public:
  Expression *refused_on_{nullptr};
};

bool TypedProgram::WorthRunning() const {
  return std::ranges::any_of(code_, [](Instr const &in) {
    switch (in.op) {
      // Scaffolding, and the escape back to the evaluator. On their own these
      // only arrange answers the evaluator produced.
      case Op::EvalTri:
      case Op::CopyTri:
      case Op::JumpIfFalseTri:
      case Op::JumpIfTrueTri:
      case Op::AndTri:
      case Op::OrTri:
      case Op::NotTri:
        return false;
      default:
        return true;
    }
  });
}

size_t TypedProgram::DelegatedOps() const {
  return static_cast<size_t>(std::ranges::count_if(code_, [](Instr const &in) { return in.op == Op::EvalTri; }));
}

std::optional<TypedProgram> TypedProgram::CompileValue(Expression *expression, Expression **refused_on) {
  if (expression == nullptr) return std::nullopt;
  TypedProgramBuilder builder;
  auto const root = builder.Build(expression);
  if (!root) {
    if (refused_on != nullptr) {
      *refused_on = builder.refused_on_ != nullptr ? builder.refused_on_ : expression;
    }
    return std::nullopt;
  }
  return builder.Finish(*root);
}

std::optional<TypedProgram> TypedProgram::Compile(Expression *expression, Expression **refused_on) {
  if (expression == nullptr) return std::nullopt;
  TypedProgramBuilder builder;
  auto const root = builder.Build(expression);
  // Only a predicate is worth compiling: the callers that run one per row want
  // a yes or no, and anything else would have to be boxed on the way out.
  if (!root || !root->is_tri) {
    if (refused_on != nullptr) {
      *refused_on = builder.refused_on_ != nullptr ? builder.refused_on_ : expression;
    }
    return std::nullopt;
  }
  auto program = builder.Finish(*root);
  if (!program.WorthRunning()) {
    if (refused_on != nullptr) *refused_on = expression;
    return std::nullopt;
  }
  return program;
}

/// How one slot stands against another, for the six comparisons a program has.
///
/// This follows what the evaluator's relations do and must keep following it:
/// a pair of unlike types has no order, so the four ordered comparisons answer
/// null for it, while equality answers false and inequality true. Two numbers
/// of unlike width are the one pair that is placed, and placing them is not a
/// cast: a double outside the integers, and an integer too wide to be carried
/// by one, are each decided without one. That is why the arithmetic is taken
/// from where the store and the evaluator both take it rather than written
/// again here.
///
/// A NaN stands against nothing, itself included, which the comparisons below
/// give for free.
Truth TypedProgram::CompareSlots(Op op, SlotKind left_kind, int64_t left, SlotKind right_kind, int64_t right) {
  auto const answer = [](bool held) { return held ? Truth::True : Truth::False; };

  if (left_kind == SlotKind::Int && right_kind == SlotKind::Int) {
    switch (op) {
      case Op::EqInt:
        return answer(left == right);
      case Op::NeInt:
        return answer(left != right);
      case Op::LtInt:
        return answer(left < right);
      case Op::GtInt:
        return answer(left > right);
      case Op::LeInt:
        return answer(left <= right);
      default:
        return answer(left >= right);
    }
  }

  if (left_kind == SlotKind::Double && right_kind == SlotKind::Double) {
    auto const x = std::bit_cast<double>(left);
    auto const y = std::bit_cast<double>(right);
    switch (op) {
      case Op::EqInt:
        return answer(x == y);
      case Op::NeInt:
        return answer(x != y);
      case Op::LtInt:
        return answer(x < y);
      case Op::GtInt:
        return answer(x > y);
      case Op::LeInt:
        return answer(x <= y);
      default:
        return answer(x >= y);
    }
  }

  if ((left_kind == SlotKind::Int && right_kind == SlotKind::Double) ||
      (left_kind == SlotKind::Double && right_kind == SlotKind::Int)) {
    auto const placed =
        left_kind == SlotKind::Int
            ? value_order::PlaceIntegerAgainstDouble(left, std::bit_cast<double>(right))
            : value_order::ReversedOrder(value_order::PlaceIntegerAgainstDouble(right, std::bit_cast<double>(left)));
    switch (op) {
      case Op::EqInt:
        return answer(std::is_eq(placed));
      case Op::NeInt:
        return answer(!std::is_eq(placed));
      case Op::LtInt:
        return answer(std::is_lt(placed));
      case Op::GtInt:
        return answer(std::is_gt(placed));
      case Op::LeInt:
        return answer(std::is_lteq(placed));
      default:
        return answer(std::is_gteq(placed));
    }
  }

  // Unlike types. Equality says they are not the same thing; nothing orders
  // them.
  if (op == Op::EqInt) return Truth::False;
  if (op == Op::NeInt) return Truth::True;
  return Truth::Null;
}

bool TypedProgram::Execute(Frame const &frame, ExpressionEvaluator *reader, Parameters const *parameters,
                           Slots &slots) const {
  // Small enough to sit on the stack for the expressions this covers; a bigger
  // one would take these from the frame alongside the other working values.
  constexpr size_t kMaxSlots = 64;
  if (int_slots_ > kMaxSlots || tri_slots_ > kMaxSlots) return false;

  auto &ints = slots.ints;
  auto &kinds = slots.kinds;
  auto &tris = slots.tris;

  // A comparison of which either side is missing is null rather than false.
  //
  // Two integers are what almost every comparison is, and deciding them here
  // keeps the loop every row walks from calling out for them. Everything else
  // the relation settles.
  auto compare = [&](Op op, int32_t a, int32_t b) {
    if (kinds[a] == SlotKind::Unknown || kinds[b] == SlotKind::Unknown) return Answer::Null;
    if (kinds[a] == SlotKind::Int && kinds[b] == SlotKind::Int) [[likely]] {
      auto const x = ints[a];
      auto const y = ints[b];
      bool held = false;
      switch (op) {
        case Op::EqInt:
          held = x == y;
          break;
        case Op::NeInt:
          held = x != y;
          break;
        case Op::LtInt:
          held = x < y;
          break;
        case Op::GtInt:
          held = x > y;
          break;
        case Op::LeInt:
          held = x <= y;
          break;
        default:
          held = x >= y;
      }
      return held ? Answer::True : Answer::False;
    }
    return CompareSlots(op, kinds[a], ints[a], kinds[b], ints[b]);
  };

  // Walked by pointer rather than by index. The body calls out through the
  // reader, which the compiler must assume could change the program, so an
  // index costs the length and the address of the instruction to be worked out
  // again every time round, and the length is a division by the size of one.
  auto const *const first = code_.data();
  auto const *const limit = first + code_.size();
  // Threaded dispatch: every instruction jumps straight to the next one's
  // code rather than back to a loop that works out where to go. The address of
  // a label is a GNU extension, which both compilers this is built with have.
  static void *const kDispatch[] = {
      &&op_TestLabels, &&op_LoadInt,   &&op_LoadPropInt, &&op_LoadParamInt, &&op_LoadTime,       &&op_LoadPropTime,
      &&op_EvalTime,   &&op_ConstInt,  &&op_ConstDouble, &&op_PropCmpConst, &&op_PropCmpParam,   &&op_AddInt,
      &&op_SubInt,     &&op_MulInt,    &&op_DivInt,      &&op_EqInt,        &&op_NeInt,          &&op_LtInt,
      &&op_GtInt,      &&op_LeInt,     &&op_GeInt,       &&op_AndTri,       &&op_OrTri,          &&op_NotTri,
      &&op_IsNullInt,  &&op_IsNullTri, &&op_CopyTri,     &&op_EvalTri,      &&op_JumpIfFalseTri, &&op_JumpIfTrueTri,
  };
#define MG_NEXT()                                    \
  do {                                               \
    if (++step == limit) goto finished;              \
    goto *kDispatch[static_cast<uint8_t>(step->op)]; \
  } while (0)

  if (first == limit) return true;
  auto const *step = first;
  goto *kDispatch[static_cast<uint8_t>(step->op)];

op_ConstInt:
  ints[step->dst] = step->literal;
  kinds[step->dst] = SlotKind::Int;
  MG_NEXT();
op_ConstDouble:
  ints[step->dst] = step->literal;
  kinds[step->dst] = SlotKind::Double;
  MG_NEXT();
op_LoadInt: {
  auto const &value = frame.elems()[step->a];
  if (value.IsInt()) {
    ints[step->dst] = value.UnsafeValueInt();
    kinds[step->dst] = SlotKind::Int;
  } else if (value.IsDouble()) {
    ints[step->dst] = std::bit_cast<int64_t>(value.UnsafeValueDouble());
    kinds[step->dst] = SlotKind::Double;
  } else if (value.IsNull()) {
    kinds[step->dst] = SlotKind::Unknown;
  } else {
    // Not what the guess settled on, so this row is not ours.
    return false;
  }
  MG_NEXT();
}
op_LoadParamInt: {
  if (parameters == nullptr) return false;
  // A position with nothing bound to it belongs to the evaluator, which
  // decides what an unbound parameter means.
  auto const *value = parameters->FindAtTokenPosition(step->a);
  if (value == nullptr) return false;
  if (value->IsDouble()) {
    ints[step->dst] = std::bit_cast<int64_t>(value->ValueDouble());
    kinds[step->dst] = SlotKind::Double;
  } else if (value->IsInt()) {
    ints[step->dst] = value->ValueInt();
    kinds[step->dst] = SlotKind::Int;
  } else if (value->IsNull()) {
    kinds[step->dst] = SlotKind::Unknown;
  } else {
    return false;
  }
  MG_NEXT();
}
op_LoadPropInt: {
  if (reader == nullptr) return false;
  auto const &record = frame.elems()[step->a];
  // Only a record has properties; anything else was not what the guess
  // settled on.
  if (!record.IsVertex() && !record.IsEdge()) {
    if (record.IsNull()) {
      kinds[step->dst] = SlotKind::Unknown;
      MG_NEXT();
    }
    return false;
  }
  bool refused = false;
  auto const value = reader->ReadScalarProperty(record, PathOf(*step), refused, step->seen);
  // Written only when it changes: a property keeps its type, and the store
  // would otherwise be made for every row of every scan.
  if (step->seen != value.kind) step->seen = value.kind;
  if (refused) return false;
  ints[step->dst] = value.bits;
  kinds[step->dst] = value.kind;
  MG_NEXT();
}
op_TestLabels: {
  if (reader == nullptr) return false;
  auto const answer = reader->TestLabels(frame.elems()[step->a], *step->labels);
  tris[step->dst] = !answer ? Answer::Null : (*answer ? Answer::True : Answer::False);
  MG_NEXT();
}
op_LoadTime:
op_LoadPropTime:
op_EvalTime:
op_EvalTri:
op_DivInt:
op_IsNullInt:
op_IsNullTri:
  if (!RareOp(*step, frame, reader, slots)) return false;
  MG_NEXT();
op_AddInt:
op_SubInt:
op_MulInt: {
  if (kinds[step->a] == SlotKind::Double || kinds[step->b] == SlotKind::Double) return false;
  kinds[step->dst] =
      (kinds[step->a] == SlotKind::Int && kinds[step->b] == SlotKind::Int) ? SlotKind::Int : SlotKind::Unknown;
  if (kinds[step->dst] != SlotKind::Unknown) {
    auto const x = ints[step->a];
    auto const y = ints[step->b];
    ints[step->dst] = step->op == Op::AddInt ? x + y : (step->op == Op::SubInt ? x - y : x * y);
  }
  MG_NEXT();
}
op_PropCmpConst:
op_PropCmpParam: {
  if (reader == nullptr) return false;
  auto const &record = frame.elems()[step->a];
  if (!record.IsVertex() && !record.IsEdge()) {
    if (record.IsNull()) {
      tris[step->dst] = Answer::Null;
      MG_NEXT();
    }
    return false;
  }
  // The property is read before the other side is looked at, because that is
  // the order the evaluator works in and reading it is what can throw. Settling
  // the comparison from the other side first would answer where the evaluator
  // would have complained.
  bool refused = false;
  auto const value = reader->ReadScalarProperty(record, PathOf(*step), refused, step->seen);
  // Written only when it changes: a property keeps its type, and the store
  // would otherwise be made for every row of every scan.
  if (step->seen != value.kind) step->seen = value.kind;
  if (refused) return false;

  Scalar other{.kind = SlotKind::Int, .bits = step->literal};
  if (step->op == Op::PropCmpParam) {
    if (parameters == nullptr) return false;
    auto const *bound = parameters->FindAtTokenPosition(static_cast<int>(step->literal));
    if (bound == nullptr) return false;
    if (bound->IsNull()) {
      tris[step->dst] = Answer::Null;
      MG_NEXT();
    }
    if (bound->IsInt()) {
      other = {.kind = SlotKind::Int, .bits = bound->ValueInt()};
    } else if (bound->IsDouble()) {
      other = {.kind = SlotKind::Double, .bits = std::bit_cast<int64_t>(bound->ValueDouble())};
    } else {
      return false;
    }
  }
  if (value.kind == SlotKind::Unknown) {
    tris[step->dst] = Answer::Null;
    MG_NEXT();
  }
  auto const fused_op = static_cast<Op>(step->b);
  if (value.kind == SlotKind::Int && other.kind == SlotKind::Int) [[likely]] {
    auto const x = value.bits;
    auto const y = other.bits;
    bool held = false;
    switch (fused_op) {
      case Op::EqInt:
        held = x == y;
        break;
      case Op::NeInt:
        held = x != y;
        break;
      case Op::LtInt:
        held = x < y;
        break;
      case Op::GtInt:
        held = x > y;
        break;
      case Op::LeInt:
        held = x <= y;
        break;
      default:
        held = x >= y;
    }
    tris[step->dst] = held ? Answer::True : Answer::False;
    MG_NEXT();
  }
  tris[step->dst] = CompareSlots(fused_op, value.kind, value.bits, other.kind, other.bits);
  MG_NEXT();
}
op_EqInt:
  tris[step->dst] = compare(step->op, step->a, step->b);
  MG_NEXT();
op_NeInt:
  tris[step->dst] = compare(step->op, step->a, step->b);
  MG_NEXT();
op_LtInt:
  tris[step->dst] = compare(step->op, step->a, step->b);
  MG_NEXT();
op_GtInt:
  tris[step->dst] = compare(step->op, step->a, step->b);
  MG_NEXT();
op_LeInt:
  tris[step->dst] = compare(step->op, step->a, step->b);
  MG_NEXT();
op_GeInt:
  tris[step->dst] = compare(step->op, step->a, step->b);
  MG_NEXT();
op_AndTri: {
  // False beats null, which is what makes this three-valued rather than
  // a null that swallows everything.
  auto const x = tris[step->a];
  auto const y = tris[step->b];
  tris[step->dst] = (x == Answer::False || y == Answer::False)
                        ? Answer::False
                        : ((x == Answer::Null || y == Answer::Null) ? Answer::Null : Answer::True);
  MG_NEXT();
}
op_OrTri: {
  auto const x = tris[step->a];
  auto const y = tris[step->b];
  tris[step->dst] = (x == Answer::True || y == Answer::True)
                        ? Answer::True
                        : ((x == Answer::Null || y == Answer::Null) ? Answer::Null : Answer::False);
  MG_NEXT();
}
op_NotTri: {
  auto const x = tris[step->a];
  tris[step->dst] = x == Answer::Null ? Answer::Null : (x == Answer::True ? Answer::False : Answer::True);
  MG_NEXT();
}
op_CopyTri:
  tris[step->dst] = tris[step->a];
  MG_NEXT();
op_JumpIfFalseTri:
  if (tris[step->a] == Answer::False) step = first + step->b - 1;
  MG_NEXT();
op_JumpIfTrueTri:
  if (tris[step->a] == Answer::True) step = first + step->b - 1;
  MG_NEXT();
finished:
#undef MG_NEXT
  return true;
}

[[gnu::noinline]] bool TypedProgram::RareOp(Instr const &in, Frame const &frame, ExpressionEvaluator *reader,
                                            Slots &slots) const {
  auto &ints = slots.ints;
  auto &kinds = slots.kinds;
  auto &tris = slots.tris;
  switch (in.op) {
    case Op::LoadTime: {
      auto const &value = frame.elems()[in.a];
      if (value.IsLocalDateTime()) {
        ints[in.dst] = value.ValueLocalDateTime().SysMicrosecondsSinceEpoch();
        kinds[in.dst] = SlotKind::Int;
      } else if (value.IsNull()) {
        kinds[in.dst] = SlotKind::Unknown;
      } else {
        return false;
      }
      return true;
    }
    case Op::LoadPropTime: {
      if (reader == nullptr) return false;
      auto const &record = frame.elems()[in.a];
      if (!record.IsVertex() && !record.IsEdge()) {
        if (record.IsNull()) {
          kinds[in.dst] = SlotKind::Unknown;
          break;
        }
        return false;
      }
      if (in.path_len != 1) return false;
      auto const value = reader->ReadProperty(record, paths_[in.path_at]);
      if (value.IsTemporalData() && value.ValueTemporalData().type == storage::TemporalType::LocalDateTime) {
        ints[in.dst] = value.ValueTemporalData().microseconds;
        kinds[in.dst] = SlotKind::Int;
      } else if (value.IsNull()) {
        kinds[in.dst] = SlotKind::Unknown;
      } else {
        return false;
      }
      return true;
    }
    case Op::EvalTime: {
      if (reader == nullptr) return false;
      bool was_null = false;
      auto const micros = reader->EvaluateLocalDateTime(*in.delegated, was_null);
      if (!micros) {
        if (!was_null) return false;
        kinds[in.dst] = SlotKind::Unknown;
        return true;
      }
      ints[in.dst] = *micros;
      kinds[in.dst] = SlotKind::Int;
      return true;
    }
    case Op::EvalTri: {
      if (reader == nullptr) return false;
      auto const answer = reader->EvaluateTruth(*in.delegated);
      if (answer == Answer::Refused) return false;
      tris[in.dst] = answer;
      return true;
    }
    case Op::DivInt: {
      if (kinds[in.a] == SlotKind::Double || kinds[in.b] == SlotKind::Double) return false;
      kinds[in.dst] =
          (kinds[in.a] == SlotKind::Int && kinds[in.b] == SlotKind::Int) ? SlotKind::Int : SlotKind::Unknown;
      if (kinds[in.dst] != SlotKind::Unknown) {
        auto const x = ints[in.a];
        auto const y = ints[in.b];
        // Dividing by zero is the evaluator's complaint to make, and the one
        // division C++ leaves undefined is handed back for the same reason.
        if (y == 0 || (y == -1 && x == std::numeric_limits<int64_t>::min())) return false;
        ints[in.dst] = x / y;
      }
      return true;
    }
    case Op::IsNullInt:
      tris[in.dst] = kinds[in.a] == SlotKind::Unknown ? Answer::True : Answer::False;
      return true;
    case Op::IsNullTri:
      tris[in.dst] = tris[in.a] == Answer::Null ? Answer::True : Answer::False;
      break;
    default:
      break;
  }
  return true;
}

TypedProgram::Answer TypedProgram::Run(Frame const &frame, ExpressionEvaluator *reader,
                                       Parameters const *parameters) const {
  Slots slots;
  if (!Execute(frame, reader, parameters, slots)) return Answer::Refused;
  return slots.tris[result_];
}

bool TypedProgram::RunInto(Frame const &frame, TypedValue &out, ExpressionEvaluator *reader,
                           Parameters const *parameters) const {
  Slots slots;
  if (!Execute(frame, reader, parameters, slots)) return false;
  if (shape_ == Shape::Integer) {
    // A missing operand leaves no integer, and null is a value a caller can
    // perfectly well take.
    switch (slots.kinds[result_]) {
      case SlotKind::Unknown:
        out = TypedValue();
        break;
      case SlotKind::Int:
        out = TypedValue(slots.ints[result_]);
        break;
      case SlotKind::Double:
        out = TypedValue(std::bit_cast<double>(slots.ints[result_]));
        break;
    }
    return true;
  }
  switch (slots.tris[result_]) {
    case Answer::True:
      out = TypedValue(true);
      break;
    case Answer::False:
      out = TypedValue(false);
      break;
    default:
      out = TypedValue();
      break;
  }
  return true;
}

}  // namespace memgraph::query
