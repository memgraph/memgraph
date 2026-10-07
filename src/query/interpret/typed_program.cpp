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

#include "query/interpret/frame.hpp"

#include <algorithm>
#include <limits>

#include "utils/typeinfo.hpp"

namespace memgraph::query {

namespace {

/// Where a compiled operand left its answer, and in what form. An integer and
/// a truth value are kept apart because they are held differently and the
/// operators that take them do not overlap.
struct Operand {
  bool is_tri;
  int32_t slot;
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
        if (!value.IsInt()) return refuse();
        auto const slot = NextInt();
        Emit(TypedProgram::Op::ConstInt, slot, 0, 0, value.ValueInt());
        return Operand{.is_tri = false, .slot = slot};
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
          Emit(TypedProgram::Op::EvalTime, slot, 0, 0, 0, PropertyIx{}, nullptr, expression);
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
        Emit(TypedProgram::Op::EvalTime, slot, 0, 0, 0, PropertyIx{}, nullptr, expression);
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
        Emit(TypedProgram::Op::TestLabels, slot, position, 0, 0, PropertyIx{}, test);
        return Operand{.is_tri = true, .slot = slot};
      }
      case utils::TypeId::AST_PROPERTY_LOOKUP: {
        auto *lookup = static_cast<PropertyLookup *>(expression);
        // Only the plain case. A plain lookup carries a path of one, which is
        // the property itself; a longer path reaches inside a value, and a
        // lookup that takes all of them is a map. Both are left to the
        // evaluator rather than guessed at.
        if (lookup->evaluation_mode_ != PropertyLookup::EvaluationMode::GET_OWN_PROPERTY) return refuse();
        if (lookup->property_path_.size() != 1) return refuse();
        if (lookup->expression_ == nullptr) return refuse();
        if (lookup->expression_->GetTypeInfo().id != utils::TypeId::AST_IDENTIFIER) return refuse();
        auto const position = static_cast<Identifier *>(lookup->expression_)->symbol_pos_;
        if (position < 0) return refuse();
        auto const slot = NextInt();
        Emit(kind == Kind::Time ? TypedProgram::Op::LoadPropTime : TypedProgram::Op::LoadPropInt,
             slot,
             position,
             0,
             0,
             lookup->property_);
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

  std::optional<Operand> Comparison(Expression *expression, TypedProgram::Op op) {
    auto *binary = static_cast<BinaryOperator *>(expression);
    auto const kind = NamesATime(binary->expression1_) || NamesATime(binary->expression2_) ? Kind::Time : Kind::Number;
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
    Emit(TypedProgram::Op::EvalTri, slot, 0, 0, 0, PropertyIx{}, nullptr, expression);
    return Operand{.is_tri = true, .slot = slot};
  }

  int32_t NextInt() { return static_cast<int32_t>(int_slots_++); }

  int32_t NextTri() { return static_cast<int32_t>(tri_slots_++); }

  void Emit(TypedProgram::Op op, int32_t dst, int32_t a, int32_t b, int64_t literal, PropertyIx property = PropertyIx{},
            LabelsTest *labels = nullptr, Expression *delegated = nullptr) {
    code_.push_back(TypedProgram::Instr{.op = op,
                                        .dst = dst,
                                        .a = a,
                                        .b = b,
                                        .literal = literal,
                                        .property = std::move(property),
                                        .labels = labels,
                                        .delegated = delegated});
  }

  std::vector<TypedProgram::Instr> code_;
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

bool TypedProgram::Execute(Frame const &frame, RecordReader *reader, Parameters const *parameters, Slots &slots) const {
  // Small enough to sit on the stack for the expressions this covers; a bigger
  // one would take these from the frame alongside the other working values.
  constexpr size_t kMaxSlots = 64;
  if (int_slots_ > kMaxSlots || tri_slots_ > kMaxSlots) return false;

  auto &ints = slots.ints;
  auto &int_known = slots.int_known;
  auto &tris = slots.tris;

  // A comparison of which either side is missing is null rather than false.
  auto compare = [&](int32_t a, int32_t b, auto decide) {
    return (int_known[a] == 0 || int_known[b] == 0) ? Answer::Null
                                                    : (decide(ints[a], ints[b]) ? Answer::True : Answer::False);
  };

  for (size_t ip = 0; ip < code_.size(); ++ip) {
    auto const &in = code_[ip];
    switch (in.op) {
      case Op::ConstInt:
        ints[in.dst] = in.literal;
        int_known[in.dst] = 1;
        break;
      case Op::LoadInt: {
        auto const &value = frame.elems()[in.a];
        if (value.IsInt()) {
          ints[in.dst] = value.UnsafeValueInt();
          int_known[in.dst] = 1;
        } else if (value.IsNull()) {
          int_known[in.dst] = 0;
        } else {
          // Not what the guess settled on, so this row is not ours.
          return false;
        }
        break;
      }
      case Op::LoadParamInt: {
        if (parameters == nullptr) return false;
        // A position with nothing bound to it belongs to the evaluator, which
        // decides what an unbound parameter means.
        auto const *value = parameters->FindAtTokenPosition(in.a);
        if (value == nullptr) return false;
        if (value->IsInt()) {
          ints[in.dst] = value->ValueInt();
          int_known[in.dst] = 1;
        } else if (value->IsNull()) {
          int_known[in.dst] = 0;
        } else {
          return false;
        }
        break;
      }
      case Op::LoadPropInt: {
        if (reader == nullptr) return false;
        auto const &record = frame.elems()[in.a];
        // Only a record has properties; anything else was not what the guess
        // settled on.
        if (!record.IsVertex() && !record.IsEdge()) {
          if (record.IsNull()) {
            int_known[in.dst] = 0;
            break;
          }
          return false;
        }
        auto const value = reader->ReadProperty(record, in.property);
        if (value.IsInt()) {
          ints[in.dst] = value.ValueInt();
          int_known[in.dst] = 1;
        } else if (value.IsNull()) {
          int_known[in.dst] = 0;
        } else {
          return false;
        }
        break;
      }
      case Op::TestLabels: {
        if (reader == nullptr) return false;
        auto const answer = reader->TestLabels(frame.elems()[in.a], *in.labels);
        tris[in.dst] = !answer ? Answer::Null : (*answer ? Answer::True : Answer::False);
        break;
      }
      case Op::LoadTime: {
        auto const &value = frame.elems()[in.a];
        if (value.IsLocalDateTime()) {
          ints[in.dst] = value.ValueLocalDateTime().SysMicrosecondsSinceEpoch();
          int_known[in.dst] = 1;
        } else if (value.IsNull()) {
          int_known[in.dst] = 0;
        } else {
          return false;
        }
        break;
      }
      case Op::LoadPropTime: {
        if (reader == nullptr) return false;
        auto const &record = frame.elems()[in.a];
        if (!record.IsVertex() && !record.IsEdge()) {
          if (record.IsNull()) {
            int_known[in.dst] = 0;
            break;
          }
          return false;
        }
        auto const value = reader->ReadProperty(record, in.property);
        if (value.IsTemporalData() && value.ValueTemporalData().type == storage::TemporalType::LocalDateTime) {
          ints[in.dst] = value.ValueTemporalData().microseconds;
          int_known[in.dst] = 1;
        } else if (value.IsNull()) {
          int_known[in.dst] = 0;
        } else {
          return false;
        }
        break;
      }
      case Op::EvalTime: {
        if (reader == nullptr) return false;
        bool was_null = false;
        auto const micros = reader->EvaluateLocalDateTime(*in.delegated, was_null);
        if (!micros) {
          if (!was_null) return false;
          int_known[in.dst] = 0;
          break;
        }
        ints[in.dst] = *micros;
        int_known[in.dst] = 1;
        break;
      }
      case Op::AddInt:
      case Op::SubInt:
      case Op::MulInt: {
        int_known[in.dst] = int_known[in.a] & int_known[in.b];
        if (int_known[in.dst] != 0) {
          auto const x = ints[in.a];
          auto const y = ints[in.b];
          ints[in.dst] = in.op == Op::AddInt ? x + y : (in.op == Op::SubInt ? x - y : x * y);
        }
        break;
      }
      case Op::DivInt: {
        int_known[in.dst] = int_known[in.a] & int_known[in.b];
        if (int_known[in.dst] != 0) {
          auto const x = ints[in.a];
          auto const y = ints[in.b];
          // Dividing by zero is the evaluator's complaint to make, and the one
          // division C++ leaves undefined is handed back for the same reason.
          if (y == 0 || (y == -1 && x == std::numeric_limits<int64_t>::min())) return false;
          ints[in.dst] = x / y;
        }
        break;
      }
      case Op::EqInt:
        tris[in.dst] = compare(in.a, in.b, [](int64_t x, int64_t y) { return x == y; });
        break;
      case Op::NeInt:
        tris[in.dst] = compare(in.a, in.b, [](int64_t x, int64_t y) { return x != y; });
        break;
      case Op::LtInt:
        tris[in.dst] = compare(in.a, in.b, [](int64_t x, int64_t y) { return x < y; });
        break;
      case Op::GtInt:
        tris[in.dst] = compare(in.a, in.b, [](int64_t x, int64_t y) { return x > y; });
        break;
      case Op::LeInt:
        tris[in.dst] = compare(in.a, in.b, [](int64_t x, int64_t y) { return x <= y; });
        break;
      case Op::GeInt:
        tris[in.dst] = compare(in.a, in.b, [](int64_t x, int64_t y) { return x >= y; });
        break;
      case Op::AndTri: {
        // False beats null, which is what makes this three-valued rather than
        // a null that swallows everything.
        auto const x = tris[in.a];
        auto const y = tris[in.b];
        tris[in.dst] = (x == Answer::False || y == Answer::False)
                           ? Answer::False
                           : ((x == Answer::Null || y == Answer::Null) ? Answer::Null : Answer::True);
        break;
      }
      case Op::OrTri: {
        auto const x = tris[in.a];
        auto const y = tris[in.b];
        tris[in.dst] = (x == Answer::True || y == Answer::True)
                           ? Answer::True
                           : ((x == Answer::Null || y == Answer::Null) ? Answer::Null : Answer::False);
        break;
      }
      case Op::NotTri: {
        auto const x = tris[in.a];
        tris[in.dst] = x == Answer::Null ? Answer::Null : (x == Answer::True ? Answer::False : Answer::True);
        break;
      }
      case Op::IsNullInt:
        tris[in.dst] = int_known[in.a] == 0 ? Answer::True : Answer::False;
        break;
      case Op::IsNullTri:
        tris[in.dst] = tris[in.a] == Answer::Null ? Answer::True : Answer::False;
        break;
      case Op::EvalTri: {
        if (reader == nullptr) return false;
        auto const answer = reader->EvaluateTruth(*in.delegated);
        if (answer == Answer::Refused) return false;
        tris[in.dst] = answer;
        break;
      }
      case Op::CopyTri:
        tris[in.dst] = tris[in.a];
        break;
      case Op::JumpIfFalseTri:
        if (tris[in.a] == Answer::False) ip = static_cast<size_t>(in.b) - 1;
        break;
      case Op::JumpIfTrueTri:
        if (tris[in.a] == Answer::True) ip = static_cast<size_t>(in.b) - 1;
        break;
    }
  }
  return true;
}

TypedProgram::Answer TypedProgram::Run(Frame const &frame, RecordReader *reader, Parameters const *parameters) const {
  Slots slots{};
  if (!Execute(frame, reader, parameters, slots)) return Answer::Refused;
  return slots.tris[result_];
}

bool TypedProgram::RunInto(Frame const &frame, TypedValue &out, RecordReader *reader,
                           Parameters const *parameters) const {
  Slots slots{};
  if (!Execute(frame, reader, parameters, slots)) return false;
  if (shape_ == Shape::Integer) {
    // A missing operand leaves no integer, and null is a value a caller can
    // perfectly well take.
    if (slots.int_known[result_] == 0) {
      out = TypedValue();
    } else {
      out = TypedValue(slots.ints[result_]);
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
