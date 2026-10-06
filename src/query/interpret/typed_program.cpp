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

}  // namespace

/// Walks the expression once, handing out working slots and emitting the
/// instructions that fill them. Refuses anything it does not cover, which
/// leaves that expression to the ordinary evaluator.
class TypedProgramBuilder {
 public:
  std::optional<Operand> Build(Expression *expression) {
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
        Emit(TypedProgram::Op::LoadInt, slot, position, 0, 0);
        return Operand{.is_tri = false, .slot = slot};
      }
      case utils::TypeId::AST_PARAMETER_LOOKUP: {
        auto const position = static_cast<ParameterLookup *>(expression)->token_position_;
        auto const slot = NextInt();
        Emit(TypedProgram::Op::LoadParamInt, slot, position, 0, 0);
        return Operand{.is_tri = false, .slot = slot};
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
        Emit(TypedProgram::Op::LoadPropInt, slot, position, 0, 0, lookup->property_);
        return Operand{.is_tri = false, .slot = slot};
      }
      case utils::TypeId::AST_ADDITION_OPERATOR:
        return Arithmetic(expression, TypedProgram::Op::AddInt);
      case utils::TypeId::AST_SUBTRACTION_OPERATOR:
        return Arithmetic(expression, TypedProgram::Op::SubInt);
      case utils::TypeId::AST_MULTIPLICATION_OPERATOR:
        return Arithmetic(expression, TypedProgram::Op::MulInt);
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
      case utils::TypeId::AST_OR_OPERATOR:
        return Logical(expression, TypedProgram::Op::OrTri);
      case utils::TypeId::AST_NOT_OPERATOR: {
        auto *op = static_cast<NotOperator *>(expression);
        auto const operand = Build(op->expression_);
        if (!operand || !operand->is_tri) return refuse();
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
    auto const lhs = Build(binary->expression1_);
    if (!lhs || lhs->is_tri) return Refuse(expression);
    auto const rhs = Build(binary->expression2_);
    if (!rhs || rhs->is_tri) return Refuse(expression);
    auto const slot = NextTri();
    Emit(op, slot, lhs->slot, rhs->slot, 0);
    return Operand{.is_tri = true, .slot = slot};
  }

  std::optional<Operand> Logical(Expression *expression, TypedProgram::Op op) {
    auto *binary = static_cast<BinaryOperator *>(expression);
    auto const lhs = Build(binary->expression1_);
    if (!lhs || !lhs->is_tri) return Refuse(expression);
    auto const rhs = Build(binary->expression2_);
    if (!rhs || !rhs->is_tri) return Refuse(expression);
    auto const slot = NextTri();
    Emit(op, slot, lhs->slot, rhs->slot, 0);
    return Operand{.is_tri = true, .slot = slot};
  }

  /// The first node to stop the walk is the one to report: the ones above it
  /// only refused because it did.
  std::nullopt_t Refuse(Expression *expression) {
    if (refused_on_ == nullptr) refused_on_ = expression;
    return std::nullopt;
  }

  int32_t NextInt() { return static_cast<int32_t>(int_slots_++); }

  int32_t NextTri() { return static_cast<int32_t>(tri_slots_++); }

  void Emit(TypedProgram::Op op, int32_t dst, int32_t a, int32_t b, int64_t literal,
            PropertyIx property = PropertyIx{}) {
    code_.push_back(
        TypedProgram::Instr{.op = op, .dst = dst, .a = a, .b = b, .literal = literal, .property = std::move(property)});
  }

  std::vector<TypedProgram::Instr> code_;
  size_t int_slots_{0};
  size_t tri_slots_{0};

 public:
  Expression *refused_on_{nullptr};
};

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
  return builder.Finish(*root);
}

TypedProgram::Answer TypedProgram::Run(Frame const &frame, PropertySource *source, Parameters const *parameters) const {
  // Small enough to sit on the stack for the expressions this covers; a bigger
  // one would take these from the frame alongside the other working values.
  constexpr size_t kMaxSlots = 64;
  if (int_slots_ > kMaxSlots || tri_slots_ > kMaxSlots) return Answer::Refused;

  std::array<int64_t, kMaxSlots> ints{};
  std::array<char, kMaxSlots> int_known{};
  std::array<Answer, kMaxSlots> tris{};

  // A comparison of which either side is missing is null rather than false.
  auto compare = [&](int32_t a, int32_t b, auto decide) {
    return (int_known[a] == 0 || int_known[b] == 0) ? Answer::Null
                                                    : (decide(ints[a], ints[b]) ? Answer::True : Answer::False);
  };

  for (auto const &in : code_) {
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
          return Answer::Refused;
        }
        break;
      }
      case Op::LoadParamInt: {
        if (parameters == nullptr) return Answer::Refused;
        // A position with nothing bound to it belongs to the evaluator, which
        // decides what an unbound parameter means.
        auto const *value = parameters->FindAtTokenPosition(in.a);
        if (value == nullptr) return Answer::Refused;
        if (value->IsInt()) {
          ints[in.dst] = value->ValueInt();
          int_known[in.dst] = 1;
        } else if (value->IsNull()) {
          int_known[in.dst] = 0;
        } else {
          return Answer::Refused;
        }
        break;
      }
      case Op::LoadPropInt: {
        if (source == nullptr) return Answer::Refused;
        auto const &record = frame.elems()[in.a];
        // Only a record has properties; anything else was not what the guess
        // settled on.
        if (!record.IsVertex() && !record.IsEdge()) {
          if (record.IsNull()) {
            int_known[in.dst] = 0;
            break;
          }
          return Answer::Refused;
        }
        auto const value = source->ReadProperty(record, in.property);
        if (value.IsInt()) {
          ints[in.dst] = value.ValueInt();
          int_known[in.dst] = 1;
        } else if (value.IsNull()) {
          int_known[in.dst] = 0;
        } else {
          return Answer::Refused;
        }
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
    }
  }
  return tris[result_];
}

}  // namespace memgraph::query
