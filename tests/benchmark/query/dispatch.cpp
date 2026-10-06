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

// Compares ways of evaluating one expression per row. Two things vary
// independently: how the evaluator reaches the code for a node, and how long
// the TypedValue each node yields stays alive. Every strategy calls the same
// TypedValue operators, so the operators cancel and what is left is the
// difference being measured.
//
// The int frame makes destruction nearly free, which isolates dispatch. The
// string frame gives every value a heap buffer, which is what the production
// profile looks like.

#include <benchmark/benchmark.h>

#include <memory>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "query/typed_value.hpp"
#include "utils/memory.hpp"

namespace {

using memgraph::query::TypedValue;
using Frame = std::vector<TypedValue>;

// The expression every strategy evaluates:
//   (s0 + s1) == s2  AND  s0 == s3     (int frame)
//   s0 == s1         AND  s2 == s3     (string frame; no concatenation, so the
//                                       cost is value lifetime, not allocation
//                                       inside the operator)
enum class Shape { Int, Str, StrVar, IntNull, IntWrong };

// The int-shaped frames, which differ from the string ones in which expression
// is built over them.
bool IsIntShape(Shape shape) { return shape == Shape::Int || shape == Shape::IntNull || shape == Shape::IntWrong; }

// Which memory the values are built from. Production evaluates against a pool
// over a monotonic arena, where a free returns a block to a list rather than
// going to the allocator, so a saving measured against new and delete is not
// the saving a query gets.
enum class Alloc { NewDelete, Pool };

struct QueryMemory {
  memgraph::utils::MonotonicBufferResource monotonic{4UL * 1024UL};
  memgraph::utils::PoolResource<> pool{64, &monotonic};
};

// The benchmarks run one at a time, so the resource in force can be file-level.
QueryMemory *g_memory = nullptr;
Alloc g_alloc = Alloc::NewDelete;

TypedValue::allocator_type CurrentAlloc() {
  if (g_alloc == Alloc::Pool && g_memory != nullptr) {
    return TypedValue::allocator_type{&g_memory->pool};
  }
  return TypedValue::allocator_type{};
}

// A scratch array whose slots already carry the allocator, so assigning into a
// slot reuses whatever that slot holds rather than taking the source's memory.
std::vector<TypedValue> MakeScratch(int slots) {
  std::vector<TypedValue> v;
  v.reserve(slots);
  for (int i = 0; i < slots; ++i) v.emplace_back(CurrentAlloc());
  return v;
}

// ---------------------------------------------------------------- strategy A
// Virtual double dispatch: a virtual Accept to recover the node type, then a
// virtual Visit on the evaluator. This is what the AST does today.
struct AId;
struct AAdd;
struct AEq;
struct AAnd;

struct AVisitor {
  virtual ~AVisitor() = default;
  virtual TypedValue Visit(AId &) = 0;
  virtual TypedValue Visit(AAdd &) = 0;
  virtual TypedValue Visit(AEq &) = 0;
  virtual TypedValue Visit(AAnd &) = 0;
};

struct ANode {
  virtual ~ANode() = default;
  virtual TypedValue Accept(AVisitor &) = 0;
};

struct AId : ANode {
  explicit AId(int s) : slot(s) {}

  int slot;

  TypedValue Accept(AVisitor &v) override { return v.Visit(*this); }
};

struct ABin : ANode {
  ABin(ANode *l, ANode *r) : lhs(l), rhs(r) {}

  ANode *lhs;
  ANode *rhs;
};

struct AAdd : ABin {
  using ABin::ABin;

  TypedValue Accept(AVisitor &v) override { return v.Visit(*this); }
};

struct AEq : ABin {
  using ABin::ABin;

  TypedValue Accept(AVisitor &v) override { return v.Visit(*this); }
};

struct AAnd : ABin {
  using ABin::ABin;

  TypedValue Accept(AVisitor &v) override { return v.Visit(*this); }
};

struct AEvaluator : AVisitor {
  explicit AEvaluator(Frame *f) : frame(f) {}

  Frame *frame;

  TypedValue Visit(AId &n) override { return TypedValue((*frame)[n.slot]); }

  TypedValue Visit(AAdd &n) override { return n.lhs->Accept(*this) + n.rhs->Accept(*this); }

  TypedValue Visit(AEq &n) override { return n.lhs->Accept(*this) == n.rhs->Accept(*this); }

  TypedValue Visit(AAnd &n) override {
    auto l = n.lhs->Accept(*this);
    if (l.IsBool() && !l.ValueBool()) return l;
    return l && n.rhs->Accept(*this);
  }
};

// ---------------------------------------------------------------- strategy B
// One virtual call per node: the node evaluates itself, so the second indirect
// branch that Accept needs to reach the visitor is gone.
struct BNode {
  virtual ~BNode() = default;
  virtual TypedValue Eval(Frame &) const = 0;
};

struct BId : BNode {
  explicit BId(int s) : slot(s) {}

  int slot;

  TypedValue Eval(Frame &f) const override { return TypedValue(f[slot]); }
};

struct BBin : BNode {
  BBin(BNode *l, BNode *r) : lhs(l), rhs(r) {}

  BNode *lhs;
  BNode *rhs;
};

struct BAdd : BBin {
  using BBin::BBin;

  TypedValue Eval(Frame &f) const override { return lhs->Eval(f) + rhs->Eval(f); }
};

struct BEq : BBin {
  using BBin::BBin;

  TypedValue Eval(Frame &f) const override { return lhs->Eval(f) == rhs->Eval(f); }
};

struct BAnd : BBin {
  using BBin::BBin;

  TypedValue Eval(Frame &f) const override {
    auto l = lhs->Eval(f);
    if (l.IsBool() && !l.ValueBool()) return l;
    return l && rhs->Eval(f);
  }
};

// ------------------------------------------------------------ strategies C/D
// One flat node type carrying a tag. C returns by value like A and B; D writes
// through a destination the caller owns, so a value's storage is reused down
// the tree instead of a fresh one being built and torn down per node.
enum class Tag : uint8_t { Id, Add, Eq, And };

struct CNode {
  Tag tag;
  int slot{0};
  int idx{0};  // scratch slot this node writes into, for D and F

  CNode *lhs{nullptr};
  CNode *rhs{nullptr};
};

TypedValue CEval(const CNode *n, Frame &f) {
  switch (n->tag) {
    case Tag::Id:
      return TypedValue(f[n->slot]);
    case Tag::Add:
      return CEval(n->lhs, f) + CEval(n->rhs, f);
    case Tag::Eq:
      return CEval(n->lhs, f) == CEval(n->rhs, f);
    case Tag::And: {
      auto l = CEval(n->lhs, f);
      if (l.IsBool() && !l.ValueBool()) return l;
      return l && CEval(n->rhs, f);
    }
  }
  __builtin_unreachable();
}

// D writes each node's result into a scratch slot assigned to that node once,
// so the value's storage is reused across rows instead of a fresh TypedValue
// being built and torn down per node. The operators still return by value, so
// one temporary per operator survives.
struct DEvaluator {
  std::vector<TypedValue> scratch;
  Frame *frame;

  void Eval(const CNode *n) {
    switch (n->tag) {
      case Tag::Id:
        scratch[n->idx] = (*frame)[n->slot];
        return;
      case Tag::Add:
        Eval(n->lhs);
        Eval(n->rhs);
        scratch[n->idx] = scratch[n->lhs->idx] + scratch[n->rhs->idx];
        return;
      case Tag::Eq:
        Eval(n->lhs);
        Eval(n->rhs);
        scratch[n->idx] = scratch[n->lhs->idx] == scratch[n->rhs->idx];
        return;
      case Tag::And: {
        Eval(n->lhs);
        auto &l = scratch[n->lhs->idx];
        if (l.IsBool() && !l.ValueBool()) {
          scratch[n->idx] = l;
          return;
        }
        Eval(n->rhs);
        scratch[n->idx] = l && scratch[n->rhs->idx];
        return;
      }
    }
  }
};

// F adds in-place forms of the operators for the cases that do not allocate, so
// the operator writes its answer into the destination rather than returning a
// value that then has to be moved in. This is the part a different call
// structure alone cannot reach: TypedValue's operators all return by value.
struct FEvaluator {
  std::vector<TypedValue> scratch;
  Frame *frame;

  static void AddInto(TypedValue &out, const TypedValue &a, const TypedValue &b) {
    if (a.IsInt() && b.IsInt()) {
      out = a.UnsafeValueInt() + b.UnsafeValueInt();
      return;
    }
    out = a + b;
  }

  static void EqInto(TypedValue &out, const TypedValue &a, const TypedValue &b) {
    if (a.IsInt() && b.IsInt()) {
      out = a.UnsafeValueInt() == b.UnsafeValueInt();
      return;
    }
    if (a.IsString() && b.IsString()) {
      out = a.UnsafeValueString() == b.UnsafeValueString();
      return;
    }
    out = a == b;
  }

  void Eval(const CNode *n) {
    switch (n->tag) {
      case Tag::Id:
        scratch[n->idx] = (*frame)[n->slot];
        return;
      case Tag::Add:
        Eval(n->lhs);
        Eval(n->rhs);
        AddInto(scratch[n->idx], scratch[n->lhs->idx], scratch[n->rhs->idx]);
        return;
      case Tag::Eq:
        Eval(n->lhs);
        Eval(n->rhs);
        EqInto(scratch[n->idx], scratch[n->lhs->idx], scratch[n->rhs->idx]);
        return;
      case Tag::And: {
        Eval(n->lhs);
        auto &l = scratch[n->lhs->idx];
        if (l.IsBool() && !l.ValueBool()) {
          scratch[n->idx] = l;
          return;
        }
        Eval(n->rhs);
        auto &r = scratch[n->rhs->idx];
        if (l.IsBool() && r.IsBool()) {
          scratch[n->idx] = l.UnsafeValueBool() && r.UnsafeValueBool();
          return;
        }
        scratch[n->idx] = l && r;
        return;
      }
    }
  }
};

// ---------------------------------------------------------------- strategy E
// A flat instruction stream over a value stack the VM owns. The stack is
// allocated once and its slots are assigned into, so no TypedValue is built or
// destroyed per node per row; only the values themselves change.
enum class Ins : uint8_t { LoadSlot, Add, Eq, And, JmpIfFalse };

struct Instr {
  Ins op;
  int32_t arg;
};

class MiniVm {
 public:
  explicit MiniVm(size_t depth) : stack_(MakeScratch(static_cast<int>(depth))) {}

  // Returns a reference into the stack: the caller reads it before the next Run.
  TypedValue &Run(const std::vector<Instr> &code, Frame &f) {
    int sp = -1;
    for (size_t pc = 0; pc < code.size(); ++pc) {
      const auto &in = code[pc];
      switch (in.op) {
        case Ins::LoadSlot:
          stack_[++sp] = f[in.arg];
          break;
        case Ins::Add:
          stack_[sp - 1] = stack_[sp - 1] + stack_[sp];
          --sp;
          break;
        case Ins::Eq:
          stack_[sp - 1] = stack_[sp - 1] == stack_[sp];
          --sp;
          break;
        case Ins::And:
          stack_[sp - 1] = stack_[sp - 1] && stack_[sp];
          --sp;
          break;
        case Ins::JmpIfFalse:
          if (stack_[sp].IsBool() && !stack_[sp].ValueBool()) pc = static_cast<size_t>(in.arg) - 1;
          break;
      }
    }
    return stack_[0];
  }

 private:
  std::vector<TypedValue> stack_;
};

// ---------------------------------------------------------------- strategy H
// A typed scratch. A pass over the expression has already settled that the
// arithmetic is all integers and the comparisons all yield bools, so the
// scratch holds int64_t and bool rather than TypedValue. Nothing is boxed in
// the middle: a value is read out of a TypedValue once on the way in, and one
// is built once on the way out.
//
// For strings the scratch holds views onto the frame's values, so an operand is
// never copied at all.
enum class TIns : uint8_t { LoadInt, AddInt, EqInt, LoadStr, EqStr, AndBool };

struct TInstr {
  TIns op;
  int32_t dst;
  int32_t a;
  int32_t b;
};

class TypedVm {
 public:
  TypedVm(size_t ints, size_t strs, size_t bools) : ints_(ints), strs_(strs), bools_(bools) {}

  bool Run(const std::vector<TInstr> &code, const Frame &f) {
    for (const auto &in : code) {
      switch (in.op) {
        case TIns::LoadInt:
          ints_[in.dst] = f[in.a].UnsafeValueInt();
          break;
        case TIns::AddInt:
          ints_[in.dst] = ints_[in.a] + ints_[in.b];
          break;
        case TIns::EqInt:
          bools_[in.dst] = ints_[in.a] == ints_[in.b];
          break;
        case TIns::LoadStr:
          // A view, so the operand is not copied.
          strs_[in.dst] = std::string_view{f[in.a].UnsafeValueString()};
          break;
        case TIns::EqStr:
          bools_[in.dst] = strs_[in.a] == strs_[in.b];
          break;
        case TIns::AndBool:
          bools_[in.dst] = bools_[in.a] && bools_[in.b];
          break;
      }
    }
    return bools_[0];
  }

 private:
  std::vector<int64_t> ints_;
  std::vector<std::string_view> strs_;
  std::vector<char> bools_;
};

// ---------------------------------------------------------------- strategy I
// What strategy H costs once it has to be true rather than only fast. Two
// things production cannot do without are added:
//
//   A value may be null, and int64_t has no null, so each slot carries whether
//   it holds anything. Cypher's AND is three-valued, so a bool slot holds
//   false, true or null rather than a bool.
//
//   The pass settled the types before the row arrived, so every load checks
//   that the value really is what was settled on. A mismatch abandons the typed
//   run and the caller falls back to the boxed path.
enum class Tri : int8_t { False = 0, True = 1, Null = 2 };

enum class NIns : uint8_t { LoadInt, AddInt, EqInt, LoadStr, EqStr, AndTri };

struct NInstr {
  NIns op;
  int32_t dst;
  int32_t a;
  int32_t b;
};

class NullableTypedVm {
 public:
  NullableTypedVm(size_t ints, size_t strs, size_t tris)
      : ints_(ints), int_ok_(ints), strs_(strs), str_ok_(strs), tris_(tris) {}

  // Returns false when a value was not the type the pass settled on, which
  // leaves the answer to the caller rather than guessing it.
  bool Run(const std::vector<NInstr> &code, const Frame &f, Tri &answer) {
    for (const auto &in : code) {
      switch (in.op) {
        case NIns::LoadInt: {
          const auto &value = f[in.a];
          if (value.IsInt()) {
            ints_[in.dst] = value.UnsafeValueInt();
            int_ok_[in.dst] = 1;
          } else if (value.IsNull()) {
            int_ok_[in.dst] = 0;
          } else {
            return false;
          }
          break;
        }
        case NIns::AddInt:
          int_ok_[in.dst] = int_ok_[in.a] & int_ok_[in.b];
          if (int_ok_[in.dst] != 0) ints_[in.dst] = ints_[in.a] + ints_[in.b];
          break;
        case NIns::EqInt:
          tris_[in.dst] = (int_ok_[in.a] == 0 || int_ok_[in.b] == 0)
                              ? Tri::Null
                              : (ints_[in.a] == ints_[in.b] ? Tri::True : Tri::False);
          break;
        case NIns::LoadStr: {
          const auto &value = f[in.a];
          if (value.IsString()) {
            strs_[in.dst] = std::string_view{value.UnsafeValueString()};
            str_ok_[in.dst] = 1;
          } else if (value.IsNull()) {
            str_ok_[in.dst] = 0;
          } else {
            return false;
          }
          break;
        }
        case NIns::EqStr:
          tris_[in.dst] = (str_ok_[in.a] == 0 || str_ok_[in.b] == 0)
                              ? Tri::Null
                              : (strs_[in.a] == strs_[in.b] ? Tri::True : Tri::False);
          break;
        case NIns::AndTri: {
          // False wins over null, which is what makes this three-valued rather
          // than a null that swallows everything.
          const auto x = tris_[in.a];
          const auto y = tris_[in.b];
          tris_[in.dst] = (x == Tri::False || y == Tri::False)
                              ? Tri::False
                              : ((x == Tri::Null || y == Tri::Null) ? Tri::Null : Tri::True);
          break;
        }
      }
    }
    answer = tris_[0];
    return true;
  }

 private:
  std::vector<int64_t> ints_;
  std::vector<char> int_ok_;
  std::vector<std::string_view> strs_;
  std::vector<char> str_ok_;
  std::vector<Tri> tris_;
};

std::vector<NInstr> NullableCode(Shape shape) {
  if (IsIntShape(shape)) {
    return {{NIns::LoadInt, 0, 0, 0},
            {NIns::LoadInt, 1, 1, 0},
            {NIns::AddInt, 2, 0, 1},
            {NIns::LoadInt, 3, 2, 0},
            {NIns::EqInt, 1, 2, 3},
            {NIns::LoadInt, 4, 3, 0},
            {NIns::EqInt, 2, 0, 4},
            {NIns::AndTri, 0, 1, 2}};
  }
  return {{NIns::LoadStr, 0, 0, 0},
          {NIns::LoadStr, 1, 1, 0},
          {NIns::EqStr, 1, 0, 1},
          {NIns::LoadStr, 2, 2, 0},
          {NIns::LoadStr, 3, 3, 0},
          {NIns::EqStr, 2, 2, 3},
          {NIns::AndTri, 0, 1, 2}};
}

// ---------------------------------------------------------------- strategy G
// The expression hand-written as native code against unboxed frame values: no
// node walk, no tag test, no TypedValue except the answer. This is the ceiling
// a JIT could reach for a fully type-specialised expression, so it bounds what
// compiling to native code can be worth.
bool GEvalInt(const Frame &f) {
  const int64_t a = f[0].UnsafeValueInt();
  const int64_t b = f[1].UnsafeValueInt();
  const int64_t c = f[2].UnsafeValueInt();
  const int64_t d = f[3].UnsafeValueInt();
  return (a + b) == c && a == d;
}

bool GEvalStr(const Frame &f) {
  return f[0].UnsafeValueString() == f[1].UnsafeValueString() && f[2].UnsafeValueString() == f[3].UnsafeValueString();
}

// ------------------------------------------------------------------ fixtures

// Several frames, cycled per row. With Str every row carries the same string
// lengths, so a reused slot always has a large enough buffer; StrVar varies the
// lengths, which is what a real scan does and which makes some reuses miss.
Frame MakeFrame(Shape shape);

std::vector<Frame> MakeFrames(Shape shape) {
  std::vector<Frame> frames;
  if (shape == Shape::IntWrong) {
    // A string where the pass settled on an integer, so the first guard fails
    // and the whole expression falls back. This is the worst case for
    // speculating on a type: the typed attempt is wasted and paid for anyway.
    for (int i = 0; i < 4; ++i) {
      Frame f;
      // A double where the pass settled on an integer: still a number, so the
      // boxed path answers normally, but the typed load cannot take it.
      f.emplace_back(static_cast<double>(i) + 0.5, CurrentAlloc());
      f.emplace_back(static_cast<int64_t>(i), CurrentAlloc());
      f.emplace_back(static_cast<int64_t>(7), CurrentAlloc());
      f.emplace_back(static_cast<int64_t>(3), CurrentAlloc());
      frames.push_back(std::move(f));
    }
    return frames;
  }
  if (shape == Shape::IntNull) {
    // Every position takes a turn at being null, and one frame has none, so
    // both the three-valued cases and the ordinary one come up.
    for (int missing = -1; missing < 4; ++missing) {
      Frame f;
      for (int i = 0; i < 4; ++i) {
        if (i == missing) {
          f.emplace_back(CurrentAlloc());
        } else {
          f.emplace_back(static_cast<int64_t>(i == 2 ? 7 : (i == 3 ? 3 : i + 3)), CurrentAlloc());
        }
      }
      frames.push_back(std::move(f));
    }
    return frames;
  }
  if (shape != Shape::StrVar) {
    frames.push_back(MakeFrame(shape));
    return frames;
  }
  const char *pool[] = {
      "short",
      "a moderately long string value here",
      "a very considerably longer string value that will not fit in a small buffer at all",
      "tiny",
      "another middling string value for variety",
  };
  for (int i = 0; i < 5; ++i) {
    Frame f;
    f.emplace_back(pool[i % 5], CurrentAlloc());
    f.emplace_back(pool[i % 5], CurrentAlloc());
    f.emplace_back(pool[(i + 1) % 5], CurrentAlloc());
    f.emplace_back(pool[(i + 1) % 5], CurrentAlloc());
    frames.push_back(std::move(f));
  }
  return frames;
}

Frame MakeFrame(Shape shape) {
  Frame f;
  if (IsIntShape(shape)) {
    f.emplace_back(int64_t{3}, CurrentAlloc());
    f.emplace_back(int64_t{4}, CurrentAlloc());
    f.emplace_back(int64_t{7}, CurrentAlloc());
    f.emplace_back(int64_t{3}, CurrentAlloc());
  } else {
    // Long enough that the string owns a heap buffer rather than living inside
    // the object, so destruction has to free.
    f.emplace_back("a sufficiently long string value to force allocation 0", CurrentAlloc());
    f.emplace_back("a sufficiently long string value to force allocation 0", CurrentAlloc());
    f.emplace_back("a sufficiently long string value to force allocation 1", CurrentAlloc());
    f.emplace_back("a sufficiently long string value to force allocation 1", CurrentAlloc());
  }
  return f;
}

// Keeps every node alive for the run.
template <typename T>
struct Arena {
  std::vector<std::unique_ptr<T>> nodes;

  template <typename U, typename... Args>
  U *Make(Args &&...args) {
    auto p = std::make_unique<U>(std::forward<Args>(args)...);
    auto *raw = p.get();
    nodes.push_back(std::move(p));
    return raw;
  }
};

}  // namespace

namespace {
CNode *BuildC(Arena<CNode> &arena, Shape shape);
int AssignSlots(CNode *n, int next);
}  // namespace

// A strategy that computes the wrong answer can look fast, so every strategy is
// checked against A on every frame before any timing is reported.
bool SameAnswers(Shape shape) {
  auto frames = MakeFrames(shape);
  Arena<ANode> aarena;
  ANode *aexpr = nullptr;
  if (IsIntShape(shape)) {
    auto *add = aarena.Make<AAdd>(aarena.Make<AId>(0), aarena.Make<AId>(1));
    aexpr = aarena.Make<AAnd>(aarena.Make<AEq>(add, aarena.Make<AId>(2)),
                              aarena.Make<AEq>(aarena.Make<AId>(0), aarena.Make<AId>(3)));
  } else {
    aexpr = aarena.Make<AAnd>(aarena.Make<AEq>(aarena.Make<AId>(0), aarena.Make<AId>(1)),
                              aarena.Make<AEq>(aarena.Make<AId>(2), aarena.Make<AId>(3)));
  }
  Arena<BNode> barena;
  BNode *bexpr = nullptr;
  if (IsIntShape(shape)) {
    auto *add = barena.Make<BAdd>(barena.Make<BId>(0), barena.Make<BId>(1));
    bexpr = barena.Make<BAnd>(barena.Make<BEq>(add, barena.Make<BId>(2)),
                              barena.Make<BEq>(barena.Make<BId>(0), barena.Make<BId>(3)));
  } else {
    bexpr = barena.Make<BAnd>(barena.Make<BEq>(barena.Make<BId>(0), barena.Make<BId>(1)),
                              barena.Make<BEq>(barena.Make<BId>(2), barena.Make<BId>(3)));
  }
  Arena<CNode> carena;
  auto *cexpr = BuildC(carena, shape);
  const int slots = AssignSlots(cexpr, 0);

  std::vector<Instr> code;
  if (IsIntShape(shape)) {
    code = {{Ins::LoadSlot, 0},
            {Ins::LoadSlot, 1},
            {Ins::Add, 0},
            {Ins::LoadSlot, 2},
            {Ins::Eq, 0},
            {Ins::JmpIfFalse, 100},
            {Ins::LoadSlot, 0},
            {Ins::LoadSlot, 3},
            {Ins::Eq, 0},
            {Ins::And, 0}};
  } else {
    code = {{Ins::LoadSlot, 0},
            {Ins::LoadSlot, 1},
            {Ins::Eq, 0},
            {Ins::JmpIfFalse, 100},
            {Ins::LoadSlot, 2},
            {Ins::LoadSlot, 3},
            {Ins::Eq, 0},
            {Ins::And, 0}};
  }

  for (auto &frame : frames) {
    AEvaluator aev(&frame);
    auto const reference = aev.Visit(*static_cast<AAnd *>(aexpr));
    auto const want_tri = reference.IsNull() ? Tri::Null : (reference.ValueBool() ? Tri::True : Tri::False);
    const bool answers_bool = !reference.IsNull();
    const bool want = answers_bool && reference.ValueBool();

    if (answers_bool && bexpr->Eval(frame).ValueBool() != want) return false;
    if (!answers_bool && !bexpr->Eval(frame).IsNull()) return false;
    if (answers_bool && CEval(cexpr, frame).ValueBool() != want) return false;
    if (!answers_bool && !CEval(cexpr, frame).IsNull()) return false;

    DEvaluator dev{MakeScratch(slots), &frame};
    dev.Eval(cexpr);
    if (answers_bool && dev.scratch[cexpr->idx].ValueBool() != want) return false;
    if (!answers_bool && !dev.scratch[cexpr->idx].IsNull()) return false;

    FEvaluator fev{MakeScratch(slots), &frame};
    fev.Eval(cexpr);
    if (answers_bool && fev.scratch[cexpr->idx].ValueBool() != want) return false;
    if (!answers_bool && !fev.scratch[cexpr->idx].IsNull()) return false;

    MiniVm vm(8);
    if (answers_bool && vm.Run(code, frame).ValueBool() != want) return false;
    if (!answers_bool && !vm.Run(code, frame).IsNull()) return false;

    if (shape != Shape::IntNull && shape != Shape::IntWrong) {
      const bool g = IsIntShape(shape) ? GEvalInt(frame) : GEvalStr(frame);
      if (!answers_bool || g != want) return false;
    }

    NullableTypedVm nvm(8, 8, 8);
    Tri answer = Tri::Null;
    // A refusal is the typed run saying the value was not the type the pass
    // settled on, which is correct behaviour and leaves the answer to the
    // boxed path. Only an answer it does give has to match.
    if (nvm.Run(NullableCode(shape), frame, answer) && answer != want_tri) return false;

    std::vector<TInstr> tcode;
    if (IsIntShape(shape)) {
      tcode = {{TIns::LoadInt, 0, 0, 0},
               {TIns::LoadInt, 1, 1, 0},
               {TIns::AddInt, 2, 0, 1},
               {TIns::LoadInt, 3, 2, 0},
               {TIns::EqInt, 1, 2, 3},
               {TIns::LoadInt, 4, 3, 0},
               {TIns::EqInt, 2, 0, 4},
               {TIns::AndBool, 0, 1, 2}};
    } else {
      tcode = {{TIns::LoadStr, 0, 0, 0},
               {TIns::LoadStr, 1, 1, 0},
               {TIns::EqStr, 1, 0, 1},
               {TIns::LoadStr, 2, 2, 0},
               {TIns::LoadStr, 3, 3, 0},
               {TIns::EqStr, 2, 2, 3},
               {TIns::AndBool, 0, 1, 2}};
    }
    if (shape != Shape::IntNull && shape != Shape::IntWrong) {
      TypedVm tvm(8, 8, 8);
      if (!answers_bool || tvm.Run(tcode, frame) != want) return false;
    }
  }
  return true;
}

// --------------------------------------------------------------- benchmarks

static void Dispatch_A_VirtualDouble(benchmark::State &state) {
  const auto shape = static_cast<Shape>(state.range(0));
  QueryMemory memory;
  g_memory = &memory;
  g_alloc = static_cast<Alloc>(state.range(1));
  auto frames = MakeFrames(shape);
  size_t fi = 0;
  auto frame = frames[0];
  Arena<ANode> arena;
  ANode *expr = nullptr;
  if (IsIntShape(shape)) {
    auto *add = arena.Make<AAdd>(arena.Make<AId>(0), arena.Make<AId>(1));
    expr = arena.Make<AAnd>(arena.Make<AEq>(add, arena.Make<AId>(2)),
                            arena.Make<AEq>(arena.Make<AId>(0), arena.Make<AId>(3)));
  } else {
    expr = arena.Make<AAnd>(arena.Make<AEq>(arena.Make<AId>(0), arena.Make<AId>(1)),
                            arena.Make<AEq>(arena.Make<AId>(2), arena.Make<AId>(3)));
  }
  AEvaluator ev(&frame);
  for (auto _ : state) {
    ev.frame = &frames[fi];
    if (++fi == frames.size()) fi = 0;
    auto r = expr->Accept(ev);
    benchmark::DoNotOptimize(r);
  }
  state.SetItemsProcessed(state.iterations());
}

static void Dispatch_B_VirtualSingle(benchmark::State &state) {
  const auto shape = static_cast<Shape>(state.range(0));
  QueryMemory memory;
  g_memory = &memory;
  g_alloc = static_cast<Alloc>(state.range(1));
  auto frames = MakeFrames(shape);
  size_t fi = 0;
  Arena<BNode> arena;
  BNode *expr = nullptr;
  if (IsIntShape(shape)) {
    auto *add = arena.Make<BAdd>(arena.Make<BId>(0), arena.Make<BId>(1));
    expr = arena.Make<BAnd>(arena.Make<BEq>(add, arena.Make<BId>(2)),
                            arena.Make<BEq>(arena.Make<BId>(0), arena.Make<BId>(3)));
  } else {
    expr = arena.Make<BAnd>(arena.Make<BEq>(arena.Make<BId>(0), arena.Make<BId>(1)),
                            arena.Make<BEq>(arena.Make<BId>(2), arena.Make<BId>(3)));
  }
  for (auto _ : state) {
    auto &frame = frames[fi];
    if (++fi == frames.size()) fi = 0;
    auto r = expr->Eval(frame);
    benchmark::DoNotOptimize(r);
  }
  state.SetItemsProcessed(state.iterations());
}

namespace {
CNode *BuildC(Arena<CNode> &arena, Shape shape) {
  auto mk = [&](Tag t, int slot, CNode *l, CNode *r) {
    auto *n = arena.Make<CNode>();
    n->tag = t;
    n->slot = slot;
    n->lhs = l;
    n->rhs = r;
    return n;
  };
  if (IsIntShape(shape)) {
    auto *add = mk(Tag::Add, 0, mk(Tag::Id, 0, nullptr, nullptr), mk(Tag::Id, 1, nullptr, nullptr));
    return mk(Tag::And,
              0,
              mk(Tag::Eq, 0, add, mk(Tag::Id, 2, nullptr, nullptr)),
              mk(Tag::Eq, 0, mk(Tag::Id, 0, nullptr, nullptr), mk(Tag::Id, 3, nullptr, nullptr)));
  }
  return mk(Tag::And,
            0,
            mk(Tag::Eq, 0, mk(Tag::Id, 0, nullptr, nullptr), mk(Tag::Id, 1, nullptr, nullptr)),
            mk(Tag::Eq, 0, mk(Tag::Id, 2, nullptr, nullptr), mk(Tag::Id, 3, nullptr, nullptr)));
}

// One scratch slot per node, numbered in a post-order walk.
int AssignSlots(CNode *n, int next) {
  if (n == nullptr) return next;
  next = AssignSlots(n->lhs, next);
  next = AssignSlots(n->rhs, next);
  n->idx = next;
  return next + 1;
}
}  // namespace

static void Dispatch_C_SwitchByValue(benchmark::State &state) {
  const auto shape = static_cast<Shape>(state.range(0));
  QueryMemory memory;
  g_memory = &memory;
  g_alloc = static_cast<Alloc>(state.range(1));
  auto frames = MakeFrames(shape);
  size_t fi = 0;
  Arena<CNode> arena;
  auto *expr = BuildC(arena, shape);
  for (auto _ : state) {
    auto &frame = frames[fi];
    if (++fi == frames.size()) fi = 0;
    auto r = CEval(expr, frame);
    benchmark::DoNotOptimize(r);
  }
  state.SetItemsProcessed(state.iterations());
}

static void Dispatch_D_ScratchSlots(benchmark::State &state) {
  const auto shape = static_cast<Shape>(state.range(0));
  QueryMemory memory;
  g_memory = &memory;
  g_alloc = static_cast<Alloc>(state.range(1));
  auto frames = MakeFrames(shape);
  size_t fi = 0;
  Arena<CNode> arena;
  auto *expr = BuildC(arena, shape);
  const int slots = AssignSlots(expr, 0);
  DEvaluator ev{MakeScratch(slots), &frames[0]};
  for (auto _ : state) {
    ev.frame = &frames[fi];
    if (++fi == frames.size()) fi = 0;
    ev.Eval(expr);
    benchmark::DoNotOptimize(ev.scratch[expr->idx]);
  }
  state.SetItemsProcessed(state.iterations());
}

static void Dispatch_F_InPlaceOps(benchmark::State &state) {
  const auto shape = static_cast<Shape>(state.range(0));
  QueryMemory memory;
  g_memory = &memory;
  g_alloc = static_cast<Alloc>(state.range(1));
  auto frames = MakeFrames(shape);
  size_t fi = 0;
  Arena<CNode> arena;
  auto *expr = BuildC(arena, shape);
  const int slots = AssignSlots(expr, 0);
  FEvaluator ev{MakeScratch(slots), &frames[0]};
  for (auto _ : state) {
    ev.frame = &frames[fi];
    if (++fi == frames.size()) fi = 0;
    ev.Eval(expr);
    benchmark::DoNotOptimize(ev.scratch[expr->idx]);
  }
  state.SetItemsProcessed(state.iterations());
}

static void Dispatch_E_MiniVm(benchmark::State &state) {
  const auto shape = static_cast<Shape>(state.range(0));
  QueryMemory memory;
  g_memory = &memory;
  g_alloc = static_cast<Alloc>(state.range(1));
  auto frames = MakeFrames(shape);
  size_t fi = 0;
  std::vector<Instr> code;
  if (IsIntShape(shape)) {
    code = {{Ins::LoadSlot, 0},
            {Ins::LoadSlot, 1},
            {Ins::Add, 0},
            {Ins::LoadSlot, 2},
            {Ins::Eq, 0},
            {Ins::JmpIfFalse, 100},
            {Ins::LoadSlot, 0},
            {Ins::LoadSlot, 3},
            {Ins::Eq, 0},
            {Ins::And, 0}};
  } else {
    code = {{Ins::LoadSlot, 0},
            {Ins::LoadSlot, 1},
            {Ins::Eq, 0},
            {Ins::JmpIfFalse, 100},
            {Ins::LoadSlot, 2},
            {Ins::LoadSlot, 3},
            {Ins::Eq, 0},
            {Ins::And, 0}};
  }
  MiniVm vm(8);
  for (auto _ : state) {
    auto &frame = frames[fi];
    if (++fi == frames.size()) fi = 0;
    auto &r = vm.Run(code, frame);
    benchmark::DoNotOptimize(r);
  }
  state.SetItemsProcessed(state.iterations());
}

static void Dispatch_G_NativeSpecialised(benchmark::State &state) {
  const auto shape = static_cast<Shape>(state.range(0));
  QueryMemory memory;
  g_memory = &memory;
  g_alloc = static_cast<Alloc>(state.range(1));
  auto frames = MakeFrames(shape);
  size_t fi = 0;
  const bool ints = IsIntShape(shape);
  for (auto _ : state) {
    const auto &frame = frames[fi];
    if (++fi == frames.size()) fi = 0;
    bool r = ints ? GEvalInt(frame) : GEvalStr(frame);
    benchmark::DoNotOptimize(r);
  }
  state.SetItemsProcessed(state.iterations());
}

static void Dispatch_H_TypedScratch(benchmark::State &state) {
  const auto shape = static_cast<Shape>(state.range(0));
  QueryMemory memory;
  g_memory = &memory;
  g_alloc = static_cast<Alloc>(state.range(1));
  auto frames = MakeFrames(shape);
  size_t fi = 0;

  std::vector<TInstr> code;
  if (IsIntShape(shape)) {
    // (s0 + s1) == s2  AND  s0 == s3
    code = {{TIns::LoadInt, 0, 0, 0},
            {TIns::LoadInt, 1, 1, 0},
            {TIns::AddInt, 2, 0, 1},
            {TIns::LoadInt, 3, 2, 0},
            {TIns::EqInt, 1, 2, 3},
            {TIns::LoadInt, 4, 3, 0},
            {TIns::EqInt, 2, 0, 4},
            {TIns::AndBool, 0, 1, 2}};
  } else {
    // s0 == s1  AND  s2 == s3
    code = {{TIns::LoadStr, 0, 0, 0},
            {TIns::LoadStr, 1, 1, 0},
            {TIns::EqStr, 1, 0, 1},
            {TIns::LoadStr, 2, 2, 0},
            {TIns::LoadStr, 3, 3, 0},
            {TIns::EqStr, 2, 2, 3},
            {TIns::AndBool, 0, 1, 2}};
  }

  TypedVm vm(8, 8, 8);
  for (auto _ : state) {
    const auto &frame = frames[fi];
    if (++fi == frames.size()) fi = 0;
    // Boxed once, on the way out, which is what a caller actually reads.
    TypedValue r{vm.Run(code, frame), CurrentAlloc()};
    benchmark::DoNotOptimize(r);
  }
  state.SetItemsProcessed(state.iterations());
}

static void Dispatch_I_TypedScratchNullable(benchmark::State &state) {
  const auto shape = static_cast<Shape>(state.range(0));
  QueryMemory memory;
  g_memory = &memory;
  g_alloc = static_cast<Alloc>(state.range(1));
  auto frames = MakeFrames(shape);
  size_t fi = 0;
  auto const code = NullableCode(shape);

  // Where a failed guard sends the row: the boxed path, which is what a real
  // one would have to do, so the wasted typed attempt is paid for here too.
  Arena<ANode> arena;
  ANode *fallback = nullptr;
  if (IsIntShape(shape)) {
    auto *add = arena.Make<AAdd>(arena.Make<AId>(0), arena.Make<AId>(1));
    fallback = arena.Make<AAnd>(arena.Make<AEq>(add, arena.Make<AId>(2)),
                                arena.Make<AEq>(arena.Make<AId>(0), arena.Make<AId>(3)));
  } else {
    fallback = arena.Make<AAnd>(arena.Make<AEq>(arena.Make<AId>(0), arena.Make<AId>(1)),
                                arena.Make<AEq>(arena.Make<AId>(2), arena.Make<AId>(3)));
  }
  AEvaluator fallback_eval(&frames[0]);

  NullableTypedVm vm(8, 8, 8);
  int64_t fell_back = 0;
  for (auto _ : state) {
    auto &frame = frames[fi];
    if (++fi == frames.size()) fi = 0;
    Tri answer = Tri::Null;
    if (vm.Run(code, frame, answer)) {
      // What a caller reads: boxed once, and null stays null.
      TypedValue r = answer == Tri::Null ? TypedValue{CurrentAlloc()} : TypedValue{answer == Tri::True, CurrentAlloc()};
      benchmark::DoNotOptimize(r);
    } else {
      ++fell_back;
      fallback_eval.frame = &frame;
      auto r = fallback->Accept(fallback_eval);
      benchmark::DoNotOptimize(r);
    }
  }
  state.SetItemsProcessed(state.iterations());
  state.counters["fell_back"] = static_cast<double>(fell_back);
}

#define SHAPES                                                                      \
  Args({static_cast<int>(Shape::Int), static_cast<int>(Alloc::NewDelete)})          \
      ->Args({static_cast<int>(Shape::Int), static_cast<int>(Alloc::Pool)})         \
      ->Args({static_cast<int>(Shape::Str), static_cast<int>(Alloc::NewDelete)})    \
      ->Args({static_cast<int>(Shape::Str), static_cast<int>(Alloc::Pool)})         \
      ->Args({static_cast<int>(Shape::StrVar), static_cast<int>(Alloc::NewDelete)}) \
      ->Args({static_cast<int>(Shape::StrVar), static_cast<int>(Alloc::Pool)})      \
      ->Args({static_cast<int>(Shape::IntNull), static_cast<int>(Alloc::Pool)})     \
      ->Args({static_cast<int>(Shape::IntWrong), static_cast<int>(Alloc::Pool)})

static void Dispatch_000_AgreementCheck(benchmark::State &state) {
  const auto shape = static_cast<Shape>(state.range(0));
  QueryMemory memory;
  g_memory = &memory;
  g_alloc = static_cast<Alloc>(state.range(1));
  if (!SameAnswers(shape)) {
    state.SkipWithError("strategies disagree on the result; timings below are meaningless");
  }
  for (auto _ : state) {
  }
}

BENCHMARK(Dispatch_000_AgreementCheck)->SHAPES;

BENCHMARK(Dispatch_A_VirtualDouble)->SHAPES;
BENCHMARK(Dispatch_B_VirtualSingle)->SHAPES;
BENCHMARK(Dispatch_C_SwitchByValue)->SHAPES;
BENCHMARK(Dispatch_D_ScratchSlots)->SHAPES;
BENCHMARK(Dispatch_F_InPlaceOps)->SHAPES;
BENCHMARK(Dispatch_E_MiniVm)->SHAPES;
BENCHMARK(Dispatch_H_TypedScratch)->SHAPES;
BENCHMARK(Dispatch_I_TypedScratchNullable)->SHAPES;
BENCHMARK(Dispatch_G_NativeSpecialised)->SHAPES;

BENCHMARK_MAIN();
