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

#include "query/plan_v2/frontend/ast_converter.hpp"

#include <array>
#include <optional>
#include <span>
#include <vector>

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "query/parameters.hpp"
#include "query/plan_v2/rewrite/fold.hpp"

using memgraph::query::plan::v2::eclass;
using memgraph::query::plan::v2::egraph;

namespace memgraph::query {
namespace {

[[noreturn]] void ThrowNotImplementedYet(Tree const &op) { throw NotYetImplemented(op.GetTypeInfo().name); }

[[noreturn]] void ThrowNotImplementedYet(std::string_view feature) { throw NotYetImplemented(feature); }

class LoweringCtx;

// RAII bookmark over LoweringCtx's amortised scratch vector. The single
// underlying buffer is shared across the whole query; each frame captures
// the current size on construction and resizes back on destruction. Used
// via OpenScratch() to build N-ary child sequences (Function args, Output
// named outputs, Subquery exposed_syms). Nested frames are fine: each
// frame's destructor restores its own initial size, and as_span() is taken
// once, just before consumption, after all transitive pushes complete.
class ScratchFrame {
 public:
  explicit ScratchFrame(LoweringCtx &ctx);
  ~ScratchFrame();
  ScratchFrame(ScratchFrame const &) = delete;
  ScratchFrame &operator=(ScratchFrame const &) = delete;
  ScratchFrame(ScratchFrame &&) = delete;
  ScratchFrame &operator=(ScratchFrame &&) = delete;

  void push(eclass id) { buf_.push_back(id); }

  auto as_span() const -> std::span<eclass const> {
    return std::span<eclass const>{buf_.data() + initial_, buf_.size() - initial_};
  }

  void reset() { buf_.erase(buf_.begin() + static_cast<std::ptrdiff_t>(initial_), buf_.end()); }

  auto empty() const -> bool { return buf_.size() == initial_; }

 private:
  std::vector<eclass> &buf_;
  std::size_t initial_;
#ifndef NDEBUG
  LoweringCtx *ctx_;
  ScratchFrame *prev_;
#endif
};

class LoweringCtx {
 public:
  egraph &g;
  SymbolTable const &symbol_table;
  Parameters const &parameters;
  plan::v2::OutputColumnNames const &output_names;

  LoweringCtx(egraph &eg, SymbolTable const &st, Parameters const &params, plan::v2::OutputColumnNames const &names)
      : g(eg), symbol_table(st), parameters(params), output_names(names) {}

  auto OpenScratch() -> ScratchFrame { return ScratchFrame{*this}; }

 private:
  friend class ScratchFrame;
  std::vector<eclass> scratch_;
#ifndef NDEBUG
  ScratchFrame *top_frame_ = nullptr;
#endif
};

inline ScratchFrame::ScratchFrame(LoweringCtx &ctx)
    : buf_(ctx.scratch_),
      initial_(ctx.scratch_.size())
#ifndef NDEBUG
      ,
      ctx_(&ctx),
      prev_(ctx.top_frame_)
#endif
{
#ifndef NDEBUG
  ctx.top_frame_ = this;
#endif
}

inline ScratchFrame::~ScratchFrame() {
#ifndef NDEBUG
  DMG_ASSERT(ctx_->top_frame_ == this, "ScratchFrame destroyed out of LIFO order");
  ctx_->top_frame_ = prev_;
#endif
  buf_.erase(buf_.begin() + static_cast<std::ptrdiff_t>(initial_), buf_.end());
}

// AST symbol-bearing node (Identifier, NamedExpression) -> Symbol e-class.
// Hashconsing MakeSymbol(pos, name) ensures the same Cypher variable always
// lands in the same e-class.
template <typename AstNode>
  requires requires(AstNode &n) { n.symbol_pos_; }
auto SymEclassFor(LoweringCtx &ctx, AstNode &node) -> eclass {
  DMG_ASSERT(node.symbol_pos_ != -1, "AST symbol should have already been mapped into the frame");
  auto const &sym = ctx.symbol_table.at(node);
  return ctx.g.MakeSymbol(node.symbol_pos_, sym.name(), sym.token_position());
}

// Expression AST nodes not yet lowered. Each entry expands to a
// throwing Visit override on ExprLowering. Adding support: remove the row
// here and add a real Visit override.
//
// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define UNSUPPORTED_EXPRS(X) \
  X(RangeOperator)           \
  X(InListOperator)          \
  X(SubscriptOperator)       \
  X(ListSlicingOperator)     \
  X(IfOperator)              \
  X(IsNullOperator)          \
  X(MapLiteral)              \
  X(MapProjectionLiteral)    \
  X(PropertyLookup)          \
  X(AllPropertiesLookup)     \
  X(LabelsTest)              \
  X(Aggregation)             \
  X(Reduce)                  \
  X(Coalesce)                \
  X(Extract)                 \
  X(All)                     \
  X(Single)                  \
  X(Any)                     \
  X(None)                    \
  X(ListComprehension)       \
  X(RegexMatch)              \
  X(Exists)                  \
  X(PatternComprehension)    \
  X(EnumValueAccess)         \
  X(EdgeTypesTest)

// NOLINTEND(cppcoreguidelines-macro-usage)

auto Lower(LoweringCtx &ctx, Expression &expr) -> eclass;

// The constant value an expression denotes for this execution, or nullopt if
// any part is not constant. A `PrimitiveLiteral` carries its value directly; a
// `ParameterLookup` is constant when its value is known in `parameters` (a
// stripped literal or a bound user parameter); a `ListLiteral` is constant when
// every element is; and an operator over constant operands folds via the same
// `FoldConstant` the e-graph uses (so `[1 + 5, 2, 3]` recognises `1 + 5` as 6).
// All three recurse, so nested constants fold too.
// NOLINTBEGIN(cppcoreguidelines-macro-usage, bugprone-macro-parentheses)
auto ConstantValueOf(Expression &expr, Parameters const &parameters) -> std::optional<storage::ExternalPropertyValue> {
  if (auto *primitive = utils::Downcast<PrimitiveLiteral>(&expr)) return primitive->value_;
  if (auto *param = utils::Downcast<ParameterLookup>(&expr)) {
    if (auto const *value = parameters.MaybeAtTokenPosition(param->token_position_)) return *value;
    return std::nullopt;
  }
  if (auto *list = utils::Downcast<ListLiteral>(&expr)) {
    std::vector<storage::ExternalPropertyValue> values;
    values.reserve(list->elements_.size());
    for (auto *element : list->elements_) {
      auto value = ConstantValueOf(*element, parameters);
      if (!value) return std::nullopt;
      values.push_back(std::move(*value));
    }
    return storage::ExternalPropertyValue{std::move(values)};
  }
#define MG_FOLD_BINARY(Name, AstOp, ...)                             \
  if (auto *op = utils::Downcast<AstOp>(&expr)) {                    \
    auto lhs = ConstantValueOf(*op->expression1_, parameters);       \
    auto rhs = ConstantValueOf(*op->expression2_, parameters);       \
    if (!lhs || !rhs) return std::nullopt;                           \
    std::array const operands{std::move(*lhs), std::move(*rhs)};     \
    return plan::v2::FoldConstant(plan::v2::symbol::Name, operands); \
  }
  EGRAPH_BINARY_OPS(MG_FOLD_BINARY)
#undef MG_FOLD_BINARY
#define MG_FOLD_UNARY(Name, AstOp, ...)                              \
  if (auto *op = utils::Downcast<AstOp>(&expr)) {                    \
    auto operand = ConstantValueOf(*op->expression_, parameters);    \
    if (!operand) return std::nullopt;                               \
    std::array const operands{std::move(*operand)};                  \
    return plan::v2::FoldConstant(plan::v2::symbol::Name, operands); \
  }
  EGRAPH_UNARY_OPS(MG_FOLD_UNARY)
#undef MG_FOLD_UNARY
  return std::nullopt;
}

// NOLINTEND(cppcoreguidelines-macro-usage, bugprone-macro-parentheses)

// Slot-pattern visitor over ExpressionVisitor<void>: each Visit override
// assigns result_, which the free Lower() reads after Accept returns.
class ExprLowering : public ExpressionVisitor<void> {
 public:
  explicit ExprLowering(LoweringCtx &ctx) : ctx_(ctx) {}

  void Visit(ParameterLookup &expr) override {
    // plan_v2 is uncached, so the plan may be specialised to this execution's
    // values: when the parameter's value is known (a stripped literal, or a
    // user parameter), fold it to a constant so the analysis sees it. An
    // unknown position stays an opaque lookup.
    if (auto const *value = ctx_.parameters.MaybeAtTokenPosition(expr.token_position_)) {
      result_ = ctx_.g.MakeLiteral(*value);
    } else {
      result_ = ctx_.g.MakeParameterLookup(expr.token_position_);
    }
  }

  void Visit(PrimitiveLiteral &expr) override { result_ = ctx_.g.MakeLiteral(expr.value_); }

  void Visit(Identifier &expr) override { result_ = ctx_.g.MakeIdentifier(SymEclassFor(ctx_, expr)); }

  void Visit(Function &function) override {
    auto frame = ctx_.OpenScratch();
    for (auto *arg : function.arguments_) frame.push(Lower(ctx_, *arg));
    // Purity comes from the executor's authoritative function table; resolved
    // here at the boundary so the e-graph layer never depends on the evaluator.
    result_ = ctx_.g.MakeFunction(function.function_name_, frame.as_span(), IsFunctionPure(function.function_name_));
  }

  // A list whose elements are all (recursively) constant has a value known at
  // lowering time, so it lowers to a single folded list Literal. A non-constant
  // element needs the runtime list-construction node we don't model yet.
  void Visit(ListLiteral &list) override {
    auto value = ConstantValueOf(list, ctx_.parameters);
    if (!value) ThrowNotImplementedYet("ListLiteral with a non-constant element");
    result_ = ctx_.g.MakeLiteral(*value);
  }

  void Visit(NamedExpression & /*unused*/) override {
    // NamedExpression is parent-dispatched: RETURN wraps it as NamedOutput,
    // WITH wraps it as Bind. The wrapping clause-handler calls Lower on the
    // inner expression directly; NamedExpression itself never reaches here.
    ThrowNotImplementedYet("NamedExpression must be dispatched by its parent clause");
  }

  // NOLINTBEGIN(cppcoreguidelines-macro-usage, bugprone-macro-parentheses)
#define MG_VISIT_BINARY(Name, AstOp, ...)     \
  void Visit(AstOp &op) override {            \
    auto lhs = Lower(ctx_, *op.expression1_); \
    auto rhs = Lower(ctx_, *op.expression2_); \
    result_ = ctx_.g.Make##Name(lhs, rhs);    \
  }
  EGRAPH_BINARY_OPS(MG_VISIT_BINARY)
#undef MG_VISIT_BINARY

#define MG_VISIT_UNARY(Name, AstOp, ...)         \
  void Visit(AstOp &op) override {               \
    auto operand = Lower(ctx_, *op.expression_); \
    result_ = ctx_.g.Make##Name(operand);        \
  }
  EGRAPH_UNARY_OPS(MG_VISIT_UNARY)
#undef MG_VISIT_UNARY

#define MG_VISIT_UNSUPPORTED(T) \
  void Visit(T &op) override { ThrowNotImplementedYet(op); }
  UNSUPPORTED_EXPRS(MG_VISIT_UNSUPPORTED)
#undef MG_VISIT_UNSUPPORTED

  // NOLINTEND(cppcoreguidelines-macro-usage, bugprone-macro-parentheses)

  auto result() const -> eclass { return result_; }

 private:
  LoweringCtx &ctx_;
  eclass result_{0U};
};

auto Lower(LoweringCtx &ctx, Expression &expr) -> eclass {
  ExprLowering v{ctx};
  expr.Accept(v);
  return v.result();
}

auto LowerSingleQuery(SingleQuery &sq, LoweringCtx &ctx) -> eclass;

// ORDER BY / SKIP / LIMIT expressions are lowered for their e-graph
// presence only; their result e-classes are not chained into the row-pipe
// in the minimum scope. Hashconsing dedupes shared expressions across
// clauses; when Sort/Skip/Limit operators land, the resolver will recover
// these e-classes by hashconsing again.
void HashConsTailExpressions(ReturnBody const &body, LoweringCtx &ctx) {
  for (auto const &ob : body.order_by) (void)Lower(ctx, *ob.expression);
  if (body.skip) (void)Lower(ctx, *body.skip);
  if (body.limit) (void)Lower(ctx, *body.limit);
}

// exposed_syms come from the inner cypher_query_'s last RETURN clause.
// Hashconsing makes MakeSymbol(pos, name) return the SAME e-class the
// inner Output's NamedOutputs did, so the Subquery e-node and the inner
// Output reference identical Symbol leaves.
auto CollectSubqueryExposedSyms(SingleQuery &inner, LoweringCtx &ctx, ScratchFrame &frame) -> bool {
  bool saw_return = false;
  for (auto *clause : inner.clauses_) {
    auto *ret = utils::Downcast<query::Return>(clause);
    if (ret == nullptr) continue;
    saw_return = true;
    // Last RETURN wins.
    frame.reset();
    for (auto *ne : ret->body_.named_expressions) frame.push(SymEclassFor(ctx, *ne));
  }
  return saw_return;
}

auto LowerWith(query::With &with, eclass pipe, LoweringCtx &ctx) -> eclass {
  // A `*` projection carries its symbols in body_.all_identifiers, not in
  // named_expressions, so we never lower them to Identifier e-nodes. Bail out
  // rather than silently drop them - and rather than let referenced_syms miss a
  // `*`-referenced symbol and mis-classify a live binder as dead.
  if (with.body_.all_identifiers) ThrowNotImplementedYet("WITH * projection");
  for (auto *ne : with.body_.named_expressions) {
    auto expr = Lower(ctx, *ne->expression_);
    pipe = ctx.g.MakeBind(pipe, SymEclassFor(ctx, *ne), expr);
  }
  HashConsTailExpressions(with.body_, ctx);
  if (with.where_ != nullptr) ThrowNotImplementedYet(*with.where_);
  return pipe;
}

// Display name for a RETURN column: the alias, else the source text
// (`output_names` holds it, since stripping replaced the expression with a
// placeholder), else the AST name. The result-set header applies the same rule
// in interpreter.cpp, so both must stay in sync.
auto OutputDisplayName(LoweringCtx &ctx, NamedExpression const &ne) -> std::string {
  if (ne.is_aliased_) return ne.name_;
  if (auto const it = ctx.output_names.find(static_cast<int>(ne.token_position_)); it != ctx.output_names.end()) {
    return it->second;
  }
  return ne.name_;
}

auto LowerReturn(query::Return &ret, eclass pipe, LoweringCtx &ctx) -> eclass {
  // See LowerWith: `*` symbols bypass named_expressions, so refuse them rather
  // than drop columns or let referenced_syms miss a `*`-referenced binding.
  if (ret.body_.all_identifiers) ThrowNotImplementedYet("RETURN * projection");
  auto frame = ctx.OpenScratch();
  for (auto *ne : ret.body_.named_expressions) {
    auto expr = Lower(ctx, *ne->expression_);
    frame.push(ctx.g.MakeNamedOutput(OutputDisplayName(ctx, *ne), SymEclassFor(ctx, *ne), expr));
  }
  auto output = ctx.g.MakeOutput(pipe, frame.as_span());
  HashConsTailExpressions(ret.body_, ctx);
  return output;
}

auto LowerUnwind(query::Unwind &unwind, eclass pipe, LoweringCtx &ctx) -> eclass {
  DMG_ASSERT(unwind.named_expression_ != nullptr, "Unwind must have a named expression");
  auto &ne = *unwind.named_expression_;
  auto list_expr = Lower(ctx, *ne.expression_);
  return ctx.g.MakeUnwind(pipe, SymEclassFor(ctx, ne), list_expr);
}

auto LowerCallSubquery(query::CallSubquery &cs, eclass pipe, LoweringCtx &ctx) -> eclass {
  // Minimum scope: non-importing CALL { ... } RETURN ...
  if (cs.has_variable_scope_) ThrowNotImplementedYet("importing CALL with explicit scope clause");
  if (cs.all_variables_scoped_) ThrowNotImplementedYet("CALL with implicit star scope clause");
  DMG_ASSERT(cs.cypher_query_ != nullptr && cs.cypher_query_->single_query_ != nullptr,
             "CALL block missing inner cypher query");

  auto &inner = *cs.cypher_query_->single_query_;
  auto inner_root = LowerSingleQuery(inner, ctx);

  auto frame = ctx.OpenScratch();
  auto saw_return = CollectSubqueryExposedSyms(inner, ctx, frame);
  MG_ASSERT(saw_return && !frame.empty(), "CALL block must end in RETURN; unit subqueries are TODO");
  return ctx.g.MakeSubquery(pipe, inner_root, frame.as_span());
}

auto LowerClause(Clause &clause, eclass pipe, LoweringCtx &ctx) -> eclass {
  if (auto *w = utils::Downcast<query::With>(&clause)) return LowerWith(*w, pipe, ctx);
  if (auto *r = utils::Downcast<query::Return>(&clause)) return LowerReturn(*r, pipe, ctx);
  if (auto *u = utils::Downcast<query::Unwind>(&clause)) return LowerUnwind(*u, pipe, ctx);
  if (auto *cs = utils::Downcast<query::CallSubquery>(&clause)) return LowerCallSubquery(*cs, pipe, ctx);
  ThrowNotImplementedYet(clause);
}

auto LowerSingleQuery(SingleQuery &sq, LoweringCtx &ctx) -> eclass {
  auto pipe = ctx.g.MakeOnce();
  for (auto *clause : sq.clauses_) {
    pipe = LowerClause(*clause, pipe, ctx);
  }
  return pipe;
}

auto LowerCypherQuery(CypherQuery &cq, LoweringCtx &ctx) -> eclass {
  if (cq.memory_limit_ != nullptr) ThrowNotImplementedYet("query memory limit");
  (void)cq.memory_scale_;  // TODO
  if (cq.pre_query_directives_.commit_frequency_ != nullptr) ThrowNotImplementedYet("commit frequency directive");
  if (cq.pre_query_directives_.hops_limit_ != nullptr) ThrowNotImplementedYet("hop limit directive");
  if (!cq.pre_query_directives_.index_hints_.empty()) ThrowNotImplementedYet("index hints directive");
  DMG_ASSERT(cq.single_query_ != nullptr, "CypherQuery missing single_query_");
  return LowerSingleQuery(*cq.single_query_, ctx);
}

}  // namespace
}  // namespace memgraph::query

namespace memgraph::query::plan::v2 {

auto ConvertToEgraph(CypherQuery const &query, SymbolTable const &symbol_table, Parameters const &parameters,
                     OutputColumnNames const &output_names) -> std::tuple<egraph, eclass> {
  auto eg = egraph{};
  auto ctx = LoweringCtx{eg, symbol_table, parameters, output_names};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  auto root = LowerCypherQuery(const_cast<CypherQuery &>(query), ctx);
  return {std::move(eg), root};
}

}  // namespace memgraph::query::plan::v2
