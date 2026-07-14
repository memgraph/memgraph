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

#include "query/plan_v2/frontend/builder.hpp"

#include <ranges>
#include <vector>

#include <range/v3/range/conversion.hpp>

#include "query/plan/preprocess.hpp"
#include "query/plan_v2/egraph/child_layout.hpp"
#include "query/plan_v2/egraph/op_ast_lists.hpp"
#include "query/plan_v2/egraph/symbol_dispatch.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::plan::v2 {
namespace {

// A child slot: a shared position (from child_layout) paired with the
// build-result type expected there. The position stays single-sourced in
// child_layout; the slot adds only the builder-side type. ChildSlot names one
// child; ChildRestSlot names the variadic tail from `begin`, each of type T.
template <std::size_t Index, typename T>
struct ChildSlot {
  static constexpr std::size_t index = Index;
  using type = T;
};

template <std::size_t Begin, typename T>
struct ChildRestSlot {
  static constexpr std::size_t begin = Begin;
  using type = T;
};

template <typename S>
concept PositionalSlot = requires { S::index; };
template <typename S>
concept TailSlot = requires { S::begin; };

// Typed view over the built children: `get<Slot>()` resolves both the position
// and the build-result type from the slot, so a build body names neither.
struct Children {
  ChildrenRef refs;

  template <PositionalSlot Slot>
  auto get() const -> Slot::type const & {
    if (refs.size() <= Slot::index) ThrowPlannerBug("missing child node");
    return as<typename Slot::type>(refs[Slot::index]);
  }

  // Tail slot: a lazy view over the tail children, each built to Slot::type.
  // The caller materialises it into whatever container it needs.
  template <TailSlot Slot>
  auto get() const {
    return refs | std::views::drop(Slot::begin) |
           std::views::transform([](ChildRef c) -> typename Slot::type { return as<typename Slot::type>(c); });
  }

  // Built child at a runtime index, cast to `T`. For nodes split at a runtime
  // boundary (OrderBy: sort-key Expressions then value Symbols) that a
  // compile-time slot can't name.
  template <typename T>
  auto at(std::size_t index) const -> T const & {
    if (refs.size() <= index) ThrowPlannerBug("missing child node");
    return as<T>(refs[index]);
  }

  [[nodiscard]] auto size() const -> std::size_t { return refs.size(); }

 private:
  // Cast a built child to its expected type, or throw if it is something else.
  template <typename T>
  static auto as(ChildRef child) -> T const & {
    auto const *ptr = std::get_if<T>(&child.get());
    if (!ptr) ThrowPlannerBug("child node has unexpected type");
    return *ptr;
  }
};

/// Alive Bind emits all 3 enode children (input, sym, expr); dead Bind emits
/// just the pipe input. The resolver's emit shape is the alive/dead signal.
[[nodiscard]] auto IsAliveBind(ENodeRef node, Children children) -> bool {
  return children.size() == node.children().size();
}

// ============================================================================
// symbol_build_traits<S>: per-symbol build bodies. Every spec exposes a
// uniform `build(state, node, children)`; `Build` dispatches to it by symbol.
// ============================================================================

template <symbol S>
struct symbol_build_traits;  // primary intentionally undefined; every symbol must specialise.

template <>
struct symbol_build_traits<symbol::Once> {
  using result_type = LogicalOperatorPtr;

  static auto build(BuildState & /*state*/, ENodeRef /*node*/, Children /*children*/) -> result_type {
    return std::make_unique<Once>();
  }
};

template <>
struct symbol_build_traits<symbol::Bind> {
  using result_type = LogicalOperatorPtr;

  struct slots {
    using input = ChildSlot<child::bind::input, LogicalOperatorPtr>;
    using sym = ChildSlot<child::bind::sym, Symbol>;
    using expr = ChildSlot<child::bind::expr, Expression *>;
  };

  static auto build(BuildState &state, ENodeRef node, Children children) -> result_type {
    auto const &input = children.get<slots::input>();
    if (IsAliveBind(node, children)) {
      return build_alive(state, input, children.get<slots::sym>(), children.get<slots::expr>());
    }
    DMG_ASSERT(children.size() == 1, "dead Bind must emit exactly the input child");
    return build_dead(input);
  }

 private:
  // Alive Bind: input row pipe + bound symbol + bound expression.  Emits a
  // Produce that fuses with an immediate-parent Produce when possible, so
  // chains of binds collapse into a single multi-NamedExpression Produce.
  static auto build_alive(BuildState &state, LogicalOperatorPtr const &input, Symbol const &sym, Expression *expr)
      -> result_type {
    // Diagnostics-only loss: rebuilt NamedExpression carries no token_position_/
    // is_aliased_ (source spans, RETURN-alias naming). Plan semantics unaffected.
    auto *named_expression = state.ast_storage.Create<NamedExpression>(sym.name(), expr);
    named_expression->MapTo(sym);

    if (input->GetTypeInfo() == Produce::kType) {
      auto const &produce = static_pointer_cast<Produce>(input);
      // TODO: check if its ok to steal from the other produce (Operators make a
      // tree, we are skipping hence unused)
      auto named_expressions = produce->named_expressions_;
      named_expressions.emplace_back(named_expression);
      return std::make_shared<Produce>(produce->input(), named_expressions);
    }
    return std::make_shared<Produce>(input, std::vector{named_expression});
  }

  // Dead Bind: resolver elided sym + expr. Forward the input row pipe unchanged.
  static auto build_dead(LogicalOperatorPtr const &input) -> result_type { return input; }
};

template <>
struct symbol_build_traits<symbol::Symbol> {
  using result_type = Symbol;

  static auto build(BuildState &state, ENodeRef node, Children /*children*/) -> result_type {
    auto const sym_pos = static_cast<std::int32_t>(node.disambiguator());
    // A symbol is identity: every build of the same logical symbol must yield
    // the same compact position, so a binder and its references share a frame
    // slot. The build runs once per resolver entry, so memoise here.
    if (auto const cached = state.symbol_cache.find(sym_pos); cached != state.symbol_cache.end()) {
      return cached->second;
    }
    auto const it = state.symbol_store.find(sym_pos);
    if (it == state.symbol_store.end()) [[unlikely]] {
      ThrowPlannerBug("symbol not found in store");
    }
    // token_position is carried so result-header naming can recover an unaliased
    // column's source text. Diagnostics-only loss: no user_declared_/type_.
    auto const &symbol = state.symbol_table.CreateSymbol(
        it->second.name, false /*TODO*/, Symbol::Type::ANY, static_cast<int32_t>(it->second.token_position));
    return state.symbol_cache.emplace(sym_pos, symbol).first->second;
  }
};

template <>
struct symbol_build_traits<symbol::Literal> {
  using result_type = Expression *;

  static auto build(BuildState &state, ENodeRef node, Children /*children*/) -> result_type {
    auto const dis = node.disambiguator();
    if (dis >= state.literal_info.size()) [[unlikely]] {
      ThrowPlannerBug("literal id out of range");
    }
    return state.ast_storage.Create<PrimitiveLiteral>(*state.literal_info[dis]);
  }
};

template <>
struct symbol_build_traits<symbol::Identifier> {
  using result_type = Expression *;

  struct slots {
    using sym = ChildSlot<child::identifier::sym, Symbol>;
  };

  static auto build(BuildState &state, ENodeRef /*node*/, Children children) -> result_type {
    auto const &sym = children.get<slots::sym>();
    auto *identifier = state.ast_storage.Create<Identifier>(sym.name(), sym.user_declared());
    identifier->MapTo(sym);
    return identifier;
  }
};

template <>
struct symbol_build_traits<symbol::Output> {
  using result_type = LogicalOperatorPtr;

  struct slots {
    using pipe = ChildSlot<child::output::pipe, LogicalOperatorPtr>;
    using named = ChildRestSlot<child::output::first_named, NamedExpression *>;
  };

  static auto build(BuildState & /*state*/, ENodeRef /*node*/, Children children) -> result_type {
    auto const &input = children.get<slots::pipe>();
    auto named_expressions = children.get<slots::named>() | ranges::to<std::vector>;
    return std::make_shared<Produce>(input, std::move(named_expressions));
  }
};

template <>
struct symbol_build_traits<symbol::NamedOutput> {
  using result_type = NamedExpression *;

  struct slots {
    using sym = ChildSlot<child::named_out::sym, Symbol>;
    using expr = ChildSlot<child::named_out::expr, Expression *>;
  };

  static auto build(BuildState &state, ENodeRef node, Children children) -> result_type {
    auto const dis = node.disambiguator();
    DMG_ASSERT(dis < state.named_output_info.size(), "NamedOutput id out of range");

    auto const name = state.named_output_info[dis];
    auto const &sym = children.get<slots::sym>();
    auto const &expression = children.get<slots::expr>();

    // Diagnostics-only loss: rebuilt NamedExpression carries no token_position_/
    // is_aliased_ (source spans, RETURN-alias naming). Plan semantics unaffected.
    auto *named_expression = state.ast_storage.Create<NamedExpression>(std::string(name), expression);
    named_expression->MapTo(sym);
    return named_expression;
  }
};

template <>
struct symbol_build_traits<symbol::ParamLookup> {
  using result_type = Expression *;

  static auto build(BuildState &state, ENodeRef node, Children /*children*/) -> result_type {
    auto const dis = node.disambiguator();
    return state.ast_storage.Create<ParameterLookup>(dis);
  }
};

template <>
struct symbol_build_traits<symbol::Unwind> {
  using result_type = LogicalOperatorPtr;

  struct slots {
    using input = ChildSlot<child::unwind::input, LogicalOperatorPtr>;
    using sym = ChildSlot<child::unwind::sym, Symbol>;
    using list = ChildSlot<child::unwind::list, Expression *>;
  };

  // Dead Unwind's resolved children are the densely-packed [input, list]; the
  // sym leaf is elided. Only the input is consumed here - the list child is
  // resolved (for cost/extraction consistency) but its built form is discarded,
  // since the row count comes from its analysis fact, not an Expression (see
  // build_dead). No accessor for the list slot on purpose.
  struct dead_slots {
    using input = ChildSlot<child::unwind_dead::input, LogicalOperatorPtr>;
  };

  static auto build(BuildState &state, ENodeRef node, Children children) -> result_type {
    if (children.size() == node.children().size()) {
      return build_alive(children.get<slots::input>(), children.get<slots::sym>(), children.get<slots::list>());
    }
    DMG_ASSERT(children.size() == 2, "dead Unwind must emit exactly the input and list children");
    return build_dead(state, node, children.get<dead_slots::input>());
  }

 private:
  // Alive Unwind: bind each list element to the sym, one output row per element.
  static auto build_alive(LogicalOperatorPtr const &input, Symbol const &sym, Expression *list_expr) -> result_type {
    return std::static_pointer_cast<LogicalOperator>(std::make_shared<query::plan::Unwind>(input, list_expr, sym));
  }

  // Dead Unwind: the sym is unused and the list length is statically known (the
  // gate that picks this alt), so emit a CardinalityScale carrying that length.
  // The count comes from the list child's analysis fact on the e-node; the list
  // is never evaluated at runtime (any built Expression for it is discarded).
  static auto build_dead(BuildState &state, ENodeRef node, LogicalOperatorPtr const &input) -> result_type {
    auto const list_eclass = node.children()[child::unwind::list];
    auto const *expr = state.egraph.analysis_of(list_eclass).expression();
    DMG_ASSERT(expr != nullptr && expr->known_list_length.has_value(),
               "dead Unwind requires a statically known list length");
    auto const scale = *expr->known_list_length;
    // n=1 scales each input row to one row: the operator is identity, so drop it.
    if (scale == 1) return input;
    // n=0 scales every input row to zero rows: the result is empty.
    if (scale == 0) return std::static_pointer_cast<LogicalOperator>(std::make_shared<query::plan::EmptyResult>(input));
    return std::static_pointer_cast<LogicalOperator>(std::make_shared<query::plan::CardinalityScale>(input, scale));
  }
};

template <>
struct symbol_build_traits<symbol::Filter> {
  using result_type = LogicalOperatorPtr;

  struct slots {
    using input = ChildSlot<child::filter::input, LogicalOperatorPtr>;
    using predicate = ChildSlot<child::filter::predicate, Expression *>;
  };

  // Runtime filtering uses expression_. all_filters_ is display-only (feeds
  // Filter::ToString's EXPLAIN label). We record the whole predicate as one Generic
  // filter rather than classifying it (v1's CollectFilterExpression): index selection
  // in plan_v2 will be an e-graph rewrite that offers indexed scans as a costed
  // option, not a classification baked in at build time.
  static auto build(BuildState &state, ENodeRef /*node*/, Children children) -> result_type {
    auto const &input = children.get<slots::input>();
    auto *predicate = children.get<slots::predicate>();
    query::plan::UsedSymbolsCollector collector(state.symbol_table);
    predicate->Accept(collector);
    query::plan::Filters all_filters;
    all_filters.SetFilters(
        {query::plan::FilterInfo{query::plan::FilterInfo::Type::Generic, predicate, std::move(collector.symbols_)}});
    return std::static_pointer_cast<LogicalOperator>(std::make_shared<query::plan::Filter>(
        input, std::vector<std::shared_ptr<LogicalOperator>>{}, predicate, std::move(all_filters)));
  }
};

template <>
struct symbol_build_traits<symbol::Distinct> {
  using result_type = LogicalOperatorPtr;

  struct slots {
    using input = ChildSlot<child::distinct::input, LogicalOperatorPtr>;
    using values = ChildRestSlot<child::distinct::first_value, Symbol>;
  };

  // The value Symbols were demanded by cost (materialised), so read them straight
  // back here rather than from input->OutputSymbols() (empty for an inlined WITH).
  static auto build(BuildState & /*state*/, ENodeRef /*node*/, Children children) -> result_type {
    auto const &input = children.get<slots::input>();
    auto value_symbols = children.get<slots::values>() | ranges::to<std::vector>;
    return std::static_pointer_cast<LogicalOperator>(
        std::make_shared<query::plan::Distinct>(input, std::move(value_symbols)));
  }
};

template <>
struct symbol_build_traits<symbol::Skip> {
  using result_type = LogicalOperatorPtr;

  struct slots {
    using input = ChildSlot<child::skip::input, LogicalOperatorPtr>;
    using count = ChildSlot<child::skip::count, Expression *>;
  };

  static auto build(BuildState & /*state*/, ENodeRef /*node*/, Children children) -> result_type {
    auto const &input = children.get<slots::input>();
    auto *count = children.get<slots::count>();
    return std::static_pointer_cast<LogicalOperator>(std::make_shared<query::plan::Skip>(input, count));
  }
};

template <>
struct symbol_build_traits<symbol::Limit> {
  using result_type = LogicalOperatorPtr;

  struct slots {
    using input = ChildSlot<child::limit::input, LogicalOperatorPtr>;
    using count = ChildSlot<child::limit::count, Expression *>;
  };

  static auto build(BuildState & /*state*/, ENodeRef /*node*/, Children children) -> result_type {
    auto const &input = children.get<slots::input>();
    auto *count = children.get<slots::count>();
    return std::static_pointer_cast<LogicalOperator>(std::make_shared<query::plan::Limit>(input, count));
  }
};

template <>
struct symbol_build_traits<symbol::OrderBy> {
  using result_type = LogicalOperatorPtr;

  struct slots {
    using input = ChildSlot<child::order_by::input, LogicalOperatorPtr>;
  };

  // Split the tail by the interned orderings count: first num_keys are sort-key
  // Expressions, the rest value Symbols to remember through the sort.
  static auto build(BuildState &state, ENodeRef node, Children children) -> result_type {
    using namespace child::order_by;
    auto const &input = children.get<slots::input>();
    auto const dis = node.disambiguator();
    DMG_ASSERT(dis < state.orderby_info.size(), "OrderBy id out of range");
    auto const &orderings = state.orderby_info[dis];
    auto const num_keys = orderings.size();
    DMG_ASSERT(children.size() >= first_expr + num_keys, "OrderBy has fewer children than sort keys");

    std::vector<SortItem> order_by;
    order_by.reserve(num_keys);
    for (std::size_t i = 0; i < num_keys; ++i) {
      order_by.push_back(SortItem{.ordering = orderings[i], .expression = children.at<Expression *>(first_expr + i)});
    }
    std::vector<Symbol> output_symbols;
    output_symbols.reserve(children.size() - first_expr - num_keys);
    for (std::size_t i = first_expr + num_keys; i < children.size(); ++i) {
      output_symbols.push_back(children.at<Symbol>(i));
    }
    return std::static_pointer_cast<LogicalOperator>(
        std::make_shared<query::plan::OrderBy>(input, order_by, output_symbols));
  }
};

template <>
struct symbol_build_traits<symbol::Subquery> {
  using result_type = LogicalOperatorPtr;

  struct slots {
    using outer = ChildSlot<child::subquery::outer, LogicalOperatorPtr>;
    using inner = ChildSlot<child::subquery::inner, LogicalOperatorPtr>;
  };

  static auto build(BuildState & /*state*/, ENodeRef /*node*/, Children children) -> result_type {
    auto const &outer_input = children.get<slots::outer>();
    auto const &inner_root = children.get<slots::inner>();

    // exposed_sym children at child::subquery::first_exposed.. are structural
    // metadata for the Subquery e-node; the v1 Apply operator doesn't consume
    // them directly.
    return std::static_pointer_cast<LogicalOperator>(
        std::make_shared<query::plan::Apply>(outer_input, inner_root, /*subquery_has_return=*/true));
  }
};

template <>
struct symbol_build_traits<symbol::Function> {
  using result_type = Expression *;

  struct slots {
    using args = ChildRestSlot<child::function::first_arg, Expression *>;
  };

  static auto build(BuildState &state, ENodeRef node, Children children) -> result_type {
    auto const dis = node.disambiguator();
    if (dis >= state.function_info.size()) [[unlikely]] {
      ThrowPlannerBug("function id not found in store");
    }
    auto const &name = state.function_info[dis].name;
    auto args = children.get<slots::args>() | ranges::to<std::vector>;
    return state.ast_storage.Create<Function>(name, args);
  }
};

// Binary / unary trait specs - generated from the X-lists.
// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define MG_BUILD_BINARY_TRAIT(Name, AstOp, ...)                                                 \
  template <>                                                                                   \
  struct symbol_build_traits<symbol::Name> {                                                    \
    using result_type = Expression *;                                                           \
    struct slots {                                                                              \
      using lhs = ChildSlot<child::binary::lhs, Expression *>;                                  \
      using rhs = ChildSlot<child::binary::rhs, Expression *>;                                  \
    };                                                                                          \
    static auto build(BuildState &state, ENodeRef /*node*/, Children children) -> result_type { \
      auto const &lhs = children.get<slots::lhs>();                                             \
      auto const &rhs = children.get<slots::rhs>();                                             \
      return state.ast_storage.Create<AstOp>(lhs, rhs);                                         \
    }                                                                                           \
  };
EGRAPH_BINARY_OPS(MG_BUILD_BINARY_TRAIT)
#undef MG_BUILD_BINARY_TRAIT

#define MG_BUILD_UNARY_TRAIT(Name, AstOp, ...)                                                  \
  template <>                                                                                   \
  struct symbol_build_traits<symbol::Name> {                                                    \
    using result_type = Expression *;                                                           \
    struct slots {                                                                              \
      using operand = ChildSlot<child::unary::operand, Expression *>;                           \
    };                                                                                          \
    static auto build(BuildState &state, ENodeRef /*node*/, Children children) -> result_type { \
      auto const &operand = children.get<slots::operand>();                                     \
      return state.ast_storage.Create<AstOp>(operand);                                          \
    }                                                                                           \
  };
EGRAPH_UNARY_OPS(MG_BUILD_UNARY_TRAIT)
#undef MG_BUILD_UNARY_TRAIT

// NOLINTEND(cppcoreguidelines-macro-usage)

}  // namespace

auto BuildState::Build(ENodeRef node, ChildrenRef children) -> BuildResult {
  return DispatchBySymbol(node.symbol(), [&]<symbol S>() -> BuildResult {
    return BuildResult{symbol_build_traits<S>::build(*this, node, Children{children})};
  });
}

}  // namespace memgraph::query::plan::v2
