// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/v2/plan/operator_distributed.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <queue>
#include <random>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <cppitertools/chain.hpp>
#include <cppitertools/imap.hpp>

#include "query/v2/context.hpp"
#include "query/v2/db_accessor.hpp"
#include "query/v2/exceptions.hpp"
#include "query/v2/frontend/ast/ast.hpp"
#include "query/v2/frontend/semantic/symbol_table.hpp"
#include "query/v2/interpret/eval.hpp"
#include "query/v2/path.hpp"
#include "query/v2/plan/scoped_profile.hpp"
#include "query/v2/procedure/cypher_types.hpp"
#include "query/v2/procedure/mg_procedure_impl.hpp"
#include "query/v2/procedure/module.hpp"
#include "storage/v3/property_value.hpp"
#include "utils/algorithm.hpp"
#include "utils/csv_parsing.hpp"
#include "utils/event_counter.hpp"
#include "utils/exceptions.hpp"
#include "utils/fnv.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/unordered_map.hpp"
#include "utils/pmr/unordered_set.hpp"
#include "utils/pmr/vector.hpp"
#include "utils/readable_size.hpp"
#include "utils/string.hpp"
#include "utils/temporal.hpp"
#include "utils/variant_helpers.hpp"

// macro for the default implementation of LogicalOperator::Accept
// that accepts the visitor and visits it's input_ operator
#define ACCEPT_WITH_INPUT(class_name)                                    \
  bool class_name::Accept(HierarchicalLogicalOperatorVisitor &visitor) { \
    if (visitor.PreVisit(*this)) {                                       \
      input_->Accept(visitor);                                           \
    }                                                                    \
    return visitor.PostVisit(*this);                                     \
  }

#define WITHOUT_SINGLE_INPUT(class_name)                         \
  bool class_name::HasSingleInput() const { return false; }      \
  std::shared_ptr<LogicalOperator> class_name::input() const {   \
    LOG_FATAL("Operator " #class_name " has no single input!");  \
  }                                                              \
  void class_name::set_input(std::shared_ptr<LogicalOperator>) { \
    LOG_FATAL("Operator " #class_name " has no single input!");  \
  }

namespace EventCounter {
extern const Event OnceOperator;
extern const Event CreateExpandOperator;
extern const Event ScanAllOperator;
extern const Event ScanAllByLabelOperator;
extern const Event ScanAllByLabelPropertyRangeOperator;
extern const Event ScanAllByLabelPropertyValueOperator;
extern const Event ScanAllByLabelPropertyOperator;
extern const Event ScanAllByIdOperator;
extern const Event ExpandOperator;
extern const Event ExpandVariableOperator;
extern const Event ConstructNamedPathOperator;
extern const Event FilterOperator;
extern const Event ProduceOperator;
extern const Event DeleteOperator;
extern const Event SetPropertyOperator;
extern const Event SetPropertiesOperator;
extern const Event SetLabelsOperator;
extern const Event RemovePropertyOperator;
extern const Event RemoveLabelsOperator;
extern const Event EdgeUniquenessFilterOperator;
extern const Event AccumulateOperator;
extern const Event AggregateOperator;
extern const Event SkipOperator;
extern const Event LimitOperator;
extern const Event OrderByOperator;
extern const Event MergeOperator;
extern const Event OptionalOperator;
extern const Event UnwindOperator;
extern const Event DistinctOperator;
extern const Event UnionOperator;
extern const Event CartesianOperator;
extern const Event CallProcedureOperator;
extern const Event ForeachOperator;
}  // namespace EventCounter

namespace memgraph::query::v2::plan::distributed {

namespace {

// Custom equality function for a vector of typed values.
// Used in unordered_maps in Aggregate and Distinct operators.
struct TypedValueVectorEqual {
  template <class TAllocator>
  bool operator()(const std::vector<TypedValue, TAllocator> &left,
                  const std::vector<TypedValue, TAllocator> &right) const {
    MG_ASSERT(left.size() == right.size(),
              "TypedValueVector comparison should only be done over vectors "
              "of the same size");
    return std::equal(left.begin(), left.end(), right.begin(), TypedValue::BoolEqual{});
  }
};

// Returns boolean result of evaluating filter expression. Null is treated as
// false. Other non boolean values raise a QueryRuntimeException.
bool EvaluateFilter(ExpressionEvaluator &evaluator, Expression *filter) {
  TypedValue result = filter->Accept(evaluator);
  // Null is treated like false.
  if (result.IsNull()) return false;
  if (result.type() != TypedValue::Type::Bool)
    throw QueryRuntimeException("Filter expression must evaluate to bool or null, got {}.", result.type());
  return result.ValueBool();
}

template <typename T>
uint64_t ComputeProfilingKey(const T *obj) {
  static_assert(sizeof(T *) == sizeof(uint64_t));
  return reinterpret_cast<uint64_t>(obj);
}

void ResizeFrames(Frames &frames, int last_filled_frame) {
  MG_ASSERT(last_filled_frame >= 0);
  MG_ASSERT(frames.size() > last_filled_frame);

  frames.resize(last_filled_frame + 1);
}
}  // namespace

#define SCOPED_PROFILE_OP(name) ScopedProfile profile{ComputeProfilingKey(this), name, &context};

bool Once::OnceCursor::Pull(Frames &, ExecutionContext &context) {
  SCOPED_PROFILE_OP("Once");

  if (!did_pull_) {
    did_pull_ = true;
    return true;
  }
  return false;
}

UniqueCursorPtr Once::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::OnceOperator);

  return MakeUniqueCursorPtr<OnceCursor>(mem);
}

WITHOUT_SINGLE_INPUT(Once);

void Once::OnceCursor::Shutdown() {}

void Once::OnceCursor::Reset() { did_pull_ = false; }

template <class TVerticesFun>
class ScanAllCursor : public Cursor {
 public:
  explicit ScanAllCursor(Symbol output_symbol, UniqueCursorPtr input_cursor, TVerticesFun get_vertices,
                         const char *op_name)
      : output_symbol_(output_symbol),
        input_cursor_(std::move(input_cursor)),
        get_vertices_(std::move(get_vertices)),
        op_name_(op_name) {}

  bool Pull(Frames &frames, ExecutionContext &context) override {
    SCOPED_PROFILE_OP(op_name_);
    if (MustAbort(context)) {
      throw HintedAbortError();
    }

    while (!vertices_ || vertices_it_.value() == vertices_.value().end()) {
      if (!input_cursor_->Pull(frames, context)) {
        return false;
      }
      // We need a getter function, because in case of exhausting a lazy
      // iterable, we cannot simply reset it by calling begin().
      auto next_vertices = get_vertices_(frames, context);
      if (!next_vertices) {
        continue;
      }
      // Since vertices iterator isn't nothrow_move_assignable, we have to use
      // the roundabout assignment + emplace, instead of simple:
      // vertices _ = get_vertices_(frame, context);
      vertices_.emplace(std::move(next_vertices.value()));
      vertices_it_.emplace(vertices_.value().begin());
    }

    auto last_filled_frame = 0;
    for (auto idx = 0; idx < frames.size(); ++idx) {
      auto &frame = *frames[idx];
      frame[output_symbol_] = *vertices_it_.value();
      ++vertices_it_.value();
      last_filled_frame = idx;

      if (vertices_it_.value() == vertices_.value().end() && idx < frames.size() - 1) {
        /*
        'vertices_it_.value() == vertices_.value().end()' means we have exhausted all vertices
        If 'idx < frames.size() - 1' means we do not have enough vertices to fill all frames and that we are at the last
        batch. In that case, we can simply reduce the number of frames.
        */
        break;
      }
    }

    ResizeFrames(frames, last_filled_frame);

    return true;
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    vertices_ = std::nullopt;
    vertices_it_ = std::nullopt;
  }

 private:
  const Symbol output_symbol_;
  const UniqueCursorPtr input_cursor_;
  TVerticesFun get_vertices_;
  std::optional<typename std::result_of<TVerticesFun(Frames &, ExecutionContext &)>::type::value_type> vertices_;
  std::optional<decltype(vertices_.value().begin())> vertices_it_;
  const char *op_name_;
};

ScanAll::ScanAll(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol, storage::v3::View view)
    : input_(input ? input : std::make_shared<Once>()), output_symbol_(output_symbol), view_(view) {}

ACCEPT_WITH_INPUT(ScanAll)

UniqueCursorPtr ScanAll::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::ScanAllOperator);

  auto vertices = [this](Frames &, ExecutionContext &context) {
    auto *db = context.db_accessor;
    return std::make_optional(db->Vertices(view_));
  };
  return MakeUniqueCursorPtr<ScanAllCursor<decltype(vertices)>>(mem, output_symbol_, input_->MakeCursor(mem),
                                                                std::move(vertices), "ScanAll");
}

std::vector<Symbol> ScanAll::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(output_symbol_);
  return symbols;
}

ScanAllByLabel::ScanAllByLabel(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol,
                               storage::v3::LabelId label, storage::v3::View view)
    : ScanAll(input, output_symbol, view), label_(label) {}

ACCEPT_WITH_INPUT(ScanAllByLabel)

UniqueCursorPtr ScanAllByLabel::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::ScanAllByLabelOperator);

  auto vertices = [this](Frames &, ExecutionContext &context) {
    auto *db = context.db_accessor;
    return std::make_optional(db->Vertices(view_, label_));
  };
  return MakeUniqueCursorPtr<ScanAllCursor<decltype(vertices)>>(mem, output_symbol_, input_->MakeCursor(mem),
                                                                std::move(vertices), "ScanAllByLabel");
}

ScanAllByLabelPropertyValue::ScanAllByLabelPropertyValue(const std::shared_ptr<LogicalOperator> &input,
                                                         Symbol output_symbol, storage::v3::LabelId label,
                                                         storage::v3::PropertyId property,
                                                         const std::string &property_name,
                                                         Expression *expression_property_value, storage::v3::View view)
    : ScanAll(input, output_symbol, view),
      label_(label),
      property_(property),
      property_name_(property_name),
      expression_property_value_(expression_property_value) {
  DMG_ASSERT(expression_property_value, "Expression is not optional.");
}

ACCEPT_WITH_INPUT(ScanAllByLabelPropertyValue)

UniqueCursorPtr ScanAllByLabelPropertyValue::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::ScanAllByLabelPropertyValueOperator);

  auto vertices =
      [this](Frames &frames, ExecutionContext &context) -> std::optional<decltype(context.db_accessor->Vertices(
                                                            view_, label_, property_, storage::v3::PropertyValue()))> {
    MG_ASSERT(!frames.empty());
    auto &frame = *frames[0];
    auto *db = context.db_accessor;
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor, view_);
    auto value = expression_property_value_->Accept(evaluator);

    MG_ASSERT(std::all_of(frames.begin(), frames.end(),
                          [&value, &view = view_, &expression_property_value = expression_property_value_,
                           &context](auto *frame) -> bool {
                            ExpressionEvaluator evaluator(frame, context.symbol_table, context.evaluation_context,
                                                          context.db_accessor, view);
                            auto other_value = expression_property_value->Accept(evaluator);
                            auto result = value == other_value;
                            if (!result.IsBool()) {
                              return false;
                            }
                            return result.ValueBool();
                          }));

    if (value.IsNull()) {
      return std::nullopt;
    }
    if (!value.IsPropertyValue()) {
      throw QueryRuntimeException("'{}' cannot be used as a property value.", value.type());
    }
    return std::make_optional(db->Vertices(view_, label_, property_, storage::v3::PropertyValue(value)));
  };
  return MakeUniqueCursorPtr<ScanAllCursor<decltype(vertices)>>(mem, output_symbol_, input_->MakeCursor(mem),
                                                                std::move(vertices), "ScanAllByLabelPropertyValue");
}

ScanAllById::ScanAllById(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol, Expression *expression_id,
                         storage::v3::View view)
    : ScanAll(input, output_symbol, view), expression_id_(expression_id) {
  MG_ASSERT(expression_id);
}

ACCEPT_WITH_INPUT(ScanAllById)

UniqueCursorPtr ScanAllById::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::ScanAllByIdOperator);

  auto vertices = [this](Frames &frames, ExecutionContext &context) -> std::optional<std::vector<VertexAccessor>> {
    MG_ASSERT(!frames.empty());
    auto &frame = *frames[0];
    auto *db = context.db_accessor;
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor, view_);
    auto value = expression_id_->Accept(evaluator);

    MG_ASSERT(std::all_of(frames.begin(), frames.end(),
                          [&value, &view = view_, &expression_id = expression_id_, &context](auto *frame) -> bool {
                            ExpressionEvaluator evaluator(frame, context.symbol_table, context.evaluation_context,
                                                          context.db_accessor, view);
                            auto other_value = expression_id->Accept(evaluator);
                            auto result = value == other_value;
                            if (!result.IsBool()) {
                              return false;
                            }
                            return result.ValueBool();
                          }));

    if (!value.IsNumeric()) {
      return std::nullopt;
    }
    int64_t id = value.IsInt() ? value.ValueInt() : value.ValueDouble();
    if (value.IsDouble() && id != value.ValueDouble()) {
      return std::nullopt;
    }
    auto maybe_vertex = db->FindVertex(storage::v3::Gid::FromInt(id), view_);
    if (!maybe_vertex) {
      return std::nullopt;
    }
    return std::vector<VertexAccessor>{*maybe_vertex};
  };
  return MakeUniqueCursorPtr<ScanAllCursor<decltype(vertices)>>(mem, output_symbol_, input_->MakeCursor(mem),
                                                                std::move(vertices), "ScanAllById");
}

namespace {

template <class TEdges>
auto UnwrapEdgesResult(storage::v3::Result<TEdges> &&result) {
  if (result.HasError()) {
    switch (result.GetError()) {
      case storage::v3::Error::DELETED_OBJECT:
        throw QueryRuntimeException("Trying to get relationships of a deleted node.");
      case storage::v3::Error::NONEXISTENT_OBJECT:
        throw query::v2::QueryRuntimeException("Trying to get relationships from a node that doesn't exist.");
      case storage::v3::Error::VERTEX_HAS_EDGES:
      case storage::v3::Error::SERIALIZATION_ERROR:
      case storage::v3::Error::PROPERTIES_DISABLED:
        throw QueryRuntimeException("Unexpected error when accessing relationships.");
    }
  }
  return std::move(*result);
}

}  // namespace

Expand::Expand(const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol, Symbol node_symbol,
               Symbol edge_symbol, EdgeAtom::Direction direction,
               const std::vector<storage::v3::EdgeTypeId> &edge_types, bool existing_node, storage::v3::View view)
    : input_(input ? input : std::make_shared<Once>()),
      input_symbol_(input_symbol),
      common_{node_symbol, edge_symbol, direction, edge_types, existing_node},
      view_(view) {}

ACCEPT_WITH_INPUT(Expand)

UniqueCursorPtr Expand::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::ExpandOperator);

  return MakeUniqueCursorPtr<ExpandCursor>(mem, *this, mem);
}

std::vector<Symbol> Expand::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(common_.node_symbol);
  symbols.emplace_back(common_.edge_symbol);
  return symbols;
}

Expand::ExpandCursor::ExpandCursor(const Expand &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self.input_->MakeCursor(mem)) {}

bool Expand::ExpandCursor::Pull(Frames &frames, ExecutionContext &context) {
  SCOPED_PROFILE_OP("Expand");
  MG_ASSERT(frames.size() == 1);
  MG_ASSERT(in_out_edges_and_iterators_.size() == 1 || in_out_edges_and_iterators_.empty());  // #NoCommit
  auto &frame = *frames[0];                                                                   // #NoCommit impl
  // A helper function for expanding a node from an edge.
  auto pull_node_from_edge = [this](const EdgeAccessor &new_edge, EdgeAtom::Direction direction, Frame &frame) {
    if (self_.common_.existing_node) {
      return;
    }

    switch (direction) {
      case EdgeAtom::Direction::IN:
        frame[self_.common_.node_symbol] = new_edge.From();
        break;
      case EdgeAtom::Direction::OUT:
        frame[self_.common_.node_symbol] = new_edge.To();
        break;
      case EdgeAtom::Direction::BOTH:
        LOG_FATAL("Must indicate exact expansion direction here");
    }
  };

  while (true) {
    if (MustAbort(context)) {
      throw HintedAbortError();
    }

    auto &first =
        in_out_edges_and_iterators_[0];  // #NoCommit of course changte this; this will only work for single frame

    // attempt to get a value from the incoming edges
    if (!in_out_edges_and_iterators_.empty() && first->in_edges_ && *first->in_edges_it_ != first->in_edges_->end()) {
      auto edge = *(*first->in_edges_it_)++;
      frame[self_.common_.edge_symbol] = edge;
      pull_node_from_edge(edge, EdgeAtom::Direction::IN, frame);
      return true;
    }

    // attempt to get a value from the outgoing edges
    if (!in_out_edges_and_iterators_.empty() && first->out_edges_ &&
        *first->out_edges_it_ != first->out_edges_->end()) {
      auto edge = *(*first->out_edges_it_)++;
      // when expanding in EdgeAtom::Direction::BOTH directions
      // we should do only one expansion for cycles, and it was
      // already done in the block above
      if (self_.common_.direction == EdgeAtom::Direction::BOTH && edge.IsCycle()) {
        continue;
      }
      frame[self_.common_.edge_symbol] = edge;
      pull_node_from_edge(edge, EdgeAtom::Direction::OUT, frame);
      return true;
    }

    // If we are here, either the edges have not been initialized,
    // or they have been exhausted. Attempt to initialize the edges.
    if (!InitEdges(frames, context)) {
      return false;
    }

    // we have re-initialized the edges, continue with the loop
  }
}

void Expand::ExpandCursor::Shutdown() { input_cursor_->Shutdown(); }

void Expand::ExpandCursor::Reset() {
  input_cursor_->Reset();
  in_out_edges_and_iterators_.clear();
}

bool Expand::ExpandCursor::InitEdges(Frames &frames, ExecutionContext &context) {
  // Input Vertex could be null if it is created by a failed optional match. In
  // those cases we skip that input pull and continue with the next.
  in_out_edges_and_iterators_.clear();
  while (true) {
    MG_ASSERT(frames.size() == 1);
    auto &frame = *frames[0];  // #NoCommit to impl
    if (!input_cursor_->Pull(frames, context)) {
      return false;
    }
    TypedValue &vertex_value = frame[self_.input_symbol_];

    // Null check due to possible failed optional match.
    if (vertex_value.IsNull()) {
      continue;
      //#NoCommit add nullopt?
    }
    in_out_edges_and_iterators_.emplace_back(InOutEdgesAndIterators{});
    auto &in_out_edges_and_iterators = *in_out_edges_and_iterators_.back();

    ExpectType(self_.input_symbol_, vertex_value, TypedValue::Type::Vertex);
    auto &vertex = vertex_value.ValueVertex();

    auto direction = self_.common_.direction;
    if (direction == EdgeAtom::Direction::IN || direction == EdgeAtom::Direction::BOTH) {
      if (self_.common_.existing_node) {
        TypedValue &existing_node = frame[self_.common_.node_symbol];
        // old_node_value may be Null when using optional matching
        if (!existing_node.IsNull()) {
          ExpectType(self_.common_.node_symbol, existing_node, TypedValue::Type::Vertex);
          in_out_edges_and_iterators.in_edges_.emplace(
              UnwrapEdgesResult(vertex.InEdges(self_.view_, self_.common_.edge_types, existing_node.ValueVertex())));
        }
      } else {
        in_out_edges_and_iterators.in_edges_.emplace(
            UnwrapEdgesResult(vertex.InEdges(self_.view_, self_.common_.edge_types)));
      }
      if (in_out_edges_and_iterators.in_edges_) {
        in_out_edges_and_iterators.in_edges_it_.emplace(in_out_edges_and_iterators.in_edges_->begin());
      }
    }

    if (direction == EdgeAtom::Direction::OUT || direction == EdgeAtom::Direction::BOTH) {
      if (self_.common_.existing_node) {
        TypedValue &existing_node = frame[self_.common_.node_symbol];
        // old_node_value may be Null when using optional matching
        if (!existing_node.IsNull()) {
          ExpectType(self_.common_.node_symbol, existing_node, TypedValue::Type::Vertex);
          in_out_edges_and_iterators.out_edges_.emplace(
              UnwrapEdgesResult(vertex.OutEdges(self_.view_, self_.common_.edge_types, existing_node.ValueVertex())));
        }
      } else {
        in_out_edges_and_iterators.out_edges_.emplace(
            UnwrapEdgesResult(vertex.OutEdges(self_.view_, self_.common_.edge_types)));
      }
      if (in_out_edges_and_iterators.out_edges_) {
        in_out_edges_and_iterators.out_edges_it_.emplace(in_out_edges_and_iterators.out_edges_->begin());
      }
    }

    return true;
  }
}

namespace {

/**
 * Helper function that returns an iterable over
 * <EdgeAtom::Direction, EdgeAccessor> pairs
 * for the given params.
 *
 * @param vertex - The vertex to expand from.
 * @param direction - Expansion direction. All directions (IN, OUT, BOTH)
 *    are supported.
 * @param memory - Used to allocate the result.
 * @return See above.
 */
auto ExpandFromVertex(const VertexAccessor &vertex, EdgeAtom::Direction direction,
                      const std::vector<storage::v3::EdgeTypeId> &edge_types, utils::MemoryResource *memory) {
  // wraps an EdgeAccessor into a pair <accessor, direction>
  auto wrapper = [](EdgeAtom::Direction direction, auto &&edges) {
    return iter::imap([direction](const auto &edge) { return std::make_pair(edge, direction); },
                      std::forward<decltype(edges)>(edges));
  };

  storage::v3::View view = storage::v3::View::OLD;
  utils::pmr::vector<decltype(wrapper(direction, *vertex.InEdges(view, edge_types)))> chain_elements(memory);

  if (direction != EdgeAtom::Direction::OUT) {
    auto edges = UnwrapEdgesResult(vertex.InEdges(view, edge_types));
    if (edges.begin() != edges.end()) {
      chain_elements.emplace_back(wrapper(EdgeAtom::Direction::IN, std::move(edges)));
    }
  }
  if (direction != EdgeAtom::Direction::IN) {
    auto edges = UnwrapEdgesResult(vertex.OutEdges(view, edge_types));
    if (edges.begin() != edges.end()) {
      chain_elements.emplace_back(wrapper(EdgeAtom::Direction::OUT, std::move(edges)));
    }
  }

  // TODO: Investigate whether itertools perform heap allocation?
  return iter::chain.from_iterable(std::move(chain_elements));
}

}  // namespace

Produce::Produce(const std::shared_ptr<LogicalOperator> &input, const std::vector<NamedExpression *> &named_expressions)
    : input_(input ? input : std::make_shared<Once>()), named_expressions_(named_expressions) {}

ACCEPT_WITH_INPUT(Produce)

UniqueCursorPtr Produce::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::ProduceOperator);

  return MakeUniqueCursorPtr<ProduceCursor>(mem, *this, mem);
}

std::vector<Symbol> Produce::OutputSymbols(const SymbolTable &symbol_table) const {
  std::vector<Symbol> symbols;
  for (const auto &named_expr : named_expressions_) {
    symbols.emplace_back(symbol_table.at(*named_expr));
  }
  return symbols;
}

std::vector<Symbol> Produce::ModifiedSymbols(const SymbolTable &table) const { return OutputSymbols(table); }

Produce::ProduceCursor::ProduceCursor(const Produce &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self_.input_->MakeCursor(mem)) {}

bool Produce::ProduceCursor::Pull(Frames &frames, ExecutionContext &context) {
  SCOPED_PROFILE_OP("Produce");

  if (input_cursor_->Pull(frames, context)) {
    // Produce should always yield the latest results.
    for (auto *frame : frames) {
      ExpressionEvaluator evaluator(frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                    storage::v3::View::NEW);
      for (auto named_expr : self_.named_expressions_) {
        named_expr->Accept(evaluator);
      }
    }

    return true;
  }
  return false;
}

void Produce::ProduceCursor::Shutdown() { input_cursor_->Shutdown(); }

void Produce::ProduceCursor::Reset() { input_cursor_->Reset(); }

}  // namespace memgraph::query::v2::plan::distributed
