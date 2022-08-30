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

#include "query/v2/interpret/eval.hpp"
#include "query/v2/interpret/multiframe.hpp"
#include "query/v2/plan/scoped_profile.hpp"
#include "utils/event_counter.hpp"

// macro for the default implementation of LogicalOperator::Accept
// that accepts the visitor and visits it's input_ operator
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ACCEPT_WITH_INPUT(class_name)                                    \
  bool class_name::Accept(HierarchicalLogicalOperatorVisitor &visitor) { \
    if (visitor.PreVisit(*this)) {                                       \
      input_->Accept(visitor);                                           \
    }                                                                    \
    return visitor.PostVisit(*this);                                     \
  }

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
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
extern const Event ScanAllOperator;
extern const Event ScanAllByLabelOperator;
extern const Event ScanAllByLabelPropertyValueOperator;
extern const Event ScanAllByIdOperator;
extern const Event ExpandOperator;
extern const Event ProduceOperator;
extern const Event UnwindOperator;
extern const Event FilterOperator;
}  // namespace EventCounter

namespace memgraph::query::v2::plan::distributed {

template <class TCursor, class... TArgs>
std::unique_ptr<Cursor, std::function<void(Cursor *)>> MakeUniqueCursorPtr(utils::Allocator<TCursor> allocator,
                                                                           TArgs &&...args) {
  auto *ptr = allocator.allocate(1);
  try {
    auto *cursor = new (ptr) TCursor(std::forward<TArgs>(args)...);
    return std::unique_ptr<Cursor, std::function<void(Cursor *)>>(cursor, [allocator](Cursor *base_ptr) mutable {
      auto *p = static_cast<TCursor *>(base_ptr);
      p->~TCursor();
      allocator.deallocate(p, 1);
    });
  } catch (...) {
    allocator.deallocate(ptr, 1);
    throw;
  }
}

namespace {

template <typename T>
uint64_t ComputeProfilingKey(const T *obj) {
  static_assert(sizeof(T *) == sizeof(uint64_t));
  return reinterpret_cast<uint64_t>(obj);
}

// void ResizeFrames(MultiFrame &multiframe, int last_filled_frame) {
//   // #NoCommit maybe we don't need any more
//   MG_ASSERT(last_filled_frame >= 0);
//   MG_ASSERT(frames.Size() > last_filled_frame);

//   frames.resize(last_filled_frame + 1);
// }
}  // namespace

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define SCOPED_PROFILE_OP(name) ScopedProfile profile{ComputeProfilingKey(this), name, &context};

bool Once::OnceCursor::Pull(MultiFrame & /*frames*/, ExecutionContext &context) {
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
                         const char *op_name, bool perform_full_enumeration)
      : output_symbol_(output_symbol),
        input_cursor_(std::move(input_cursor)),
        get_vertices_(std::move(get_vertices)),
        op_name_(op_name),
        perform_full_enumeration_(perform_full_enumeration) {}

  bool Pull(MultiFrame &multiframe, ExecutionContext &context) override {
    SCOPED_PROFILE_OP(op_name_);
    if (MustAbort(context)) {
      throw HintedAbortError();
    }

    while (!vertices_ || vertices_it_.value() == vertices_.value().end()) {
      if (!input_cursor_->Pull(multiframe, context)) {
        return false;
      }
      // We need a getter function, because in case of exhausting a lazy
      // iterable, we cannot simply reset it by calling begin().
      auto next_vertices = get_vertices_(multiframe, context);
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
    for (auto idx = 0; idx < multiframe.Size(); ++idx) {
      auto &frame = multiframe.GetFrame(idx);
      if (!frame.IsValid()) {
        continue;
      }

      frame[output_symbol_] = *vertices_it_.value();
      if (!perform_full_enumeration_) {
        ++vertices_it_.value();
      }
      last_filled_frame = idx;

      if (vertices_it_.value() == vertices_.value().end() && idx < multiframe.Size() - 1) {
        /*
        'vertices_it_.value() == vertices_.value().end()' means we have exhausted all vertices
        If 'idx < multiframe.size() - 1' means we do not have enough vertices to fill all frames and that we are at the
        last batch. In that case, we can simply reduce the number of frames.
        */
        break;
      }
    }
    if (perform_full_enumeration_) {
      ++vertices_it_.value();
    }
    const auto number_of_frames_to_keep = last_filled_frame + 1;
    multiframe.Resize(number_of_frames_to_keep);

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
  std::optional<typename std::result_of<TVerticesFun(MultiFrame &, ExecutionContext &)>::type::value_type> vertices_;
  std::optional<decltype(vertices_.value().begin())> vertices_it_;
  const char *op_name_;
  bool perform_full_enumeration_ = false;
};

ScanAll::ScanAll(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol, storage::v3::View view)
    : input_(input ? input : std::make_shared<Once>()),
      output_symbol_(output_symbol),
      view_(view),
      perform_full_enumeration_(false) {
  input_->PerformFullEnumeration();
}

ACCEPT_WITH_INPUT(ScanAll)

UniqueCursorPtr ScanAll::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::ScanAllOperator);

  auto vertices = [this](MultiFrame &, ExecutionContext &context) {
    auto *db = context.db_accessor;
    return std::make_optional(db->Vertices(view_));
  };
  return MakeUniqueCursorPtr<ScanAllCursor<decltype(vertices)>>(
      mem, output_symbol_, input_->MakeCursor(mem), std::move(vertices), "ScanAll", perform_full_enumeration_);
}

std::vector<Symbol> ScanAll::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(output_symbol_);
  return symbols;
}

void ScanAll::PerformFullEnumeration() { perform_full_enumeration_ = true; }

ScanAllByLabel::ScanAllByLabel(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol,
                               storage::v3::LabelId label, storage::v3::View view)
    : ScanAll(input, output_symbol, view), label_(label) {}

ACCEPT_WITH_INPUT(ScanAllByLabel)

UniqueCursorPtr ScanAllByLabel::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::ScanAllByLabelOperator);

  auto vertices = [this](MultiFrame &, ExecutionContext &context) {
    auto *db = context.db_accessor;
    return std::make_optional(db->Vertices(view_, label_));
  };
  return MakeUniqueCursorPtr<ScanAllCursor<decltype(vertices)>>(
      mem, output_symbol_, input_->MakeCursor(mem), std::move(vertices), "ScanAllByLabel", perform_full_enumeration_);
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

  auto vertices = [this](MultiFrame &multiframe, ExecutionContext &context)
      -> std::optional<decltype(context.db_accessor->Vertices(view_, label_, property_,
                                                              storage::v3::PropertyValue()))> {
    MG_ASSERT(!multiframe.Empty());
    auto &frame = multiframe.GetFrame(0);
    auto *db = context.db_accessor;
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor, view_);
    auto value = expression_property_value_->Accept(evaluator);

    const auto &frame_vec = multiframe.GetFrames();

    MG_ASSERT(std::all_of(frame_vec.begin(), frame_vec.end(),
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
                                                                std::move(vertices), "ScanAllByLabelPropertyValue",
                                                                perform_full_enumeration_);
}

ScanAllById::ScanAllById(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol, Expression *expression_id,
                         storage::v3::View view)
    : ScanAll(input, output_symbol, view), expression_id_(expression_id) {
  MG_ASSERT(expression_id);
}

ACCEPT_WITH_INPUT(ScanAllById)

UniqueCursorPtr ScanAllById::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::ScanAllByIdOperator);

  auto vertices = [this](MultiFrame &multiframe,
                         ExecutionContext &context) -> std::optional<std::vector<VertexAccessor>> {
    MG_ASSERT(!multiframe.Empty());
    auto &frame = multiframe.GetFrame(0);

    auto *db = context.db_accessor;
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor, view_);
    auto value = expression_id_->Accept(evaluator);

    const auto &frame_vec = multiframe.GetFrames();

    MG_ASSERT(std::all_of(frame_vec.begin(), frame_vec.end(),
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
    auto id = (int64_t)(value.IsInt() ? value.ValueInt() : value.ValueDouble());
    if (value.IsDouble() && id != (int64_t)value.ValueDouble()) {
      return std::nullopt;
    }
    auto maybe_vertex = db->FindVertex(storage::v3::Gid::FromInt(id), view_);
    if (!maybe_vertex) {
      return std::nullopt;
    }
    return std::vector<VertexAccessor>{*maybe_vertex};
  };
  return MakeUniqueCursorPtr<ScanAllCursor<decltype(vertices)>>(
      mem, output_symbol_, input_->MakeCursor(mem), std::move(vertices), "ScanAllById", perform_full_enumeration_);
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

// #NoCommit Use pmr thing as well here!!!

bool Expand::ExpandCursor::Pull(MultiFrame &multiframe, ExecutionContext &context) {
  SCOPED_PROFILE_OP("Expand");
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

    auto at_least_one_result = false;
    auto invalid_frames = std::vector<bool>(multiframe.Size(), false);

    for (auto idx = 0; idx < std::min(multiframe.Size(), in_out_edges.size()); ++idx) {
      // #NoCommit in std::min correct here?
      MG_ASSERT(idx < in_out_edges.size());

      auto &in_out_edge = in_out_edges[idx];
      auto &frame = multiframe.GetFrame(idx);
      if (!frame.IsValid()) {
        continue;
      }

      // Do we have in-going edges for this frame?
      auto has_put_any_value_on_frame = false;
      if (in_out_edge.in_.has_value()) {
        auto &in_edges = in_out_edge.in_.value();
        // Do we still have some in-going edges to look at for this frame? i.e.: is the iterator not yet at the end of
        // known edges
        if (in_edges.in_edges_it_ != in_edges.in_edges_.end()) {
          auto edge = *in_edges.in_edges_it_;  // We get the edge corresponding to iterator
          ++in_edges.in_edges_it_;             // We increment the iterator for next round

          frame[self_.common_.edge_symbol] = edge;                    // We put the edge in the frame
          pull_node_from_edge(edge, EdgeAtom::Direction::IN, frame);  // We put the node, if needed, in the frame

          at_least_one_result = true;
          has_put_any_value_on_frame = true;
        }
      }

      // Do we have out-going edges for this frame?
      if (in_out_edge.out_.has_value()) {
        auto &out_edges = in_out_edge.out_.value();
        // Do we still have some out-going edges to look at for this frame? i.e.: is the iterator not yet at the end of
        // known edges
        if (out_edges.out_edges_it_ != out_edges.out_edges_.end()) {
          auto edge = *out_edges.out_edges_it_;  // We get the edge corresponding to iterator;
          ++out_edges.out_edges_it_;             // We increment the iterator for next round

          if (EdgeAtom::Direction::BOTH == self_.common_.direction && edge.IsCycle()) {
            // When expanding in EdgeAtom::Direction::BOTH directions, we should do only one expansion for cycles, and
            // it was already done in the block above
            continue;
          }

          frame[self_.common_.edge_symbol] = edge;                     // We put the edge in the frame
          pull_node_from_edge(edge, EdgeAtom::Direction::OUT, frame);  // We put the node, if needed, in the frame

          at_least_one_result = true;
          has_put_any_value_on_frame = true;
        }
      }

      if (has_put_any_value_on_frame) {
        invalid_frames[idx] = true;
      }
    }

    if (at_least_one_result) {
      for (auto idx = 0; idx < invalid_frames.size(); ++idx) {
        auto isValid = invalid_frames[idx];
        if (!isValid) {
          auto &frame = multiframe.GetFrame(idx);
          frame.MakeInvalid();
        }
      }
      return true;
    }

    // If we are here, either the edges have not been initialized, or they have all been exhausted. Attempt to
    // initialize the edges (i.e. the structure in_out_edges).
    in_out_edges.clear();
    if (!InitEdges(multiframe, context)) {
      return false;
    }

    // we have re-initialized the edges, continue with the loop
  }
}

void Expand::ExpandCursor::Shutdown() { input_cursor_->Shutdown(); }

void Expand::ExpandCursor::Reset() {
  input_cursor_->Reset();
  in_out_edges.clear();
}

bool Expand::ExpandCursor::InitEdges(MultiFrame &multiframe, ExecutionContext &context) {
  // Input Vertex could be null if it is created by a failed optional match. In
  // those cases we skip that input pull and continue with the next.
  while (multiframe.HasValidFrames()) {
    if (!input_cursor_->Pull(multiframe, context)) {
      return false;
    }
    auto value_for_at_least_one_frame = false;

    for (auto idx = 0; idx < multiframe.Size(); ++idx) {
      auto &frame = multiframe.GetFrame(idx);
      if (!frame.IsValid()) {
        continue;
      }
      TypedValue &vertex_value = frame[self_.input_symbol_];

      if (vertex_value.IsNull()) {                  // Null check due to possible failed optional match.
        LOG_FATAL("Not supported at the moment!");  // #NoCommit do we want to implement it now?
        frame.MakeInvalid();                        // #NoCommit double check
        continue;
      }
      in_out_edges.emplace_back(InOutEdges{});
      auto &in_out_edge = in_out_edges.back();

      ExpectType(self_.input_symbol_, vertex_value, TypedValue::Type::Vertex);
      auto &vertex = vertex_value.ValueVertex();

      auto direction = self_.common_.direction;
      if (direction == EdgeAtom::Direction::IN || direction == EdgeAtom::Direction::BOTH) {
        if (self_.common_.existing_node) {
          TypedValue &existing_node = frame[self_.common_.node_symbol];
          // old_node_value may be Null when using optional matching
          if (!existing_node.IsNull()) {
            ExpectType(self_.common_.node_symbol, existing_node, TypedValue::Type::Vertex);
            auto edges =
                UnwrapEdgesResult(vertex.InEdges(self_.view_, self_.common_.edge_types, existing_node.ValueVertex()));
            auto it = edges.begin();
            in_out_edge.in_.emplace(InEdge{.in_edges_ = std::move(edges), .in_edges_it_ = std::move(it)});
            in_out_edge.in_.value().in_edges_it_ = in_out_edge.in_.value().in_edges_.begin();  // #NoCommit
          }
        } else {
          auto edges = UnwrapEdgesResult(vertex.InEdges(self_.view_, self_.common_.edge_types));
          auto it = edges.begin();
          in_out_edge.in_.emplace(InEdge{.in_edges_ = std::move(edges), .in_edges_it_ = std::move(it)});
          in_out_edge.in_.value().in_edges_it_ = in_out_edge.in_.value().in_edges_.begin();  // #NoCommit
        }
      }

      if (direction == EdgeAtom::Direction::OUT || direction == EdgeAtom::Direction::BOTH) {
        if (self_.common_.existing_node) {
          TypedValue &existing_node = frame[self_.common_.node_symbol];
          // old_node_value may be Null when using optional matching
          if (!existing_node.IsNull()) {
            ExpectType(self_.common_.node_symbol, existing_node, TypedValue::Type::Vertex);
            auto edges =
                UnwrapEdgesResult(vertex.OutEdges(self_.view_, self_.common_.edge_types, existing_node.ValueVertex()));
            auto it = edges.begin();
            in_out_edge.out_.emplace(OutEdge{.out_edges_ = std::move(edges), .out_edges_it_ = std::move(it)});
            in_out_edge.out_.value().out_edges_it_ = in_out_edge.out_.value().out_edges_.begin();  // #NoCommit
          }
        } else {
          auto edges = UnwrapEdgesResult(vertex.OutEdges(self_.view_, self_.common_.edge_types));
          auto it = edges.begin();
          in_out_edge.out_.emplace(OutEdge{.out_edges_ = std::move(edges), .out_edges_it_ = std::move(it)});
          in_out_edge.out_.value().out_edges_it_ = in_out_edge.out_.value().out_edges_.begin();  // #NoCommit
        }
      }

      value_for_at_least_one_frame = true;
      MG_ASSERT(in_out_edge.in_.has_value() || in_out_edge.out_.has_value());
    }

    if (value_for_at_least_one_frame) {
      return true;
    }
    // else we want to continue and do an extra pull, there is nothing to conclude from this round
  }

  return false;
}

Produce::Produce(const std::shared_ptr<LogicalOperator> &input, const std::vector<NamedExpression *> &named_expressions)
    : input_(input ? input : std::make_shared<Once>()), named_expressions_(named_expressions) {}

ACCEPT_WITH_INPUT(Produce)

UniqueCursorPtr Produce::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::ProduceOperator);

  return MakeUniqueCursorPtr<ProduceCursor>(mem, *this, mem);
}

std::vector<Symbol> Produce::OutputSymbols(const SymbolTable &symbol_table) const {
  std::vector<Symbol> symbols;
  for (const auto *named_expr : named_expressions_) {
    symbols.emplace_back(symbol_table.at(*named_expr));
  }
  return symbols;
}

std::vector<Symbol> Produce::ModifiedSymbols(const SymbolTable &table) const { return OutputSymbols(table); }

Produce::ProduceCursor::ProduceCursor(const Produce &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self_.input_->MakeCursor(mem)) {}

bool Produce::ProduceCursor::Pull(MultiFrame &multiframe, ExecutionContext &context) {
  SCOPED_PROFILE_OP("Produce");

  if (input_cursor_->Pull(multiframe, context)) {
    // Produce should always yield the latest results.
    for (auto *frame : multiframe.GetFrames()) {
      if (!frame->IsValid()) {
        continue;
      }
      ExpressionEvaluator evaluator(frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                    storage::v3::View::NEW);
      for (auto *named_expr : self_.named_expressions_) {
        named_expr->Accept(evaluator);
      }
    }

    return true;
  }
  return false;
}

void Produce::ProduceCursor::Shutdown() { input_cursor_->Shutdown(); }

void Produce::ProduceCursor::Reset() { input_cursor_->Reset(); }

Unwind::Unwind(const std::shared_ptr<LogicalOperator> &input, Expression *input_expression, Symbol output_symbol)
    : input_(input ? input : std::make_shared<Once>()),
      input_expression_(input_expression),
      output_symbol_(output_symbol) {}

ACCEPT_WITH_INPUT(Unwind)

std::vector<Symbol> Unwind::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(output_symbol_);
  return symbols;
}

class UnwindCursor : public Cursor {
 public:
  UnwindCursor(const Unwind &self, utils::MemoryResource *mem)
      : self_(self), input_cursor_(self.input_->MakeCursor(mem)), input_value_(mem) {}

  bool Pull(MultiFrame &multiframe, ExecutionContext &context) override {
    SCOPED_PROFILE_OP("Unwind");
    while (true) {
      if (MustAbort(context)) throw HintedAbortError();
      // There are two important dimensions: multiframe size, unwind list size.
      // "Cartesian product" between MultiFrame values and input values because for each Frame the whole input list has
      // to be evaluated. Evaluation result depends on the frame.
      if (!frame_idx_) {
        if (!input_cursor_->Pull(multiframe, context)) return false;
        frame_idx_ = 0;
      }

      for (; *frame_idx_ < multiframe.Size();) {
        auto &frame = multiframe.GetFrame(*frame_idx_);
        if (!frame.IsValid()) {
          frame_idx_ = *frame_idx_ + 1;
          continue;
        }
        if (input_value_it_ == input_value_.end()) {
          ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                        storage::v3::View::OLD);
          TypedValue input_value = self_.input_expression_->Accept(evaluator);
          if (input_value.type() != TypedValue::Type::List) {
            throw QueryRuntimeException("Argument of UNWIND must be a list, but '{}' was provided.",
                                        input_value.type());
          }
          input_value_ = input_value.ValueList();
          input_value_it_ = input_value_.begin();
        }
        // NOTE: Here we have to populate to as many frames as we have them, but not more.
        while (input_value_it_ != input_value_.end()) {
          // TODO(gitbuda): Take invalid frames into account (BUG HERE)!
          auto &frame = multiframe.GetFrame(populated_frames_);
          frame[self_.output_symbol_] = *input_value_it_++;
          populated_frames_++;
          // Taking care if multiframe size is smaller than unwind list size.
          if (populated_frames_ < multiframe.Size()) {
            continue;
          }
          // TODO(gitbuda): Here we have an issue if size is lower then the batch size.
          if (populated_frames_ == multiframe.Size()) {  // TODO(gitbuda): Assert no bigger.
            populated_frames_ = 0;
            if (input_value_it_ != input_value_.end()) {
              return true;
            }
            if (input_value_it_ == input_value_.end()) {
              frame_idx_ = *frame_idx_ + 1;
            }
            if (*frame_idx_ == multiframe.Size()) {  // TODO(gitbuda): Assert no bigger.
              frame_idx_ = std::nullopt;
            }
            return true;
          }
        }
        frame_idx_ = *frame_idx_ + 1;
      }
      frame_idx_ = std::nullopt;
    }
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    input_value_.clear();
    input_value_it_ = input_value_.end();
  }

 private:
  const Unwind &self_;
  const UniqueCursorPtr input_cursor_;
  // typed values we are unwinding and yielding
  utils::pmr::vector<TypedValue> input_value_;
  // current position in input_value_
  decltype(input_value_)::iterator input_value_it_ = input_value_.end();
  std::optional<size_t> frame_idx_{std::nullopt};
  size_t populated_frames_{0};
};

UniqueCursorPtr Unwind::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::UnwindOperator);

  return MakeUniqueCursorPtr<UnwindCursor>(mem, *this, mem);
}

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

Filter::Filter(const std::shared_ptr<LogicalOperator> &input, Expression *expression)
    : input_(input ? input : std::make_shared<Once>()), expression_(expression) {}

ACCEPT_WITH_INPUT(Filter)

UniqueCursorPtr Filter::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::FilterOperator);

  return MakeUniqueCursorPtr<FilterCursor>(mem, *this, mem);
}

std::vector<Symbol> Filter::ModifiedSymbols(const SymbolTable &table) const { return input_->ModifiedSymbols(table); }

Filter::FilterCursor::FilterCursor(const Filter &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self_.input_->MakeCursor(mem)) {}

bool Filter::FilterCursor::Pull(MultiFrame &multiframe, ExecutionContext &context) {
  SCOPED_PROFILE_OP("Filter");

  while (input_cursor_->Pull(multiframe, context)) {
    // TODO(gitbuda): Make frames invalid only there is something to return because in the case with many pull without
    // return multiframe will be in a "polluted" state.
    for (auto idx = 0; idx < multiframe.Size(); ++idx) {
      auto &frame = multiframe.GetFrame(idx);
      if (!frame.IsValid()) {
        continue;
      }
      ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                    storage::v3::View::OLD);
      if (!EvaluateFilter(evaluator, self_.expression_)) {
        frame.MakeInvalid();
      }
    }
    return true;
  }
  return false;
}

void Filter::FilterCursor::Shutdown() { input_cursor_->Shutdown(); }

void Filter::FilterCursor::Reset() { input_cursor_->Reset(); }

}  // namespace memgraph::query::v2::plan::distributed
