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

    for (auto idx = 0; idx < frames.size(); ++idx) {
      auto &frame = *frames[idx];
      frame[output_symbol_] = *vertices_it_.value();
      ++vertices_it_.value();

      if (vertices_it_.value() == vertices_.value().end() && idx < frames.size() - 1) {
        /*
        'vertices_it_.value() == vertices_.value().end()' means we have exhausted all vertices
        If 'idx < frames.size() - 1' means we do not have enough vertices to fill all frames and that we are at the last
        batch. In that case, we can simply reduce the number of frames.
        */
        frames.resize(++idx);
        break;
      }
    }

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

// TODO(buda): Implement ScanAllByLabelProperty operator to iterate over
// vertices that have the label and some value for the given property.

ScanAllByLabelPropertyValue::ScanAllByLabelPropertyValue(const std::shared_ptr<LogicalOperator> &input,
                                                         Symbol output_symbol, storage::v3::LabelId label,
                                                         storage::v3::PropertyId property,
                                                         const std::string &property_name, Expression *expression,
                                                         storage::v3::View view)
    : ScanAll(input, output_symbol, view),
      label_(label),
      property_(property),
      property_name_(property_name),
      expression_(expression) {
  DMG_ASSERT(expression, "Expression is not optional.");
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
    auto value = expression_->Accept(evaluator);
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

ScanAllById::ScanAllById(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol, Expression *expression,
                         storage::v3::View view)
    : ScanAll(input, output_symbol, view), expression_(expression) {
  MG_ASSERT(expression);
}

ACCEPT_WITH_INPUT(ScanAllById)

UniqueCursorPtr ScanAllById::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::ScanAllByIdOperator);

  auto vertices = [this](Frames &frames, ExecutionContext &context) -> std::optional<std::vector<VertexAccessor>> {
    auto &frame = *frames[0];  // #NoCommit
    auto *db = context.db_accessor;
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor, view_);
    auto value = expression_->Accept(evaluator);
    if (!value.IsNumeric()) return std::nullopt;
    int64_t id = value.IsInt() ? value.ValueInt() : value.ValueDouble();
    if (value.IsDouble() && id != value.ValueDouble()) return std::nullopt;
    auto maybe_vertex = db->FindVertex(storage::v3::Gid::FromInt(id), view_);
    if (!maybe_vertex) return std::nullopt;
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

bool Filter::FilterCursor::Pull(Frames &frames, ExecutionContext &context) {
  SCOPED_PROFILE_OP("Filter");
  auto &frame = *frames[0];  // #NoCommit
  // Like all filters, newly set values should not affect filtering of old
  // nodes and edges.
  ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                storage::v3::View::OLD);
  while (input_cursor_->Pull(frames, context)) {
    if (EvaluateFilter(evaluator, self_.expression_)) return true;
  }
  return false;
}

void Filter::FilterCursor::Shutdown() { input_cursor_->Shutdown(); }

void Filter::FilterCursor::Reset() { input_cursor_->Reset(); }

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
  auto &frame = *frames[0];  // #NoCommit JBA
  if (input_cursor_->Pull(frames, context)) {
    // Produce should always yield the latest results.
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                  storage::v3::View::NEW);
    for (auto named_expr : self_.named_expressions_) named_expr->Accept(evaluator);

    return true;
  }
  return false;
}

void Produce::ProduceCursor::Shutdown() { input_cursor_->Shutdown(); }

void Produce::ProduceCursor::Reset() { input_cursor_->Reset(); }

Union::Union(const std::shared_ptr<LogicalOperator> &left_op, const std::shared_ptr<LogicalOperator> &right_op,
             const std::vector<Symbol> &union_symbols, const std::vector<Symbol> &left_symbols,
             const std::vector<Symbol> &right_symbols)
    : left_op_(left_op),
      right_op_(right_op),
      union_symbols_(union_symbols),
      left_symbols_(left_symbols),
      right_symbols_(right_symbols) {}

UniqueCursorPtr Union::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::UnionOperator);

  return MakeUniqueCursorPtr<Union::UnionCursor>(mem, *this, mem);
}

bool Union::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    if (left_op_->Accept(visitor)) {
      right_op_->Accept(visitor);
    }
  }
  return visitor.PostVisit(*this);
}

std::vector<Symbol> Union::OutputSymbols(const SymbolTable &) const { return union_symbols_; }

std::vector<Symbol> Union::ModifiedSymbols(const SymbolTable &) const { return union_symbols_; }

WITHOUT_SINGLE_INPUT(Union);

Union::UnionCursor::UnionCursor(const Union &self, utils::MemoryResource *mem)
    : self_(self), left_cursor_(self.left_op_->MakeCursor(mem)), right_cursor_(self.right_op_->MakeCursor(mem)) {}

bool Union::UnionCursor::Pull(Frames &frames, ExecutionContext &context) {
  SCOPED_PROFILE_OP("Union");
  auto &frame = *frames[0];  // #NoCommit
  utils::pmr::unordered_map<std::string, TypedValue> results(context.evaluation_context.memory);
  if (left_cursor_->Pull(frames, context)) {
    // collect values from the left child
    for (const auto &output_symbol : self_.left_symbols_) {
      results[output_symbol.name()] = frame[output_symbol];
    }
  } else if (right_cursor_->Pull(frames, context)) {
    // collect values from the right child
    for (const auto &output_symbol : self_.right_symbols_) {
      results[output_symbol.name()] = frame[output_symbol];
    }
  } else {
    return false;
  }

  // put collected values on frame under union symbols
  for (const auto &symbol : self_.union_symbols_) {
    frame[symbol] = results[symbol.name()];
  }
  return true;
}

void Union::UnionCursor::Shutdown() {
  left_cursor_->Shutdown();
  right_cursor_->Shutdown();
}

void Union::UnionCursor::Reset() {
  left_cursor_->Reset();
  right_cursor_->Reset();
}

OutputTable::OutputTable(std::vector<Symbol> output_symbols, std::vector<std::vector<TypedValue>> rows)
    : output_symbols_(std::move(output_symbols)), callback_([rows](Frames &, ExecutionContext *) { return rows; }) {}

OutputTable::OutputTable(std::vector<Symbol> output_symbols,
                         std::function<std::vector<std::vector<TypedValue>>(Frames &, ExecutionContext *)> callback)
    : output_symbols_(std::move(output_symbols)), callback_(std::move(callback)) {}

WITHOUT_SINGLE_INPUT(OutputTable);

class OutputTableCursor : public Cursor {
 public:
  OutputTableCursor(const OutputTable &self) : self_(self) {}

  bool Pull(Frames &frames, ExecutionContext &context) override {
    auto &frame = *frames[0];  // #NoCommit
    if (!pulled_) {
      rows_ = self_.callback_(frames, &context);
      for (const auto &row : rows_) {
        MG_ASSERT(row.size() == self_.output_symbols_.size(), "Wrong number of columns in row!");
      }
      pulled_ = true;
    }
    if (current_row_ < rows_.size()) {
      for (size_t i = 0; i < self_.output_symbols_.size(); ++i) {
        frame[self_.output_symbols_[i]] = rows_[current_row_][i];
      }
      current_row_++;
      return true;
    }
    return false;
  }

  void Reset() override {
    pulled_ = false;
    current_row_ = 0;
    rows_.clear();
  }

  void Shutdown() override {}

 private:
  const OutputTable &self_;
  size_t current_row_{0};
  std::vector<std::vector<TypedValue>> rows_;
  bool pulled_{false};
};

UniqueCursorPtr OutputTable::MakeCursor(utils::MemoryResource *mem) const {
  return MakeUniqueCursorPtr<OutputTableCursor>(mem, *this);
}

OutputTableStream::OutputTableStream(
    std::vector<Symbol> output_symbols,
    std::function<std::optional<std::vector<TypedValue>>(Frames &, ExecutionContext *)> callback)
    : output_symbols_(std::move(output_symbols)), callback_(std::move(callback)) {}

WITHOUT_SINGLE_INPUT(OutputTableStream);

class OutputTableStreamCursor : public Cursor {
 public:
  explicit OutputTableStreamCursor(const OutputTableStream *self) : self_(self) {}

  bool Pull(Frames &frames, ExecutionContext &context) override {
    const auto row = self_->callback_(frames, &context);
    auto &frame = *frames[0];  // #NoCommit
    if (row) {
      MG_ASSERT(row->size() == self_->output_symbols_.size(), "Wrong number of columns in row!");
      for (size_t i = 0; i < self_->output_symbols_.size(); ++i) {
        frame[self_->output_symbols_[i]] = row->at(i);
      }
      return true;
    }
    return false;
  }

  // TODO(tsabolcec): Come up with better approach for handling `Reset()`.
  // One possibility is to implement a custom closure utility class with
  // `Reset()` method.
  void Reset() override { throw utils::NotYetImplemented("OutputTableStreamCursor::Reset"); }

  void Shutdown() override {}

 private:
  const OutputTableStream *self_;
};

UniqueCursorPtr OutputTableStream::MakeCursor(utils::MemoryResource *mem) const {
  return MakeUniqueCursorPtr<OutputTableStreamCursor>(mem, this);
}

LoadCsv::LoadCsv(std::shared_ptr<LogicalOperator> input, Expression *file, bool with_header, bool ignore_bad,
                 Expression *delimiter, Expression *quote, Symbol row_var)
    : input_(input ? input : (std::make_shared<Once>())),
      file_(file),
      with_header_(with_header),
      ignore_bad_(ignore_bad),
      delimiter_(delimiter),
      quote_(quote),
      row_var_(row_var) {
  MG_ASSERT(file_, "Something went wrong - '{}' member file_ shouldn't be a nullptr", __func__);
}

bool LoadCsv::Accept(HierarchicalLogicalOperatorVisitor &visitor) { return false; };

class LoadCsvCursor;

std::vector<Symbol> LoadCsv::OutputSymbols(const SymbolTable &sym_table) const { return {row_var_}; };

std::vector<Symbol> LoadCsv::ModifiedSymbols(const SymbolTable &sym_table) const {
  auto symbols = input_->ModifiedSymbols(sym_table);
  symbols.push_back(row_var_);
  return symbols;
};

namespace {
// copy-pasted from interpreter.cpp
TypedValue EvaluateOptionalExpression(Expression *expression, ExpressionEvaluator *eval) {
  return expression ? expression->Accept(*eval) : TypedValue();
}

auto ToOptionalString(ExpressionEvaluator *evaluator, Expression *expression) -> std::optional<utils::pmr::string> {
  const auto evaluated_expr = EvaluateOptionalExpression(expression, evaluator);
  if (evaluated_expr.IsString()) {
    return utils::pmr::string(evaluated_expr.ValueString(), utils::NewDeleteResource());
  }
  return std::nullopt;
};

TypedValue CsvRowToTypedList(csv::Reader::Row row) {
  auto *mem = row.get_allocator().GetMemoryResource();
  auto typed_columns = utils::pmr::vector<TypedValue>(mem);
  typed_columns.reserve(row.size());
  for (auto &column : row) {
    typed_columns.emplace_back(std::move(column));
  }
  return TypedValue(typed_columns, mem);
}

TypedValue CsvRowToTypedMap(csv::Reader::Row row, csv::Reader::Header header) {
  // a valid row has the same number of elements as the header
  auto *mem = row.get_allocator().GetMemoryResource();
  utils::pmr::map<utils::pmr::string, TypedValue> m(mem);
  for (auto i = 0; i < row.size(); ++i) {
    m.emplace(std::move(header[i]), std::move(row[i]));
  }
  return TypedValue(m, mem);
}

}  // namespace

class LoadCsvCursor : public Cursor {
  const LoadCsv *self_;
  const UniqueCursorPtr input_cursor_;
  bool input_is_once_;
  std::optional<csv::Reader> reader_{};

 public:
  LoadCsvCursor(const LoadCsv *self, utils::MemoryResource *mem)
      : self_(self), input_cursor_(self_->input_->MakeCursor(mem)) {
    input_is_once_ = dynamic_cast<Once *>(self_->input_.get());
  }

  bool Pull(Frames &frames, ExecutionContext &context) override {
    SCOPED_PROFILE_OP("LoadCsv");
    auto &frame = *frames[0];  // #NoCommit
    if (MustAbort(context)) throw HintedAbortError();

    // ToDo(the-joksim):
    //  - this is an ungodly hack because the pipeline of creating a plan
    //  doesn't allow evaluating the expressions contained in self_->file_,
    //  self_->delimiter_, and self_->quote_ earlier (say, in the interpreter.cpp)
    //  without massacring the code even worse than I did here
    if (UNLIKELY(!reader_)) {
      reader_ = MakeReader(&context.evaluation_context);
    }

    bool input_pulled = input_cursor_->Pull(frames, context);

    // If the input is Once, we have to keep going until we read all the rows,
    // regardless of whether the pull on Once returned false.
    // If we have e.g. MATCH(n) LOAD CSV ... AS x SET n.name = x.name, then we
    // have to read at most cardinality(n) rows (but we can read less and stop
    // pulling MATCH).
    if (!input_is_once_ && !input_pulled) return false;

    if (auto row = reader_->GetNextRow(context.evaluation_context.memory)) {
      if (!reader_->HasHeader()) {
        frame[self_->row_var_] = CsvRowToTypedList(std::move(*row));
      } else {
        frame[self_->row_var_] = CsvRowToTypedMap(
            std::move(*row), csv::Reader::Header(reader_->GetHeader(), context.evaluation_context.memory));
      }
      return true;
    }

    return false;
  }

  void Reset() override { input_cursor_->Reset(); }
  void Shutdown() override { input_cursor_->Shutdown(); }

 private:
  csv::Reader MakeReader(EvaluationContext *eval_context) {
    Frame frame(0);
    SymbolTable symbol_table;
    DbAccessor *dba = nullptr;
    auto evaluator = ExpressionEvaluator(&frame, symbol_table, *eval_context, dba, storage::v3::View::OLD);

    auto maybe_file = ToOptionalString(&evaluator, self_->file_);
    auto maybe_delim = ToOptionalString(&evaluator, self_->delimiter_);
    auto maybe_quote = ToOptionalString(&evaluator, self_->quote_);

    // No need to check if maybe_file is std::nullopt, as the parser makes sure
    // we can't get a nullptr for the 'file_' member in the LoadCsv clause.
    // Note that the reader has to be given its own memory resource, as it
    // persists between pulls, so it can't use the evalutation context memory
    // resource.
    return csv::Reader(
        *maybe_file,
        csv::Reader::Config(self_->with_header_, self_->ignore_bad_, std::move(maybe_delim), std::move(maybe_quote)),
        utils::NewDeleteResource());
  }
};

UniqueCursorPtr LoadCsv::MakeCursor(utils::MemoryResource *mem) const {
  return MakeUniqueCursorPtr<LoadCsvCursor>(mem, this, mem);
};

}  // namespace memgraph::query::v2::plan::distributed
