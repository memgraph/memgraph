#include "query/plan/operator.hpp"

#include <algorithm>
#include <limits>
#include <queue>
#include <random>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "glog/logging.h"

#include "auth/auth.hpp"
#include "communication/result_stream_faker.hpp"
#include "database/graph_db_accessor.hpp"
#include "glue/auth.hpp"
#include "glue/communication.hpp"
#include "integrations/kafka/exceptions.hpp"
#include "integrations/kafka/streams.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/eval.hpp"
#include "query/path.hpp"
#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"
#include "utils/hashing/fnv.hpp"
#include "utils/string.hpp"
#include "utils/thread/sync.hpp"

// macro for the default implementation of LogicalOperator::Accept
// that accepts the visitor and visits it's input_ operator
#define ACCEPT_WITH_INPUT(class_name)                                    \
  bool class_name::Accept(HierarchicalLogicalOperatorVisitor &visitor) { \
    if (visitor.PreVisit(*this)) {                                       \
      input_->Accept(visitor);                                           \
    }                                                                    \
    return visitor.PostVisit(*this);                                     \
  }

#define WITHOUT_SINGLE_INPUT(class_name)                                 \
  bool class_name::HasSingleInput() const { return false; }              \
  std::shared_ptr<LogicalOperator> class_name::input() const {           \
    LOG(FATAL) << "Operator " << #class_name << " has no single input!"; \
  }                                                                      \
  void class_name::set_input(std::shared_ptr<LogicalOperator>) {         \
    LOG(FATAL) << "Operator " << #class_name << " has no single input!"; \
  }

namespace query::plan {

namespace {

// Returns boolean result of evaluating filter expression. Null is treated as
// false. Other non boolean values raise a QueryRuntimeException.
bool EvaluateFilter(ExpressionEvaluator &evaluator, Expression *filter) {
  TypedValue result = filter->Accept(evaluator);
  // Null is treated like false.
  if (result.IsNull()) return false;
  if (result.type() != TypedValue::Type::Bool)
    throw QueryRuntimeException(
        "Filter expression must evaluate to bool or null, got {}.",
        result.type());
  return result.Value<bool>();
}

}  // namespace

bool Once::OnceCursor::Pull(Frame &, Context &) {
  if (!did_pull_) {
    did_pull_ = true;
    return true;
  }
  return false;
}

std::unique_ptr<Cursor> Once::MakeCursor(database::GraphDbAccessor &) const {
  return std::make_unique<OnceCursor>();
}

WITHOUT_SINGLE_INPUT(Once);

void Once::OnceCursor::Shutdown() {}

void Once::OnceCursor::Reset() { did_pull_ = false; }

CreateNode::CreateNode(const std::shared_ptr<LogicalOperator> &input,
                       NodeAtom *node_atom)
    : input_(input ? input : std::make_shared<Once>()), node_atom_(node_atom) {}

// Creates a vertex on this GraphDb. Returns a reference to vertex placed on the
// frame.
VertexAccessor &CreateLocalVertex(NodeAtom *node_atom, Frame &frame,
                                  Context &context) {
  auto &dba = context.db_accessor_;
  auto new_node = dba.InsertVertex();
  for (auto label : node_atom->labels_) new_node.add_label(label);

  // Evaluator should use the latest accessors, as modified in this query, when
  // setting properties on new nodes.
  ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                context.evaluation_context_,
                                &context.db_accessor_, GraphView::NEW);
  for (auto &kv : node_atom->properties_)
    PropsSetChecked(&new_node, kv.first.second, kv.second->Accept(evaluator));
  frame[context.symbol_table_.at(*node_atom->identifier_)] = new_node;
  return frame[context.symbol_table_.at(*node_atom->identifier_)].ValueVertex();
}

ACCEPT_WITH_INPUT(CreateNode)

std::unique_ptr<Cursor> CreateNode::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<CreateNodeCursor>(*this, db);
}

std::vector<Symbol> CreateNode::ModifiedSymbols(
    const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(table.at(*node_atom_->identifier_));
  return symbols;
}

CreateNode::CreateNodeCursor::CreateNodeCursor(const CreateNode &self,
                                               database::GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

bool CreateNode::CreateNodeCursor::Pull(Frame &frame, Context &context) {
  if (input_cursor_->Pull(frame, context)) {
    CreateLocalVertex(self_.node_atom_, frame, context);
    return true;
  }
  return false;
}

void CreateNode::CreateNodeCursor::Shutdown() { input_cursor_->Shutdown(); }

void CreateNode::CreateNodeCursor::Reset() { input_cursor_->Reset(); }

CreateExpand::CreateExpand(NodeAtom *node_atom, EdgeAtom *edge_atom,
                           const std::shared_ptr<LogicalOperator> &input,
                           Symbol input_symbol, bool existing_node)
    : node_atom_(node_atom),
      edge_atom_(edge_atom),
      input_(input ? input : std::make_shared<Once>()),
      input_symbol_(input_symbol),
      existing_node_(existing_node) {}

ACCEPT_WITH_INPUT(CreateExpand)

std::unique_ptr<Cursor> CreateExpand::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<CreateExpandCursor>(*this, db);
}

std::vector<Symbol> CreateExpand::ModifiedSymbols(
    const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(table.at(*node_atom_->identifier_));
  symbols.emplace_back(table.at(*edge_atom_->identifier_));
  return symbols;
}

CreateExpand::CreateExpandCursor::CreateExpandCursor(
    const CreateExpand &self, database::GraphDbAccessor &db)
    : self_(self), db_(db), input_cursor_(self.input_->MakeCursor(db)) {}

bool CreateExpand::CreateExpandCursor::Pull(Frame &frame, Context &context) {
  if (!input_cursor_->Pull(frame, context)) return false;

  // get the origin vertex
  TypedValue &vertex_value = frame[self_.input_symbol_];
  ExpectType(self_.input_symbol_, vertex_value, TypedValue::Type::Vertex);
  auto &v1 = vertex_value.Value<VertexAccessor>();

  // Similarly to CreateNode, newly created edges and nodes should use the
  // latest accesors.
  ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                context.evaluation_context_,
                                &context.db_accessor_, GraphView::NEW);
  // E.g. we pickup new properties: `CREATE (n {p: 42}) -[:r {ep: n.p}]-> ()`
  v1.SwitchNew();

  // get the destination vertex (possibly an existing node)
  auto &v2 = OtherVertex(frame, context);
  v2.SwitchNew();

  // create an edge between the two nodes
  switch (self_.edge_atom_->direction_) {
    case EdgeAtom::Direction::IN:
      CreateEdge(v2, v1, frame, context.symbol_table_, evaluator);
      break;
    case EdgeAtom::Direction::OUT:
      CreateEdge(v1, v2, frame, context.symbol_table_, evaluator);
      break;
    case EdgeAtom::Direction::BOTH:
      // in the case of an undirected CreateExpand we choose an arbitrary
      // direction. this is used in the MERGE clause
      // it is not allowed in the CREATE clause, and the semantic
      // checker needs to ensure it doesn't reach this point
      CreateEdge(v1, v2, frame, context.symbol_table_, evaluator);
  }

  return true;
}

void CreateExpand::CreateExpandCursor::Shutdown() { input_cursor_->Shutdown(); }

void CreateExpand::CreateExpandCursor::Reset() { input_cursor_->Reset(); }

VertexAccessor &CreateExpand::CreateExpandCursor::OtherVertex(
    Frame &frame, Context &context) {
  if (self_.existing_node_) {
    const auto &dest_node_symbol =
        context.symbol_table_.at(*self_.node_atom_->identifier_);
    TypedValue &dest_node_value = frame[dest_node_symbol];
    ExpectType(dest_node_symbol, dest_node_value, TypedValue::Type::Vertex);
    return dest_node_value.Value<VertexAccessor>();
  } else {
    return CreateLocalVertex(self_.node_atom_, frame, context);
  }
}

void CreateExpand::CreateExpandCursor::CreateEdge(
    VertexAccessor &from, VertexAccessor &to, Frame &frame,
    const SymbolTable &symbol_table, ExpressionEvaluator &evaluator) {
  EdgeAccessor edge =
      db_.InsertEdge(from, to, self_.edge_atom_->edge_types_[0]);
  for (auto kv : self_.edge_atom_->properties_)
    PropsSetChecked(&edge, kv.first.second, kv.second->Accept(evaluator));
  frame[symbol_table.at(*self_.edge_atom_->identifier_)] = edge;
}

template <class TVerticesFun>
class ScanAllCursor : public Cursor {
 public:
  explicit ScanAllCursor(Symbol output_symbol,
                         std::unique_ptr<Cursor> &&input_cursor,
                         TVerticesFun &&get_vertices,
                         database::GraphDbAccessor &db)
      : output_symbol_(output_symbol),
        input_cursor_(std::move(input_cursor)),
        get_vertices_(std::move(get_vertices)),
        db_(db) {}

  bool Pull(Frame &frame, Context &context) override {
    if (db_.should_abort()) throw HintedAbortError();

    while (!vertices_ || vertices_it_.value() == vertices_.value().end()) {
      if (!input_cursor_->Pull(frame, context)) return false;
      // We need a getter function, because in case of exhausting a lazy
      // iterable, we cannot simply reset it by calling begin().
      auto next_vertices = get_vertices_(frame, context);
      if (!next_vertices) continue;
      // Since vertices iterator isn't nothrow_move_assignable, we have to use
      // the roundabout assignment + emplace, instead of simple:
      // vertices _ = get_vertices_(frame, context);
      vertices_.emplace(std::move(next_vertices.value()));
      vertices_it_.emplace(vertices_.value().begin());
    }

    frame[output_symbol_] = *vertices_it_.value()++;
    return true;
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    vertices_ = std::experimental::nullopt;
    vertices_it_ = std::experimental::nullopt;
  }

 private:
  const Symbol output_symbol_;
  const std::unique_ptr<Cursor> input_cursor_;
  TVerticesFun get_vertices_;
  std::experimental::optional<typename std::result_of<TVerticesFun(
      Frame &, Context &)>::type::value_type>
      vertices_;
  std::experimental::optional<decltype(vertices_.value().begin())> vertices_it_;
  database::GraphDbAccessor &db_;
};

ScanAll::ScanAll(const std::shared_ptr<LogicalOperator> &input,
                 Symbol output_symbol, GraphView graph_view)
    : input_(input ? input : std::make_shared<Once>()),
      output_symbol_(output_symbol),
      graph_view_(graph_view) {}

ACCEPT_WITH_INPUT(ScanAll)

std::unique_ptr<Cursor> ScanAll::MakeCursor(
    database::GraphDbAccessor &db) const {
  auto vertices = [this, &db](Frame &, Context &) {
    return std::experimental::make_optional(
        db.Vertices(graph_view_ == GraphView::NEW));
  };
  return std::make_unique<ScanAllCursor<decltype(vertices)>>(
      output_symbol_, input_->MakeCursor(db), std::move(vertices), db);
}

std::vector<Symbol> ScanAll::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(output_symbol_);
  return symbols;
}

ScanAllByLabel::ScanAllByLabel(const std::shared_ptr<LogicalOperator> &input,
                               Symbol output_symbol, storage::Label label,
                               GraphView graph_view)
    : ScanAll(input, output_symbol, graph_view), label_(label) {}

ACCEPT_WITH_INPUT(ScanAllByLabel)

std::unique_ptr<Cursor> ScanAllByLabel::MakeCursor(
    database::GraphDbAccessor &db) const {
  auto vertices = [this, &db](Frame &, Context &) {
    return std::experimental::make_optional(
        db.Vertices(label_, graph_view_ == GraphView::NEW));
  };
  return std::make_unique<ScanAllCursor<decltype(vertices)>>(
      output_symbol_, input_->MakeCursor(db), std::move(vertices), db);
}

ScanAllByLabelPropertyRange::ScanAllByLabelPropertyRange(
    const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol,
    storage::Label label, storage::Property property,
    std::experimental::optional<Bound> lower_bound,
    std::experimental::optional<Bound> upper_bound, GraphView graph_view)
    : ScanAll(input, output_symbol, graph_view),
      label_(label),
      property_(property),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound) {
  DCHECK(lower_bound_ || upper_bound_) << "Only one bound can be left out";
}

ACCEPT_WITH_INPUT(ScanAllByLabelPropertyRange)

std::unique_ptr<Cursor> ScanAllByLabelPropertyRange::MakeCursor(
    database::GraphDbAccessor &db) const {
  auto vertices = [this, &db](Frame &frame, Context &context)
      -> std::experimental::optional<decltype(
          db.Vertices(label_, property_, std::experimental::nullopt,
                      std::experimental::nullopt, false))> {
    ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                  context.evaluation_context_,
                                  &context.db_accessor_, graph_view_);
    auto convert = [&evaluator](const auto &bound)
        -> std::experimental::optional<utils::Bound<PropertyValue>> {
      if (!bound) return std::experimental::nullopt;
      auto value = bound->value()->Accept(evaluator);
      try {
        return std::experimental::make_optional(
            utils::Bound<PropertyValue>(PropertyValue(value), bound->type()));
      } catch (const TypedValueException &) {
        throw QueryRuntimeException("'{}' cannot be used as a property value.",
                                    value.type());
      }
    };
    auto maybe_lower = convert(lower_bound_);
    auto maybe_upper = convert(upper_bound_);
    // If any bound is null, then the comparison would result in nulls. This
    // is treated as not satisfying the filter, so return no vertices.
    if (maybe_lower && maybe_lower->value().IsNull())
      return std::experimental::nullopt;
    if (maybe_upper && maybe_upper->value().IsNull())
      return std::experimental::nullopt;
    return std::experimental::make_optional(
        db.Vertices(label_, property_, maybe_lower, maybe_upper,
                    graph_view_ == GraphView::NEW));
  };
  return std::make_unique<ScanAllCursor<decltype(vertices)>>(
      output_symbol_, input_->MakeCursor(db), std::move(vertices), db);
}

ScanAllByLabelPropertyValue::ScanAllByLabelPropertyValue(
    const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol,
    storage::Label label, storage::Property property, Expression *expression,
    GraphView graph_view)
    : ScanAll(input, output_symbol, graph_view),
      label_(label),
      property_(property),
      expression_(expression) {
  DCHECK(expression) << "Expression is not optional.";
}

ACCEPT_WITH_INPUT(ScanAllByLabelPropertyValue)

std::unique_ptr<Cursor> ScanAllByLabelPropertyValue::MakeCursor(
    database::GraphDbAccessor &db) const {
  auto vertices = [this, &db](Frame &frame, Context &context)
      -> std::experimental::optional<decltype(
          db.Vertices(label_, property_, PropertyValue::Null, false))> {
    ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                  context.evaluation_context_,
                                  &context.db_accessor_, graph_view_);
    auto value = expression_->Accept(evaluator);
    if (value.IsNull()) return std::experimental::nullopt;
    if (!value.IsPropertyValue()) {
      throw QueryRuntimeException("'{}' cannot be used as a property value.",
                                  value.type());
    }
    return std::experimental::make_optional(
        db.Vertices(label_, property_, PropertyValue(value),
                    graph_view_ == GraphView::NEW));
  };
  return std::make_unique<ScanAllCursor<decltype(vertices)>>(
      output_symbol_, input_->MakeCursor(db), std::move(vertices), db);
}

namespace {
bool CheckExistingNode(const VertexAccessor &new_node,
                       const Symbol &existing_node_sym, Frame &frame) {
  const TypedValue &existing_node = frame[existing_node_sym];
  if (existing_node.IsNull()) return false;
  ExpectType(existing_node_sym, existing_node, TypedValue::Type::Vertex);
  return existing_node.ValueVertex() != new_node;
}
}  // namespace

Expand::Expand(const std::shared_ptr<LogicalOperator> &input,
               Symbol input_symbol, Symbol node_symbol, Symbol edge_symbol,
               EdgeAtom::Direction direction,
               const std::vector<storage::EdgeType> &edge_types,
               bool existing_node, GraphView graph_view)
    : input_(input ? input : std::make_shared<Once>()),
      input_symbol_(input_symbol),
      common_{node_symbol, edge_symbol,   direction,
              edge_types,  existing_node, graph_view} {}

ACCEPT_WITH_INPUT(Expand)

std::unique_ptr<Cursor> Expand::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<ExpandCursor>(*this, db);
}

std::vector<Symbol> Expand::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(common_.node_symbol);
  symbols.emplace_back(common_.edge_symbol);
  return symbols;
}

Expand::ExpandCursor::ExpandCursor(const Expand &self,
                                   database::GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)), db_(db) {}

bool Expand::ExpandCursor::Pull(Frame &frame, Context &context) {
  // A helper function for expanding a node from an edge.
  auto pull_node = [this, &frame](const EdgeAccessor &new_edge,
                                  EdgeAtom::Direction direction) {
    if (self_.common_.existing_node) return;
    switch (direction) {
      case EdgeAtom::Direction::IN:
        frame[self_.common_.node_symbol] = new_edge.from();
        break;
      case EdgeAtom::Direction::OUT:
        frame[self_.common_.node_symbol] = new_edge.to();
        break;
      case EdgeAtom::Direction::BOTH:
        LOG(FATAL) << "Must indicate exact expansion direction here";
    }
  };

  while (true) {
    if (db_.should_abort()) throw HintedAbortError();
    // attempt to get a value from the incoming edges
    if (in_edges_ && *in_edges_it_ != in_edges_->end()) {
      auto edge = *(*in_edges_it_)++;
      frame[self_.common_.edge_symbol] = edge;
      pull_node(edge, EdgeAtom::Direction::IN);
      return true;
    }

    // attempt to get a value from the outgoing edges
    if (out_edges_ && *out_edges_it_ != out_edges_->end()) {
      auto edge = *(*out_edges_it_)++;
      // when expanding in EdgeAtom::Direction::BOTH directions
      // we should do only one expansion for cycles, and it was
      // already done in the block above
      if (self_.common_.direction == EdgeAtom::Direction::BOTH &&
          edge.is_cycle())
        continue;
      frame[self_.common_.edge_symbol] = edge;
      pull_node(edge, EdgeAtom::Direction::OUT);
      return true;
    }

    // If we are here, either the edges have not been initialized,
    // or they have been exhausted. Attempt to initialize the edges.
    if (!InitEdges(frame, context)) return false;

    // we have re-initialized the edges, continue with the loop
  }
}

void Expand::ExpandCursor::Shutdown() { input_cursor_->Shutdown(); }

void Expand::ExpandCursor::Reset() {
  input_cursor_->Reset();
  in_edges_ = std::experimental::nullopt;
  in_edges_it_ = std::experimental::nullopt;
  out_edges_ = std::experimental::nullopt;
  out_edges_it_ = std::experimental::nullopt;
}

bool Expand::ExpandCursor::InitEdges(Frame &frame, Context &context) {
  // Input Vertex could be null if it is created by a failed optional match. In
  // those cases we skip that input pull and continue with the next.
  while (true) {
    if (!input_cursor_->Pull(frame, context)) return false;
    TypedValue &vertex_value = frame[self_.input_symbol_];

    // Null check due to possible failed optional match.
    if (vertex_value.IsNull()) continue;

    ExpectType(self_.input_symbol_, vertex_value, TypedValue::Type::Vertex);
    auto &vertex = vertex_value.Value<VertexAccessor>();
    SwitchAccessor(vertex, self_.common_.graph_view);

    auto direction = self_.common_.direction;
    if (direction == EdgeAtom::Direction::IN ||
        direction == EdgeAtom::Direction::BOTH) {
      if (self_.common_.existing_node) {
        TypedValue &existing_node = frame[self_.common_.node_symbol];
        // old_node_value may be Null when using optional matching
        if (!existing_node.IsNull()) {
          ExpectType(self_.common_.node_symbol, existing_node,
                     TypedValue::Type::Vertex);
          in_edges_.emplace(vertex.in(existing_node.ValueVertex(),
                                      &self_.common_.edge_types));
        }
      } else {
        in_edges_.emplace(vertex.in(&self_.common_.edge_types));
      }
      if (in_edges_) {
        in_edges_it_.emplace(in_edges_->begin());
      }
    }

    if (direction == EdgeAtom::Direction::OUT ||
        direction == EdgeAtom::Direction::BOTH) {
      if (self_.common_.existing_node) {
        TypedValue &existing_node = frame[self_.common_.node_symbol];
        // old_node_value may be Null when using optional matching
        if (!existing_node.IsNull()) {
          ExpectType(self_.common_.node_symbol, existing_node,
                     TypedValue::Type::Vertex);
          out_edges_.emplace(vertex.out(existing_node.ValueVertex(),
                                        &self_.common_.edge_types));
        }
      } else {
        out_edges_.emplace(vertex.out(&self_.common_.edge_types));
      }
      if (out_edges_) {
        out_edges_it_.emplace(out_edges_->begin());
      }
    }

    return true;
  }
}

ExpandVariable::ExpandVariable(
    const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol,
    Symbol node_symbol, Symbol edge_symbol, EdgeAtom::Type type,
    EdgeAtom::Direction direction,
    const std::vector<storage::EdgeType> &edge_types, bool is_reverse,
    Expression *lower_bound, Expression *upper_bound, bool existing_node,
    ExpansionLambda filter_lambda,
    std::experimental::optional<ExpansionLambda> weight_lambda,
    std::experimental::optional<Symbol> total_weight, GraphView graph_view)
    : input_(input ? input : std::make_shared<Once>()),
      input_symbol_(input_symbol),
      common_{node_symbol, edge_symbol,   direction,
              edge_types,  existing_node, graph_view},
      type_(type),
      is_reverse_(is_reverse),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      filter_lambda_(filter_lambda),
      weight_lambda_(weight_lambda),
      total_weight_(total_weight) {
  DCHECK(type_ == EdgeAtom::Type::DEPTH_FIRST ||
         type_ == EdgeAtom::Type::BREADTH_FIRST ||
         type_ == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH)
      << "ExpandVariable can only be used with breadth first, depth first or "
         "weighted shortest path type";
  DCHECK(!(type_ == EdgeAtom::Type::BREADTH_FIRST && is_reverse))
      << "Breadth first expansion can't be reversed";
}

ACCEPT_WITH_INPUT(ExpandVariable)

std::vector<Symbol> ExpandVariable::ModifiedSymbols(
    const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(common_.node_symbol);
  symbols.emplace_back(common_.edge_symbol);
  return symbols;
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
 * @return See above.
 */
auto ExpandFromVertex(const VertexAccessor &vertex,
                      EdgeAtom::Direction direction,
                      const std::vector<storage::EdgeType> &edge_types) {
  // wraps an EdgeAccessor into a pair <accessor, direction>
  auto wrapper = [](EdgeAtom::Direction direction, auto &&vertices) {
    return iter::imap(
        [direction](const EdgeAccessor &edge) {
          return std::make_pair(edge, direction);
        },
        std::forward<decltype(vertices)>(vertices));
  };

  // prepare a vector of elements we'll pass to the itertools
  std::vector<decltype(wrapper(direction, vertex.in()))> chain_elements;

  if (direction != EdgeAtom::Direction::OUT && vertex.in_degree() > 0) {
    auto edges = vertex.in(&edge_types);
    if (edges.begin() != edges.end()) {
      chain_elements.emplace_back(
          wrapper(EdgeAtom::Direction::IN, std::move(edges)));
    }
  }
  if (direction != EdgeAtom::Direction::IN && vertex.out_degree() > 0) {
    auto edges = vertex.out(&edge_types);
    if (edges.begin() != edges.end()) {
      chain_elements.emplace_back(
          wrapper(EdgeAtom::Direction::OUT, std::move(edges)));
    }
  }

  return iter::chain.from_iterable(std::move(chain_elements));
}

}  // namespace

class ExpandVariableCursor : public Cursor {
 public:
  ExpandVariableCursor(const ExpandVariable &self,
                       database::GraphDbAccessor &db)
      : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

  bool Pull(Frame &frame, Context &context) override {
    ExpressionEvaluator evaluator(
        &frame, context.symbol_table_, context.evaluation_context_,
        &context.db_accessor_, self_.common_.graph_view);
    while (true) {
      if (Expand(frame, context)) return true;

      if (PullInput(frame, context)) {
        // if lower bound is zero we also yield empty paths
        if (lower_bound_ == 0) {
          auto &start_vertex =
              frame[self_.input_symbol_].Value<VertexAccessor>();
          if (!self_.common_.existing_node ||
              CheckExistingNode(start_vertex, self_.common_.node_symbol,
                                frame)) {
            frame[self_.common_.node_symbol] = start_vertex;
            return true;
          }
        }
        // if lower bound is not zero, we just continue, the next
        // loop iteration will attempt to expand and we're good
      } else
        return false;
      // else continue with the loop, try to expand again
      // because we succesfully pulled from the input
    }
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    edges_.clear();
    edges_it_.clear();
  }

 private:
  const ExpandVariable &self_;
  const std::unique_ptr<Cursor> input_cursor_;
  // bounds. in the cursor they are not optional but set to
  // default values if missing in the ExpandVariable operator
  // initialize to arbitrary values, they should only be used
  // after a successful pull from the input
  int64_t upper_bound_{-1};
  int64_t lower_bound_{-1};

  // a stack of edge iterables corresponding to the level/depth of
  // the expansion currently being Pulled
  std::vector<decltype(ExpandFromVertex(std::declval<VertexAccessor>(),
                                        EdgeAtom::Direction::IN,
                                        self_.common_.edge_types))>
      edges_;

  // an iterator indicating the possition in the corresponding edges_
  // element
  std::vector<decltype(edges_.begin()->begin())> edges_it_;

  /**
   * Helper function that Pulls from the input vertex and
   * makes iteration over it's edges possible.
   *
   * @return If the Pull succeeded. If not, this VariableExpandCursor
   * is exhausted.
   */
  bool PullInput(Frame &frame, Context &context) {
    // Input Vertex could be null if it is created by a failed optional
    // match.
    // In those cases we skip that input pull and continue with the next.
    while (true) {
      if (context.db_accessor_.should_abort()) throw HintedAbortError();
      if (!input_cursor_->Pull(frame, context)) return false;
      TypedValue &vertex_value = frame[self_.input_symbol_];

      // Null check due to possible failed optional match.
      if (vertex_value.IsNull()) continue;

      ExpectType(self_.input_symbol_, vertex_value, TypedValue::Type::Vertex);
      auto &vertex = vertex_value.Value<VertexAccessor>();
      SwitchAccessor(vertex, self_.common_.graph_view);

      // Evaluate the upper and lower bounds.
      ExpressionEvaluator evaluator(&frame, context.symbol_table_,

                                    context.evaluation_context_,
                                    &context.db_accessor_,
                                    self_.common_.graph_view);
      auto calc_bound = [&evaluator](auto &bound) {
        auto value = EvaluateInt(&evaluator, bound, "Variable expansion bound");
        if (value < 0)
          throw QueryRuntimeException(
              "Variable expansion bound must be a non-negative integer.");
        return value;
      };

      lower_bound_ = self_.lower_bound_ ? calc_bound(self_.lower_bound_) : 1;
      upper_bound_ = self_.upper_bound_ ? calc_bound(self_.upper_bound_)
                                        : std::numeric_limits<int64_t>::max();

      if (upper_bound_ > 0) {
        SwitchAccessor(vertex, self_.common_.graph_view);
        edges_.emplace_back(ExpandFromVertex(vertex, self_.common_.direction,
                                             self_.common_.edge_types));
        edges_it_.emplace_back(edges_.back().begin());
      }

      // reset the frame value to an empty edge list
      frame[self_.common_.edge_symbol] = std::vector<TypedValue>();

      return true;
    }
  }

  // Helper function for appending an edge to the list on the frame.
  void AppendEdge(const EdgeAccessor &new_edge,
                  std::vector<TypedValue> &edges_on_frame) {
    // We are placing an edge on the frame. It is possible that there already
    // exists an edge on the frame for this level. If so first remove it.
    DCHECK(edges_.size() > 0) << "Edges are empty";
    if (self_.is_reverse_) {
      // TODO: This is innefficient, we should look into replacing
      // vector with something else for TypedValue::List.
      size_t diff = edges_on_frame.size() -
                    std::min(edges_on_frame.size(), edges_.size() - 1U);
      if (diff > 0U)
        edges_on_frame.erase(edges_on_frame.begin(),
                             edges_on_frame.begin() + diff);
      edges_on_frame.insert(edges_on_frame.begin(), new_edge);
    } else {
      edges_on_frame.resize(
          std::min(edges_on_frame.size(), edges_.size() - 1U));
      edges_on_frame.emplace_back(new_edge);
    }
  }

  /**
   * Performs a single expansion for the current state of this
   * VariableExpansionCursor.
   *
   * @return True if the expansion was a success and this Cursor's
   * consumer can consume it. False if the expansion failed. In that
   * case no more expansions are available from the current input
   * vertex and another Pull from the input cursor should be performed.
   */
  bool Expand(Frame &frame, Context &context) {
    ExpressionEvaluator evaluator(
        &frame, context.symbol_table_, context.evaluation_context_,
        &context.db_accessor_, self_.common_.graph_view);
    // Some expansions might not be valid due to edge uniqueness and
    // existing_node criterions, so expand in a loop until either the input
    // vertex is exhausted or a valid variable-length expansion is available.
    while (true) {
      if (context.db_accessor_.should_abort()) throw HintedAbortError();
      // pop from the stack while there is stuff to pop and the current
      // level is exhausted
      while (!edges_.empty() && edges_it_.back() == edges_.back().end()) {
        edges_.pop_back();
        edges_it_.pop_back();
      }

      // check if we exhausted everything, if so return false
      if (edges_.empty()) return false;

      // we use this a lot
      std::vector<TypedValue> &edges_on_frame =
          frame[self_.common_.edge_symbol].Value<std::vector<TypedValue>>();

      // it is possible that edges_on_frame does not contain as many
      // elements as edges_ due to edge-uniqueness (when a whole layer
      // gets exhausted but no edges are valid). for that reason only
      // pop from edges_on_frame if they contain enough elements
      if (self_.is_reverse_) {
        auto diff = edges_on_frame.size() -
                    std::min(edges_on_frame.size(), edges_.size());
        if (diff > 0) {
          edges_on_frame.erase(edges_on_frame.begin(),
                               edges_on_frame.begin() + diff);
        }
      } else {
        edges_on_frame.resize(std::min(edges_on_frame.size(), edges_.size()));
      }

      // if we are here, we have a valid stack,
      // get the edge, increase the relevant iterator
      std::pair<EdgeAccessor, EdgeAtom::Direction> current_edge =
          *edges_it_.back()++;

      // Check edge-uniqueness.
      bool found_existing =
          std::any_of(edges_on_frame.begin(), edges_on_frame.end(),
                      [&current_edge](const TypedValue &edge) {
                        return current_edge.first == edge.Value<EdgeAccessor>();
                      });
      if (found_existing) continue;

      AppendEdge(current_edge.first, edges_on_frame);
      VertexAccessor current_vertex =
          current_edge.second == EdgeAtom::Direction::IN
              ? current_edge.first.from()
              : current_edge.first.to();

      if (self_.common_.existing_node &&
          !CheckExistingNode(current_vertex, self_.common_.node_symbol, frame))
        continue;

      frame[self_.common_.node_symbol] = current_vertex;

      // Skip expanding out of filtered expansion.
      frame[self_.filter_lambda_.inner_edge_symbol] = current_edge.first;
      frame[self_.filter_lambda_.inner_node_symbol] = current_vertex;
      if (self_.filter_lambda_.expression &&
          !EvaluateFilter(evaluator, self_.filter_lambda_.expression))
        continue;

      // we are doing depth-first search, so place the current
      // edge's expansions onto the stack, if we should continue to expand
      if (upper_bound_ > static_cast<int64_t>(edges_.size())) {
        SwitchAccessor(current_vertex, self_.common_.graph_view);
        edges_.emplace_back(ExpandFromVertex(
            current_vertex, self_.common_.direction, self_.common_.edge_types));
        edges_it_.emplace_back(edges_.back().begin());
      }

      // We only yield true if we satisfy the lower bound.
      if (static_cast<int64_t>(edges_on_frame.size()) >= lower_bound_)
        return true;
      else
        continue;
    }
  }
};

class STShortestPathCursor : public query::plan::Cursor {
 public:
  STShortestPathCursor(const ExpandVariable &self,
                       database::GraphDbAccessor &dba)
      : self_(self), input_cursor_(self_.input()->MakeCursor(dba)) {
    CHECK(self_.common_.graph_view == GraphView::OLD)
        << "ExpandVariable should only be planned with GraphView::OLD";
    CHECK(self_.common_.existing_node)
        << "s-t shortest path algorithm should only "
           "be used when `existing_node` flag is "
           "set!";
  }

  bool Pull(Frame &frame, Context &context) override {
    ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                  context.evaluation_context_,
                                  &context.db_accessor_, GraphView::OLD);
    while (input_cursor_->Pull(frame, context)) {
      auto source_tv = frame[self_.input_symbol_];
      auto sink_tv = frame[self_.common_.node_symbol];

      // It is possible that source or sink vertex is Null due to optional
      // matching.
      if (source_tv.IsNull() || sink_tv.IsNull()) continue;

      auto source = source_tv.ValueVertex();
      auto sink = sink_tv.ValueVertex();

      int64_t lower_bound =
          self_.lower_bound_
              ? EvaluateInt(&evaluator, self_.lower_bound_,
                            "Min depth in breadth-first expansion")
              : 1;
      int64_t upper_bound =
          self_.upper_bound_
              ? EvaluateInt(&evaluator, self_.upper_bound_,
                            "Max depth in breadth-first expansion")
              : std::numeric_limits<int64_t>::max();

      if (upper_bound < 1 || lower_bound > upper_bound) continue;

      if (FindPath(context.db_accessor_, source, sink, lower_bound, upper_bound,
                   &frame, &evaluator)) {
        return true;
      }
    }
    return false;
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override { input_cursor_->Reset(); }

 private:
  const ExpandVariable &self_;
  std::unique_ptr<query::plan::Cursor> input_cursor_;

  using VertexEdgeMapT =
      std::unordered_map<VertexAccessor,
                         std::experimental::optional<EdgeAccessor>>;

  void ReconstructPath(const VertexAccessor &midpoint,
                       const VertexEdgeMapT &in_edge,
                       const VertexEdgeMapT &out_edge, Frame *frame) {
    std::vector<TypedValue> result;
    auto last_vertex = midpoint;
    while (true) {
      const auto &last_edge = in_edge.at(last_vertex);
      if (!last_edge) break;
      last_vertex =
          last_edge->from_is(last_vertex) ? last_edge->to() : last_edge->from();
      result.emplace_back(*last_edge);
    }
    std::reverse(result.begin(), result.end());
    last_vertex = midpoint;
    while (true) {
      const auto &last_edge = out_edge.at(last_vertex);
      if (!last_edge) break;
      last_vertex =
          last_edge->from_is(last_vertex) ? last_edge->to() : last_edge->from();
      result.emplace_back(*last_edge);
    }
    frame->at(self_.common_.edge_symbol) = std::move(result);
  }

  bool ShouldExpand(const VertexAccessor &vertex, const EdgeAccessor &edge,
                    Frame *frame, ExpressionEvaluator *evaluator) {
    if (!self_.filter_lambda_.expression) return true;

    frame->at(self_.filter_lambda_.inner_node_symbol) = vertex;
    frame->at(self_.filter_lambda_.inner_edge_symbol) = edge;

    TypedValue result = self_.filter_lambda_.expression->Accept(*evaluator);
    if (result.IsNull()) return false;
    if (result.IsBool()) return result.ValueBool();

    throw QueryRuntimeException(
        "Expansion condition must evaluate to boolean or null");
  }

  bool FindPath(const database::GraphDbAccessor &dba,
                const VertexAccessor &source, const VertexAccessor &sink,
                int64_t lower_bound, int64_t upper_bound, Frame *frame,
                ExpressionEvaluator *evaluator) {
    using utils::Contains;

    if (source == sink) return false;

    // We expand from both directions, both from the source and the sink.
    // Expansions meet at the middle of the path if it exists. This should
    // perform better for real-world like graphs where the expansion front
    // grows exponentially, effectively reducing the exponent by half.

    // Holds vertices at the current level of expansion from the source
    // (sink).
    std::vector<VertexAccessor> source_frontier;
    std::vector<VertexAccessor> sink_frontier;

    // Holds vertices we can expand to from `source_frontier`
    // (`sink_frontier`).
    std::vector<VertexAccessor> source_next;
    std::vector<VertexAccessor> sink_next;

    // Maps each vertex we visited expanding from the source (sink) to the
    // edge used. Necessary for path reconstruction.
    VertexEdgeMapT in_edge;
    VertexEdgeMapT out_edge;

    size_t current_length = 0;

    source_frontier.emplace_back(source);
    in_edge[source] = std::experimental::nullopt;
    sink_frontier.emplace_back(sink);
    out_edge[sink] = std::experimental::nullopt;

    while (true) {
      if (dba.should_abort()) throw HintedAbortError();
      // Top-down step (expansion from the source).
      ++current_length;
      if (current_length > upper_bound) return false;

      for (const auto &vertex : source_frontier) {
        if (self_.common_.direction != EdgeAtom::Direction::IN) {
          for (const auto &edge : vertex.out(&self_.common_.edge_types)) {
            if (ShouldExpand(edge.to(), edge, frame, evaluator) &&
                !Contains(in_edge, edge.to())) {
              in_edge.emplace(edge.to(), edge);
              if (Contains(out_edge, edge.to())) {
                if (current_length >= lower_bound) {
                  ReconstructPath(edge.to(), in_edge, out_edge, frame);
                  return true;
                } else {
                  return false;
                }
              }
              source_next.push_back(edge.to());
            }
          }
        }
        if (self_.common_.direction != EdgeAtom::Direction::OUT) {
          for (const auto &edge : vertex.in(&self_.common_.edge_types)) {
            if (ShouldExpand(edge.from(), edge, frame, evaluator) &&
                !Contains(in_edge, edge.from())) {
              in_edge.emplace(edge.from(), edge);
              if (Contains(out_edge, edge.from())) {
                if (current_length >= lower_bound) {
                  ReconstructPath(edge.from(), in_edge, out_edge, frame);
                  return true;
                } else {
                  return false;
                }
              }
              source_next.push_back(edge.from());
            }
          }
        }
      }

      if (source_next.empty()) return false;
      source_frontier.clear();
      std::swap(source_frontier, source_next);

      // Bottom-up step (expansion from the sink).
      ++current_length;
      if (current_length > upper_bound) return false;

      // When expanding from the sink we have to be careful which edge
      // endpoint we pass to `should_expand`, because everything is
      // reversed.
      for (const auto &vertex : sink_frontier) {
        if (self_.common_.direction != EdgeAtom::Direction::OUT) {
          for (const auto &edge : vertex.out(&self_.common_.edge_types)) {
            if (ShouldExpand(vertex, edge, frame, evaluator) &&
                !Contains(out_edge, edge.to())) {
              out_edge.emplace(edge.to(), edge);
              if (Contains(in_edge, edge.to())) {
                if (current_length >= lower_bound) {
                  ReconstructPath(edge.to(), in_edge, out_edge, frame);
                  return true;
                } else {
                  return false;
                }
              }
              sink_next.push_back(edge.to());
            }
          }
        }
        if (self_.common_.direction != EdgeAtom::Direction::IN) {
          for (const auto &edge : vertex.in(&self_.common_.edge_types)) {
            if (ShouldExpand(vertex, edge, frame, evaluator) &&
                !Contains(out_edge, edge.from())) {
              out_edge.emplace(edge.from(), edge);
              if (Contains(in_edge, edge.from())) {
                if (current_length >= lower_bound) {
                  ReconstructPath(edge.from(), in_edge, out_edge, frame);
                  return true;
                } else {
                  return false;
                }
              }
              sink_next.push_back(edge.from());
            }
          }
        }
      }

      if (sink_next.empty()) return false;
      sink_frontier.clear();
      std::swap(sink_frontier, sink_next);
    }
  }
};

class SingleSourceShortestPathCursor : public query::plan::Cursor {
 public:
  SingleSourceShortestPathCursor(const ExpandVariable &self,
                                 database::GraphDbAccessor &db)
      : self_(self), input_cursor_(self_.input()->MakeCursor(db)) {
    CHECK(self_.common_.graph_view == GraphView::OLD)
        << "ExpandVariable should only be planned with GraphView::OLD";
    CHECK(!self_.common_.existing_node)
        << "Single source shortest path algorithm "
           "should not be used when `existing_node` "
           "flag is set, s-t shortest path algorithm "
           "should be used instead!";
  }

  bool Pull(Frame &frame, Context &context) override {
    ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                  context.evaluation_context_,
                                  &context.db_accessor_, GraphView::OLD);

    // for the given (edge, vertex) pair checks if they satisfy the
    // "where" condition. if so, places them in the to_visit_ structure.
    auto expand_pair = [this, &evaluator, &frame](EdgeAccessor edge,
                                                  VertexAccessor vertex) {
      // if we already processed the given vertex it doesn't get expanded
      if (processed_.find(vertex) != processed_.end()) return;

      frame[self_.filter_lambda_.inner_edge_symbol] = edge;
      frame[self_.filter_lambda_.inner_node_symbol] = vertex;

      if (self_.filter_lambda_.expression) {
        TypedValue result = self_.filter_lambda_.expression->Accept(evaluator);
        switch (result.type()) {
          case TypedValue::Type::Null:
            return;
          case TypedValue::Type::Bool:
            if (!result.Value<bool>()) return;
            break;
          default:
            throw QueryRuntimeException(
                "Expansion condition must evaluate to boolean or null.");
        }
      }
      to_visit_next_.emplace_back(edge, vertex);
      processed_.emplace(vertex, edge);
    };

    // populates the to_visit_next_ structure with expansions
    // from the given vertex. skips expansions that don't satisfy
    // the "where" condition.
    auto expand_from_vertex = [this, &expand_pair](VertexAccessor &vertex) {
      if (self_.common_.direction != EdgeAtom::Direction::IN) {
        for (const EdgeAccessor &edge : vertex.out(&self_.common_.edge_types))
          expand_pair(edge, edge.to());
      }
      if (self_.common_.direction != EdgeAtom::Direction::OUT) {
        for (const EdgeAccessor &edge : vertex.in(&self_.common_.edge_types))
          expand_pair(edge, edge.from());
      }
    };

    // do it all in a loop because we skip some elements
    while (true) {
      if (context.db_accessor_.should_abort()) throw HintedAbortError();
      // if we have nothing to visit on the current depth, switch to next
      if (to_visit_current_.empty()) to_visit_current_.swap(to_visit_next_);

      // if current is still empty, it means both are empty, so pull from
      // input
      if (to_visit_current_.empty()) {
        if (!input_cursor_->Pull(frame, context)) return false;

        to_visit_current_.clear();
        to_visit_next_.clear();
        processed_.clear();

        auto vertex_value = frame[self_.input_symbol_];
        // it is possible that the vertex is Null due to optional matching
        if (vertex_value.IsNull()) continue;
        lower_bound_ = self_.lower_bound_
                           ? EvaluateInt(&evaluator, self_.lower_bound_,
                                         "Min depth in breadth-first expansion")
                           : 1;
        upper_bound_ = self_.upper_bound_
                           ? EvaluateInt(&evaluator, self_.upper_bound_,
                                         "Max depth in breadth-first expansion")
                           : std::numeric_limits<int64_t>::max();

        if (upper_bound_ < 1 || lower_bound_ > upper_bound_) continue;

        auto vertex = vertex_value.Value<VertexAccessor>();
        processed_.emplace(vertex, std::experimental::nullopt);
        expand_from_vertex(vertex);

        // go back to loop start and see if we expanded anything
        continue;
      }

      // take the next expansion from the queue
      std::pair<EdgeAccessor, VertexAccessor> expansion =
          to_visit_current_.back();
      to_visit_current_.pop_back();

      // create the frame value for the edges
      std::vector<TypedValue> edge_list{expansion.first};
      auto last_vertex = expansion.second;
      while (true) {
        const EdgeAccessor &last_edge = edge_list.back().Value<EdgeAccessor>();
        last_vertex =
            last_edge.from() == last_vertex ? last_edge.to() : last_edge.from();
        // origin_vertex must be in processed
        const auto &previous_edge = processed_.find(last_vertex)->second;
        if (!previous_edge) break;

        edge_list.push_back(previous_edge.value());
      }

      // expand only if what we've just expanded is less then max depth
      if (static_cast<int64_t>(edge_list.size()) < upper_bound_)
        expand_from_vertex(expansion.second);

      if (static_cast<int64_t>(edge_list.size()) < lower_bound_) continue;

      frame[self_.common_.node_symbol] = expansion.second;

      // place edges on the frame in the correct order
      std::reverse(edge_list.begin(), edge_list.end());
      frame[self_.common_.edge_symbol] = std::move(edge_list);

      return true;
    }
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    processed_.clear();
    to_visit_next_.clear();
    to_visit_current_.clear();
  }

 private:
  const ExpandVariable &self_;
  const std::unique_ptr<query::plan::Cursor> input_cursor_;

  // Depth bounds. Calculated on each pull from the input, the initial value
  // is irrelevant.
  int64_t lower_bound_{-1};
  int64_t upper_bound_{-1};

  // maps vertices to the edge they got expanded from. it is an optional
  // edge because the root does not get expanded from anything.
  // contains visited vertices as well as those scheduled to be visited.
  std::unordered_map<VertexAccessor, std::experimental::optional<EdgeAccessor>>
      processed_;
  // edge/vertex pairs we have yet to visit, for current and next depth
  std::vector<std::pair<EdgeAccessor, VertexAccessor>> to_visit_current_;
  std::vector<std::pair<EdgeAccessor, VertexAccessor>> to_visit_next_;
};

class ExpandWeightedShortestPathCursor : public query::plan::Cursor {
 public:
  ExpandWeightedShortestPathCursor(const ExpandVariable &self,
                                   database::GraphDbAccessor &db)
      : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

  bool Pull(Frame &frame, Context &context) override {
    ExpressionEvaluator evaluator(
        &frame, context.symbol_table_, context.evaluation_context_,
        &context.db_accessor_, self_.common_.graph_view);
    auto create_state = [this](VertexAccessor vertex, int depth) {
      return std::make_pair(vertex, upper_bound_set_ ? depth : 0);
    };

    // For the given (edge, vertex, weight, depth) tuple checks if they
    // satisfy the "where" condition. if so, places them in the priority
    // queue.
    auto expand_pair = [this, &evaluator, &frame, &create_state](
                           EdgeAccessor edge, VertexAccessor vertex,
                           double weight, int depth) {
      SwitchAccessor(edge, self_.common_.graph_view);
      SwitchAccessor(vertex, self_.common_.graph_view);

      if (self_.filter_lambda_.expression) {
        frame[self_.filter_lambda_.inner_edge_symbol] = edge;
        frame[self_.filter_lambda_.inner_node_symbol] = vertex;

        if (!EvaluateFilter(evaluator, self_.filter_lambda_.expression)) return;
      }

      frame[self_.weight_lambda_->inner_edge_symbol] = edge;
      frame[self_.weight_lambda_->inner_node_symbol] = vertex;

      TypedValue typed_weight =
          self_.weight_lambda_->expression->Accept(evaluator);

      if (!typed_weight.IsNumeric()) {
        throw QueryRuntimeException(
            "Calculated weight must be numeric, got {}.", typed_weight.type());
      }
      if ((typed_weight < 0).Value<bool>()) {
        throw QueryRuntimeException("Calculated weight must be non-negative!");
      }

      auto next_state = create_state(vertex, depth);
      auto next_weight = weight + typed_weight;
      auto found_it = total_cost_.find(next_state);
      if (found_it != total_cost_.end() &&
          found_it->second.Value<double>() <= next_weight.Value<double>())
        return;

      pq_.push({next_weight.Value<double>(), depth + 1, vertex, edge});
    };

    // Populates the priority queue structure with expansions
    // from the given vertex. skips expansions that don't satisfy
    // the "where" condition.
    auto expand_from_vertex = [this, &expand_pair](VertexAccessor &vertex,
                                                   double weight, int depth) {
      if (self_.common_.direction != EdgeAtom::Direction::IN) {
        for (const EdgeAccessor &edge : vertex.out(&self_.common_.edge_types)) {
          expand_pair(edge, edge.to(), weight, depth);
        }
      }
      if (self_.common_.direction != EdgeAtom::Direction::OUT) {
        for (const EdgeAccessor &edge : vertex.in(&self_.common_.edge_types)) {
          expand_pair(edge, edge.from(), weight, depth);
        }
      }
    };

    while (true) {
      if (context.db_accessor_.should_abort()) throw HintedAbortError();
      if (pq_.empty()) {
        if (!input_cursor_->Pull(frame, context)) return false;
        auto vertex_value = frame[self_.input_symbol_];
        if (vertex_value.IsNull()) continue;
        auto vertex = vertex_value.Value<VertexAccessor>();
        if (self_.common_.existing_node) {
          TypedValue &node = frame[self_.common_.node_symbol];
          // Due to optional matching the existing node could be null.
          // Skip expansion for such nodes.
          if (node.IsNull()) continue;
        }
        SwitchAccessor(vertex, self_.common_.graph_view);
        if (self_.upper_bound_) {
          upper_bound_ =
              EvaluateInt(&evaluator, self_.upper_bound_,
                          "Max depth in weighted shortest path expansion");
          upper_bound_set_ = true;
        } else {
          upper_bound_ = std::numeric_limits<int64_t>::max();
          upper_bound_set_ = false;
        }
        if (upper_bound_ < 1)
          throw QueryRuntimeException(
              "Maximum depth in weighted shortest path expansion must be at "
              "least 1.");

        // Clear existing data structures.
        previous_.clear();
        total_cost_.clear();
        yielded_vertices_.clear();

        pq_.push({0.0, 0, vertex, std::experimental::nullopt});
        // We are adding the starting vertex to the set of yielded vertices
        // because we don't want to yield paths that end with the starting
        // vertex.
        yielded_vertices_.insert(vertex);
      }

      while (!pq_.empty()) {
        if (context.db_accessor_.should_abort()) throw HintedAbortError();
        auto current = pq_.top();
        double current_weight = std::get<0>(current);
        int current_depth = std::get<1>(current);
        VertexAccessor current_vertex = std::get<2>(current);
        std::experimental::optional<EdgeAccessor> current_edge =
            std::get<3>(current);
        pq_.pop();

        auto current_state = create_state(current_vertex, current_depth);

        // Check if the vertex has already been processed.
        if (total_cost_.find(current_state) != total_cost_.end()) {
          continue;
        }
        previous_.emplace(current_state, current_edge);
        total_cost_.emplace(current_state, current_weight);

        // Expand only if what we've just expanded is less than max depth.
        if (current_depth < upper_bound_)
          expand_from_vertex(current_vertex, current_weight, current_depth);

        // If we yielded a path for a vertex already, make the expansion but
        // don't return the path again.
        if (yielded_vertices_.find(current_vertex) != yielded_vertices_.end())
          continue;

        // Reconstruct the path.
        auto last_vertex = current_vertex;
        auto last_depth = current_depth;
        std::vector<TypedValue> edge_list{};
        while (true) {
          // Origin_vertex must be in previous.
          const auto &previous_edge =
              previous_.find(create_state(last_vertex, last_depth))->second;
          if (!previous_edge) break;
          last_vertex = previous_edge->from() == last_vertex
                            ? previous_edge->to()
                            : previous_edge->from();
          last_depth--;
          edge_list.push_back(previous_edge.value());
        }

        // Place destination node on the frame, handle existence flag.
        if (self_.common_.existing_node) {
          TypedValue &node = frame[self_.common_.node_symbol];
          if ((node != current_vertex).Value<bool>())
            continue;
          else
            // Prevent expanding other paths, because we found the
            // shortest to existing node.
            ClearQueue();
        } else {
          frame[self_.common_.node_symbol] = current_vertex;
        }

        if (!self_.is_reverse_) {
          // Place edges on the frame in the correct order.
          std::reverse(edge_list.begin(), edge_list.end());
        }
        frame[self_.common_.edge_symbol] = std::move(edge_list);
        frame[self_.total_weight_.value()] = current_weight;
        yielded_vertices_.insert(current_vertex);
        return true;
      }
    }
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    previous_.clear();
    total_cost_.clear();
    yielded_vertices_.clear();
    ClearQueue();
  }

 private:
  const ExpandVariable &self_;
  const std::unique_ptr<query::plan::Cursor> input_cursor_;

  // Upper bound on the path length.
  int64_t upper_bound_{-1};
  bool upper_bound_set_{false};

  struct WspStateHash {
    size_t operator()(const std::pair<VertexAccessor, int> &key) const {
      return utils::HashCombine<VertexAccessor, int>{}(key.first, key.second);
    }
  };

  // Maps vertices to weights they got in expansion.
  std::unordered_map<std::pair<VertexAccessor, int>, TypedValue, WspStateHash>
      total_cost_;

  // Maps vertices to edges used to reach them.
  std::unordered_map<std::pair<VertexAccessor, int>,
                     std::experimental::optional<EdgeAccessor>, WspStateHash>
      previous_;

  // Keeps track of vertices for which we yielded a path already.
  std::unordered_set<VertexAccessor> yielded_vertices_;

  // Priority queue comparator. Keep lowest weight on top of the queue.
  class PriorityQueueComparator {
   public:
    bool operator()(
        const std::tuple<double, int, VertexAccessor,
                         std::experimental::optional<EdgeAccessor>> &lhs,
        const std::tuple<double, int, VertexAccessor,
                         std::experimental::optional<EdgeAccessor>> &rhs) {
      return std::get<0>(lhs) > std::get<0>(rhs);
    }
  };

  std::priority_queue<
      std::tuple<double, int, VertexAccessor,
                 std::experimental::optional<EdgeAccessor>>,
      std::vector<std::tuple<double, int, VertexAccessor,
                             std::experimental::optional<EdgeAccessor>>>,
      PriorityQueueComparator>
      pq_;

  void ClearQueue() {
    while (!pq_.empty()) pq_.pop();
  }
};

std::unique_ptr<Cursor> ExpandVariable::MakeCursor(
    database::GraphDbAccessor &db) const {
  switch (type_) {
    case EdgeAtom::Type::BREADTH_FIRST:
      if (common_.existing_node) {
        return std::make_unique<STShortestPathCursor>(*this, db);
      } else {
        return std::make_unique<SingleSourceShortestPathCursor>(*this, db);
      }
    case EdgeAtom::Type::DEPTH_FIRST:
      return std::make_unique<ExpandVariableCursor>(*this, db);
    case EdgeAtom::Type::WEIGHTED_SHORTEST_PATH:
      return std::make_unique<ExpandWeightedShortestPathCursor>(*this, db);
    case EdgeAtom::Type::SINGLE:
      LOG(FATAL)
          << "ExpandVariable should not be planned for a single expansion!";
  }
}

class ConstructNamedPathCursor : public Cursor {
 public:
  ConstructNamedPathCursor(const ConstructNamedPath &self,
                           database::GraphDbAccessor &db)
      : self_(self), input_cursor_(self_.input()->MakeCursor(db)) {}

  bool Pull(Frame &frame, Context &context) override {
    if (!input_cursor_->Pull(frame, context)) return false;

    auto symbol_it = self_.path_elements_.begin();
    DCHECK(symbol_it != self_.path_elements_.end())
        << "Named path must contain at least one node";

    TypedValue start_vertex = frame[*symbol_it++];

    // In an OPTIONAL MATCH everything could be Null.
    if (start_vertex.IsNull()) {
      frame[self_.path_symbol_] = TypedValue::Null;
      return true;
    }

    DCHECK(start_vertex.IsVertex())
        << "First named path element must be a vertex";
    query::Path path(start_vertex.ValueVertex());

    // If the last path element symbol was for an edge list, then
    // the next symbol is a vertex and it should not append to the path
    // because
    // expansion already did it.
    bool last_was_edge_list = false;

    for (; symbol_it != self_.path_elements_.end(); symbol_it++) {
      TypedValue expansion = frame[*symbol_it];
      //  We can have Null (OPTIONAL MATCH), a vertex, an edge, or an edge
      //  list (variable expand or BFS).
      switch (expansion.type()) {
        case TypedValue::Type::Null:
          frame[self_.path_symbol_] = TypedValue::Null;
          return true;
        case TypedValue::Type::Vertex:
          if (!last_was_edge_list) path.Expand(expansion.ValueVertex());
          last_was_edge_list = false;
          break;
        case TypedValue::Type::Edge:
          path.Expand(expansion.ValueEdge());
          break;
        case TypedValue::Type::List: {
          last_was_edge_list = true;
          // We need to expand all edges in the list and intermediary
          // vertices.
          const std::vector<TypedValue> &edges = expansion.ValueList();
          for (const auto &edge_value : edges) {
            const EdgeAccessor &edge = edge_value.ValueEdge();
            const VertexAccessor from = edge.from();
            if (path.vertices().back() == from)
              path.Expand(edge, edge.to());
            else
              path.Expand(edge, from);
          }
          break;
        }
        default:
          LOG(FATAL) << "Unsupported type in named path construction";

          break;
      }
    }

    frame[self_.path_symbol_] = path;
    return true;
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override { input_cursor_->Reset(); }

 private:
  const ConstructNamedPath self_;
  const std::unique_ptr<Cursor> input_cursor_;
};

ACCEPT_WITH_INPUT(ConstructNamedPath)

std::unique_ptr<Cursor> ConstructNamedPath::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<ConstructNamedPathCursor>(*this, db);
}

std::vector<Symbol> ConstructNamedPath::ModifiedSymbols(
    const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(path_symbol_);
  return symbols;
}

Filter::Filter(const std::shared_ptr<LogicalOperator> &input,
               Expression *expression)
    : input_(input ? input : std::make_shared<Once>()),
      expression_(expression) {}

ACCEPT_WITH_INPUT(Filter)

std::unique_ptr<Cursor> Filter::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<FilterCursor>(*this, db);
}

std::vector<Symbol> Filter::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

Filter::FilterCursor::FilterCursor(const Filter &self,
                                   database::GraphDbAccessor &db)
    : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

bool Filter::FilterCursor::Pull(Frame &frame, Context &context) {
  // Like all filters, newly set values should not affect filtering of old
  // nodes and edges.
  ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                context.evaluation_context_,
                                &context.db_accessor_, GraphView::OLD);
  while (input_cursor_->Pull(frame, context)) {
    if (EvaluateFilter(evaluator, self_.expression_)) return true;
  }
  return false;
}

void Filter::FilterCursor::Shutdown() { input_cursor_->Shutdown(); }

void Filter::FilterCursor::Reset() { input_cursor_->Reset(); }

Produce::Produce(const std::shared_ptr<LogicalOperator> &input,
                 const std::vector<NamedExpression *> &named_expressions)
    : input_(input ? input : std::make_shared<Once>()),
      named_expressions_(named_expressions) {}

ACCEPT_WITH_INPUT(Produce)

std::unique_ptr<Cursor> Produce::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<ProduceCursor>(*this, db);
}

std::vector<Symbol> Produce::OutputSymbols(
    const SymbolTable &symbol_table) const {
  std::vector<Symbol> symbols;
  for (const auto &named_expr : named_expressions_) {
    symbols.emplace_back(symbol_table.at(*named_expr));
  }
  return symbols;
}

std::vector<Symbol> Produce::ModifiedSymbols(const SymbolTable &table) const {
  return OutputSymbols(table);
}

Produce::ProduceCursor::ProduceCursor(const Produce &self,
                                      database::GraphDbAccessor &db)
    : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

bool Produce::ProduceCursor::Pull(Frame &frame, Context &context) {
  if (input_cursor_->Pull(frame, context)) {
    // Produce should always yield the latest results.
    ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                  context.evaluation_context_,
                                  &context.db_accessor_, GraphView::NEW);
    for (auto named_expr : self_.named_expressions_)
      named_expr->Accept(evaluator);
    return true;
  }
  return false;
}

void Produce::ProduceCursor::Shutdown() { input_cursor_->Shutdown(); }

void Produce::ProduceCursor::Reset() { input_cursor_->Reset(); }

Delete::Delete(const std::shared_ptr<LogicalOperator> &input_,
               const std::vector<Expression *> &expressions, bool detach_)
    : input_(input_), expressions_(expressions), detach_(detach_) {}

ACCEPT_WITH_INPUT(Delete)

std::unique_ptr<Cursor> Delete::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<DeleteCursor>(*this, db);
}

std::vector<Symbol> Delete::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

Delete::DeleteCursor::DeleteCursor(const Delete &self,
                                   database::GraphDbAccessor &db)
    : self_(self), db_(db), input_cursor_(self_.input_->MakeCursor(db)) {}

bool Delete::DeleteCursor::Pull(Frame &frame, Context &context) {
  if (!input_cursor_->Pull(frame, context)) return false;

  // Delete should get the latest information, this way it is also possible
  // to
  // delete newly added nodes and edges.
  ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                context.evaluation_context_,
                                &context.db_accessor_, GraphView::NEW);
  // collect expressions results so edges can get deleted before vertices
  // this is necessary because an edge that gets deleted could block vertex
  // deletion
  std::vector<TypedValue> expression_results;
  expression_results.reserve(self_.expressions_.size());
  for (Expression *expression : self_.expressions_) {
    expression_results.emplace_back(expression->Accept(evaluator));
  }

  // delete edges first
  for (TypedValue &expression_result : expression_results) {
    if (db_.should_abort()) throw HintedAbortError();
    if (expression_result.type() == TypedValue::Type::Edge)
      db_.RemoveEdge(expression_result.Value<EdgeAccessor>());
  }

  // delete vertices
  for (TypedValue &expression_result : expression_results) {
    if (db_.should_abort()) throw HintedAbortError();
    switch (expression_result.type()) {
      case TypedValue::Type::Vertex: {
        VertexAccessor &va = expression_result.Value<VertexAccessor>();
        va.SwitchNew();  //  necessary because an edge deletion could have
                         //  updated
        if (self_.detach_)
          db_.DetachRemoveVertex(va);
        else if (!db_.RemoveVertex(va))
          throw RemoveAttachedVertexException();
        break;
      }

      // skip Edges (already deleted) and Nulls (can occur in optional
      // match)
      case TypedValue::Type::Edge:
      case TypedValue::Type::Null:
        break;
      // check we're not trying to delete anything except vertices and edges
      default:
        throw QueryRuntimeException("Only edges and vertices can be deleted.");
    }
  }

  return true;
}

void Delete::DeleteCursor::Shutdown() { input_cursor_->Shutdown(); }

void Delete::DeleteCursor::Reset() { input_cursor_->Reset(); }

SetProperty::SetProperty(const std::shared_ptr<LogicalOperator> &input,
                         PropertyLookup *lhs, Expression *rhs)
    : input_(input), lhs_(lhs), rhs_(rhs) {}

ACCEPT_WITH_INPUT(SetProperty)

std::unique_ptr<Cursor> SetProperty::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<SetPropertyCursor>(*this, db);
}

std::vector<Symbol> SetProperty::ModifiedSymbols(
    const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

SetProperty::SetPropertyCursor::SetPropertyCursor(const SetProperty &self,
                                                  database::GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

bool SetProperty::SetPropertyCursor::Pull(Frame &frame, Context &context) {
  if (!input_cursor_->Pull(frame, context)) return false;

  // Set, just like Create needs to see the latest changes.
  ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                context.evaluation_context_,
                                &context.db_accessor_, GraphView::NEW);
  TypedValue lhs = self_.lhs_->expression_->Accept(evaluator);
  TypedValue rhs = self_.rhs_->Accept(evaluator);

  switch (lhs.type()) {
    case TypedValue::Type::Vertex:
      PropsSetChecked(&lhs.Value<VertexAccessor>(), self_.lhs_->property_, rhs);
      break;
    case TypedValue::Type::Edge:
      PropsSetChecked(&lhs.Value<EdgeAccessor>(), self_.lhs_->property_, rhs);
      break;
    case TypedValue::Type::Null:
      // Skip setting properties on Null (can occur in optional match).
      break;
    case TypedValue::Type::Map:
    // Semantically modifying a map makes sense, but it's not supported due
    // to
    // all the copying we do (when PropertyValue -> TypedValue and in
    // ExpressionEvaluator). So even though we set a map property here, that
    // is never visible to the user and it's not stored.
    // TODO: fix above described bug
    default:
      throw QueryRuntimeException(
          "Properties can only be set on edges and vertices.");
  }
  return true;
}

void SetProperty::SetPropertyCursor::Shutdown() { input_cursor_->Shutdown(); }

void SetProperty::SetPropertyCursor::Reset() { input_cursor_->Reset(); }

SetProperties::SetProperties(const std::shared_ptr<LogicalOperator> &input,
                             Symbol input_symbol, Expression *rhs, Op op)
    : input_(input), input_symbol_(input_symbol), rhs_(rhs), op_(op) {}

ACCEPT_WITH_INPUT(SetProperties)

std::unique_ptr<Cursor> SetProperties::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<SetPropertiesCursor>(*this, db);
}

std::vector<Symbol> SetProperties::ModifiedSymbols(
    const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

SetProperties::SetPropertiesCursor::SetPropertiesCursor(
    const SetProperties &self, database::GraphDbAccessor &db)
    : self_(self), db_(db), input_cursor_(self.input_->MakeCursor(db)) {}

bool SetProperties::SetPropertiesCursor::Pull(Frame &frame, Context &context) {
  if (!input_cursor_->Pull(frame, context)) return false;

  TypedValue &lhs = frame[self_.input_symbol_];

  // Set, just like Create needs to see the latest changes.
  ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                context.evaluation_context_,
                                &context.db_accessor_, GraphView::NEW);
  TypedValue rhs = self_.rhs_->Accept(evaluator);

  switch (lhs.type()) {
    case TypedValue::Type::Vertex:
      Set(lhs.Value<VertexAccessor>(), rhs);
      break;
    case TypedValue::Type::Edge:
      Set(lhs.Value<EdgeAccessor>(), rhs);
      break;
    case TypedValue::Type::Null:
      // Skip setting properties on Null (can occur in optional match).
      break;
    default:
      throw QueryRuntimeException(
          "Properties can only be set on edges and vertices.");
  }
  return true;
}

void SetProperties::SetPropertiesCursor::Shutdown() {
  input_cursor_->Shutdown();
}

void SetProperties::SetPropertiesCursor::Reset() { input_cursor_->Reset(); }

template <typename TRecordAccessor>
void SetProperties::SetPropertiesCursor::Set(TRecordAccessor &record,
                                             const TypedValue &rhs) const {
  record.SwitchNew();
  if (self_.op_ == Op::REPLACE) {
    try {
      record.PropsClear();
    } catch (const RecordDeletedError &) {
      throw QueryRuntimeException(
          "Trying to set properties on a deleted graph element.");
    }
  }

  auto set_props = [&record](const auto &properties) {
    try {
      for (const auto &kv : properties) record.PropsSet(kv.first, kv.second);
    } catch (const RecordDeletedError &) {
      throw QueryRuntimeException(
          "Trying to set properties on a deleted graph element.");
    }
  };

  switch (rhs.type()) {
    case TypedValue::Type::Edge:
      set_props(rhs.Value<EdgeAccessor>().Properties());
      break;
    case TypedValue::Type::Vertex:
      set_props(rhs.Value<VertexAccessor>().Properties());
      break;
    case TypedValue::Type::Map: {
      for (const auto &kv : rhs.Value<std::map<std::string, TypedValue>>())
        PropsSetChecked(&record, db_.Property(kv.first), kv.second);
      break;
    }
    default:
      throw QueryRuntimeException(
          "Right-hand side in SET expression must be a node, an edge or a "
          "map.");
  }
}

// instantiate the SetProperties function with concrete TRecordAccessor
// types
template void SetProperties::SetPropertiesCursor::Set(
    RecordAccessor<Vertex> &record, const TypedValue &rhs) const;
template void SetProperties::SetPropertiesCursor::Set(
    RecordAccessor<Edge> &record, const TypedValue &rhs) const;

SetLabels::SetLabels(const std::shared_ptr<LogicalOperator> &input,
                     Symbol input_symbol,
                     const std::vector<storage::Label> &labels)
    : input_(input), input_symbol_(input_symbol), labels_(labels) {}

ACCEPT_WITH_INPUT(SetLabels)

std::unique_ptr<Cursor> SetLabels::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<SetLabelsCursor>(*this, db);
}

std::vector<Symbol> SetLabels::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

SetLabels::SetLabelsCursor::SetLabelsCursor(const SetLabels &self,
                                            database::GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

bool SetLabels::SetLabelsCursor::Pull(Frame &frame, Context &context) {
  if (!input_cursor_->Pull(frame, context)) return false;

  TypedValue &vertex_value = frame[self_.input_symbol_];
  // Skip setting labels on Null (can occur in optional match).
  if (vertex_value.IsNull()) return true;
  ExpectType(self_.input_symbol_, vertex_value, TypedValue::Type::Vertex);
  auto &vertex = vertex_value.Value<VertexAccessor>();
  vertex.SwitchNew();
  try {
    for (auto label : self_.labels_) vertex.add_label(label);
  } catch (const RecordDeletedError &) {
    throw QueryRuntimeException("Trying to set labels on a deleted node.");
  }

  return true;
}

void SetLabels::SetLabelsCursor::Shutdown() { input_cursor_->Shutdown(); }

void SetLabels::SetLabelsCursor::Reset() { input_cursor_->Reset(); }

RemoveProperty::RemoveProperty(const std::shared_ptr<LogicalOperator> &input,
                               PropertyLookup *lhs)
    : input_(input), lhs_(lhs) {}

ACCEPT_WITH_INPUT(RemoveProperty)

std::unique_ptr<Cursor> RemoveProperty::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<RemovePropertyCursor>(*this, db);
}

std::vector<Symbol> RemoveProperty::ModifiedSymbols(
    const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

RemoveProperty::RemovePropertyCursor::RemovePropertyCursor(
    const RemoveProperty &self, database::GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

bool RemoveProperty::RemovePropertyCursor::Pull(Frame &frame,
                                                Context &context) {
  if (!input_cursor_->Pull(frame, context)) return false;

  // Remove, just like Delete needs to see the latest changes.
  ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                context.evaluation_context_,
                                &context.db_accessor_, GraphView::NEW);
  TypedValue lhs = self_.lhs_->expression_->Accept(evaluator);

  switch (lhs.type()) {
    case TypedValue::Type::Vertex:
      try {
        lhs.Value<VertexAccessor>().PropsErase(self_.lhs_->property_);
      } catch (const RecordDeletedError &) {
        throw QueryRuntimeException(
            "Trying to remove properties from a deleted node.");
      }
      break;
    case TypedValue::Type::Edge:
      try {
        lhs.Value<EdgeAccessor>().PropsErase(self_.lhs_->property_);
      } catch (const RecordDeletedError &) {
        throw QueryRuntimeException(
            "Trying to remove properties from a deleted edge.");
      }
      break;
    case TypedValue::Type::Null:
      // Skip removing properties on Null (can occur in optional match).
      break;
    default:
      throw QueryRuntimeException(
          "Properties can only be removed from vertices and edges.");
  }
  return true;
}

void RemoveProperty::RemovePropertyCursor::Shutdown() {
  input_cursor_->Shutdown();
}

void RemoveProperty::RemovePropertyCursor::Reset() { input_cursor_->Reset(); }

RemoveLabels::RemoveLabels(const std::shared_ptr<LogicalOperator> &input,
                           Symbol input_symbol,
                           const std::vector<storage::Label> &labels)
    : input_(input), input_symbol_(input_symbol), labels_(labels) {}

ACCEPT_WITH_INPUT(RemoveLabels)

std::unique_ptr<Cursor> RemoveLabels::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<RemoveLabelsCursor>(*this, db);
}

std::vector<Symbol> RemoveLabels::ModifiedSymbols(
    const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

RemoveLabels::RemoveLabelsCursor::RemoveLabelsCursor(
    const RemoveLabels &self, database::GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

bool RemoveLabels::RemoveLabelsCursor::Pull(Frame &frame, Context &context) {
  if (!input_cursor_->Pull(frame, context)) return false;

  TypedValue &vertex_value = frame[self_.input_symbol_];
  // Skip removing labels on Null (can occur in optional match).
  if (vertex_value.IsNull()) return true;
  ExpectType(self_.input_symbol_, vertex_value, TypedValue::Type::Vertex);
  auto &vertex = vertex_value.Value<VertexAccessor>();
  vertex.SwitchNew();
  try {
    for (auto label : self_.labels_) vertex.remove_label(label);
  } catch (const RecordDeletedError &) {
    throw QueryRuntimeException("Trying to remove labels from a deleted node.");
  }

  return true;
}

void RemoveLabels::RemoveLabelsCursor::Shutdown() { input_cursor_->Shutdown(); }

void RemoveLabels::RemoveLabelsCursor::Reset() { input_cursor_->Reset(); }

EdgeUniquenessFilter::EdgeUniquenessFilter(
    const std::shared_ptr<LogicalOperator> &input, Symbol expand_symbol,
    const std::vector<Symbol> &previous_symbols)
    : input_(input),
      expand_symbol_(expand_symbol),
      previous_symbols_(previous_symbols) {}

ACCEPT_WITH_INPUT(EdgeUniquenessFilter)

std::unique_ptr<Cursor> EdgeUniquenessFilter::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<EdgeUniquenessFilterCursor>(*this, db);
}

std::vector<Symbol> EdgeUniquenessFilter::ModifiedSymbols(
    const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

EdgeUniquenessFilter::EdgeUniquenessFilterCursor::
    EdgeUniquenessFilterCursor(const EdgeUniquenessFilter &self,
                                 database::GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

namespace {
/**
 * Returns true if:
 *    - a and b are either edge or edge-list values, and there
 *    is at least one matching edge in the two values
 */
bool ContainsSameEdge(const TypedValue &a, const TypedValue &b) {
  auto compare_to_list = [](const TypedValue &list, const TypedValue &other) {
    for (const TypedValue &list_elem : list.Value<std::vector<TypedValue>>())
      if (ContainsSameEdge(list_elem, other)) return true;
    return false;
  };

  if (a.type() == TypedValue::Type::List) return compare_to_list(a, b);
  if (b.type() == TypedValue::Type::List) return compare_to_list(b, a);

  return a.Value<EdgeAccessor>() == b.Value<EdgeAccessor>();
}
}  // namespace

bool EdgeUniquenessFilter::EdgeUniquenessFilterCursor::Pull(
    Frame &frame, Context &context) {
  auto expansion_ok = [&]() {
    TypedValue &expand_value = frame[self_.expand_symbol_];
    for (const auto &previous_symbol : self_.previous_symbols_) {
      TypedValue &previous_value = frame[previous_symbol];
      // This shouldn't raise a TypedValueException, because the planner
      // makes sure these are all of the expected type. In case they are not
      // an error should be raised long before this code is executed.
      if (ContainsSameEdge(previous_value, expand_value)) return false;
    }
    return true;
  };

  while (input_cursor_->Pull(frame, context))
    if (expansion_ok()) return true;
  return false;
}

void EdgeUniquenessFilter::EdgeUniquenessFilterCursor::Shutdown() {
  input_cursor_->Shutdown();
}

void EdgeUniquenessFilter::EdgeUniquenessFilterCursor::Reset() {
  input_cursor_->Reset();
}

Accumulate::Accumulate(const std::shared_ptr<LogicalOperator> &input,
                       const std::vector<Symbol> &symbols, bool advance_command)
    : input_(input), symbols_(symbols), advance_command_(advance_command) {}

ACCEPT_WITH_INPUT(Accumulate)

std::unique_ptr<Cursor> Accumulate::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<Accumulate::AccumulateCursor>(*this, db);
}

std::vector<Symbol> Accumulate::ModifiedSymbols(const SymbolTable &) const {
  return symbols_;
}

Accumulate::AccumulateCursor::AccumulateCursor(const Accumulate &self,
                                               database::GraphDbAccessor &db)
    : self_(self), db_(db), input_cursor_(self.input_->MakeCursor(db)) {}

bool Accumulate::AccumulateCursor::Pull(Frame &frame, Context &context) {
  // cache all the input
  if (!pulled_all_input_) {
    while (input_cursor_->Pull(frame, context)) {
      std::vector<TypedValue> row;
      row.reserve(self_.symbols_.size());
      for (const Symbol &symbol : self_.symbols_)
        row.emplace_back(frame[symbol]);
      cache_.emplace_back(std::move(row));
    }
    pulled_all_input_ = true;
    cache_it_ = cache_.begin();

    if (self_.advance_command_) {
      db_.AdvanceCommand();
      for (auto &row : cache_)
        for (auto &col : row) query::ReconstructTypedValue(col);
    }
  }

  if (db_.should_abort()) throw HintedAbortError();
  if (cache_it_ == cache_.end()) return false;
  auto row_it = (cache_it_++)->begin();
  for (const Symbol &symbol : self_.symbols_) frame[symbol] = *row_it++;
  return true;
}

void Accumulate::AccumulateCursor::Shutdown() { input_cursor_->Shutdown(); }

void Accumulate::AccumulateCursor::Reset() {
  input_cursor_->Reset();
  cache_.clear();
  cache_it_ = cache_.begin();
  pulled_all_input_ = false;
}

Aggregate::Aggregate(const std::shared_ptr<LogicalOperator> &input,
                     const std::vector<Aggregate::Element> &aggregations,
                     const std::vector<Expression *> &group_by,
                     const std::vector<Symbol> &remember)
    : input_(input ? input : std::make_shared<Once>()),
      aggregations_(aggregations),
      group_by_(group_by),
      remember_(remember) {}

ACCEPT_WITH_INPUT(Aggregate)

std::unique_ptr<Cursor> Aggregate::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<AggregateCursor>(*this, db);
}

std::vector<Symbol> Aggregate::ModifiedSymbols(const SymbolTable &) const {
  auto symbols = remember_;
  for (const auto &elem : aggregations_) symbols.push_back(elem.output_sym);
  return symbols;
}

Aggregate::AggregateCursor::AggregateCursor(const Aggregate &self,
                                            database::GraphDbAccessor &db)
    : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

namespace {
/** Returns the default TypedValue for an Aggregation element.
 * This value is valid both for returning when where are no inputs
 * to the aggregation op, and for initializing an aggregation result
 * when there are */
TypedValue DefaultAggregationOpValue(const Aggregate::Element &element) {
  switch (element.op) {
    case Aggregation::Op::COUNT:
      return TypedValue(0);
    case Aggregation::Op::SUM:
    case Aggregation::Op::MIN:
    case Aggregation::Op::MAX:
    case Aggregation::Op::AVG:
      return TypedValue::Null;
    case Aggregation::Op::COLLECT_LIST:
      return TypedValue(std::vector<TypedValue>());
    case Aggregation::Op::COLLECT_MAP:
      return TypedValue(std::map<std::string, TypedValue>());
  }
}
}  // namespace

bool Aggregate::AggregateCursor::Pull(Frame &frame, Context &context) {
  if (!pulled_all_input_) {
    ProcessAll(frame, context);
    pulled_all_input_ = true;
    aggregation_it_ = aggregation_.begin();

    // in case there is no input and no group_bys we need to return true
    // just this once
    if (aggregation_.empty() && self_.group_by_.empty()) {
      // place default aggregation values on the frame
      for (const auto &elem : self_.aggregations_)
        frame[elem.output_sym] = DefaultAggregationOpValue(elem);
      // place null as remember values on the frame
      for (const Symbol &remember_sym : self_.remember_)
        frame[remember_sym] = TypedValue::Null;
      return true;
    }
  }

  if (aggregation_it_ == aggregation_.end()) return false;

  // place aggregation values on the frame
  auto aggregation_values_it = aggregation_it_->second.values_.begin();
  for (const auto &aggregation_elem : self_.aggregations_)
    frame[aggregation_elem.output_sym] = *aggregation_values_it++;

  // place remember values on the frame
  auto remember_values_it = aggregation_it_->second.remember_.begin();
  for (const Symbol &remember_sym : self_.remember_)
    frame[remember_sym] = *remember_values_it++;

  aggregation_it_++;
  return true;
}

void Aggregate::AggregateCursor::ProcessAll(Frame &frame, Context &context) {
  ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                context.evaluation_context_,
                                &context.db_accessor_, GraphView::NEW);
  while (input_cursor_->Pull(frame, context)) {
    ProcessOne(frame, context.symbol_table_, evaluator);
  }

  // calculate AVG aggregations (so far they have only been summed)
  for (int pos = 0; pos < static_cast<int>(self_.aggregations_.size()); ++pos) {
    if (self_.aggregations_[pos].op != Aggregation::Op::AVG) continue;
    for (auto &kv : aggregation_) {
      AggregationValue &agg_value = kv.second;
      int count = agg_value.counts_[pos];
      if (count > 0)
        agg_value.values_[pos] = agg_value.values_[pos] / (double)count;
    }
  }
}

void Aggregate::AggregateCursor::ProcessOne(Frame &frame,
                                            const SymbolTable &symbol_table,
                                            ExpressionEvaluator &evaluator) {
  std::vector<TypedValue> group_by;
  group_by.reserve(self_.group_by_.size());
  for (Expression *expression : self_.group_by_) {
    group_by.emplace_back(expression->Accept(evaluator));
  }

  AggregationValue &agg_value = aggregation_[group_by];
  EnsureInitialized(frame, agg_value);
  Update(frame, symbol_table, evaluator, agg_value);
}

void Aggregate::AggregateCursor::EnsureInitialized(
    Frame &frame,
    Aggregate::AggregateCursor::AggregationValue &agg_value) const {
  if (agg_value.values_.size() > 0) return;

  for (const auto &agg_elem : self_.aggregations_)
    agg_value.values_.emplace_back(DefaultAggregationOpValue(agg_elem));
  agg_value.counts_.resize(self_.aggregations_.size(), 0);

  for (const Symbol &remember_sym : self_.remember_)
    agg_value.remember_.push_back(frame[remember_sym]);
}

void Aggregate::AggregateCursor::Update(
    Frame &, const SymbolTable &, ExpressionEvaluator &evaluator,
    Aggregate::AggregateCursor::AggregationValue &agg_value) {
  DCHECK(self_.aggregations_.size() == agg_value.values_.size())
      << "Expected as much AggregationValue.values_ as there are "
         "aggregations.";
  DCHECK(self_.aggregations_.size() == agg_value.counts_.size())
      << "Expected as much AggregationValue.counts_ as there are "
         "aggregations.";

  // we iterate over counts, values and aggregation info at the same time
  auto count_it = agg_value.counts_.begin();
  auto value_it = agg_value.values_.begin();
  auto agg_elem_it = self_.aggregations_.begin();
  for (; count_it < agg_value.counts_.end();
       count_it++, value_it++, agg_elem_it++) {
    // COUNT(*) is the only case where input expression is optional
    // handle it here
    auto input_expr_ptr = agg_elem_it->value;
    if (!input_expr_ptr) {
      *count_it += 1;
      *value_it = *count_it;
      continue;
    }

    TypedValue input_value = input_expr_ptr->Accept(evaluator);

    // Aggregations skip Null input values.
    if (input_value.IsNull()) continue;
    const auto &agg_op = agg_elem_it->op;
    *count_it += 1;
    if (*count_it == 1) {
      // first value, nothing to aggregate. check type, set and continue.
      switch (agg_op) {
        case Aggregation::Op::MIN:
        case Aggregation::Op::MAX:
          *value_it = input_value;
          EnsureOkForMinMax(input_value);
          break;
        case Aggregation::Op::SUM:
        case Aggregation::Op::AVG:
          *value_it = input_value;
          EnsureOkForAvgSum(input_value);
          break;
        case Aggregation::Op::COUNT:
          *value_it = 1;
          break;
        case Aggregation::Op::COLLECT_LIST:
          value_it->Value<std::vector<TypedValue>>().push_back(input_value);
          break;
        case Aggregation::Op::COLLECT_MAP:
          auto key = agg_elem_it->key->Accept(evaluator);
          if (key.type() != TypedValue::Type::String)
            throw QueryRuntimeException("Map key must be a string.");
          value_it->Value<std::map<std::string, TypedValue>>().emplace(
              key.Value<std::string>(), input_value);
          break;
      }
      continue;
    }

    // aggregation of existing values
    switch (agg_op) {
      case Aggregation::Op::COUNT:
        *value_it = *count_it;
        break;
      case Aggregation::Op::MIN: {
        EnsureOkForMinMax(input_value);
        try {
          TypedValue comparison_result = input_value < *value_it;
          // since we skip nulls we either have a valid comparison, or
          // an exception was just thrown above
          // safe to assume a bool TypedValue
          if (comparison_result.Value<bool>()) *value_it = input_value;
        } catch (const TypedValueException &) {
          throw QueryRuntimeException("Unable to get MIN of '{}' and '{}'.",
                                      input_value.type(), value_it->type());
        }
        break;
      }
      case Aggregation::Op::MAX: {
        //  all comments as for Op::Min
        EnsureOkForMinMax(input_value);
        try {
          TypedValue comparison_result = input_value > *value_it;
          if (comparison_result.Value<bool>()) *value_it = input_value;
        } catch (const TypedValueException &) {
          throw QueryRuntimeException("Unable to get MAX of '{}' and '{}'.",
                                      input_value.type(), value_it->type());
        }
        break;
      }
      case Aggregation::Op::AVG:
      // for averaging we sum first and divide by count once all
      // the input has been processed
      case Aggregation::Op::SUM:
        EnsureOkForAvgSum(input_value);
        *value_it = *value_it + input_value;
        break;
      case Aggregation::Op::COLLECT_LIST:
        value_it->Value<std::vector<TypedValue>>().push_back(input_value);
        break;
      case Aggregation::Op::COLLECT_MAP:
        auto key = agg_elem_it->key->Accept(evaluator);
        if (key.type() != TypedValue::Type::String)
          throw QueryRuntimeException("Map key must be a string.");
        value_it->Value<std::map<std::string, TypedValue>>().emplace(
            key.Value<std::string>(), input_value);
        break;
    }  // end switch over Aggregation::Op enum
  }    // end loop over all aggregations
}

void Aggregate::AggregateCursor::Shutdown() { input_cursor_->Shutdown(); }

void Aggregate::AggregateCursor::Reset() {
  input_cursor_->Reset();
  aggregation_.clear();
  aggregation_it_ = aggregation_.begin();
  pulled_all_input_ = false;
}

void Aggregate::AggregateCursor::EnsureOkForMinMax(
    const TypedValue &value) const {
  switch (value.type()) {
    case TypedValue::Type::Bool:
    case TypedValue::Type::Int:
    case TypedValue::Type::Double:
    case TypedValue::Type::String:
      return;
    default:
      throw QueryRuntimeException(
          "Only boolean, numeric and string values are allowed in "
          "MIN and MAX aggregations.");
  }
}
void Aggregate::AggregateCursor::EnsureOkForAvgSum(
    const TypedValue &value) const {
  switch (value.type()) {
    case TypedValue::Type::Int:
    case TypedValue::Type::Double:
      return;
    default:
      throw QueryRuntimeException(
          "Only numeric values allowed in SUM and AVG aggregations.");
  }
}

bool TypedValueVectorEqual::operator()(
    const std::vector<TypedValue> &left,
    const std::vector<TypedValue> &right) const {
  DCHECK(left.size() == right.size())
      << "TypedValueVector comparison should only be done over vectors "
         "of the same size";
  return std::equal(left.begin(), left.end(), right.begin(),
                    TypedValue::BoolEqual{});
}

Skip::Skip(const std::shared_ptr<LogicalOperator> &input,
           Expression *expression)
    : input_(input), expression_(expression) {}

ACCEPT_WITH_INPUT(Skip)

std::unique_ptr<Cursor> Skip::MakeCursor(database::GraphDbAccessor &db) const {
  return std::make_unique<SkipCursor>(*this, db);
}

std::vector<Symbol> Skip::OutputSymbols(const SymbolTable &symbol_table) const {
  // Propagate this to potential Produce.
  return input_->OutputSymbols(symbol_table);
}

std::vector<Symbol> Skip::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

Skip::SkipCursor::SkipCursor(const Skip &self, database::GraphDbAccessor &db)
    : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

bool Skip::SkipCursor::Pull(Frame &frame, Context &context) {
  while (input_cursor_->Pull(frame, context)) {
    if (to_skip_ == -1) {
      // First successful pull from the input, evaluate the skip expression.
      // The skip expression doesn't contain identifiers so graph view
      // parameter is not important.
      ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                    context.evaluation_context_,
                                    &context.db_accessor_, GraphView::OLD);
      TypedValue to_skip = self_.expression_->Accept(evaluator);
      if (to_skip.type() != TypedValue::Type::Int)
        throw QueryRuntimeException(
            "Number of elements to skip must be an integer.");

      to_skip_ = to_skip.Value<int64_t>();
      if (to_skip_ < 0)
        throw QueryRuntimeException(
            "Number of elements to skip must be non-negative.");
    }

    if (skipped_++ < to_skip_) continue;
    return true;
  }
  return false;
}

void Skip::SkipCursor::Shutdown() { input_cursor_->Shutdown(); }

void Skip::SkipCursor::Reset() {
  input_cursor_->Reset();
  to_skip_ = -1;
  skipped_ = 0;
}

Limit::Limit(const std::shared_ptr<LogicalOperator> &input,
             Expression *expression)
    : input_(input), expression_(expression) {}

ACCEPT_WITH_INPUT(Limit)

std::unique_ptr<Cursor> Limit::MakeCursor(database::GraphDbAccessor &db) const {
  return std::make_unique<LimitCursor>(*this, db);
}

std::vector<Symbol> Limit::OutputSymbols(
    const SymbolTable &symbol_table) const {
  // Propagate this to potential Produce.
  return input_->OutputSymbols(symbol_table);
}

std::vector<Symbol> Limit::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

Limit::LimitCursor::LimitCursor(const Limit &self,
                                database::GraphDbAccessor &db)
    : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

bool Limit::LimitCursor::Pull(Frame &frame, Context &context) {
  // We need to evaluate the limit expression before the first input Pull
  // because it might be 0 and thereby we shouldn't Pull from input at all.
  // We can do this before Pulling from the input because the limit expression
  // is not allowed to contain any identifiers.
  if (limit_ == -1) {
    // Limit expression doesn't contain identifiers so graph view is not
    // important.
    ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                  context.evaluation_context_,
                                  &context.db_accessor_, GraphView::OLD);
    TypedValue limit = self_.expression_->Accept(evaluator);
    if (limit.type() != TypedValue::Type::Int)
      throw QueryRuntimeException(
          "Limit on number of returned elements must be an integer.");

    limit_ = limit.Value<int64_t>();
    if (limit_ < 0)
      throw QueryRuntimeException(
          "Limit on number of returned elements must be non-negative.");
  }

  // check we have not exceeded the limit before pulling
  if (pulled_++ >= limit_) return false;

  return input_cursor_->Pull(frame, context);
}

void Limit::LimitCursor::Shutdown() { input_cursor_->Shutdown(); }

void Limit::LimitCursor::Reset() {
  input_cursor_->Reset();
  limit_ = -1;
  pulled_ = 0;
}

OrderBy::OrderBy(const std::shared_ptr<LogicalOperator> &input,
                 const std::vector<SortItem> &order_by,
                 const std::vector<Symbol> &output_symbols)
    : input_(input), output_symbols_(output_symbols) {
  // split the order_by vector into two vectors of orderings and expressions
  std::vector<Ordering> ordering;
  ordering.reserve(order_by.size());
  order_by_.reserve(order_by.size());
  for (const auto &ordering_expression_pair : order_by) {
    ordering.emplace_back(ordering_expression_pair.ordering);
    order_by_.emplace_back(ordering_expression_pair.expression);
  }
  compare_ = TypedValueVectorCompare(ordering);
}

ACCEPT_WITH_INPUT(OrderBy)

std::unique_ptr<Cursor> OrderBy::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<OrderByCursor>(*this, db);
}

std::vector<Symbol> OrderBy::OutputSymbols(
    const SymbolTable &symbol_table) const {
  // Propagate this to potential Produce.
  return input_->OutputSymbols(symbol_table);
}

std::vector<Symbol> OrderBy::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

OrderBy::OrderByCursor::OrderByCursor(const OrderBy &self,
                                      database::GraphDbAccessor &db)
    : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

bool OrderBy::OrderByCursor::Pull(Frame &frame, Context &context) {
  if (!did_pull_all_) {
    ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                  context.evaluation_context_,
                                  &context.db_accessor_, GraphView::OLD);
    while (input_cursor_->Pull(frame, context)) {
      // collect the order_by elements
      std::vector<TypedValue> order_by;
      order_by.reserve(self_.order_by_.size());
      for (auto expression_ptr : self_.order_by_) {
        order_by.emplace_back(expression_ptr->Accept(evaluator));
      }

      // collect the output elements
      std::vector<TypedValue> output;
      output.reserve(self_.output_symbols_.size());
      for (const Symbol &output_sym : self_.output_symbols_)
        output.emplace_back(frame[output_sym]);

      cache_.emplace_back(std::move(order_by), std::move(output));
    }

    std::sort(cache_.begin(), cache_.end(),
              [this](const auto &pair1, const auto &pair2) {
                return self_.compare_(pair1.first, pair2.first);
              });

    did_pull_all_ = true;
    cache_it_ = cache_.begin();
  }

  if (cache_it_ == cache_.end()) return false;

  if (context.db_accessor_.should_abort()) throw HintedAbortError();

  // place the output values on the frame
  DCHECK(self_.output_symbols_.size() == cache_it_->second.size())
      << "Number of values does not match the number of output symbols "
         "in OrderBy";
  auto output_sym_it = self_.output_symbols_.begin();
  for (const TypedValue &output : cache_it_->second)
    frame[*output_sym_it++] = output;

  cache_it_++;
  return true;
}

void OrderBy::OrderByCursor::Shutdown() { input_cursor_->Shutdown(); }

void OrderBy::OrderByCursor::Reset() {
  input_cursor_->Reset();
  did_pull_all_ = false;
  cache_.clear();
  cache_it_ = cache_.begin();
}

Merge::Merge(const std::shared_ptr<LogicalOperator> &input,
             const std::shared_ptr<LogicalOperator> &merge_match,
             const std::shared_ptr<LogicalOperator> &merge_create)
    : input_(input ? input : std::make_shared<Once>()),
      merge_match_(merge_match),
      merge_create_(merge_create) {}

bool Merge::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    input_->Accept(visitor) && merge_match_->Accept(visitor) &&
        merge_create_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

std::unique_ptr<Cursor> Merge::MakeCursor(database::GraphDbAccessor &db) const {
  return std::make_unique<MergeCursor>(*this, db);
}

std::vector<Symbol> Merge::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  // Match and create branches should have the same symbols, so just take one
  // of them.
  auto my_symbols = merge_match_->OutputSymbols(table);
  symbols.insert(symbols.end(), my_symbols.begin(), my_symbols.end());
  return symbols;
}

Merge::MergeCursor::MergeCursor(const Merge &self,
                                database::GraphDbAccessor &db)
    : input_cursor_(self.input_->MakeCursor(db)),
      merge_match_cursor_(self.merge_match_->MakeCursor(db)),
      merge_create_cursor_(self.merge_create_->MakeCursor(db)) {}

bool Merge::MergeCursor::Pull(Frame &frame, Context &context) {
  while (true) {
    if (pull_input_) {
      if (input_cursor_->Pull(frame, context)) {
        // after a successful input from the input
        // reset merge_match (it's expand iterators maintain state)
        // and merge_create (could have a Once at the beginning)
        merge_match_cursor_->Reset();
        merge_create_cursor_->Reset();
      } else
        // input is exhausted, we're done
        return false;
    }

    // pull from the merge_match cursor
    if (merge_match_cursor_->Pull(frame, context)) {
      // if successful, next Pull from this should not pull_input_
      pull_input_ = false;
      return true;
    } else {
      // failed to Pull from the merge_match cursor
      if (pull_input_) {
        // if we have just now pulled from the input
        // and failed to pull from merge_match, we should create
        __attribute__((unused)) bool merge_create_pull_result =
            merge_create_cursor_->Pull(frame, context);
        DCHECK(merge_create_pull_result) << "MergeCreate must never fail";
        return true;
      }
      // We have exhausted merge_match_cursor_ after 1 or more successful
      // Pulls. Attempt next input_cursor_ pull
      pull_input_ = true;
      continue;
    }
  }
}

void Merge::MergeCursor::Shutdown() {
  input_cursor_->Shutdown();
  merge_match_cursor_->Shutdown();
  merge_create_cursor_->Shutdown();
}

void Merge::MergeCursor::Reset() {
  input_cursor_->Reset();
  merge_match_cursor_->Reset();
  merge_create_cursor_->Reset();
  pull_input_ = true;
}

Optional::Optional(const std::shared_ptr<LogicalOperator> &input,
                   const std::shared_ptr<LogicalOperator> &optional,
                   const std::vector<Symbol> &optional_symbols)
    : input_(input ? input : std::make_shared<Once>()),
      optional_(optional),
      optional_symbols_(optional_symbols) {}

bool Optional::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    input_->Accept(visitor) && optional_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

std::unique_ptr<Cursor> Optional::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<OptionalCursor>(*this, db);
}

std::vector<Symbol> Optional::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  auto my_symbols = optional_->ModifiedSymbols(table);
  symbols.insert(symbols.end(), my_symbols.begin(), my_symbols.end());
  return symbols;
}

Optional::OptionalCursor::OptionalCursor(const Optional &self,
                                         database::GraphDbAccessor &db)
    : self_(self),
      input_cursor_(self.input_->MakeCursor(db)),
      optional_cursor_(self.optional_->MakeCursor(db)) {}

bool Optional::OptionalCursor::Pull(Frame &frame, Context &context) {
  while (true) {
    if (pull_input_) {
      if (input_cursor_->Pull(frame, context)) {
        // after a successful input from the input
        // reset optional_ (it's expand iterators maintain state)
        optional_cursor_->Reset();
      } else
        // input is exhausted, we're done
        return false;
    }

    // pull from the optional_ cursor
    if (optional_cursor_->Pull(frame, context)) {
      // if successful, next Pull from this should not pull_input_
      pull_input_ = false;
      return true;
    } else {
      // failed to Pull from the merge_match cursor
      if (pull_input_) {
        // if we have just now pulled from the input
        // and failed to pull from optional_ so set the
        // optional symbols to Null, ensure next time the
        // input gets pulled and return true
        for (const Symbol &sym : self_.optional_symbols_)
          frame[sym] = TypedValue::Null;
        pull_input_ = true;
        return true;
      }
      // we have exhausted optional_cursor_ after 1 or more successful Pulls
      // attempt next input_cursor_ pull
      pull_input_ = true;
      continue;
    }
  }
}

void Optional::OptionalCursor::Shutdown() {
  input_cursor_->Shutdown();
  optional_cursor_->Shutdown();
}

void Optional::OptionalCursor::Reset() {
  input_cursor_->Reset();
  optional_cursor_->Reset();
  pull_input_ = true;
}

Unwind::Unwind(const std::shared_ptr<LogicalOperator> &input,
               Expression *input_expression, Symbol output_symbol)
    : input_(input ? input : std::make_shared<Once>()),
      input_expression_(input_expression),
      output_symbol_(output_symbol) {}

ACCEPT_WITH_INPUT(Unwind)

std::unique_ptr<Cursor> Unwind::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<UnwindCursor>(*this, db);
}

std::vector<Symbol> Unwind::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(output_symbol_);
  return symbols;
}

Unwind::UnwindCursor::UnwindCursor(const Unwind &self,
                                   database::GraphDbAccessor &db)
    : self_(self), db_(db), input_cursor_(self.input_->MakeCursor(db)) {}

bool Unwind::UnwindCursor::Pull(Frame &frame, Context &context) {
  while (true) {
    if (db_.should_abort()) throw HintedAbortError();
    // if we reached the end of our list of values
    // pull from the input
    if (input_value_it_ == input_value_.end()) {
      if (!input_cursor_->Pull(frame, context)) return false;

      // successful pull from input, initialize value and iterator
      ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                    context.evaluation_context_,
                                    &context.db_accessor_, GraphView::OLD);
      TypedValue input_value = self_.input_expression_->Accept(evaluator);
      if (input_value.type() != TypedValue::Type::List)
        throw QueryRuntimeException(
            "Argument of UNWIND must be a list, but '{}' was provided.",
            input_value.type());
      input_value_ = input_value.Value<std::vector<TypedValue>>();
      input_value_it_ = input_value_.begin();
    }

    // if we reached the end of our list of values goto back to top
    if (input_value_it_ == input_value_.end()) continue;

    frame[self_.output_symbol_] = *input_value_it_++;
    return true;
  }
}

void Unwind::UnwindCursor::Shutdown() { input_cursor_->Shutdown(); }

void Unwind::UnwindCursor::Reset() {
  input_cursor_->Reset();
  input_value_.clear();
  input_value_it_ = input_value_.end();
}

Distinct::Distinct(const std::shared_ptr<LogicalOperator> &input,
                   const std::vector<Symbol> &value_symbols)
    : input_(input ? input : std::make_shared<Once>()),
      value_symbols_(value_symbols) {}

ACCEPT_WITH_INPUT(Distinct)

std::unique_ptr<Cursor> Distinct::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<DistinctCursor>(*this, db);
}

std::vector<Symbol> Distinct::OutputSymbols(
    const SymbolTable &symbol_table) const {
  // Propagate this to potential Produce.
  return input_->OutputSymbols(symbol_table);
}

std::vector<Symbol> Distinct::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

Distinct::DistinctCursor::DistinctCursor(const Distinct &self,
                                         database::GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

bool Distinct::DistinctCursor::Pull(Frame &frame, Context &context) {
  while (true) {
    if (!input_cursor_->Pull(frame, context)) return false;

    std::vector<TypedValue> row;
    row.reserve(self_.value_symbols_.size());
    for (const auto &symbol : self_.value_symbols_)
      row.emplace_back(frame[symbol]);
    if (seen_rows_.insert(std::move(row)).second) return true;
  }
}

void Distinct::DistinctCursor::Shutdown() { input_cursor_->Shutdown(); }

void Distinct::DistinctCursor::Reset() {
  input_cursor_->Reset();
  seen_rows_.clear();
}

Union::Union(const std::shared_ptr<LogicalOperator> &left_op,
             const std::shared_ptr<LogicalOperator> &right_op,
             const std::vector<Symbol> &union_symbols,
             const std::vector<Symbol> &left_symbols,
             const std::vector<Symbol> &right_symbols)
    : left_op_(left_op),
      right_op_(right_op),
      union_symbols_(union_symbols),
      left_symbols_(left_symbols),
      right_symbols_(right_symbols) {}

std::unique_ptr<Cursor> Union::MakeCursor(database::GraphDbAccessor &db) const {
  return std::make_unique<Union::UnionCursor>(*this, db);
}

bool Union::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    if (left_op_->Accept(visitor)) {
      right_op_->Accept(visitor);
    }
  }
  return visitor.PostVisit(*this);
}

std::vector<Symbol> Union::OutputSymbols(const SymbolTable &) const {
  return union_symbols_;
}

std::vector<Symbol> Union::ModifiedSymbols(const SymbolTable &) const {
  return union_symbols_;
}

WITHOUT_SINGLE_INPUT(Union);

Union::UnionCursor::UnionCursor(const Union &self,
                                database::GraphDbAccessor &db)
    : self_(self),
      left_cursor_(self.left_op_->MakeCursor(db)),
      right_cursor_(self.right_op_->MakeCursor(db)) {}

bool Union::UnionCursor::Pull(Frame &frame, Context &context) {
  std::unordered_map<std::string, TypedValue> results;
  if (left_cursor_->Pull(frame, context)) {
    // collect values from the left child
    for (const auto &output_symbol : self_.left_symbols_) {
      results[output_symbol.name()] = frame[output_symbol];
    }
  } else if (right_cursor_->Pull(frame, context)) {
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

std::vector<Symbol> Cartesian::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = left_op_->ModifiedSymbols(table);
  auto right = right_op_->ModifiedSymbols(table);
  symbols.insert(symbols.end(), right.begin(), right.end());
  return symbols;
}

bool Cartesian::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    left_op_->Accept(visitor) && right_op_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

WITHOUT_SINGLE_INPUT(Cartesian);

namespace {

class CartesianCursor : public Cursor {
 public:
  CartesianCursor(const Cartesian &self, database::GraphDbAccessor &db)
      : self_(self),
        left_op_cursor_(self.left_op_->MakeCursor(db)),
        right_op_cursor_(self_.right_op_->MakeCursor(db)) {
    CHECK(left_op_cursor_ != nullptr)
        << "CartesianCursor: Missing left operator cursor.";
    CHECK(right_op_cursor_ != nullptr)
        << "CartesianCursor: Missing right operator cursor.";
  }

  bool Pull(Frame &frame, Context &context) override {
    if (!cartesian_pull_initialized_) {
      // Pull all left_op frames.
      while (left_op_cursor_->Pull(frame, context)) {
        left_op_frames_.emplace_back(frame.elems());
      }

      // We're setting the iterator to 'end' here so it pulls the right
      // cursor.
      left_op_frames_it_ = left_op_frames_.end();
      cartesian_pull_initialized_ = true;
    }

    // If left operator yielded zero results there is no cartesian product.
    if (left_op_frames_.empty()) {
      return false;
    }

    auto restore_frame = [&frame](const std::vector<Symbol> &symbols,
                                  const std::vector<TypedValue> &restore_from) {
      for (const auto &symbol : symbols) {
        frame[symbol] = restore_from[symbol.position()];
      }
    };

    if (left_op_frames_it_ == left_op_frames_.end()) {
      // Advance right_op_cursor_.
      if (!right_op_cursor_->Pull(frame, context)) return false;

      right_op_frame_ = frame.elems();
      left_op_frames_it_ = left_op_frames_.begin();
    } else {
      // Make sure right_op_cursor last pulled results are on frame.
      restore_frame(self_.right_symbols_, right_op_frame_);
    }

    if (context.db_accessor_.should_abort()) throw HintedAbortError();

    restore_frame(self_.left_symbols_, *left_op_frames_it_);
    left_op_frames_it_++;
    return true;
  }

  void Shutdown() override {
    left_op_cursor_->Shutdown();
    right_op_cursor_->Shutdown();
  }

  void Reset() override {
    left_op_cursor_->Reset();
    right_op_cursor_->Reset();
    right_op_frame_.clear();
    left_op_frames_.clear();
    left_op_frames_it_ = left_op_frames_.end();
    cartesian_pull_initialized_ = false;
  }

 private:
  const Cartesian &self_;
  std::vector<std::vector<TypedValue>> left_op_frames_;
  std::vector<TypedValue> right_op_frame_;
  const std::unique_ptr<Cursor> left_op_cursor_;
  const std::unique_ptr<Cursor> right_op_cursor_;
  std::vector<std::vector<TypedValue>>::iterator left_op_frames_it_;
  bool cartesian_pull_initialized_{false};
};

}  // namespace

std::unique_ptr<Cursor> Cartesian::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<CartesianCursor>(*this, db);
}

OutputTable::OutputTable(std::vector<Symbol> output_symbols,
                         std::vector<std::vector<TypedValue>> rows)
    : output_symbols_(std::move(output_symbols)),
      callback_([rows]() { return rows; }) {}

OutputTable::OutputTable(
    std::vector<Symbol> output_symbols,
    std::function<std::vector<std::vector<TypedValue>>()> callback)
    : output_symbols_(std::move(output_symbols)),
      callback_(std::move(callback)) {}

WITHOUT_SINGLE_INPUT(OutputTable);

class OutputTableCursor : public Cursor {
 public:
  OutputTableCursor(const OutputTable &self) : self_(self) {}

  bool Pull(Frame &frame, Context &context) override {
    if (!pulled_) {
      rows_ = self_.callback_();
      for (const auto &row : rows_) {
        CHECK(row.size() == self_.output_symbols_.size())
            << "Wrong number of columns in row!";
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

std::unique_ptr<Cursor> OutputTable::MakeCursor(
    database::GraphDbAccessor &dba) const {
  return std::make_unique<OutputTableCursor>(*this);
}

}  // namespace query::plan
