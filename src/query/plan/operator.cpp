#include <algorithm>

#include "query/plan/operator.hpp"

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/eval.hpp"

namespace query {
namespace plan {

CreateNode::CreateNode(const NodeAtom *node_atom,
                       const std::shared_ptr<LogicalOperator> &input)
    : node_atom_(node_atom), input_(input) {}

void CreateNode::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    if (input_) input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> CreateNode::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<CreateNodeCursor>(*this, db);
}

CreateNode::CreateNodeCursor::CreateNodeCursor(const CreateNode &self,
                                               GraphDbAccessor &db)
    : self_(self),
      db_(db),
      input_cursor_(self.input_ ? self.input_->MakeCursor(db) : nullptr) {}

bool CreateNode::CreateNodeCursor::Pull(Frame &frame,
                                        const SymbolTable &symbol_table) {
  if (input_cursor_) {
    if (input_cursor_->Pull(frame, symbol_table)) {
      Create(frame, symbol_table);
      return true;
    } else
      return false;
  } else if (!did_create_) {
    Create(frame, symbol_table);
    did_create_ = true;
    return true;
  } else
    return false;
}

void CreateNode::CreateNodeCursor::Create(Frame &frame,
                                          const SymbolTable &symbol_table) {
  auto new_node = db_.insert_vertex();
  for (auto label : self_.node_atom_->labels_) new_node.add_label(label);

  ExpressionEvaluator evaluator(frame, symbol_table);
  // Evaluator should use the latest accessors, as modified in this query, when
  // setting properties on new nodes.
  evaluator.SwitchNew();
  for (auto &kv : self_.node_atom_->properties_) {
    kv.second->Accept(evaluator);
    new_node.PropsSet(kv.first, evaluator.PopBack());
  }
  frame[symbol_table.at(*self_.node_atom_->identifier_)] = new_node;
}

CreateExpand::CreateExpand(const NodeAtom *node_atom, const EdgeAtom *edge_atom,
                           const std::shared_ptr<LogicalOperator> &input,
                           Symbol input_symbol, bool node_existing)
    : node_atom_(node_atom),
      edge_atom_(edge_atom),
      input_(input),
      input_symbol_(input_symbol),
      node_existing_(node_existing) {}

void CreateExpand::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> CreateExpand::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<CreateExpandCursor>(*this, db);
}

CreateExpand::CreateExpandCursor::CreateExpandCursor(const CreateExpand &self,
                                                     GraphDbAccessor &db)
    : self_(self), db_(db), input_cursor_(self.input_->MakeCursor(db)) {}

bool CreateExpand::CreateExpandCursor::Pull(Frame &frame,
                                            const SymbolTable &symbol_table) {
  if (!input_cursor_->Pull(frame, symbol_table)) return false;

  // get the origin vertex
  TypedValue &vertex_value = frame[self_.input_symbol_];
  auto &v1 = vertex_value.Value<VertexAccessor>();

  ExpressionEvaluator evaluator(frame, symbol_table);
  // Similarly to CreateNode, newly created edges and nodes should use the
  // latest accesors.
  // E.g. we pickup new properties: `CREATE (n {p: 42}) -[:r {ep: n.p}]-> ()`
  v1.SwitchNew();
  evaluator.SwitchNew();

  // get the destination vertex (possibly an existing node)
  auto &v2 = OtherVertex(frame, symbol_table, evaluator);
  v2.SwitchNew();

  // create an edge between the two nodes
  switch (self_.edge_atom_->direction_) {
    case EdgeAtom::Direction::LEFT:
      CreateEdge(v2, v1, frame, symbol_table, evaluator);
      break;
    case EdgeAtom::Direction::RIGHT:
      CreateEdge(v1, v2, frame, symbol_table, evaluator);
      break;
    case EdgeAtom::Direction::BOTH:
      permanent_fail("Undefined direction not allowed in create");
  }

  return true;
}

VertexAccessor &CreateExpand::CreateExpandCursor::OtherVertex(
    Frame &frame, const SymbolTable &symbol_table,
    ExpressionEvaluator &evaluator) {
  if (self_.node_existing_) {
    TypedValue &dest_node_value =
        frame[symbol_table.at(*self_.node_atom_->identifier_)];
    return dest_node_value.Value<VertexAccessor>();
  } else {
    // the node does not exist, it needs to be created
    auto node = db_.insert_vertex();
    for (auto label : self_.node_atom_->labels_) node.add_label(label);
    for (auto kv : self_.node_atom_->properties_) {
      kv.second->Accept(evaluator);
      node.PropsSet(kv.first, evaluator.PopBack());
    }
    auto symbol = symbol_table.at(*self_.node_atom_->identifier_);
    frame[symbol] = node;
    return frame[symbol].Value<VertexAccessor>();
  }
}

void CreateExpand::CreateExpandCursor::CreateEdge(
    VertexAccessor &from, VertexAccessor &to, Frame &frame,
    const SymbolTable &symbol_table, ExpressionEvaluator &evaluator) {
  EdgeAccessor edge =
      db_.insert_edge(from, to, self_.edge_atom_->edge_types_[0]);
  for (auto kv : self_.edge_atom_->properties_) {
    kv.second->Accept(evaluator);
    edge.PropsSet(kv.first, evaluator.PopBack());
  }
  frame[symbol_table.at(*self_.edge_atom_->identifier_)] = edge;
}

ScanAll::ScanAll(const NodeAtom *node_atom)
    : node_atom_(node_atom), input_(nullptr) {}

ScanAll::ScanAll(const NodeAtom *node_atom,
                 const std::shared_ptr<LogicalOperator> &input)
    : node_atom_(node_atom), input_(input) {}

void ScanAll::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    if (input_) input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> ScanAll::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<ScanAllCursor>(*this, db);
}

ScanAll::ScanAllCursor::ScanAllCursor(const ScanAll &self, GraphDbAccessor &db)
    : self_(self),
      input_cursor_(self.input_ ? self.input_->MakeCursor(db) : nullptr),
      vertices_(db.vertices()),
      vertices_it_(vertices_.begin()) {}

bool ScanAll::ScanAllCursor::Pull(Frame &frame,
                                  const SymbolTable &symbol_table) {
  if (input_cursor_) {
    // using an input. we need to pull from it if we are in the first pull
    // of this cursor, or if we have exhausted vertices_it_
    if (first_pull_ || vertices_it_ == vertices_.end()) {
      first_pull_ = false;
      // if the input is empty, we are for sure done
      if (!input_cursor_->Pull(frame, symbol_table)) return false;
      vertices_it_ = vertices_.begin();
    }
  }

  // if we have no more vertices, we're done (if input_ is set we have
  // just tried to re-init vertices_it_, and if not we only iterate
  // through it once
  if (vertices_it_ == vertices_.end()) return false;

  frame[symbol_table.at(*self_.node_atom_->identifier_)] = *vertices_it_++;
  return true;
}

Expand::Expand(const NodeAtom *node_atom, const EdgeAtom *edge_atom,
               const std::shared_ptr<LogicalOperator> &input,
               Symbol input_symbol, bool node_cycle, bool edge_cycle)
    : node_atom_(node_atom),
      edge_atom_(edge_atom),
      input_(input),
      input_symbol_(input_symbol),
      node_cycle_(node_cycle),
      edge_cycle_(edge_cycle) {}

void Expand::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> Expand::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<ExpandCursor>(*this, db);
}

Expand::ExpandCursor::ExpandCursor(const Expand &self, GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

bool Expand::ExpandCursor::Pull(Frame &frame, const SymbolTable &symbol_table) {
  while (true) {
    // attempt to get a value from the incoming edges
    if (in_edges_ && *in_edges_it_ != in_edges_->end()) {
      EdgeAccessor edge = *(*in_edges_it_)++;
      if (HandleEdgeCycle(edge, frame, symbol_table) &&
          PullNode(edge, EdgeAtom::Direction::LEFT, frame, symbol_table))
        return true;
      else
        continue;
    }

    // attempt to get a value from the outgoing edges
    if (out_edges_ && *out_edges_it_ != out_edges_->end()) {
      EdgeAccessor edge = *(*out_edges_it_)++;
      // when expanding in EdgeAtom::Direction::BOTH directions
      // we should do only one expansion for cycles, and it was
      // already done in the block above
      if (self_.edge_atom_->direction_ == EdgeAtom::Direction::BOTH &&
          edge.is_cycle())
        continue;
      if (HandleEdgeCycle(edge, frame, symbol_table) &&
          PullNode(edge, EdgeAtom::Direction::RIGHT, frame, symbol_table))
        return true;
      else
        continue;
    }

    // if we are here, either the edges have not been initialized,
    // or they have been exhausted. attempt to initialize the edges,
    // if the input is exhausted
    if (!InitEdges(frame, symbol_table)) return false;

    // we have re-initialized the edges, continue with the loop
  }
}

bool Expand::ExpandCursor::InitEdges(Frame &frame,
                                     const SymbolTable &symbol_table) {
  if (!input_cursor_->Pull(frame, symbol_table)) return false;

  TypedValue &vertex_value = frame[self_.input_symbol_];
  auto &vertex = vertex_value.Value<VertexAccessor>();
  // We don't want newly created edges, so switch to old. If we included new
  // edges, e.g. created by CreateExpand operator, the behaviour would be wrong
  // and may cause infinite loops by continually creating edges and traversing
  // them.
  vertex.SwitchOld();

  auto direction = self_.edge_atom_->direction_;
  if (direction == EdgeAtom::Direction::LEFT ||
      direction == EdgeAtom::Direction::BOTH) {
    in_edges_ = std::make_unique<InEdgeT>(vertex.in());
    in_edges_it_ = std::make_unique<InEdgeIteratorT>(in_edges_->begin());
  }

  if (direction == EdgeAtom::Direction::RIGHT ||
      direction == EdgeAtom::Direction::BOTH) {
    out_edges_ = std::make_unique<InEdgeT>(vertex.out());
    out_edges_it_ = std::make_unique<InEdgeIteratorT>(out_edges_->begin());
  }

  // TODO add support for Front and Back expansion (when QueryPlanner
  // will need it). For now only Back expansion (left to right) is
  // supported
  // TODO add support for named paths

  return true;
}

bool Expand::ExpandCursor::HandleEdgeCycle(const EdgeAccessor &new_edge,
                                           Frame &frame,
                                           const SymbolTable &symbol_table) {
  if (self_.edge_cycle_) {
    TypedValue &old_edge_value =
        frame[symbol_table.at(*self_.edge_atom_->identifier_)];
    return old_edge_value.Value<EdgeAccessor>() == new_edge;
  } else {
    // not doing a cycle, so put the new_edge into the frame and return true
    frame[symbol_table.at(*self_.edge_atom_->identifier_)] = new_edge;
    return true;
  }
}

bool Expand::ExpandCursor::PullNode(const EdgeAccessor &new_edge,
                                    EdgeAtom::Direction direction, Frame &frame,
                                    const SymbolTable &symbol_table) {
  switch (direction) {
    case EdgeAtom::Direction::LEFT:
      return HandleNodeCycle(new_edge.from(), frame, symbol_table);
    case EdgeAtom::Direction::RIGHT:
      return HandleNodeCycle(new_edge.to(), frame, symbol_table);
    case EdgeAtom::Direction::BOTH:
      permanent_fail("Must indicate exact expansion direction here");
  }
}

bool Expand::ExpandCursor::HandleNodeCycle(const VertexAccessor new_node,
                                           Frame &frame,
                                           const SymbolTable &symbol_table) {
  if (self_.node_cycle_) {
    TypedValue &old_node_value =
        frame[symbol_table.at(*self_.node_atom_->identifier_)];
    return old_node_value.Value<VertexAccessor>() == new_node;
  } else {
    // not doing a cycle, so put the new_edge into the frame and return true
    frame[symbol_table.at(*self_.node_atom_->identifier_)] = new_node;
    return true;
  }
}

NodeFilter::NodeFilter(const std::shared_ptr<LogicalOperator> &input,
                       Symbol input_symbol, const NodeAtom *node_atom)
    : input_(input), input_symbol_(input_symbol), node_atom_(node_atom) {}

void NodeFilter::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> NodeFilter::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<NodeFilterCursor>(*this, db);
}

NodeFilter::NodeFilterCursor::NodeFilterCursor(const NodeFilter &self,
                                               GraphDbAccessor &db)
    : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

bool NodeFilter::NodeFilterCursor::Pull(Frame &frame,
                                        const SymbolTable &symbol_table) {
  while (input_cursor_->Pull(frame, symbol_table)) {
    auto &vertex = frame[self_.input_symbol_].Value<VertexAccessor>();
    // Filter needs to use the old, unmodified vertex, even though we may change
    // properties or labels during the current command.
    vertex.SwitchOld();
    if (VertexPasses(vertex, frame, symbol_table)) return true;
  }
  return false;
}

bool NodeFilter::NodeFilterCursor::VertexPasses(
    const VertexAccessor &vertex, Frame &frame,
    const SymbolTable &symbol_table) {
  for (auto label : self_.node_atom_->labels_)
    if (!vertex.has_label(label)) return false;

  ExpressionEvaluator expression_evaluator(frame, symbol_table);
  // We don't want newly set properties to affect filtering.
  expression_evaluator.SwitchOld();
  for (auto prop_pair : self_.node_atom_->properties_) {
    prop_pair.second->Accept(expression_evaluator);
    TypedValue comparison_result =
        vertex.PropsAt(prop_pair.first) == expression_evaluator.PopBack();
    if (comparison_result.type() == TypedValue::Type::Null ||
        !comparison_result.Value<bool>())
      return false;
  }
  return true;
}

EdgeFilter::EdgeFilter(const std::shared_ptr<LogicalOperator> &input,
                       Symbol input_symbol, const EdgeAtom *edge_atom)
    : input_(input), input_symbol_(input_symbol), edge_atom_(edge_atom) {}

void EdgeFilter::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> EdgeFilter::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<EdgeFilterCursor>(*this, db);
}
EdgeFilter::EdgeFilterCursor::EdgeFilterCursor(const EdgeFilter &self,
                                               GraphDbAccessor &db)
    : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

bool EdgeFilter::EdgeFilterCursor::Pull(Frame &frame,
                                        const SymbolTable &symbol_table) {
  while (input_cursor_->Pull(frame, symbol_table)) {
    auto &edge = frame[self_.input_symbol_].Value<EdgeAccessor>();
    // Filter needs to use the old, unmodified edge, even though we may change
    // properties or types during the current command.
    edge.SwitchOld();
    if (EdgePasses(edge, frame, symbol_table)) return true;
  }
  return false;
}

bool EdgeFilter::EdgeFilterCursor::EdgePasses(const EdgeAccessor &edge,
                                              Frame &frame,
                                              const SymbolTable &symbol_table) {
  // edge type filtering - logical OR
  const auto &types = self_.edge_atom_->edge_types_;
  GraphDbTypes::EdgeType type = edge.edge_type();
  if (types.size() && std::none_of(types.begin(), types.end(),
                                   [type](auto t) { return t == type; }))
    return false;

  ExpressionEvaluator expression_evaluator(frame, symbol_table);
  // We don't want newly set properties to affect filtering.
  expression_evaluator.SwitchOld();
  for (auto prop_pair : self_.edge_atom_->properties_) {
    prop_pair.second->Accept(expression_evaluator);
    TypedValue comparison_result =
        edge.PropsAt(prop_pair.first) == expression_evaluator.PopBack();
    if (comparison_result.type() == TypedValue::Type::Null ||
        !comparison_result.Value<bool>())
      return false;
  }
  return true;
}

Filter::Filter(const std::shared_ptr<LogicalOperator> &input_,
               Expression *expression_)
    : input_(input_), expression_(expression_) {}

void Filter::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> Filter::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<FilterCursor>(*this, db);
}

Filter::FilterCursor::FilterCursor(const Filter &self, GraphDbAccessor &db)
    : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

bool Filter::FilterCursor::Pull(Frame &frame, const SymbolTable &symbol_table) {
  ExpressionEvaluator evaluator(frame, symbol_table);
  // Like all filters, newly set values should not affect filtering of old nodes
  // and edges.
  evaluator.SwitchOld();
  while (input_cursor_->Pull(frame, symbol_table)) {
    self_.expression_->Accept(evaluator);
    TypedValue result = evaluator.PopBack();
    if (result.type() == TypedValue::Type::Null || !result.Value<bool>())
      continue;
    return true;
  }
  return false;
}

Produce::Produce(const std::shared_ptr<LogicalOperator> &input,
                 const std::vector<NamedExpression *> named_expressions)
    : input_(input), named_expressions_(named_expressions) {}

void Produce::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    if (input_) input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> Produce::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<ProduceCursor>(*this, db);
}

const std::vector<NamedExpression *> &Produce::named_expressions() {
  return named_expressions_;
}

Produce::ProduceCursor::ProduceCursor(const Produce &self, GraphDbAccessor &db)
    : self_(self),
      input_cursor_(self.input_ ? self_.input_->MakeCursor(db) : nullptr) {}
bool Produce::ProduceCursor::Pull(Frame &frame,
                                  const SymbolTable &symbol_table) {
  ExpressionEvaluator evaluator(frame, symbol_table);
  // Produce should always yield the latest results.
  evaluator.SwitchNew();
  if (input_cursor_) {
    if (input_cursor_->Pull(frame, symbol_table)) {
      for (auto named_expr : self_.named_expressions_)
        named_expr->Accept(evaluator);
      return true;
    }
    return false;
  } else if (!did_produce_) {
    for (auto named_expr : self_.named_expressions_)
      named_expr->Accept(evaluator);
    did_produce_ = true;
    return true;
  } else
    return false;
}

Delete::Delete(const std::shared_ptr<LogicalOperator> &input_,
               const std::vector<Expression *> &expressions, bool detach_)
    : input_(input_), expressions_(expressions), detach_(detach_) {}

void Delete::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> Delete::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<DeleteCursor>(*this, db);
}

Delete::DeleteCursor::DeleteCursor(const Delete &self, GraphDbAccessor &db)
    : self_(self), db_(db), input_cursor_(self_.input_->MakeCursor(db)) {}

bool Delete::DeleteCursor::Pull(Frame &frame, const SymbolTable &symbol_table) {
  if (!input_cursor_->Pull(frame, symbol_table)) return false;

  ExpressionEvaluator evaluator(frame, symbol_table);
  // Delete should get the latest information, this way it is also possible to
  // delete newly added nodes and edges.
  evaluator.SwitchNew();
  // collect expressions results so edges can get deleted before vertices
  // this is necessary because an edge that gets deleted could block vertex
  // deletion
  std::vector<TypedValue> expression_results;
  expression_results.reserve(self_.expressions_.size());
  for (Expression *expression : self_.expressions_) {
    expression->Accept(evaluator);
    expression_results.emplace_back(evaluator.PopBack());
  }

  // delete edges first
  for (TypedValue &expression_result : expression_results)
    if (expression_result.type() == TypedValue::Type::Edge)
      db_.remove_edge(expression_result.Value<EdgeAccessor>());

  // delete vertices
  for (TypedValue &expression_result : expression_results)
    switch (expression_result.type()) {
      case TypedValue::Type::Vertex: {
        VertexAccessor &va = expression_result.Value<VertexAccessor>();
        va.SwitchNew();  //  necessary because an edge deletion could have
                         //  updated
        if (self_.detach_)
          db_.detach_remove_vertex(va);
        else if (!db_.remove_vertex(va))
          throw query::QueryRuntimeException(
              "Failed to remove vertex because of it's existing "
              "connections. Consider using DETACH DELETE.");
        break;
      }
      case TypedValue::Type::Edge:
        break;
      // check we're not trying to delete anything except vertices and edges
      default:
        throw TypedValueException("Can only delete edges and vertices");
    }

  return true;
}

SetProperty::SetProperty(const std::shared_ptr<LogicalOperator> &input,
                         PropertyLookup *lhs, Expression *rhs)
    : input_(input), lhs_(lhs), rhs_(rhs) {}

void SetProperty::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> SetProperty::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<SetPropertyCursor>(*this, db);
}

SetProperty::SetPropertyCursor::SetPropertyCursor(const SetProperty &self,
                                                  GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

bool SetProperty::SetPropertyCursor::Pull(Frame &frame,
                                          const SymbolTable &symbol_table) {
  if (!input_cursor_->Pull(frame, symbol_table)) return false;

  ExpressionEvaluator evaluator(frame, symbol_table);
  // Set, just like Create needs to see the latest changes.
  evaluator.SwitchNew();
  self_.lhs_->expression_->Accept(evaluator);
  TypedValue lhs = evaluator.PopBack();
  self_.rhs_->Accept(evaluator);
  TypedValue rhs = evaluator.PopBack();

  // TODO the following code uses implicit TypedValue to PropertyValue
  // conversion which throws a TypedValueException if impossible
  // this is correct, but the end user will get a not-very-informative
  // error message, so address this when improving error feedback

  switch (lhs.type()) {
    case TypedValue::Type::Vertex:
      lhs.Value<VertexAccessor>().PropsSet(self_.lhs_->property_, rhs);
      break;
    case TypedValue::Type::Edge:
      lhs.Value<EdgeAccessor>().PropsSet(self_.lhs_->property_, rhs);
      break;
    default:
      throw QueryRuntimeException(
          "Properties can only be set on Vertices and Edges");
  }
  return true;
}

SetProperties::SetProperties(const std::shared_ptr<LogicalOperator> &input,
                             Symbol input_symbol, Expression *rhs, Op op)
    : input_(input), input_symbol_(input_symbol), rhs_(rhs), op_(op) {}

void SetProperties::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> SetProperties::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<SetPropertiesCursor>(*this, db);
}

SetProperties::SetPropertiesCursor::SetPropertiesCursor(
    const SetProperties &self, GraphDbAccessor &db)
    : self_(self), db_(db), input_cursor_(self.input_->MakeCursor(db)) {}

bool SetProperties::SetPropertiesCursor::Pull(Frame &frame,
                                              const SymbolTable &symbol_table) {
  if (!input_cursor_->Pull(frame, symbol_table)) return false;

  TypedValue &lhs = frame[self_.input_symbol_];

  ExpressionEvaluator evaluator(frame, symbol_table);
  // Set, just like Create needs to see the latest changes.
  evaluator.SwitchNew();
  self_.rhs_->Accept(evaluator);
  TypedValue rhs = evaluator.PopBack();

  switch (lhs.type()) {
    case TypedValue::Type::Vertex:
      Set(lhs.Value<VertexAccessor>(), rhs);
      break;
    case TypedValue::Type::Edge:
      Set(lhs.Value<EdgeAccessor>(), rhs);
      break;
    default:
      throw QueryRuntimeException(
          "Properties can only be set on Vertices and Edges");
  }
  return true;
}

template <typename TRecordAccessor>
void SetProperties::SetPropertiesCursor::Set(TRecordAccessor &record,
                                             const TypedValue &rhs) {
  record.SwitchNew();
  if (self_.op_ == Op::REPLACE) record.PropsClear();

  auto set_props = [&record](const auto &properties) {
    for (const auto &kv : properties) record.PropsSet(kv.first, kv.second);
  };

  switch (rhs.type()) {
    case TypedValue::Type::Edge:
      set_props(rhs.Value<EdgeAccessor>().Properties());
      break;
    case TypedValue::Type::Vertex:
      set_props(rhs.Value<VertexAccessor>().Properties());
      break;
    case TypedValue::Type::Map: {
      // TODO the following code uses implicit TypedValue to PropertyValue
      // conversion which throws a TypedValueException if impossible
      // this is correct, but the end user will get a not-very-informative
      // error message, so address this when improving error feedback
      for (const auto &kv : rhs.Value<std::map<std::string, TypedValue>>())
        record.PropsSet(db_.property(kv.first), kv.second);
      break;
    }
    default:
      throw QueryRuntimeException(
          "Can only set Vertices, Edges and maps as properties");
  }
}

// instantiate the SetProperties function with concrete TRecordAccessor types
template void SetProperties::SetPropertiesCursor::Set(
    RecordAccessor<Vertex> &record, const TypedValue &rhs);
template void SetProperties::SetPropertiesCursor::Set(
    RecordAccessor<Edge> &record, const TypedValue &rhs);

SetLabels::SetLabels(const std::shared_ptr<LogicalOperator> &input,
                     Symbol input_symbol,
                     const std::vector<GraphDbTypes::Label> &labels)
    : input_(input), input_symbol_(input_symbol), labels_(labels) {}

void SetLabels::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> SetLabels::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<SetLabelsCursor>(*this, db);
}

SetLabels::SetLabelsCursor::SetLabelsCursor(const SetLabels &self,
                                            GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

bool SetLabels::SetLabelsCursor::Pull(Frame &frame,
                                      const SymbolTable &symbol_table) {
  if (!input_cursor_->Pull(frame, symbol_table)) return false;

  TypedValue &vertex_value = frame[self_.input_symbol_];
  auto &vertex = vertex_value.Value<VertexAccessor>();
  vertex.SwitchNew();
  for (auto label : self_.labels_) vertex.add_label(label);

  return true;
}

RemoveProperty::RemoveProperty(const std::shared_ptr<LogicalOperator> &input,
                               PropertyLookup *lhs)
    : input_(input), lhs_(lhs) {}

void RemoveProperty::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> RemoveProperty::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<RemovePropertyCursor>(*this, db);
}

RemoveProperty::RemovePropertyCursor::RemovePropertyCursor(
    const RemoveProperty &self, GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

bool RemoveProperty::RemovePropertyCursor::Pull(
    Frame &frame, const SymbolTable &symbol_table) {
  if (!input_cursor_->Pull(frame, symbol_table)) return false;

  ExpressionEvaluator evaluator(frame, symbol_table);
  // Remove, just like Delete needs to see the latest changes.
  evaluator.SwitchNew();
  self_.lhs_->expression_->Accept(evaluator);
  TypedValue lhs = evaluator.PopBack();

  switch (lhs.type()) {
    case TypedValue::Type::Vertex:
      lhs.Value<VertexAccessor>().PropsErase(self_.lhs_->property_);
      break;
    case TypedValue::Type::Edge:
      lhs.Value<EdgeAccessor>().PropsErase(self_.lhs_->property_);
      break;
    default:
      // TODO consider throwing a TypedValueException here
      // deal with this when we'll be overhauling error-feedback
      throw QueryRuntimeException(
          "Properties can only be removed on Vertices and Edges");
  }
  return true;
}

RemoveLabels::RemoveLabels(const std::shared_ptr<LogicalOperator> &input,
                           Symbol input_symbol,
                           const std::vector<GraphDbTypes::Label> &labels)
    : input_(input), input_symbol_(input_symbol), labels_(labels) {}

void RemoveLabels::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> RemoveLabels::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<RemoveLabelsCursor>(*this, db);
}

RemoveLabels::RemoveLabelsCursor::RemoveLabelsCursor(const RemoveLabels &self,
                                                     GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

bool RemoveLabels::RemoveLabelsCursor::Pull(Frame &frame,
                                            const SymbolTable &symbol_table) {
  if (!input_cursor_->Pull(frame, symbol_table)) return false;

  TypedValue &vertex_value = frame[self_.input_symbol_];
  auto &vertex = vertex_value.Value<VertexAccessor>();
  vertex.SwitchNew();
  for (auto label : self_.labels_) vertex.remove_label(label);

  return true;
}

template <typename TAccessor>
ExpandUniquenessFilter<TAccessor>::ExpandUniquenessFilter(
    const std::shared_ptr<LogicalOperator> &input, Symbol expand_symbol,
    const std::vector<Symbol> &previous_symbols)
    : input_(input),
      expand_symbol_(expand_symbol),
      previous_symbols_(previous_symbols) {}

template <typename TAccessor>
void ExpandUniquenessFilter<TAccessor>::Accept(
    LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

template <typename TAccessor>
std::unique_ptr<Cursor> ExpandUniquenessFilter<TAccessor>::MakeCursor(
    GraphDbAccessor &db) {
  return std::make_unique<ExpandUniquenessFilterCursor>(*this, db);
}

template <typename TAccessor>
ExpandUniquenessFilter<TAccessor>::ExpandUniquenessFilterCursor::
    ExpandUniquenessFilterCursor(const ExpandUniquenessFilter &self,
                                 GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

template <typename TAccessor>
bool ExpandUniquenessFilter<TAccessor>::ExpandUniquenessFilterCursor::Pull(
    Frame &frame, const SymbolTable &symbol_table) {
  auto expansion_ok = [&]() {
    TypedValue &expand_value = frame[self_.expand_symbol_];
    TAccessor &expand_accessor = expand_value.Value<TAccessor>();
    for (const auto &previous_symbol : self_.previous_symbols_) {
      TypedValue &previous_value = frame[previous_symbol];
      TAccessor &previous_accessor = previous_value.Value<TAccessor>();
      if (expand_accessor == previous_accessor) return false;
    }
    return true;
  };

  while (input_cursor_->Pull(frame, symbol_table))
    if (expansion_ok()) return true;
  return false;
}

// instantiations of the ExpandUniquenessFilter template class
// we only ever need these two
template class ExpandUniquenessFilter<VertexAccessor>;
template class ExpandUniquenessFilter<EdgeAccessor>;

namespace {

/**
 * Helper function for recursively reconstructing all the accessors in the
 * given TypedValue.
 */
void ReconstructTypedValue(TypedValue &value) {
  switch (value.type()) {
    case TypedValue::Type::Vertex:
      if (!value.Value<VertexAccessor>().Reconstruct())
        throw QueryRuntimeException(
            "Vertex invalid after WITH clause, (most likely deleted by a "
            "preceeding DELETE clause)");
      break;
    case TypedValue::Type::Edge:
      if (!value.Value<VertexAccessor>().Reconstruct())
        throw QueryRuntimeException(
            "Edge invalid after WITH clause, (most likely deleted by a "
            "preceeding DELETE clause)");
      break;
    case TypedValue::Type::List:
      for (TypedValue &inner_value : value.Value<std::vector<TypedValue>>())
        ReconstructTypedValue(inner_value);
      break;
    case TypedValue::Type::Map:
      for (auto &kv : value.Value<std::map<std::string, TypedValue>>())
        ReconstructTypedValue(kv.second);
      break;
    case TypedValue::Type::Path:
      // TODO implement path reconstruct?
      throw NotYetImplemented("Path reconstruction not yet supported");
    default:
      break;
  }
}
}

Accumulate::Accumulate(const std::shared_ptr<LogicalOperator> &input,
                       const std::vector<Symbol> &symbols, bool advance_command)
    : input_(input), symbols_(symbols), advance_command_(advance_command) {}

void Accumulate::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}
std::unique_ptr<Cursor> Accumulate::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<Accumulate::AccumulateCursor>(*this, db);
}

Accumulate::AccumulateCursor::AccumulateCursor(const Accumulate &self,
                                               GraphDbAccessor &db)
    : self_(self), db_(db), input_cursor_(self.input_->MakeCursor(db)) {}

bool Accumulate::AccumulateCursor::Pull(Frame &frame,
                                        const SymbolTable &symbol_table) {
  // cache all the input
  if (!pulled_all_input_) {
    while (input_cursor_->Pull(frame, symbol_table)) {
      cache_.emplace_back();
      auto &row = cache_.back();
      for (const Symbol &symbol : self_.symbols_)
        row.emplace_back(frame[symbol]);
    }
    pulled_all_input_ = true;
    cache_it_ = cache_.begin();

    if (self_.advance_command_) {
      db_.advance_command();
      for (auto &row : cache_)
        for (auto &col : row) ReconstructTypedValue(col);
    }
  }

  if (cache_it_ == cache_.end()) return false;
  auto row_it = (cache_it_++)->begin();
  for (const Symbol &symbol : self_.symbols_) frame[symbol] = *row_it++;
  return true;
}

Aggregate::Aggregate(const std::shared_ptr<LogicalOperator> &input,
                     const std::vector<Aggregate::Element> &aggregations,
                     const std::vector<Expression *> &group_by,
                     const std::vector<Symbol> &remember, bool advance_command)
    : input_(input),
      aggregations_(aggregations),
      group_by_(group_by),
      remember_(remember),
      advance_command_(advance_command) {}

void Aggregate::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> Aggregate::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<AggregateCursor>(*this, db);
}

Aggregate::AggregateCursor::AggregateCursor(Aggregate &self,
                                            GraphDbAccessor &db)
    : self_(self),
      db_(db),
      input_cursor_(self.input_ ? self_.input_->MakeCursor(db) : nullptr) {}

bool Aggregate::AggregateCursor::Pull(Frame &frame,
                                      const SymbolTable &symbol_table) {
  if (!pulled_all_input_) {
    ProcessAll(frame, symbol_table);

    pulled_all_input_ = true;
    aggregation_it_ = aggregation_.begin();

    if (self_.advance_command_) {
      db_.advance_command();
      // regarding reconstruction after advance_command
      // we have to reconstruct only the remember values
      // because aggregation results are primitives and
      // group-by elements won't be used directly (possibly re-evaluated
      // using remember values)
      for (auto &kv : aggregation_)
        for (TypedValue &remember : kv.second.remember_)
          ReconstructTypedValue(remember);
    }
  }

  if (aggregation_it_ == aggregation_.end()) return false;

  // place aggregation values on the frame
  auto aggregation_values_it = aggregation_it_->second.values_.begin();
  for (const auto &aggregation_elem : self_.aggregations_)
    frame[std::get<2>(aggregation_elem)] = *aggregation_values_it++;

  // place remember values on the frame
  auto remember_values_it = aggregation_it_->second.remember_.begin();
  for (const Symbol &remember_sym : self_.remember_)
    frame[remember_sym] = *remember_values_it++;

  aggregation_it_++;
  return true;
}

void Aggregate::AggregateCursor::ProcessAll(Frame &frame,
                                            const SymbolTable &symbol_table) {
  ExpressionEvaluator evaluator(frame, symbol_table);
  evaluator.SwitchNew();
  if (input_cursor_)
    while (input_cursor_->Pull(frame, symbol_table))
      ProcessOne(frame, symbol_table, evaluator);
  else
    ProcessOne(frame, symbol_table, evaluator);

  // calculate AVG aggregations (so far they have only been summed)
  for (int pos = 0; pos < self_.aggregations_.size(); ++pos) {
    if (std::get<1>(self_.aggregations_[pos]) != Aggregation::Op::AVG) continue;
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
  // create the group-by list of values
  std::list<TypedValue> group_by;
  for (Expression *expression : self_.group_by_) {
    expression->Accept(evaluator);
    group_by.emplace_back(evaluator.PopBack());
  }

  AggregationValue &agg_value = aggregation_[group_by];
  EnsureInitialized(frame, agg_value);
  Update(frame, symbol_table, evaluator, agg_value);
}

void Aggregate::AggregateCursor::EnsureInitialized(
    Frame &frame, Aggregate::AggregateCursor::AggregationValue &agg_value) {
  if (agg_value.values_.size() > 0) return;

  for (const auto &agg_elem : self_.aggregations_) {
    if (std::get<1>(agg_elem) == Aggregation::Op::COUNT)
      agg_value.values_.emplace_back(TypedValue(0));
    else
      agg_value.values_.emplace_back(TypedValue::Null);
  }
  agg_value.counts_.resize(self_.aggregations_.size(), 0);

  for (const Symbol &remember_sym : self_.remember_)
    agg_value.remember_.push_back(frame[remember_sym]);
}

void Aggregate::AggregateCursor::Update(
    Frame &frame, const SymbolTable &symbol_table,
    ExpressionEvaluator &evaluator,
    Aggregate::AggregateCursor::AggregationValue &agg_value) {
  debug_assert(self_.aggregations_.size() == agg_value.values_.size(),
               "Inappropriate AggregationValue.values_ size");
  debug_assert(self_.aggregations_.size() == agg_value.counts_.size(),
               "Inappropriate AggregationValue.counts_ size");

  // we iterate over counts, values and aggregation info at the same time
  auto count_it = agg_value.counts_.begin();
  auto value_it = agg_value.values_.begin();
  auto agg_elem_it = self_.aggregations_.begin();
  for (; count_it < agg_value.counts_.end();
       count_it++, value_it++, agg_elem_it++) {
    std::get<0>(*agg_elem_it)->Accept(evaluator);
    TypedValue input_value = evaluator.PopBack();

    // Aggregations skip Null input values.
    if (input_value.type() == TypedValue::Type::Null) continue;

    const auto &agg_op = std::get<1>(*agg_elem_it);
    *count_it += 1;
    if (*count_it == 1) {
      // first value, nothing to aggregate. check type, set and continue.
      switch (agg_op) {
        case Aggregation::Op::MIN:
        case Aggregation::Op::MAX:
          EnsureOkForMinMax(input_value);
          break;
        case Aggregation::Op::SUM:
        case Aggregation::Op::AVG:
          EnsureOkForAvgSum(input_value);
          break;
        case Aggregation::Op::COUNT:
          break;
      }
      *value_it = agg_op == Aggregation::Op::COUNT ? 1 : input_value;
      continue;
    }

    // aggregation of existing values
    switch (agg_op) {
      case Aggregation::Op::COUNT:
        *value_it = *count_it;
        break;
      case Aggregation::Op::MIN: {
        EnsureOkForMinMax(input_value);
        // TODO an illegal comparison here will throw a TypedValueException
        // consider catching and throwing something else
        TypedValue comparison_result = input_value < *value_it;
        // since we skip nulls we either have a valid comparison, or
        // an exception was just thrown above
        // safe to assume a bool TypedValue
        if (comparison_result.Value<bool>()) *value_it = input_value;
        break;
      }
      case Aggregation::Op::MAX: {
        //  all comments as for Op::Min
        EnsureOkForMinMax(input_value);
        TypedValue comparison_result = input_value > *value_it;
        if (comparison_result.Value<bool>()) *value_it = input_value;
        break;
      }
      case Aggregation::Op::AVG:
      // for averaging we sum first and divide by count once all
      // the input has been processed
      case Aggregation::Op::SUM:
        EnsureOkForAvgSum(input_value);
        *value_it = *value_it + input_value;
        break;
    }  // end switch over Aggregation::Op enum
  }    // end loop over all aggregations
}

void Aggregate::AggregateCursor::EnsureOkForMinMax(const TypedValue &value) {
  switch (value.type()) {
    case TypedValue::Type::Bool:
    case TypedValue::Type::Int:
    case TypedValue::Type::Double:
    case TypedValue::Type::String:
      return;
    default:
      // TODO consider better error feedback
      throw TypedValueException(
          "Only Bool, Int, Double and String properties are allowed in "
          "MIN and MAX aggregations");
  }
}
void Aggregate::AggregateCursor::EnsureOkForAvgSum(const TypedValue &value) {
  switch (value.type()) {
    case TypedValue::Type::Int:
    case TypedValue::Type::Double:
      return;
    default:
      // TODO consider better error feedback
      throw TypedValueException(
          "Only numeric properties allowed in SUM and AVG aggregations");
  }
}

bool Aggregate::AggregateCursor::TypedValueListEqual::operator()(
    const std::list<TypedValue> &left,
    const std::list<TypedValue> &right) const {
  return std::equal(left.begin(), left.end(), right.begin(),
                    TypedValue::BoolEqual{});
}

Skip::Skip(const std::shared_ptr<LogicalOperator> &input,
           Expression *expression)
    : input_(input), expression_(expression) {}

void Skip::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> Skip::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<SkipCursor>(*this, db);
}

Skip::SkipCursor::SkipCursor(Skip &self, GraphDbAccessor &db)
    : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

bool Skip::SkipCursor::Pull(Frame &frame, const SymbolTable &symbol_table) {
  while (input_cursor_->Pull(frame, symbol_table)) {
    if (to_skip_ == -1) {
      // first successful pull from the input
      // evaluate the skip expression
      ExpressionEvaluator evaluator(frame, symbol_table);
      self_.expression_->Accept(evaluator);
      TypedValue to_skip = evaluator.PopBack();
      if (to_skip.type() != TypedValue::Type::Int)
        throw QueryRuntimeException("Result of SKIP expression must be an int");

      to_skip_ = to_skip.Value<int64_t>();
      if (to_skip_ < 0)
        throw QueryRuntimeException(
            "Result of SKIP expression must be greater or equal to zero");
    }

    if (skipped_++ < to_skip_) continue;
    return true;
  }
  return false;
}

Limit::Limit(const std::shared_ptr<LogicalOperator> &input,
             Expression *expression)
    : input_(input), expression_(expression) {}

void Limit::Accept(LogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    visitor.Visit(*this);
    input_->Accept(visitor);
    visitor.PostVisit(*this);
  }
}

std::unique_ptr<Cursor> Limit::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<LimitCursor>(*this, db);
}

Limit::LimitCursor::LimitCursor(Limit &self, GraphDbAccessor &db)
    : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

bool Limit::LimitCursor::Pull(Frame &frame, const SymbolTable &symbol_table) {

  // we need to evaluate the limit expression before the first input Pull
  // because it might be 0 and thereby we shouldn't Pull from input at all
  // we can do this before Pulling from the input because the limit expression
  // is not allowed to contain any identifiers
  if (limit_ == -1) {
    ExpressionEvaluator evaluator(frame, symbol_table);
    self_.expression_->Accept(evaluator);
    TypedValue limit = evaluator.PopBack();
    if (limit.type() != TypedValue::Type::Int)
      throw QueryRuntimeException("Result of LIMIT expression must be an int");

    limit_ = limit.Value<int64_t>();
    if (limit_ < 0)
      throw QueryRuntimeException(
          "Result of LIMIT expression must be greater or equal to zero");
  }

  // check we have not exceeded the limit before pulling
  if (pulled_++ >= limit_)
    return false;

  return input_cursor_->Pull(frame, symbol_table);
}

}  // namespace plan
}  // namespace query
