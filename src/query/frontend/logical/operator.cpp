#include "query/frontend/logical/operator.hpp"

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/interpret/interpret.hpp"

namespace query {
namespace plan {

CreateNode::CreateNode(NodeAtom *node_atom,
                       std::shared_ptr<LogicalOperator> input)
    : node_atom_(node_atom), input_(input) {}

void CreateNode::Accept(LogicalOperatorVisitor &visitor) {
  visitor.Visit(*this);
  if (input_) input_->Accept(visitor);
  visitor.PostVisit(*this);
}

std::unique_ptr<Cursor> CreateNode::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<CreateNodeCursor>(*this, db);
}

CreateNode::CreateNodeCursor::CreateNodeCursor(CreateNode &self,
                                               GraphDbAccessor &db)
    : self_(self),
      db_(db),
      input_cursor_(self.input_ ? self.input_->MakeCursor(db) : nullptr) {}

bool CreateNode::CreateNodeCursor::Pull(Frame &frame,
                                        SymbolTable &symbol_table) {
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
                                          SymbolTable &symbol_table) {
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
  frame[symbol_table[*self_.node_atom_->identifier_]] = new_node;
}

CreateExpand::CreateExpand(NodeAtom *node_atom, EdgeAtom *edge_atom,
                           const std::shared_ptr<LogicalOperator> &input,
                           const Symbol &input_symbol, bool node_existing)
    : node_atom_(node_atom),
      edge_atom_(edge_atom),
      input_(input),
      input_symbol_(input_symbol),
      node_existing_(node_existing) {}

void CreateExpand::Accept(LogicalOperatorVisitor &visitor) {
  visitor.Visit(*this);
  input_->Accept(visitor);
  visitor.PostVisit(*this);
}

std::unique_ptr<Cursor> CreateExpand::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<CreateExpandCursor>(*this, db);
}

CreateExpand::CreateExpandCursor::CreateExpandCursor(CreateExpand &self,
                                                     GraphDbAccessor &db)
    : self_(self), db_(db), input_cursor_(self.input_->MakeCursor(db)) {}

bool CreateExpand::CreateExpandCursor::Pull(Frame &frame,
                                            SymbolTable &symbol_table) {
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
    Frame &frame, SymbolTable &symbol_table, ExpressionEvaluator &evaluator) {
  if (self_.node_existing_) {
    TypedValue &dest_node_value =
        frame[symbol_table[*self_.node_atom_->identifier_]];
    return dest_node_value.Value<VertexAccessor>();
  } else {
    // the node does not exist, it needs to be created
    auto node = db_.insert_vertex();
    for (auto label : self_.node_atom_->labels_) node.add_label(label);
    for (auto kv : self_.node_atom_->properties_) {
      kv.second->Accept(evaluator);
      node.PropsSet(kv.first, evaluator.PopBack());
    }
    auto symbol = symbol_table[*self_.node_atom_->identifier_];
    frame[symbol] = node;
    return frame[symbol].Value<VertexAccessor>();
  }
}

void CreateExpand::CreateExpandCursor::CreateEdge(
    VertexAccessor &from, VertexAccessor &to, Frame &frame,
    SymbolTable &symbol_table, ExpressionEvaluator &evaluator) {
  EdgeAccessor edge =
      db_.insert_edge(from, to, self_.edge_atom_->edge_types_[0]);
  for (auto kv : self_.edge_atom_->properties_) {
    kv.second->Accept(evaluator);
    edge.PropsSet(kv.first, evaluator.PopBack());
  }
  frame[symbol_table[*self_.edge_atom_->identifier_]] = edge;
}

ScanAll::ScanAll(NodeAtom *node_atom) : node_atom_(node_atom) {}

std::unique_ptr<Cursor> ScanAll::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<ScanAllCursor>(*this, db);
}

ScanAll::ScanAllCursor::ScanAllCursor(ScanAll &self, GraphDbAccessor &db)
    : self_(self), vertices_(db.vertices()), vertices_it_(vertices_.begin()) {}

bool ScanAll::ScanAllCursor::Pull(Frame &frame, SymbolTable &symbol_table) {
  if (vertices_it_ == vertices_.end()) return false;
  frame[symbol_table[*self_.node_atom_->identifier_]] = *vertices_it_++;
  return true;
}

Expand::Expand(NodeAtom *node_atom, EdgeAtom *edge_atom,
               const std::shared_ptr<LogicalOperator> &input,
               const Symbol &input_symbol, bool node_cycle, bool edge_cycle)
    : node_atom_(node_atom),
      edge_atom_(edge_atom),
      input_(input),
      input_symbol_(input_symbol),
      node_cycle_(node_cycle),
      edge_cycle_(edge_cycle) {}

void Expand::Accept(LogicalOperatorVisitor &visitor) {
  visitor.Visit(*this);
  input_->Accept(visitor);
  visitor.PostVisit(*this);
}

std::unique_ptr<Cursor> Expand::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<ExpandCursor>(*this, db);
}

Expand::ExpandCursor::ExpandCursor(Expand &self, GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

bool Expand::ExpandCursor::Pull(Frame &frame, SymbolTable &symbol_table) {
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

bool Expand::ExpandCursor::InitEdges(Frame &frame, SymbolTable &symbol_table) {
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
  // TODO add support for uniqueness (edge, vertex)

  return true;
}

bool Expand::ExpandCursor::HandleEdgeCycle(EdgeAccessor &new_edge, Frame &frame,
                                           SymbolTable &symbol_table) {
  if (self_.edge_cycle_) {
    TypedValue &old_edge_value =
        frame[symbol_table[*self_.edge_atom_->identifier_]];
    return old_edge_value.Value<EdgeAccessor>() == new_edge;
  } else {
    // not doing a cycle, so put the new_edge into the frame and return true
    frame[symbol_table[*self_.edge_atom_->identifier_]] = new_edge;
    return true;
  }
}

bool Expand::ExpandCursor::PullNode(EdgeAccessor &new_edge,
                                    EdgeAtom::Direction direction, Frame &frame,
                                    SymbolTable &symbol_table) {
  switch (direction) {
    case EdgeAtom::Direction::LEFT:
      return HandleNodeCycle(new_edge.from(), frame, symbol_table);
    case EdgeAtom::Direction::RIGHT:
      return HandleNodeCycle(new_edge.to(), frame, symbol_table);
    case EdgeAtom::Direction::BOTH:
      permanent_fail("Must indicate exact expansion direction here");
  }
}

bool Expand::ExpandCursor::HandleNodeCycle(VertexAccessor new_node,
                                           Frame &frame,
                                           SymbolTable &symbol_table) {
  if (self_.node_cycle_) {
    TypedValue &old_node_value =
        frame[symbol_table[*self_.node_atom_->identifier_]];
    return old_node_value.Value<VertexAccessor>() == new_node;
  } else {
    // not doing a cycle, so put the new_edge into the frame and return true
    frame[symbol_table[*self_.node_atom_->identifier_]] = new_node;
    return true;
  }
}

NodeFilter::NodeFilter(std::shared_ptr<LogicalOperator> input,
                       Symbol input_symbol, NodeAtom *node_atom)
    : input_(input), input_symbol_(input_symbol), node_atom_(node_atom) {}

void NodeFilter::Accept(LogicalOperatorVisitor &visitor) {
  visitor.Visit(*this);
  input_->Accept(visitor);
  visitor.PostVisit(*this);
}

std::unique_ptr<Cursor> NodeFilter::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<NodeFilterCursor>(*this, db);
}

NodeFilter::NodeFilterCursor::NodeFilterCursor(NodeFilter &self,
                                               GraphDbAccessor &db)
    : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

bool NodeFilter::NodeFilterCursor::Pull(Frame &frame,
                                        SymbolTable &symbol_table) {
  while (input_cursor_->Pull(frame, symbol_table)) {
    auto &vertex = frame[self_.input_symbol_].Value<VertexAccessor>();
    // Filter needs to use the old, unmodified vertex, even though we may change
    // properties or labels during the current command.
    vertex.SwitchOld();
    if (VertexPasses(vertex, frame, symbol_table)) return true;
  }
  return false;
}

bool NodeFilter::NodeFilterCursor::VertexPasses(const VertexAccessor &vertex,
                                                Frame &frame,
                                                SymbolTable &symbol_table) {
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

EdgeFilter::EdgeFilter(std::shared_ptr<LogicalOperator> input,
                       Symbol input_symbol, EdgeAtom *edge_atom)
    : input_(input), input_symbol_(input_symbol), edge_atom_(edge_atom) {}

void EdgeFilter::Accept(LogicalOperatorVisitor &visitor) {
  visitor.Visit(*this);
  input_->Accept(visitor);
  visitor.PostVisit(*this);
}

std::unique_ptr<Cursor> EdgeFilter::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<EdgeFilterCursor>(*this, db);
}
EdgeFilter::EdgeFilterCursor::EdgeFilterCursor(EdgeFilter &self,
                                               GraphDbAccessor &db)
    : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

bool EdgeFilter::EdgeFilterCursor::Pull(Frame &frame,
                                        SymbolTable &symbol_table) {
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
                                              SymbolTable &symbol_table) {
  // edge type filtering - logical OR
  const auto &types = self_.edge_atom_->edge_types_;
  GraphDbTypes::EdgeType type = edge.edge_type();
  if (!std::any_of(types.begin(), types.end(),
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
  visitor.Visit(*this);
  input_->Accept(visitor);
  visitor.PostVisit(*this);
}

std::unique_ptr<Cursor> Filter::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<FilterCursor>(*this, db);
}

Filter::FilterCursor::FilterCursor(Filter &self, GraphDbAccessor &db)
    : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

bool Filter::FilterCursor::Pull(Frame &frame, SymbolTable &symbol_table) {
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

Produce::Produce(std::shared_ptr<LogicalOperator> input,
                 std::vector<NamedExpression *> named_expressions)
    : input_(input), named_expressions_(named_expressions) {}

void Produce::Accept(LogicalOperatorVisitor &visitor) {
  visitor.Visit(*this);
  if (input_) input_->Accept(visitor);
  visitor.PostVisit(*this);
}

std::unique_ptr<Cursor> Produce::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<ProduceCursor>(*this, db);
}

const std::vector<NamedExpression *> &Produce::named_expressions() {
  return named_expressions_;
}

Produce::ProduceCursor::ProduceCursor(Produce &self, GraphDbAccessor &db)
    : self_(self),
      input_cursor_(self.input_ ? self_.input_->MakeCursor(db) : nullptr) {}
bool Produce::ProduceCursor::Pull(Frame &frame, SymbolTable &symbol_table) {
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
  visitor.Visit(*this);
  input_->Accept(visitor);
  visitor.PostVisit(*this);
}

std::unique_ptr<Cursor> Delete::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<DeleteCursor>(*this, db);
}

Delete::DeleteCursor::DeleteCursor(Delete &self, GraphDbAccessor &db)
    : self_(self), db_(db), input_cursor_(self_.input_->MakeCursor(db)) {}

bool Delete::DeleteCursor::Pull(Frame &frame, SymbolTable &symbol_table) {
  if (!input_cursor_->Pull(frame, symbol_table)) return false;

  ExpressionEvaluator evaluator(frame, symbol_table);
  // Delete should get the latest information, this way it is also possible to
  // delete newly added nodes and edges.
  evaluator.SwitchNew();
  for (Expression *expression : self_.expressions_) {
    expression->Accept(evaluator);
    TypedValue value = evaluator.PopBack();
    switch (value.type()) {
      case TypedValue::Type::Null:
        // if we got a Null, that's OK, probably it's an OPTIONAL MATCH
        return true;
      case TypedValue::Type::Vertex:
        if (self_.detach_)
          db_.detach_remove_vertex(value.Value<VertexAccessor>());
        else if (!db_.remove_vertex(value.Value<VertexAccessor>()))
          throw query::QueryRuntimeException(
              "Failed to remove vertex because of it's existing "
              "connections. Consider using DETACH DELETE.");
        break;
      case TypedValue::Type::Edge:
        db_.remove_edge(value.Value<EdgeAccessor>());
        break;
      case TypedValue::Type::Path:
      // TODO consider path deletion
      default:
        throw TypedValueException("Can only delete edges and vertices");
    }
  }
  return true;
}

SetProperty::SetProperty(const std::shared_ptr<LogicalOperator> input,
                         PropertyLookup *lhs, Expression *rhs)
    : input_(input), lhs_(lhs), rhs_(rhs) {}

void SetProperty::Accept(LogicalOperatorVisitor &visitor) {
  visitor.Visit(*this);
  input_->Accept(visitor);
  visitor.PostVisit(*this);
}

std::unique_ptr<Cursor> SetProperty::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<SetPropertyCursor>(*this, db);
}

SetProperty::SetPropertyCursor::SetPropertyCursor(SetProperty &self,
                                                  GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

bool SetProperty::SetPropertyCursor::Pull(Frame &frame,
                                          SymbolTable &symbol_table) {
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

SetProperties::SetProperties(const std::shared_ptr<LogicalOperator> input,
                             const Symbol input_symbol, Expression *rhs, Op op)
    : input_(input), input_symbol_(input_symbol), rhs_(rhs), op_(op) {}

void SetProperties::Accept(LogicalOperatorVisitor &visitor) {
  visitor.Visit(*this);
  input_->Accept(visitor);
  visitor.PostVisit(*this);
}

std::unique_ptr<Cursor> SetProperties::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<SetPropertiesCursor>(*this, db);
}

SetProperties::SetPropertiesCursor::SetPropertiesCursor(SetProperties &self,
                                                        GraphDbAccessor &db)
    : self_(self), db_(db), input_cursor_(self.input_->MakeCursor(db)) {}

bool SetProperties::SetPropertiesCursor::Pull(Frame &frame,
                                              SymbolTable &symbol_table) {
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

SetLabels::SetLabels(const std::shared_ptr<LogicalOperator> input,
                     const Symbol input_symbol,
                     const std::vector<GraphDbTypes::Label> &labels)
    : input_(input), input_symbol_(input_symbol), labels_(labels) {}

void SetLabels::Accept(LogicalOperatorVisitor &visitor) {
  visitor.Visit(*this);
  input_->Accept(visitor);
  visitor.PostVisit(*this);
}

std::unique_ptr<Cursor> SetLabels::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<SetLabelsCursor>(*this, db);
}

SetLabels::SetLabelsCursor::SetLabelsCursor(SetLabels &self,
                                            GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

bool SetLabels::SetLabelsCursor::Pull(Frame &frame, SymbolTable &symbol_table) {
  if (!input_cursor_->Pull(frame, symbol_table)) return false;

  TypedValue &vertex_value = frame[self_.input_symbol_];
  auto &vertex = vertex_value.Value<VertexAccessor>();
  vertex.SwitchNew();
  for (auto label : self_.labels_) vertex.add_label(label);

  return true;
}

RemoveProperty::RemoveProperty(const std::shared_ptr<LogicalOperator> input,
                               PropertyLookup *lhs)
    : input_(input), lhs_(lhs) {}

void RemoveProperty::Accept(LogicalOperatorVisitor &visitor) {
  visitor.Visit(*this);
  input_->Accept(visitor);
  visitor.PostVisit(*this);
}

std::unique_ptr<Cursor> RemoveProperty::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<RemovePropertyCursor>(*this, db);
}

RemoveProperty::RemovePropertyCursor::RemovePropertyCursor(RemoveProperty &self,
                                                           GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

bool RemoveProperty::RemovePropertyCursor::Pull(Frame &frame,
                                                SymbolTable &symbol_table) {
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

RemoveLabels::RemoveLabels(const std::shared_ptr<LogicalOperator> input,
                           const Symbol input_symbol,
                           const std::vector<GraphDbTypes::Label> &labels)
    : input_(input), input_symbol_(input_symbol), labels_(labels) {}

void RemoveLabels::Accept(LogicalOperatorVisitor &visitor) {
  visitor.Visit(*this);
  input_->Accept(visitor);
  visitor.PostVisit(*this);
}

std::unique_ptr<Cursor> RemoveLabels::MakeCursor(GraphDbAccessor &db) {
  return std::make_unique<RemoveLabelsCursor>(*this, db);
}

RemoveLabels::RemoveLabelsCursor::RemoveLabelsCursor(RemoveLabels &self,
                                                     GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

bool RemoveLabels::RemoveLabelsCursor::Pull(Frame &frame,
                                            SymbolTable &symbol_table) {
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
void ExpandUniquenessFilter<TAccessor>::Accept(LogicalOperatorVisitor &visitor) {
  visitor.Visit(*this);
  input_->Accept(visitor);
  visitor.PostVisit(*this);
}

template <typename TAccessor>
std::unique_ptr<Cursor> ExpandUniquenessFilter<TAccessor>::MakeCursor(
    GraphDbAccessor &db) {
  return std::make_unique<ExpandUniquenessFilterCursor>(*this, db);
}

template <typename TAccessor>
ExpandUniquenessFilter<TAccessor>::ExpandUniquenessFilterCursor::
    ExpandUniquenessFilterCursor(ExpandUniquenessFilter &self,
                                 GraphDbAccessor &db)
    : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

template <typename TAccessor>
bool ExpandUniquenessFilter<TAccessor>::ExpandUniquenessFilterCursor::Pull(
    Frame &frame, SymbolTable &symbol_table) {

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
    if (expansion_ok())
      return true;
  return false;
}

// instantiations of the ExpandUniquenessFilter template class
// we only ever need these two
template class ExpandUniquenessFilter<VertexAccessor>;
template class ExpandUniquenessFilter<EdgeAccessor>;

}  // namespace plan
}  // namespace query
