#pragma once

#include <memory>
#include <sstream>
#include <vector>

#include "database/graph_db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/interpret/interpret.hpp"
#include "query/frontend/semantic/symbol_table.hpp"

namespace query {

class Cursor {
 public:
  virtual bool Pull(Frame&, SymbolTable&) = 0;
  virtual ~Cursor() {}
};

class LogicalOperator {
 public:
  auto children() { return children_; };
  virtual std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor& db) = 0;
  virtual ~LogicalOperator() {}

 protected:
  std::vector<std::shared_ptr<LogicalOperator>> children_;
};

class CreateOp : public LogicalOperator {
 public:
  CreateOp(NodeAtom* node_atom) : node_atom_(node_atom) {}

 private:
  class CreateOpCursor : public Cursor {
   public:
    CreateOpCursor(CreateOp& self, GraphDbAccessor& db)
        : self_(self), db_(db) {}

    bool Pull(Frame& frame, SymbolTable& symbol_table) override {
      if (!did_create_) {
        auto new_node = db_.insert_vertex();
        for (auto label : self_.node_atom_->labels_) new_node.add_label(label);

        ExpressionEvaluator evaluator(frame, symbol_table);
        for (auto& kv : self_.node_atom_->properties_) {
          kv.second->Accept(evaluator);
          new_node.PropsSet(kv.first, evaluator.PopBack());
        }

        did_create_ = true;
        return true;
      } else
        return false;
    }

   private:
    CreateOp& self_;
    GraphDbAccessor& db_;
    bool did_create_{false};
  };

 public:
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor& db) override {
    return std::make_unique<CreateOpCursor>(*this, db);
  }

 private:
  NodeAtom* node_atom_ = nullptr;
};

class ScanAll : public LogicalOperator {
 public:
  ScanAll(NodeAtom* node_atom) : node_atom_(node_atom) {}

 private:
  class ScanAllCursor : public Cursor {
   public:
    ScanAllCursor(ScanAll& self, GraphDbAccessor& db)
        : self_(self),
          vertices_(db.vertices()),
          vertices_it_(vertices_.begin()) {}

    bool Pull(Frame& frame, SymbolTable& symbol_table) override {
      if (vertices_it_ == vertices_.end()) return false;
      frame[symbol_table[*self_.node_atom_->identifier_]] = *vertices_it_++;
      return true;
    }

   private:
    ScanAll& self_;
    decltype(std::declval<GraphDbAccessor>().vertices()) vertices_;
    decltype(vertices_.begin()) vertices_it_;
  };

 public:
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor& db) override {
    return std::make_unique<ScanAllCursor>(*this, db);
  }

 private:
  NodeAtom* node_atom_ = nullptr;
};

/**
 * Expansion operator. For a node existing in the frame it
 * expands one edge and one node and places them on the frame.
 *
 * This class does not handle node/edge filtering based on
 * properties, labels and edge types. However, it does handle
 * cycle filtering.
 *
 * Cycle filtering means that for a pattern that references
 * the same node or edge in two places (for example (n)-->(n)),
 * only expansions that match defined equalities are succesfully
 * pulled.
 */
class Expand : public LogicalOperator {
  using InEdgeT = decltype(std::declval<VertexAccessor>().in());
  using InEdgeIteratorT = decltype(std::declval<VertexAccessor>().in().begin());
  using OutEdgeT = decltype(std::declval<VertexAccessor>().out());
  using OutEdgeIteratorT =
      decltype(std::declval<VertexAccessor>().out().begin());

 public:
  /**
   * Creates an expansion.
   *
   * Cycle-checking is controlled via booleans. A true value
   * simply denotes that this expansion references an already
   * Pulled node/edge, and should only be checked for equalities
   * during expansion.
   *
   * @param node_atom Describes the node to be expanded. Only the
   *    identifier is used, labels and properties are ignored.
   * @param edge_atom Describes the edge to be expanded. Identifier
   *    and direction are used, edge type and properties are ignored.
   * @param input LogicalOperation that preceeds this one.
   * @param input_symbol Symbol that points to a VertexAccessor
   *    in the Frame that expansion should emanate from.
   * @param node_cycle If or not the node to be expanded is already
   *    present in the Frame and should just be checked for equality.
   * @param edge_cycle Same like 'node_cycle', but for edges.
   */
  Expand(NodeAtom* node_atom, EdgeAtom* edge_atom,
         const std::shared_ptr<LogicalOperator>& input,
         const Symbol& input_symbol,
         bool node_cycle, bool edge_cycle)
      : node_atom_(node_atom),
        edge_atom_(edge_atom),
        input_(input),
        input_symbol_(input_symbol),
        node_cycle_(node_cycle),
        edge_cycle_(edge_cycle) {}

 private:
  class ExpandCursor : public Cursor {
   public:
    ExpandCursor(Expand& self, GraphDbAccessor& db)
        : self_(self), input_cursor_(self.input_->MakeCursor(db)) {}

    bool Pull(Frame& frame, SymbolTable& symbol_table) override {

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

   private:
    Expand& self_;
    std::unique_ptr<Cursor> input_cursor_;

    // the iterable over edges and the current edge iterator are referenced via
    // unique pointers because they can not be initialized in the constructor of
    // this class. they are initialized once for each pull from the input
    std::unique_ptr<InEdgeT> in_edges_;
    std::unique_ptr<InEdgeIteratorT> in_edges_it_;
    std::unique_ptr<OutEdgeT> out_edges_;
    std::unique_ptr<OutEdgeIteratorT> out_edges_it_;

    bool InitEdges(Frame& frame, SymbolTable& symbol_table) {
      if (!input_cursor_->Pull(frame, symbol_table)) return false;

      TypedValue vertex_value = frame[self_.input_symbol_];
      auto vertex = vertex_value.Value<VertexAccessor>();

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

    /**
     * For a newly expanded edge handles cycle checking and frame insertion.
     *
     * @return If or not the given new_edge is a valid expansion. It is not
     * valid only when doing an edge-cycle and the new_edge does not match the
     * old.
     */
    bool HandleEdgeCycle(EdgeAccessor& new_edge, Frame& frame,
                         SymbolTable& symbol_table) {
      if (self_.edge_cycle_) {
        TypedValue& old_edge_value =
            frame[symbol_table[*self_.edge_atom_->identifier_]];
        return old_edge_value.Value<EdgeAccessor>() == new_edge;
      } else {
        // not doing a cycle, so put the new_edge into the frame and return true
        frame[symbol_table[*self_.edge_atom_->identifier_]] = new_edge;
        return true;
      }
    }

    /**
     * Expands a node for the given newly expanded edge.
     *
     * @return True if after this call a new node has been successfully
     * expanded. Returns false only when doing a node-cycle and the
     * new node does not qualify.
     */
    bool PullNode(EdgeAccessor& new_edge, EdgeAtom::Direction direction,
                  Frame& frame, SymbolTable& symbol_table) {
      switch (direction) {
        case EdgeAtom::Direction::LEFT:
          return HandleNodeCycle(new_edge.from(), frame, symbol_table);
        case EdgeAtom::Direction::RIGHT:
          return HandleNodeCycle(new_edge.to(), frame, symbol_table);
        case EdgeAtom::Direction::BOTH:
          permanent_fail("Must indicate exact expansion direction here");
      }
    }

    /**
     * For a newly expanded node handles cycle checking and frame insertion.
     *
     * @return If or not the given new_node is a valid expansion. It is not
     * valid only when doing a node-cycle and the new_node does not match the
     * old.
     */
    bool HandleNodeCycle(VertexAccessor new_node, Frame& frame,
                         SymbolTable& symbol_table) {
      if (self_.node_cycle_) {
        TypedValue& old_node_value =
            frame[symbol_table[*self_.node_atom_->identifier_]];
        return old_node_value.Value<VertexAccessor>() == new_node;
      } else {
        // not doing a cycle, so put the new_edge into the frame and return true
        frame[symbol_table[*self_.node_atom_->identifier_]] = new_node;
        return true;
      }
    }
  };

 public:
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor& db) override {
    return std::make_unique<ExpandCursor>(*this, db);
  }

 private:
  // info on what's getting expanded
  NodeAtom* node_atom_;
  EdgeAtom* edge_atom_;

  // the input op and the symbol under which the op's result
  // can be found in the frame
  std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;

  // if the given node and edge atom refer to symbols
  // (query identifiers) that have already been expanded
  // and should be just validated in the frame
  bool node_cycle_;
  bool edge_cycle_;
};

class NodeFilter : public LogicalOperator {
 public:
  NodeFilter(std::shared_ptr<LogicalOperator> input, Symbol input_symbol,
             NodeAtom* node_atom)
      : input_(input), input_symbol_(input_symbol), node_atom_(node_atom) {}

 private:
  class NodeFilterCursor : public Cursor {
   public:
    NodeFilterCursor(NodeFilter& self, GraphDbAccessor& db)
        : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

    bool Pull(Frame& frame, SymbolTable& symbol_table) override {
      while (input_cursor_->Pull(frame, symbol_table)) {
        const auto& vertex = frame[self_.input_symbol_].Value<VertexAccessor>();
        if (VertexPasses(vertex, frame, symbol_table)) return true;
      }
      return false;
    }

   private:
    NodeFilter& self_;
    std::unique_ptr<Cursor> input_cursor_;

    bool VertexPasses(const VertexAccessor& vertex, Frame& frame,
                      SymbolTable& symbol_table) {
      for (auto label : self_.node_atom_->labels_)
        if (!vertex.has_label(label)) return false;

      ExpressionEvaluator expression_evaluator(frame, symbol_table);
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
  };

 public:
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor& db) override {
    return std::make_unique<NodeFilterCursor>(*this, db);
  }

 private:
  std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;
  NodeAtom* node_atom_;
};

class EdgeFilter : public LogicalOperator {
 public:
  EdgeFilter(std::shared_ptr<LogicalOperator> input, Symbol input_symbol,
             EdgeAtom* edge_atom)
      : input_(input), input_symbol_(input_symbol), edge_atom_(edge_atom) {}

 private:
  class EdgeFilterCursor : public Cursor {
   public:
    EdgeFilterCursor(EdgeFilter& self, GraphDbAccessor& db)
        : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

    bool Pull(Frame& frame, SymbolTable& symbol_table) override {
      while (input_cursor_->Pull(frame, symbol_table)) {
        const auto& edge = frame[self_.input_symbol_].Value<EdgeAccessor>();
        if (EdgePasses(edge, frame, symbol_table)) return true;
      }
      return false;
    }

   private:
    EdgeFilter& self_;
    std::unique_ptr<Cursor> input_cursor_;

    bool EdgePasses(const EdgeAccessor& edge, Frame& frame,
                    SymbolTable& symbol_table) {
      for (auto edge_type : self_.edge_atom_->edge_types_)
        if (edge.edge_type() != edge_type) return false;

      ExpressionEvaluator expression_evaluator(frame, symbol_table);
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
  };

 public:
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor& db) override {
    return std::make_unique<EdgeFilterCursor>(*this, db);
  }

 private:
  std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;
  EdgeAtom* edge_atom_;
};

class Produce : public LogicalOperator {
 public:
  Produce(std::shared_ptr<LogicalOperator> input,
          std::vector<NamedExpression*> named_expressions)
      : input_(input), named_expressions_(named_expressions) {
    children_.emplace_back(input);
  }

  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor& db) override {
    return std::make_unique<ProduceCursor>(*this, db);
  }

  const auto& named_expressions() { return named_expressions_; }

 private:
  class ProduceCursor : public Cursor {
   public:
    ProduceCursor(Produce& self, GraphDbAccessor& db)
        : self_(self), self_cursor_(self_.input_->MakeCursor(db)) {}
    bool Pull(Frame& frame, SymbolTable& symbol_table) override {
      ExpressionEvaluator evaluator(frame, symbol_table);
      if (self_cursor_->Pull(frame, symbol_table)) {
        for (auto named_expr : self_.named_expressions_) {
          named_expr->Accept(evaluator);
        }
        return true;
      }
      return false;
    }

   private:
    Produce& self_;
    std::unique_ptr<Cursor> self_cursor_;
  };

 private:
  std::shared_ptr<LogicalOperator> input_;
  std::vector<NamedExpression*> named_expressions_;
};
}
