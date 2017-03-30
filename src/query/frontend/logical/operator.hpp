#pragma once

#include <memory>
#include <vector>

#include "database/graph_db_accessor.hpp"
#include "database/graph_db_datatypes.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/interpret/interpret.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "utils/visitor/visitable.hpp"
#include "utils/visitor/visitor.hpp"

namespace query {
namespace plan {

class Cursor {
 public:
  virtual bool Pull(Frame &, SymbolTable &) = 0;
  virtual ~Cursor() {}
};

class CreateNode;
class CreateExpand;
class ScanAll;
class Expand;
class NodeFilter;
class EdgeFilter;
class Filter;
class Produce;
class Delete;
class SetProperty;
class SetProperties;
class SetLabels;
class RemoveProperty;
class RemoveLabels;

using LogicalOperatorVisitor =
    ::utils::Visitor<CreateNode, CreateExpand, ScanAll, Expand, NodeFilter,
                     EdgeFilter, Filter, Produce, Delete, SetProperty,
                     SetProperties, SetLabels, RemoveProperty, RemoveLabels>;

class LogicalOperator : public ::utils::Visitable<LogicalOperatorVisitor> {
 public:
  virtual std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) = 0;
  virtual ~LogicalOperator() {}
};

/**
 * Operator for creating a node. This op is used both for
 * creating a single node (CREATE statement without
 * a preceeding MATCH), or multiple nodes (MATCH CREATE).
 *
 * This node
 */
class CreateNode : public LogicalOperator {
 public:
  /**
   *
   * @param node_atom
   * @param input Optional. If nullptr, then a single node will be
   *    created (a single successful Pull from this Op's Cursor).
   *    If a valid input, then a node will be created for each
   *    successful pull from the given input.
   */
  CreateNode(NodeAtom *node_atom, std::shared_ptr<LogicalOperator> input);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  NodeAtom *node_atom_ = nullptr;
  std::shared_ptr<LogicalOperator> input_;

  class CreateNodeCursor : public Cursor {
   public:
    CreateNodeCursor(CreateNode &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, SymbolTable &symbol_table) override;

   private:
    CreateNode &self_;
    GraphDbAccessor &db_;
    // optional, used in situations in which this create op
    // pulls from an input (in MATCH CREATE, CREATE ... CREATE)
    std::unique_ptr<Cursor> input_cursor_;
    // control switch when creating only one node (nullptr input)
    bool did_create_{false};

    /**
     * Creates a single node and places it in the frame.
     */
    void Create(Frame &frame, SymbolTable &symbol_table);
  };
};

class CreateExpand : public LogicalOperator {
 public:
  CreateExpand(NodeAtom *node_atom, EdgeAtom *edge_atom,
               const std::shared_ptr<LogicalOperator> &input,
               const Symbol &input_symbol, bool node_existing);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  // info on what's getting expanded
  NodeAtom *node_atom_;
  EdgeAtom *edge_atom_;

  // the input op and the symbol under which the op's result
  // can be found in the frame
  std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;

  // if the given node atom refers to an existing node
  // (either matched or created)
  bool node_existing_;

  class CreateExpandCursor : public Cursor {
   public:
    CreateExpandCursor(CreateExpand &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, SymbolTable &symbol_table) override;

   private:
    CreateExpand &self_;
    GraphDbAccessor &db_;
    std::unique_ptr<Cursor> input_cursor_;

    /**
     *  Helper function for getting an existing node or creating a new one.
     * @return The newly created or already existing node.
     */
    VertexAccessor OtherVertex(Frame &frame, SymbolTable &symbol_table,
                               ExpressionEvaluator &evaluator);

    /**
     * Helper function for creating an edge and adding it
     * to the frame.
     *
     * @param from  Origin vertex of the edge.
     * @param to  Destination vertex of the edge.
     * @param evaluator Expression evaluator for property value eval.
     */
    void CreateEdge(VertexAccessor &from, VertexAccessor &to, Frame &frame,
                    SymbolTable &symbol_table, ExpressionEvaluator &evaluator);
  };
};

class ScanAll : public LogicalOperator {
 public:
  ScanAll(NodeAtom *node_atom);
  DEFVISITABLE(LogicalOperatorVisitor);
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  NodeAtom *node_atom_ = nullptr;

  class ScanAllCursor : public Cursor {
   public:
    ScanAllCursor(ScanAll &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, SymbolTable &symbol_table) override;

   private:
    ScanAll &self_;
    decltype(std::declval<GraphDbAccessor>().vertices()) vertices_;
    decltype(vertices_.begin()) vertices_it_;
  };
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
  Expand(NodeAtom *node_atom, EdgeAtom *edge_atom,
         const std::shared_ptr<LogicalOperator> &input,
         const Symbol &input_symbol, bool node_cycle, bool edge_cycle);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  // info on what's getting expanded
  NodeAtom *node_atom_;
  EdgeAtom *edge_atom_;

  // the input op and the symbol under which the op's result
  // can be found in the frame
  std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;

  // if the given node and edge atom refer to symbols
  // (query identifiers) that have already been expanded
  // and should be just validated in the frame
  bool node_cycle_;
  bool edge_cycle_;

  class ExpandCursor : public Cursor {
   public:
    ExpandCursor(Expand &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, SymbolTable &symbol_table) override;

   private:
    Expand &self_;
    std::unique_ptr<Cursor> input_cursor_;

    // the iterable over edges and the current edge iterator are referenced via
    // unique pointers because they can not be initialized in the constructor of
    // this class. they are initialized once for each pull from the input
    std::unique_ptr<InEdgeT> in_edges_;
    std::unique_ptr<InEdgeIteratorT> in_edges_it_;
    std::unique_ptr<OutEdgeT> out_edges_;
    std::unique_ptr<OutEdgeIteratorT> out_edges_it_;

    bool InitEdges(Frame &frame, SymbolTable &symbol_table);

    /**
     * For a newly expanded edge handles cycle checking and frame insertion.
     *
     * @return If or not the given new_edge is a valid expansion. It is not
     * valid only when doing an edge-cycle and the new_edge does not match the
     * old.
     */
    bool HandleEdgeCycle(EdgeAccessor &new_edge, Frame &frame,
                         SymbolTable &symbol_table);

    /**
     * Expands a node for the given newly expanded edge.
     *
     * @return True if after this call a new node has been successfully
     * expanded. Returns false only when doing a node-cycle and the
     * new node does not qualify.
     */
    bool PullNode(EdgeAccessor &new_edge, EdgeAtom::Direction direction,
                  Frame &frame, SymbolTable &symbol_table);

    /**
     * For a newly expanded node handles cycle checking and frame insertion.
     *
     * @return If or not the given new_node is a valid expansion. It is not
     * valid only when doing a node-cycle and the new_node does not match the
     * old.
     */
    bool HandleNodeCycle(VertexAccessor new_node, Frame &frame,
                         SymbolTable &symbol_table);
  };
};

class NodeFilter : public LogicalOperator {
 public:
  NodeFilter(std::shared_ptr<LogicalOperator> input, Symbol input_symbol,
             NodeAtom *node_atom);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;
  NodeAtom *node_atom_;

  class NodeFilterCursor : public Cursor {
   public:
    NodeFilterCursor(NodeFilter &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, SymbolTable &symbol_table) override;

   private:
    NodeFilter &self_;
    std::unique_ptr<Cursor> input_cursor_;

    /** Helper function for checking if the given vertex
     * passes this filter. */
    bool VertexPasses(const VertexAccessor &vertex, Frame &frame,
                      SymbolTable &symbol_table);
  };
};

class EdgeFilter : public LogicalOperator {
 public:
  EdgeFilter(std::shared_ptr<LogicalOperator> input, Symbol input_symbol,
             EdgeAtom *edge_atom);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;
  EdgeAtom *edge_atom_;

  class EdgeFilterCursor : public Cursor {
   public:
    EdgeFilterCursor(EdgeFilter &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, SymbolTable &symbol_table) override;

   private:
    EdgeFilter &self_;
    std::unique_ptr<Cursor> input_cursor_;

    /** Helper function for checking if the given edge satisfied
     *  the criteria of this edge filter. */
    bool EdgePasses(const EdgeAccessor &edge, Frame &frame,
                    SymbolTable &symbol_table);
  };
};

/**
 * Filter whose Pull returns true only when the given expression
 * evaluates into true. The given expression is assumed to
 * return either NULL (treated as false) or a boolean value.
 */
class Filter : public LogicalOperator {
 public:
  Filter(const std::shared_ptr<LogicalOperator> &input_,
         Expression *expression_);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  std::shared_ptr<LogicalOperator> input_;
  Expression *expression_;

  class FilterCursor : public Cursor {
   public:
    FilterCursor(Filter &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, SymbolTable &symbol_table) override;

   private:
    Filter &self_;
    std::unique_ptr<Cursor> input_cursor_;
  };
};

/**
 * A logical operator that places an arbitrary number
 * if named expressions on the frame (the logical operator
 * for the RETURN clause).
 *
 * Supports optional input. When the input is provided,
 * it is Pulled from and the Produce succeds once for
 * every input Pull (typically a MATCH/RETURN query).
 * When the input is not provided (typically a standalone
 * RETURN clause) the Produce's pull succeeds exactly once.
 */
class Produce : public LogicalOperator {
 public:
  Produce(std::shared_ptr<LogicalOperator> input,
          std::vector<NamedExpression *> named_expressions);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;
  const std::vector<NamedExpression *> &named_expressions();

 private:
  std::shared_ptr<LogicalOperator> input_;
  std::vector<NamedExpression *> named_expressions_;

  class ProduceCursor : public Cursor {
   public:
    ProduceCursor(Produce &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, SymbolTable &symbol_table) override;

   private:
    Produce &self_;
    // optional, see class documentation
    std::unique_ptr<Cursor> input_cursor_;
    // control switch when creating only one node (nullptr input)
    bool did_produce_{false};
  };
};

/**
 * Operator for deleting vertices and edges.
 * Has a flag for using DETACH DELETE when deleting
 * vertices.
 */
class Delete : public LogicalOperator {
 public:
  Delete(const std::shared_ptr<LogicalOperator> &input_,
         const std::vector<Expression *> &expressions, bool detach_);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  std::shared_ptr<LogicalOperator> input_;
  std::vector<Expression *> expressions_;
  // if the vertex should be detached before deletion
  // if not detached, and has connections, an error is raised
  // ignored when deleting edges
  bool detach_;

  class DeleteCursor : public Cursor {
   public:
    DeleteCursor(Delete &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, SymbolTable &symbol_table) override;

   private:
    Delete &self_;
    GraphDbAccessor &db_;
    std::unique_ptr<Cursor> input_cursor_;
  };
};

/**
 * Logical Op for setting a single property
 * on a single vertex or edge. The property value
 * is an expression that must evaluate to some
 * type that can be stored (a TypedValue that can
 * be converted to PropertyValue).
 */
class SetProperty : public LogicalOperator {
 public:
  SetProperty(const std::shared_ptr<LogicalOperator> input, PropertyLookup *lhs,
              Expression *rhs);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  std::shared_ptr<LogicalOperator> input_;
  PropertyLookup *lhs_;
  Expression *rhs_;

  class SetPropertyCursor : public Cursor {
   public:
    SetPropertyCursor(SetProperty &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, SymbolTable &symbol_table) override;

   private:
    SetProperty &self_;
    std::unique_ptr<Cursor> input_cursor_;
  };
};

/**
 * Logical op for setting the whole properties set
 * on a vertex or an edge. The value being set is
 * an expression that must evaluate to a vertex,
 * edge or map (literal or parameter).
 *
 * Supports setting (replacing the whole properties
 * set with another) and updating.
 */
class SetProperties : public LogicalOperator {
 public:
  /**
   * Defines how setting the properties works. UPDATE means
   * that the current property set is augmented with additional
   * ones (existing props of the same name are replaced), while
   * REPLACE means that the old props are discarded and replaced
   * with new ones.
   */
  enum class Op { UPDATE, REPLACE };

  SetProperties(const std::shared_ptr<LogicalOperator> input,
                const Symbol input_symbol, Expression *rhs, Op op);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;
  Expression *rhs_;
  Op op_;

  class SetPropertiesCursor : public Cursor {
   public:
    SetPropertiesCursor(SetProperties &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, SymbolTable &symbol_table) override;

   private:
    SetProperties &self_;
    GraphDbAccessor &db_;
    std::unique_ptr<Cursor> input_cursor_;

    /** Helper function that sets the given values on either
     * a VertexRecord or an EdgeRecord.
     * @tparam TRecordAccessor Either RecordAccessor<Vertex> or
     * RecordAccessor<Edge>
     */
    template <typename TRecordAccessor>
    void Set(TRecordAccessor &record, const TypedValue &rhs);
  };
};

/**
 * Logical operator for setting an arbitrary number of
 * labels on a Vertex. It does NOT remove labels that
 * are already set on that Vertex.
 */
class SetLabels : public LogicalOperator {
 public:
  SetLabels(const std::shared_ptr<LogicalOperator> input,
            const Symbol input_symbol,
            const std::vector<GraphDbTypes::Label> &labels);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;
  std::vector<GraphDbTypes::Label> labels_;

  class SetLabelsCursor : public Cursor {
   public:
    SetLabelsCursor(SetLabels &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, SymbolTable &symbol_table) override;

   private:
    SetLabels &self_;
    std::unique_ptr<Cursor> input_cursor_;
  };
};

/**
 * Logical op for removing a property from an
 * edge or a vertex.
 */
class RemoveProperty : public LogicalOperator {
 public:
  RemoveProperty(const std::shared_ptr<LogicalOperator> input,
                 PropertyLookup *lhs);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  std::shared_ptr<LogicalOperator> input_;
  PropertyLookup *lhs_;

  class RemovePropertyCursor : public Cursor {
   public:
    RemovePropertyCursor(RemoveProperty &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, SymbolTable &symbol_table) override;

   private:
    RemoveProperty &self_;
    std::unique_ptr<Cursor> input_cursor_;
  };
};

/**
 * Logical operator for removing an arbitrary number of
 * labels on a Vertex. If a label does not exist on a Vertex,
 * nothing happens.
 */
class RemoveLabels : public LogicalOperator {
 public:
  RemoveLabels(const std::shared_ptr<LogicalOperator> input,
               const Symbol input_symbol,
               const std::vector<GraphDbTypes::Label> &labels);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;
  std::vector<GraphDbTypes::Label> labels_;

  class RemoveLabelsCursor : public Cursor {
   public:
    RemoveLabelsCursor(RemoveLabels &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, SymbolTable &symbol_table) override;

   private:
    RemoveLabels &self_;
    std::unique_ptr<Cursor> input_cursor_;
  };
};

}  // namespace plan
}  // namespace query
