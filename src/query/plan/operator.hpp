/** @file */

#pragma once

#include <algorithm>
#include <memory>
#include <query/exceptions.hpp>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "database/graph_db_accessor.hpp"
#include "database/graph_db_datatypes.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "utils/hashing/fnv.hpp"
#include "utils/visitor/visitable.hpp"
#include "utils/visitor/visitor.hpp"

namespace query {

class Frame;
class ExpressionEvaluator;

namespace plan {

/** @brief Base class for iteration cursors of @c LogicalOperator classes.
 *
 *  Each @c LogicalOperator must produce a concrete @c Cursor, which provides
 *  the iteration mechanism.
 */
class Cursor {
 public:
  /** @brief Run an iteration of a @c LogicalOperator.
   *
   *  Since operators may be chained, the iteration may pull results from
   *  multiple operators.
   *
   *  @param Frame May be read from or written to while performing the
   *      iteration.
   *  @param SymbolTable Used to get the position of symbols in frame.
   */
  virtual bool Pull(Frame &, const SymbolTable &) = 0;

  /**
   * Resets the Cursor to it's initial state.
   */
  virtual void Reset() = 0;

  virtual ~Cursor() {}
};

class Once;
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
template <typename TAccessor>
class ExpandUniquenessFilter;
class Accumulate;
class AdvanceCommand;
class Aggregate;
class Skip;
class Limit;
class OrderBy;

/** @brief Base class for visitors of @c LogicalOperator class hierarchy. */
using LogicalOperatorVisitor =
    ::utils::Visitor<Once, CreateNode, CreateExpand, ScanAll, Expand,
                     NodeFilter, EdgeFilter, Filter, Produce, Delete,
                     SetProperty, SetProperties, SetLabels, RemoveProperty,
                     RemoveLabels, ExpandUniquenessFilter<VertexAccessor>,
                     ExpandUniquenessFilter<EdgeAccessor>, Accumulate,
                     AdvanceCommand, Aggregate, Skip, Limit, OrderBy>;

/** @brief Base class for logical operators.
 *
 *  Each operator describes an operation, which is to be performed on the
 *  database. Operators are iterated over using a @c Cursor. Various operators
 *  can serve as inputs to others and thus a sequence of operations is formed.
 */
class LogicalOperator : public ::utils::Visitable<LogicalOperatorVisitor> {
 public:
  /** @brief Constructs a @c Cursor which is used to run this operator.
   *
   *  @param GraphDbAccessor Used to perform operations on the database.
   */
  virtual std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) = 0;
  virtual ~LogicalOperator() {}
};

/**
 * A logical operator whose Cursor returns true on the first Pull
 * and false on every following Pull.
 */
class Once : public LogicalOperator {
 public:
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  class OnceCursor : public Cursor {
   public:
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    bool did_pull_{false};
  };
};

/** @brief Operator for creating a node.
 *
 *  This op is used both for creating a single node (`CREATE` statement without
 *  a preceeding `MATCH`), or multiple nodes (`MATCH ... CREATE` or
 *  `CREATE (), () ...`).
 *
 *  @sa CreateExpand
 */
class CreateNode : public LogicalOperator {
 public:
  /**
   *
   * @param node_atom @c NodeAtom with information on how to create a node.
   * @param input Optional. If @c nullptr, then a single node will be
   *    created (a single successful @c Cursor::Pull from this op's @c Cursor).
   *    If a valid input, then a node will be created for each
   *    successful pull from the given input.
   */
  CreateNode(const NodeAtom *node_atom,
             const std::shared_ptr<LogicalOperator> &input);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  const NodeAtom *node_atom_ = nullptr;
  const std::shared_ptr<LogicalOperator> input_;

  class CreateNodeCursor : public Cursor {
   public:
    CreateNodeCursor(const CreateNode &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const CreateNode &self_;
    GraphDbAccessor &db_;
    const std::unique_ptr<Cursor> input_cursor_;

    /**
     * Creates a single node and places it in the frame.
     */
    void Create(Frame &frame, const SymbolTable &symbol_table);
  };
};

/** @brief Operator for creating edges and destination nodes.
 *
 *  This operator extends already created nodes with an edge. If the other node
 *  on the edge does not exist, it will be created. For example, in `MATCH (n)
 *  CREATE (n) -[r:r]-> (n)` query, this operator will create just the edge `r`.
 *  In `MATCH (n) CREATE (n) -[r:r]-> (m)` query, the operator will create both
 *  the edge `r` and the node `m`. In case of `CREATE (n) -[r:r]-> (m)` the
 *  first node `n` is created by @c CreateNode operator, while @c CreateExpand
 *  will create the edge `r` and `m`. Similarly, multiple @c CreateExpand are
 *  chained in cases when longer paths need creating.
 *
 *  @sa CreateNode
 */
class CreateExpand : public LogicalOperator {
 public:
  /** @brief Construct @c CreateExpand.
   *
   * @param node_atom @c NodeAtom at the end of the edge. Used to create a node,
   *     unless it refers to an existing one.
   * @param edge_atom @c EdgeAtom with information for the edge to be created.
   * @param input Required. Previous @c LogicalOperator which will be pulled.
   *     For each successful @c Cursor::Pull, this operator will create an
   *     expansion.
   * @param input_symbol @c Symbol for the node at the start of the edge.
   * @param existing_node @c bool indicating whether the @c node_atom refers to
   *     an existing node. If @c false, the operator will also create the node.
   */
  CreateExpand(const NodeAtom *node_atom, const EdgeAtom *edge_atom,
               const std::shared_ptr<LogicalOperator> &input,
               Symbol input_symbol, bool existing_node);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  // info on what's getting expanded
  const NodeAtom *node_atom_;
  const EdgeAtom *edge_atom_;

  // the input op and the symbol under which the op's result
  // can be found in the frame
  const std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;

  // if the given node atom refers to an existing node
  // (either matched or created)
  const bool existing_node_;

  class CreateExpandCursor : public Cursor {
   public:
    CreateExpandCursor(const CreateExpand &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const CreateExpand &self_;
    GraphDbAccessor &db_;
    const std::unique_ptr<Cursor> input_cursor_;

    /**
     *  Helper function for getting an existing node or creating a new one.
     *  @return The newly created or already existing node.
     */
    VertexAccessor &OtherVertex(Frame &frame, const SymbolTable &symbol_table,
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
                    const SymbolTable &symbol_table,
                    ExpressionEvaluator &evaluator);
  };
};

/**
 * @brief Operator which iterates over all the nodes currently in the database.
 * When given an input (optional), does a cartesian product.
 *
 * It accepts an optional input. If provided then this op scans all the nodes
 * currently in the database for each successful Pull from it's input, thereby
 * producing a cartesian product of input Pulls and database elements.
 */
class ScanAll : public LogicalOperator {
 public:
  ScanAll(const NodeAtom *node_atom,
          const std::shared_ptr<LogicalOperator> &input);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  const NodeAtom *node_atom_ = nullptr;
  const std::shared_ptr<LogicalOperator> input_;

  class ScanAllCursor : public Cursor {
   public:
    ScanAllCursor(const ScanAll &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const ScanAll &self_;
    const std::unique_ptr<Cursor> input_cursor_;
    decltype(std::declval<GraphDbAccessor>().vertices()) vertices_;
    decltype(vertices_.begin()) vertices_it_;
  };
};

/**
 * @brief Expansion operator. For a node existing in the frame it
 * expands one edge and one node and places them on the frame.
 *
 * This class does not handle node/edge filtering based on
 * properties, labels and edge types. However, it does handle
 * filtering on existing node / edge.
 *
 * Filtering on existing means that for a pattern that references
 * an already declared node or edge (for example in
 * MATCH (a) MATCH (a)--(b)),
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
   * @brief Creates an expansion.
   *
   * Edge/Node existence is controlled via booleans. A true value
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
   * @param existing_node If or not the node to be expanded is already
   *    present in the Frame and should just be checked for equality.
   * @param existing_edge Same like 'existing_node', but for edges.
   */
  Expand(const NodeAtom *node_atom, const EdgeAtom *edge_atom,
         const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol,
         bool existing_node, bool existing_edge);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  // info on what's getting expanded
  const NodeAtom *node_atom_;
  const EdgeAtom *edge_atom_;

  // the input op and the symbol under which the op's result
  // can be found in the frame
  const std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;

  // if the given node and edge atom refer to symbols
  // (query identifiers) that have already been expanded
  // and should be just validated in the frame
  const bool existing_node_;
  const bool existing_edge_;

  class ExpandCursor : public Cursor {
   public:
    ExpandCursor(const Expand &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const Expand &self_;
    const std::unique_ptr<Cursor> input_cursor_;

    // the iterable over edges and the current edge iterator are referenced via
    // unique pointers because they can not be initialized in the constructor of
    // this class. They are initialized once for each pull from the input
    std::unique_ptr<InEdgeT> in_edges_;
    std::unique_ptr<InEdgeIteratorT> in_edges_it_;
    std::unique_ptr<OutEdgeT> out_edges_;
    std::unique_ptr<OutEdgeIteratorT> out_edges_it_;

    bool InitEdges(Frame &frame, const SymbolTable &symbol_table);

    /**
     * For a newly expanded edge handles existence checking and frame insertion.
     *
     * @return If or not the given new_edge is a valid expansion. It is not
     * valid only when matching and existing edge and new_edge does not match
     * the
     * old.
     */
    bool HandleExistingEdge(const EdgeAccessor &new_edge, Frame &frame,
                            const SymbolTable &symbol_table);

    /**
     * Expands a node for the given newly expanded edge.
     *
     * @return True if after this call a new node has been successfully
     * expanded. Returns false only when matching an existing node and the
     * new node does not qualify.
     */
    bool PullNode(const EdgeAccessor &new_edge, EdgeAtom::Direction direction,
                  Frame &frame, const SymbolTable &symbol_table);

    /**
     * For a newly expanded node handles existence checking and frame insertion.
     *
     * @return If or not the given new_node is a valid expansion. It is not
     * valid only when matching and existing node and new_node does not match
     * the
     * old.
     */
    bool HandleExistingNode(const VertexAccessor new_node, Frame &frame,
                            const SymbolTable &symbol_table);
  };
};

/** @brief Operator which filters nodes by labels and properties.
 *
 *  This operator is used to implement `MATCH (n :label {prop: value})`, so that
 *  it filters nodes with specified labels and properties by value.
 */
class NodeFilter : public LogicalOperator {
 public:
  /** @brief Construct @c NodeFilter.
   *
   * @param input Required, preceding @c LogicalOperator.
   * @param input_symbol @c Symbol where the node to be filtered is stored.
   * @param node_atom @c NodeAtom with labels and properties to filter by.
   */
  NodeFilter(const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol,
             const NodeAtom *node_atom);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  const std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;
  const NodeAtom *node_atom_;

  class NodeFilterCursor : public Cursor {
   public:
    NodeFilterCursor(const NodeFilter &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const NodeFilter &self_;
    GraphDbAccessor &db_;
    const std::unique_ptr<Cursor> input_cursor_;

    /** Helper function for checking if the given vertex
     * passes this filter. */
    bool VertexPasses(const VertexAccessor &vertex, Frame &frame,
                      const SymbolTable &symbol_table);
  };
};

/** @brief Operator which filters edges by relationship type and properties.
 *
 *  This operator is used to implement `MATCH () -[r :label {prop: value}]- ()`,
 *  so that it filters edges with specified types and properties by value.
 */
class EdgeFilter : public LogicalOperator {
 public:
  /** @brief Construct @c EdgeFilter.
   *
   * @param input Required, preceding @c LogicalOperator.
   * @param input_symbol @c Symbol where the edge to be filtered is stored.
   * @param edge_atom @c EdgeAtom with edge types and properties to filter by.
   */
  EdgeFilter(const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol,
             const EdgeAtom *edge_atom);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  const std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;
  const EdgeAtom *edge_atom_;

  class EdgeFilterCursor : public Cursor {
   public:
    EdgeFilterCursor(const EdgeFilter &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const EdgeFilter &self_;
    GraphDbAccessor &db_;
    const std::unique_ptr<Cursor> input_cursor_;

    /** Helper function for checking if the given edge satisfied
     *  the criteria of this edge filter. */
    bool EdgePasses(const EdgeAccessor &edge, Frame &frame,
                    const SymbolTable &symbol_table);
  };
};

/**
 * @brief Filter whose Pull returns true only when the given expression
 * evaluates into true.
 *
 * The given expression is assumed to return either NULL (treated as false) or a
 * boolean value.
 */
class Filter : public LogicalOperator {
 public:
  Filter(const std::shared_ptr<LogicalOperator> &input_,
         Expression *expression_);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  const std::shared_ptr<LogicalOperator> input_;
  Expression *expression_;

  class FilterCursor : public Cursor {
   public:
    FilterCursor(const Filter &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const Filter &self_;
    GraphDbAccessor &db_;
    const std::unique_ptr<Cursor> input_cursor_;
  };
};

/**
 * @brief A logical operator that places an arbitrary number
 * if named expressions on the frame (the logical operator
 * for the RETURN clause).
 *
 * Supports optional input. When the input is provided,
 * it is Pulled from and the Produce succeeds once for
 * every input Pull (typically a MATCH/RETURN query).
 * When the input is not provided (typically a standalone
 * RETURN clause) the Produce's pull succeeds exactly once.
 */
class Produce : public LogicalOperator {
 public:
  Produce(const std::shared_ptr<LogicalOperator> &input,
          const std::vector<NamedExpression *> named_expressions);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;
  const std::vector<NamedExpression *> &named_expressions();

 private:
  const std::shared_ptr<LogicalOperator> input_;
  const std::vector<NamedExpression *> named_expressions_;

  class ProduceCursor : public Cursor {
   public:
    ProduceCursor(const Produce &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const Produce &self_;
    GraphDbAccessor &db_;
    const std::unique_ptr<Cursor> input_cursor_;
  };
};

/**
 * @brief Operator for deleting vertices and edges.
 *
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
  const std::shared_ptr<LogicalOperator> input_;
  const std::vector<Expression *> expressions_;
  // if the vertex should be detached before deletion
  // if not detached, and has connections, an error is raised
  // ignored when deleting edges
  const bool detach_;

  class DeleteCursor : public Cursor {
   public:
    DeleteCursor(const Delete &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const Delete &self_;
    GraphDbAccessor &db_;
    const std::unique_ptr<Cursor> input_cursor_;
  };
};

/**
 * @brief Logical Op for setting a single property on a single vertex or edge.
 *
 * The property value is an expression that must evaluate to some type that can
 * be stored (a TypedValue that can be converted to PropertyValue).
 */
class SetProperty : public LogicalOperator {
 public:
  SetProperty(const std::shared_ptr<LogicalOperator> &input,
              PropertyLookup *lhs, Expression *rhs);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  const std::shared_ptr<LogicalOperator> input_;
  PropertyLookup *lhs_;
  Expression *rhs_;

  class SetPropertyCursor : public Cursor {
   public:
    SetPropertyCursor(const SetProperty &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const SetProperty &self_;
    GraphDbAccessor &db_;
    const std::unique_ptr<Cursor> input_cursor_;
  };
};

/**
 * @brief Logical op for setting the whole properties set on a vertex or an
 * edge.
 *
 * The value being set is an expression that must evaluate to a vertex, edge or
 * map (literal or parameter).
 *
 * Supports setting (replacing the whole properties set with another) and
 * updating.
 */
class SetProperties : public LogicalOperator {
 public:
  /**
   * @brief Defines how setting the properties works.
   *
   * @c UPDATE means that the current property set is augmented with additional
   * ones (existing props of the same name are replaced), while @c REPLACE means
   * that the old props are discarded and replaced with new ones.
   */
  enum class Op { UPDATE, REPLACE };

  SetProperties(const std::shared_ptr<LogicalOperator> &input,
                Symbol input_symbol, Expression *rhs, Op op);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  const std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;
  Expression *rhs_;
  Op op_;

  class SetPropertiesCursor : public Cursor {
   public:
    SetPropertiesCursor(const SetProperties &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const SetProperties &self_;
    GraphDbAccessor &db_;
    const std::unique_ptr<Cursor> input_cursor_;

    /** Helper function that sets the given values on either
     * a VertexRecord or an EdgeRecord.
     * @tparam TRecordAccessor Either RecordAccessor<Vertex> or
     * RecordAccessor<Edge>
     */
    template <typename TRecordAccessor>
    void Set(TRecordAccessor &record, const TypedValue &rhs) const;
  };
};

/**
 * @brief Logical operator for setting an arbitrary number of labels on a
 * Vertex.
 *
 * It does NOT remove labels that are already set on that Vertex.
 */
class SetLabels : public LogicalOperator {
 public:
  SetLabels(const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol,
            const std::vector<GraphDbTypes::Label> &labels);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  const std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;
  const std::vector<GraphDbTypes::Label> labels_;

  class SetLabelsCursor : public Cursor {
   public:
    SetLabelsCursor(const SetLabels &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const SetLabels &self_;
    const std::unique_ptr<Cursor> input_cursor_;
  };
};

/**
 * @brief Logical op for removing a property from an
 * edge or a vertex.
 */
class RemoveProperty : public LogicalOperator {
 public:
  RemoveProperty(const std::shared_ptr<LogicalOperator> &input,
                 PropertyLookup *lhs);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  const std::shared_ptr<LogicalOperator> input_;
  PropertyLookup *lhs_;

  class RemovePropertyCursor : public Cursor {
   public:
    RemovePropertyCursor(const RemoveProperty &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const RemoveProperty &self_;
    GraphDbAccessor &db_;
    const std::unique_ptr<Cursor> input_cursor_;
  };
};

/**
 * @brief Logical operator for removing an arbitrary number of
 * labels on a Vertex.
 *
 * If a label does not exist on a Vertex, nothing happens.
 */
class RemoveLabels : public LogicalOperator {
 public:
  RemoveLabels(const std::shared_ptr<LogicalOperator> &input,
               Symbol input_symbol,
               const std::vector<GraphDbTypes::Label> &labels);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  const std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;
  const std::vector<GraphDbTypes::Label> labels_;

  class RemoveLabelsCursor : public Cursor {
   public:
    RemoveLabelsCursor(const RemoveLabels &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const RemoveLabels &self_;
    const std::unique_ptr<Cursor> input_cursor_;
  };
};

/**
 * Filter whose Pull returns true only when the given
 * expand_symbol frame value (the latest expansion) is not
 * equal to any of the previous_symbols frame values.
 *
 * Used for implementing [iso|cypher]morphism.
 * Isomorphism is vertex-uniqueness. It means that
 * two different vertices in a pattern can not map to the
 * same data vertex. For example, if the database
 * contains one vertex with a recursive relationship,
 * then the query
 * MATCH ()-[]->() combined with vertex uniqueness
 * yields no results (no uniqueness yields one).
 * Cyphermorphism is edge-uniqueness (the above
 * explanation applies). By default Neo4j uses
 * Cyphermorphism (that's where the name stems from,
 * it is not a valid graph-theory term).
 *
 * Works for both Edge and Vertex uniqueness checks
 * (provide the accessor type as a template argument).
 */
template <typename TAccessor>
class ExpandUniquenessFilter : public LogicalOperator {
 public:
  ExpandUniquenessFilter(const std::shared_ptr<LogicalOperator> &input,
                         Symbol expand_symbol,
                         const std::vector<Symbol> &previous_symbols);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  const std::shared_ptr<LogicalOperator> input_;
  Symbol expand_symbol_;
  const std::vector<Symbol> previous_symbols_;

  class ExpandUniquenessFilterCursor : public Cursor {
   public:
    ExpandUniquenessFilterCursor(const ExpandUniquenessFilter &self,
                                 GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const ExpandUniquenessFilter &self_;
    const std::unique_ptr<Cursor> input_cursor_;
  };
};

/** @brief Pulls everything from the input before passing it through.
 * Optionally advances the command after accumulation and before emitting.
 *
 * On the first Pull from this Op's Cursor the input Cursor will be
 * Pulled until it is empty. The results will be accumulated in the
 * temporary cache. Once the input Cursor is empty, this Op's Cursor
 * will start returning cached stuff from it's Pull.
 *
 * This technique is used for ensuring all the operations from the
 * previous LogicalOp have been performed before exposing data
 * to the next. A typical use-case is the `MATCH - SET - RETURN`
 * query in which every SET iteration must be performed before
 * RETURN starts iterating (see Memgraph Wiki for detailed reasoning).
 *
 * IMPORTANT: This Op does not cache all the results but only those
 * elements from the frame whose symbols (frame positions) it was given.
 * All other frame positions will contain undefined junk after this
 * op has executed, and should not be used.
 *
 * This op can also advance the command after the accumulation and
 * before emitting. If the command gets advanced, every value that
 * has been cached will be reconstructed before Pull returns.
 *
 * @param input Input @c LogicalOperator.
 * @param symbols A vector of Symbols that need to be accumulated
 *  and exposed to the next op.
 */
class Accumulate : public LogicalOperator {
 public:
  Accumulate(const std::shared_ptr<LogicalOperator> &input,
             const std::vector<Symbol> &symbols, bool advance_command = false);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

  const auto &symbols() const { return symbols_; };

 private:
  const std::shared_ptr<LogicalOperator> input_;
  const std::vector<Symbol> symbols_;
  const bool advance_command_;

  class AccumulateCursor : public Cursor {
   public:
    AccumulateCursor(const Accumulate &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const Accumulate &self_;
    GraphDbAccessor &db_;
    const std::unique_ptr<Cursor> input_cursor_;
    std::list<std::list<TypedValue>> cache_;
    decltype(cache_.begin()) cache_it_ = cache_.begin();
    bool pulled_all_input_{false};
  };
};

/** @brief Performs an arbitrary number of aggregations of data
 * from the given input grouped by the given criteria.
 *
 * Aggregations are defined by triples that define
 * (input data expression, type of aggregation, output symbol).
 * Input data is grouped based on the given set of named
 * expressions. Grouping is done on unique values.
 *
 * IMPORTANT:
 * Ops taking their input from an aggregation are only
 * allowed to use frame values that are either aggregation
 * outputs or group-by named-expressions. All other frame
 * elements are in an undefined state after aggregation.
 */
class Aggregate : public LogicalOperator {
 public:
  /** @brief An aggregation element, contains:
   * (input data expression, type of aggregation, output symbol).
   */
  using Element = std::tuple<Expression *, Aggregation::Op, Symbol>;

  Aggregate(const std::shared_ptr<LogicalOperator> &input,
            const std::vector<Element> &aggregations,
            const std::vector<Expression *> &group_by,
            const std::vector<Symbol> &remember);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

  const auto &aggregations() const { return aggregations_; }
  const auto &group_by() const { return group_by_; }

 private:
  const std::shared_ptr<LogicalOperator> input_;
  const std::vector<Element> aggregations_;
  const std::vector<Expression *> group_by_;
  const std::vector<Symbol> remember_;

  class AggregateCursor : public Cursor {
   public:
    AggregateCursor(Aggregate &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    // custom equality function for the unordered map
    struct TypedValueListEqual {
      bool operator()(const std::list<TypedValue> &left,
                      const std::list<TypedValue> &right) const;
    };

    // Data structure for a single aggregation cache.
    // does NOT include the group-by values since those
    // are a key in the aggregation map.
    // The vectors in an AggregationValue contain one element
    // for each aggregation in this LogicalOp.
    struct AggregationValue {
      // how many input rows has been aggregated in respective
      // values_ element so far
      std::vector<int> counts_;
      // aggregated values. Initially Null (until at least one
      // input row with a valid value gets processed)
      std::vector<TypedValue> values_;
      // remember values.
      std::vector<TypedValue> remember_;
    };

    const Aggregate &self_;
    GraphDbAccessor &db_;
    const std::unique_ptr<Cursor> input_cursor_;
    // storage for aggregated data
    // map key is the list of group-by values
    // map value is an AggregationValue struct
    std::unordered_map<
        std::list<TypedValue>, AggregationValue,
        // use FNV collection hashing specialized for a list of TypedValues
        FnvCollection<std::list<TypedValue>, TypedValue, TypedValue::Hash>,
        // custom equality
        TypedValueListEqual>
        aggregation_;
    // iterator over the accumulated cache
    decltype(aggregation_.begin()) aggregation_it_ = aggregation_.begin();
    // this LogicalOp pulls all from the input on it's first pull
    // this switch tracks if this has been performed
    bool pulled_all_input_{false};

    /**
     * Pulls from the input operator until exhausted and aggregates the
     * results. If the input operator is not provided, a single call
     * to ProcessOne is issued.
     *
     * Accumulation automatically groups the results so that `aggregation_`
     * cache cardinality depends on number of
     * aggregation results, and not on the number of inputs.
     */
    void ProcessAll(Frame &frame, const SymbolTable &symbol_table);

    /**
     * Performs a single accumulation.
     */
    void ProcessOne(Frame &frame, const SymbolTable &symbolTable,
                    ExpressionEvaluator &evaluator);

    /** Ensures the new AggregationValue has been initialized. This means
     * that the value vectors are filled with an appropriate number of Nulls,
     * counts are set to 0 and remember values are remembered.
     */
    void EnsureInitialized(Frame &frame, AggregationValue &agg_value) const;

    /** Updates the given AggregationValue with new data. Assumes that
     * the AggregationValue has been initialized */
    void Update(Frame &frame, const SymbolTable &symbol_table,
                ExpressionEvaluator &evaluator, AggregationValue &agg_value);

    /** Checks if the given TypedValue is legal in MIN and MAX. If not
     * an appropriate exception is thrown. */
    void EnsureOkForMinMax(const TypedValue &value) const;

    /** Checks if the given TypedValue is legal in AVG and SUM. If not
     * an appropriate exception is thrown. */
    void EnsureOkForAvgSum(const TypedValue &value) const;
  };
};

/** @brief Skips a number of Pulls from the input op.
 *
 * The given expression determines how many Pulls from the input
 * should be skipped (ignored).
 * All other successful Pulls from the
 * input are simply passed through.
 *
 * The given expression is evaluated after the first Pull from
 * the input, and only once. Neo does not allow this expression
 * to contain identifiers, and neither does Memgraph, but this
 * operator's implementation does not expect this.
 */
class Skip : public LogicalOperator {
 public:
  Skip(const std::shared_ptr<LogicalOperator> &input, Expression *expression);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  const std::shared_ptr<LogicalOperator> input_;
  Expression *expression_;

  class SkipCursor : public Cursor {
   public:
    SkipCursor(Skip &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const Skip &self_;
    GraphDbAccessor &db_;
    const std::unique_ptr<Cursor> input_cursor_;
    // init to_skip_ to -1, indicating
    // that it's still unknown (input has not been Pulled yet)
    int to_skip_{-1};
    int skipped_{0};
  };
};

/** @brief Limits the number of Pulls from the input op.
 *
 * The given expression determines how many
 * input Pulls should be passed through. The input is not
 * Pulled once this limit is reached. Note that this has
 * implications: the out-of-bounds input Pulls are never
 * evaluated.
 *
 * The limit expression must NOT use anything from the
 * Frame. It is evaluated before the first Pull from the
 * input. This is consistent with Neo (they don't allow
 * identifiers in limit expressions), and it's necessary
 * when limit evaluates to 0 (because 0 Pulls from the
 * input should be performed).
 */
class Limit : public LogicalOperator {
 public:
  Limit(const std::shared_ptr<LogicalOperator> &input, Expression *expression);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

 private:
  const std::shared_ptr<LogicalOperator> input_;
  Expression *expression_;

  class LimitCursor : public Cursor {
   public:
    LimitCursor(Limit &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    Limit &self_;
    GraphDbAccessor &db_;
    std::unique_ptr<Cursor> input_cursor_;
    // init limit_ to -1, indicating
    // that it's still unknown (Cursor has not been Pulled yet)
    int limit_{-1};
    int pulled_{0};
  };
};

/** @brief Logical operator for ordering (sorting) results.
 *
 * Sorts the input rows based on an arbitrary number of
 * Expressions. Ascending or descending ordering can be chosen
 * for each independently (not providing enough orderings
 * results in a runtime error).
 *
 * For each row an arbitrary number of Frame elements can be
 * remembered. Only these elements (defined by their Symbols)
 * are valid for usage after the OrderBy operator.
 */
class OrderBy : public LogicalOperator {
 public:
  OrderBy(const std::shared_ptr<LogicalOperator> &input,
          const std::vector<std::pair<Ordering, Expression *>> &order_by,
          const std::vector<Symbol> &output_symbols);
  void Accept(LogicalOperatorVisitor &visitor) override;
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor &db) override;

  const auto &output_symbols() const { return output_symbols_; }

 private:
  // custom Comparator type for comparing lists of TypedValues
  // does lexicographical ordering of elements based on the above
  // defined TypedValueCompare, and also accepts a vector of Orderings
  // the define how respective elements compare
  class TypedValueListCompare {
   public:
    TypedValueListCompare() {}
    TypedValueListCompare(const std::vector<Ordering> &ordering)
        : ordering_(ordering) {}
    bool operator()(const std::list<TypedValue> &c1,
                    const std::list<TypedValue> &c2) const;

   private:
    std::vector<Ordering> ordering_;
  };

  const std::shared_ptr<LogicalOperator> input_;
  TypedValueListCompare compare_;
  std::vector<Expression *> order_by_;
  const std::vector<Symbol> output_symbols_;

  // custom comparison for TypedValue objects
  // behaves generally like Neo's ORDER BY comparison operator:
  //  - null is greater than anything else
  //  - primitives compare naturally, only implicit cast is int->double
  //  - (list, map, path, vertex, edge) can't compare to anything
  static bool TypedValueCompare(const TypedValue &a, const TypedValue &b);

  class OrderByCursor : public Cursor {
   public:
    OrderByCursor(OrderBy &self, GraphDbAccessor &db);
    bool Pull(Frame &frame, const SymbolTable &symbol_table) override;
    void Reset() override;

   private:
    const OrderBy &self_;
    GraphDbAccessor &db_;
    const std::unique_ptr<Cursor> input_cursor_;
    bool did_pull_all_{false};
    // a cache of elements pulled from the input
    // first pair element is the order-by list
    // second pair is the remember list
    // the cache is filled and sorted (only on first pair elem) on first Pull
    std::vector<std::pair<std::list<TypedValue>, std::list<TypedValue>>> cache_;
    // iterator over the cache_, maintains state between Pulls
    decltype(cache_.begin()) cache_it_ = cache_.begin();
  };
};

}  // namespace plan
}  // namespace query
