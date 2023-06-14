// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "query/common.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/id_types.hpp"
#include "utils/bound.hpp"
#include "utils/fnv.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/visitor.hpp"

namespace memgraph {

namespace query {

struct ExecutionContext;
class ExpressionEvaluator;
class Frame;
class SymbolTable;

namespace plan {

/// Base class for iteration cursors of @c LogicalOperator classes.
///
/// Each @c LogicalOperator must produce a concrete @c Cursor, which provides
/// the iteration mechanism.
class Cursor {
 public:
  /// Run an iteration of a @c LogicalOperator.
  ///
  /// Since operators may be chained, the iteration may pull results from
  /// multiple operators.
  ///
  /// @param Frame May be read from or written to while performing the
  ///     iteration.
  /// @param ExecutionContext Used to get the position of symbols in frame and
  ///     other information.
  ///
  /// @throws QueryRuntimeException if something went wrong with execution
  virtual bool Pull(Frame &, ExecutionContext &) = 0;

  /// Resets the Cursor to its initial state.
  virtual void Reset() = 0;

  /// Perform cleanup which may throw an exception
  virtual void Shutdown() = 0;

  virtual ~Cursor() {}
};

/// unique_ptr to Cursor managed with a custom deleter.
/// This allows us to use utils::MemoryResource for allocation.
using UniqueCursorPtr = std::unique_ptr<Cursor, std::function<void(Cursor *)>>;

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

class Once;
class CreateNode;
class CreateExpand;
class ScanAll;
class ScanAllByLabel;
class ScanAllByLabelPropertyRange;
class ScanAllByLabelPropertyValue;
class ScanAllByLabelProperty;
class ScanAllById;
class Expand;
class ExpandVariable;
class ConstructNamedPath;
class Filter;
class Produce;
class Delete;
class SetProperty;
class SetProperties;
class SetLabels;
class RemoveProperty;
class RemoveLabels;
class EdgeUniquenessFilter;
class Accumulate;
class Aggregate;
class Skip;
class Limit;
class OrderBy;
class Merge;
class Optional;
class Unwind;
class Distinct;
class Union;
class Cartesian;
class CallProcedure;
class LoadCsv;
class Foreach;
class EmptyResult;
class EvaluatePatternFilter;
class Apply;

using LogicalOperatorCompositeVisitor =
    utils::CompositeVisitor<Once, CreateNode, CreateExpand, ScanAll, ScanAllByLabel, ScanAllByLabelPropertyRange,
                            ScanAllByLabelPropertyValue, ScanAllByLabelProperty, ScanAllById, Expand, ExpandVariable,
                            ConstructNamedPath, Filter, Produce, Delete, SetProperty, SetProperties, SetLabels,
                            RemoveProperty, RemoveLabels, EdgeUniquenessFilter, Accumulate, Aggregate, Skip, Limit,
                            OrderBy, Merge, Optional, Unwind, Distinct, Union, Cartesian, CallProcedure, LoadCsv,
                            Foreach, EmptyResult, EvaluatePatternFilter, Apply>;

using LogicalOperatorLeafVisitor = utils::LeafVisitor<Once>;

/**
 * @brief Base class for hierarchical visitors of @c LogicalOperator class
 * hierarchy.
 */
class HierarchicalLogicalOperatorVisitor : public LogicalOperatorCompositeVisitor, public LogicalOperatorLeafVisitor {
 public:
  using LogicalOperatorCompositeVisitor::PostVisit;
  using LogicalOperatorCompositeVisitor::PreVisit;
  using LogicalOperatorLeafVisitor::Visit;
  using typename LogicalOperatorLeafVisitor::ReturnType;
};

/// Base class for logical operators.
///
/// Each operator describes an operation, which is to be performed on the
/// database. Operators are iterated over using a @c Cursor. Various operators
/// can serve as inputs to others and thus a sequence of operations is formed.
class LogicalOperator : public utils::Visitable<HierarchicalLogicalOperatorVisitor> {
 public:
  static const utils::TypeInfo kType;
  virtual const utils::TypeInfo &GetTypeInfo() const { return kType; }

  virtual ~LogicalOperator() {}

  /** Construct a @c Cursor which is used to run this operator.
   *
   * @param utils::MemoryResource Memory resource used for allocations during
   *     the lifetime of the returned Cursor.
   */
  virtual UniqueCursorPtr MakeCursor(utils::MemoryResource *) const = 0;

  /** Return @c Symbol vector where the query results will be stored.
   *
   * Currently, output symbols are generated in @c Produce @c Union and
   * @c CallProcedure operators. @c Skip, @c Limit, @c OrderBy and @c Distinct
   * propagate the symbols from @c Produce (if it exists as input operator).
   *
   *  @param SymbolTable used to find symbols for expressions.
   *  @return std::vector<Symbol> used for results.
   */
  virtual std::vector<Symbol> OutputSymbols(const SymbolTable &) const { return std::vector<Symbol>(); }

  /**
   * Symbol vector whose values are modified by this operator sub-tree.
   *
   * This is different than @c OutputSymbols, because it returns all of the
   * modified symbols, including those that may not be returned as the
   * result of the query. Note that the modified symbols will not contain
   * those that should not be read after the operator is processed.
   *
   * For example, `MATCH (n)-[e]-(m) RETURN n AS l` will generate `ScanAll (n) >
   * Expand (e, m) > Produce (l)`. The modified symbols on Produce sub-tree will
   * be `l`, the same as output symbols, because it isn't valid to read `n`, `e`
   * nor `m` after Produce. On the other hand, modified symbols from Expand
   * contain `e` and `m`, as well as `n`, while output symbols are empty.
   * Modified symbols from ScanAll contain only `n`, while output symbols are
   * also empty.
   */
  virtual std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const = 0;

  /**
   * Returns true if the operator takes only one input operator.
   * NOTE: When this method returns true, you may use `input` and `set_input`
   * methods.
   */
  virtual bool HasSingleInput() const = 0;

  /**
   * Returns the input operator if it has any.
   * NOTE: This should only be called if `HasSingleInput() == true`.
   */
  virtual std::shared_ptr<LogicalOperator> input() const = 0;
  /**
   * Set a different input on this operator.
   * NOTE: This should only be called if `HasSingleInput() == true`.
   */
  virtual void set_input(std::shared_ptr<LogicalOperator>) = 0;

  struct SaveHelper {
    std::vector<LogicalOperator *> saved_ops;
  };

  struct LoadHelper {
    AstStorage ast_storage;
    std::vector<std::pair<uint64_t, std::shared_ptr<LogicalOperator>>> loaded_ops;
  };

  struct SlkLoadHelper {
    AstStorage ast_storage;
    std::vector<std::shared_ptr<LogicalOperator>> loaded_ops;
  };

  virtual std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const = 0;
};

/// A logical operator whose Cursor returns true on the first Pull
/// and false on every following Pull.
class Once : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Once(std::vector<Symbol> symbols = {}) : symbols_{std::move(symbols)} {}
  DEFVISITABLE(HierarchicalLogicalOperatorVisitor);
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override { return symbols_; }

  bool HasSingleInput() const override;
  std::shared_ptr<LogicalOperator> input() const override;
  void set_input(std::shared_ptr<LogicalOperator>) override;

  std::vector<Symbol> symbols_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Once>();
    object->symbols_ = symbols_;
    return object;
  }

 private:
  class OnceCursor : public Cursor {
   public:
    OnceCursor() {}
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    bool did_pull_{false};
  };
};

using PropertiesMapList = std::vector<std::pair<storage::PropertyId, Expression *>>;

struct NodeCreationInfo {
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const { return kType; }

  NodeCreationInfo() = default;

  NodeCreationInfo(Symbol symbol, std::vector<storage::LabelId> labels,
                   std::variant<PropertiesMapList, ParameterLookup *> properties)
      : symbol{std::move(symbol)}, labels{std::move(labels)}, properties{std::move(properties)} {};

  NodeCreationInfo(Symbol symbol, std::vector<storage::LabelId> labels, PropertiesMapList properties)
      : symbol{std::move(symbol)}, labels{std::move(labels)}, properties{std::move(properties)} {};

  NodeCreationInfo(Symbol symbol, std::vector<storage::LabelId> labels, ParameterLookup *properties)
      : symbol{std::move(symbol)}, labels{std::move(labels)}, properties{properties} {};

  Symbol symbol;
  std::vector<storage::LabelId> labels;
  std::variant<PropertiesMapList, ParameterLookup *> properties;

  NodeCreationInfo Clone(AstStorage *storage) const {
    NodeCreationInfo object;
    object.symbol = symbol;
    object.labels = labels;
    if (const auto *props = std::get_if<PropertiesMapList>(&properties)) {
      auto &destination_props = std::get<PropertiesMapList>(object.properties);
      destination_props.resize(props->size());
      for (auto i0 = 0; i0 < props->size(); ++i0) {
        {
          storage::PropertyId first1 = (*props)[i0].first;
          Expression *second2;
          second2 = (*props)[i0].second ? (*props)[i0].second->Clone(storage) : nullptr;
          destination_props[i0] = std::make_pair(std::move(first1), std::move(second2));
        }
      }
    } else {
      object.properties = std::get<ParameterLookup *>(properties)->Clone(storage);
    }
    return object;
  }
};

/// Operator for creating a node.
///
/// This op is used both for creating a single node (`CREATE` statement without
/// a preceding `MATCH`), or multiple nodes (`MATCH ... CREATE` or
/// `CREATE (), () ...`).
///
/// @sa CreateExpand
class CreateNode : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  CreateNode() {}

  /**
   * @param input Optional. If @c nullptr, then a single node will be
   *    created (a single successful @c Cursor::Pull from this op's @c Cursor).
   *    If a valid input, then a node will be created for each
   *    successful pull from the given input.
   * @param node_info @c NodeCreationInfo
   */
  CreateNode(const std::shared_ptr<LogicalOperator> &input, const NodeCreationInfo &node_info);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  memgraph::query::plan::NodeCreationInfo node_info_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<CreateNode>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->node_info_ = node_info_.Clone(storage);
    return object;
  }

 private:
  class CreateNodeCursor : public Cursor {
   public:
    CreateNodeCursor(const CreateNode &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const CreateNode &self_;
    const UniqueCursorPtr input_cursor_;
  };
};

struct EdgeCreationInfo {
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const { return kType; }

  EdgeCreationInfo() = default;

  EdgeCreationInfo(Symbol symbol, std::variant<PropertiesMapList, ParameterLookup *> properties,
                   storage::EdgeTypeId edge_type, EdgeAtom::Direction direction)
      : symbol{std::move(symbol)}, properties{std::move(properties)}, edge_type{edge_type}, direction{direction} {};

  EdgeCreationInfo(Symbol symbol, PropertiesMapList properties, storage::EdgeTypeId edge_type,
                   EdgeAtom::Direction direction)
      : symbol{std::move(symbol)}, properties{std::move(properties)}, edge_type{edge_type}, direction{direction} {};

  EdgeCreationInfo(Symbol symbol, ParameterLookup *properties, storage::EdgeTypeId edge_type,
                   EdgeAtom::Direction direction)
      : symbol{std::move(symbol)}, properties{properties}, edge_type{edge_type}, direction{direction} {};

  Symbol symbol;
  std::variant<PropertiesMapList, ParameterLookup *> properties;
  storage::EdgeTypeId edge_type;
  EdgeAtom::Direction direction{EdgeAtom::Direction::BOTH};

  EdgeCreationInfo Clone(AstStorage *storage) const {
    EdgeCreationInfo object;
    object.symbol = symbol;
    if (const auto *props = std::get_if<PropertiesMapList>(&properties)) {
      auto &destination_props = std::get<PropertiesMapList>(object.properties);
      destination_props.resize(props->size());
      for (auto i0 = 0; i0 < props->size(); ++i0) {
        {
          storage::PropertyId first1 = (*props)[i0].first;
          Expression *second2;
          second2 = (*props)[i0].second ? (*props)[i0].second->Clone(storage) : nullptr;
          destination_props[i0] = std::make_pair(std::move(first1), std::move(second2));
        }
      }
    } else {
      object.properties = std::get<ParameterLookup *>(properties)->Clone(storage);
    }
    object.edge_type = edge_type;
    object.direction = direction;
    return object;
  }
};

/// Operator for creating edges and destination nodes.
///
/// This operator extends already created nodes with an edge. If the other node
/// on the edge does not exist, it will be created. For example, in `MATCH (n)
/// CREATE (n) -[r:r]-> (n)` query, this operator will create just the edge `r`.
/// In `MATCH (n) CREATE (n) -[r:r]-> (m)` query, the operator will create both
/// the edge `r` and the node `m`. In case of `CREATE (n) -[r:r]-> (m)` the
/// first node `n` is created by @c CreateNode operator, while @c CreateExpand
/// will create the edge `r` and `m`. Similarly, multiple @c CreateExpand are
/// chained in cases when longer paths need creating.
///
/// @sa CreateNode
class CreateExpand : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  CreateExpand() {}

  /** @brief Construct @c CreateExpand.
   *
   * @param node_info @c NodeCreationInfo at the end of the edge.
   *     Used to create a node, unless it refers to an existing one.
   * @param edge_info @c EdgeCreationInfo for the edge to be created.
   * @param input Optional. Previous @c LogicalOperator which will be pulled.
   *     For each successful @c Cursor::Pull, this operator will create an
   *     expansion.
   * @param input_symbol @c Symbol for the node at the start of the edge.
   * @param existing_node @c bool indicating whether the @c node_atom refers to
   *     an existing node. If @c false, the operator will also create the node.
   */
  CreateExpand(const NodeCreationInfo &node_info, const EdgeCreationInfo &edge_info,
               const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol, bool existing_node);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  memgraph::query::plan::NodeCreationInfo node_info_;
  memgraph::query::plan::EdgeCreationInfo edge_info_;
  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  Symbol input_symbol_;
  /// if the given node atom refers to an existing node (either matched or created)
  bool existing_node_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<CreateExpand>();
    object->node_info_ = node_info_.Clone(storage);
    object->edge_info_ = edge_info_.Clone(storage);
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->input_symbol_ = input_symbol_;
    object->existing_node_ = existing_node_;
    return object;
  }

 private:
  class CreateExpandCursor : public Cursor {
   public:
    CreateExpandCursor(const CreateExpand &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const CreateExpand &self_;
    const UniqueCursorPtr input_cursor_;

    // Get the existing node (if existing_node_ == true), or create a new node
    VertexAccessor &OtherVertex(Frame &frame, ExecutionContext &context);
  };
};

/// Operator which iterates over all the nodes currently in the database.
/// When given an input (optional), does a cartesian product.
///
/// It accepts an optional input. If provided then this op scans all the nodes
/// currently in the database for each successful Pull from it's input, thereby
/// producing a cartesian product of input Pulls and database elements.
///
/// ScanAll can either iterate over the previous graph state (state before
/// the current transacton+command) or over current state. This is controlled
/// with a constructor argument.
///
/// @sa ScanAllByLabel
/// @sa ScanAllByLabelPropertyRange
/// @sa ScanAllByLabelPropertyValue
class ScanAll : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  ScanAll() {}
  ScanAll(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol, storage::View view = storage::View::OLD);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  Symbol output_symbol_;
  /// Controls which graph state is used to produce vertices.
  ///
  /// If @c storage::View::OLD, @c ScanAll will produce vertices visible in the
  /// previous graph state, before modifications done by current transaction &
  /// command. With @c storage::View::NEW, all vertices will be produced the current
  /// transaction sees along with their modifications.
  storage::View view_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<ScanAll>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->output_symbol_ = output_symbol_;
    object->view_ = view_;
    return object;
  }
};

/// Behaves like @c ScanAll, but this operator produces only vertices with
/// given label.
///
/// @sa ScanAll
/// @sa ScanAllByLabelPropertyRange
/// @sa ScanAllByLabelPropertyValue
class ScanAllByLabel : public memgraph::query::plan::ScanAll {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  ScanAllByLabel() {}
  ScanAllByLabel(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol, storage::LabelId label,
                 storage::View view = storage::View::OLD);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;

  storage::LabelId label_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<ScanAllByLabel>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->output_symbol_ = output_symbol_;
    object->view_ = view_;
    object->label_ = label_;
    return object;
  }
};

/// Behaves like @c ScanAll, but produces only vertices with given label and
/// property value which is inside a range (inclusive or exclusive).
///
/// @sa ScanAll
/// @sa ScanAllByLabel
/// @sa ScanAllByLabelPropertyValue
class ScanAllByLabelPropertyRange : public memgraph::query::plan::ScanAll {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  /** Bound with expression which when evaluated produces the bound value. */
  using Bound = utils::Bound<Expression *>;
  ScanAllByLabelPropertyRange() {}
  /**
   * Constructs the operator for given label and property value in range
   * (inclusive).
   *
   * Range bounds are optional, but only one bound can be left out.
   *
   * @param input Preceding operator which will serve as the input.
   * @param output_symbol Symbol where the vertices will be stored.
   * @param label Label which the vertex must have.
   * @param property Property from which the value will be looked up from.
   * @param lower_bound Optional lower @c Bound.
   * @param upper_bound Optional upper @c Bound.
   * @param view storage::View used when obtaining vertices.
   */
  ScanAllByLabelPropertyRange(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol,
                              storage::LabelId label, storage::PropertyId property, const std::string &property_name,
                              std::optional<Bound> lower_bound, std::optional<Bound> upper_bound,
                              storage::View view = storage::View::OLD);

  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;

  storage::LabelId label_;
  storage::PropertyId property_;
  std::string property_name_;
  std::optional<Bound> lower_bound_;
  std::optional<Bound> upper_bound_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<ScanAllByLabelPropertyRange>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->output_symbol_ = output_symbol_;
    object->view_ = view_;
    object->label_ = label_;
    object->property_ = property_;
    object->property_name_ = property_name_;
    if (lower_bound_) {
      object->lower_bound_.emplace(
          utils::Bound<Expression *>(lower_bound_->value()->Clone(storage), lower_bound_->type()));
    } else {
      object->lower_bound_ = std::nullopt;
    }
    if (upper_bound_) {
      object->upper_bound_.emplace(
          utils::Bound<Expression *>(upper_bound_->value()->Clone(storage), upper_bound_->type()));
    } else {
      object->upper_bound_ = std::nullopt;
    }
    return object;
  }
};

/// Behaves like @c ScanAll, but produces only vertices with given label and
/// property value.
///
/// @sa ScanAll
/// @sa ScanAllByLabel
/// @sa ScanAllByLabelPropertyRange
class ScanAllByLabelPropertyValue : public memgraph::query::plan::ScanAll {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  ScanAllByLabelPropertyValue() {}
  /**
   * Constructs the operator for given label and property value.
   *
   * @param input Preceding operator which will serve as the input.
   * @param output_symbol Symbol where the vertices will be stored.
   * @param label Label which the vertex must have.
   * @param property Property from which the value will be looked up from.
   * @param expression Expression producing the value of the vertex property.
   * @param view storage::View used when obtaining vertices.
   */
  ScanAllByLabelPropertyValue(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol,
                              storage::LabelId label, storage::PropertyId property, const std::string &property_name,
                              Expression *expression, storage::View view = storage::View::OLD);

  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;

  storage::LabelId label_;
  storage::PropertyId property_;
  std::string property_name_;
  Expression *expression_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<ScanAllByLabelPropertyValue>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->output_symbol_ = output_symbol_;
    object->view_ = view_;
    object->label_ = label_;
    object->property_ = property_;
    object->property_name_ = property_name_;
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    return object;
  }
};

/// Behaves like @c ScanAll, but this operator produces only vertices with
/// given label and property.
///
/// @sa ScanAll
/// @sa ScanAllByLabelPropertyRange
/// @sa ScanAllByLabelPropertyValue
class ScanAllByLabelProperty : public memgraph::query::plan::ScanAll {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  ScanAllByLabelProperty() {}
  ScanAllByLabelProperty(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol, storage::LabelId label,
                         storage::PropertyId property, const std::string &property_name,
                         storage::View view = storage::View::OLD);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;

  storage::LabelId label_;
  storage::PropertyId property_;
  std::string property_name_;
  Expression *expression_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<ScanAllByLabelProperty>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->output_symbol_ = output_symbol_;
    object->view_ = view_;
    object->label_ = label_;
    object->property_ = property_;
    object->property_name_ = property_name_;
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    return object;
  }
};

/// ScanAll producing a single node with ID equal to evaluated expression
class ScanAllById : public memgraph::query::plan::ScanAll {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  ScanAllById() {}
  ScanAllById(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol, Expression *expression,
              storage::View view = storage::View::OLD);

  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;

  Expression *expression_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<ScanAllById>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->output_symbol_ = output_symbol_;
    object->view_ = view_;
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    return object;
  }
};

struct ExpandCommon {
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const { return kType; }

  /// Symbol pointing to the node to be expanded.
  /// This is where the new node will be stored.
  Symbol node_symbol;
  /// Symbol for the edges to be expanded.
  /// This is where a TypedValue containing a list of expanded edges will be stored.
  Symbol edge_symbol;
  /// EdgeAtom::Direction determining the direction of edge
  /// expansion. The direction is relative to the starting vertex for each expansion.
  EdgeAtom::Direction direction;
  /// storage::EdgeTypeId specifying which edges we want
  /// to expand. If empty, all edges are valid. If not empty, only edges with one of
  /// the given types are valid.
  std::vector<storage::EdgeTypeId> edge_types;
  /// If the given node atom refer to a symbol
  /// that has already been expanded and should be just validated in the frame.
  bool existing_node;
};

/// Expansion operator. For a node existing in the frame it
/// expands one edge and one node and places them on the frame.
///
/// This class does not handle node/edge filtering based on
/// properties, labels and edge types. However, it does handle
/// filtering on existing node / edge.
///
/// Filtering on existing means that for a pattern that references
/// an already declared node or edge (for example in
/// MATCH (a) MATCH (a)--(b)),
/// only expansions that match defined equalities are successfully
/// pulled.
class Expand : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  /**
   * Creates an expansion. All parameters except input and input_symbol are
   * forwarded to @c ExpandCommon and are documented there.
   *
   * @param input Optional logical operator that precedes this one.
   * @param input_symbol Symbol that points to a VertexAccessor in the frame
   *    that expansion should emanate from.
   */
  Expand(const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol, Symbol node_symbol, Symbol edge_symbol,
         EdgeAtom::Direction direction, const std::vector<storage::EdgeTypeId> &edge_types, bool existing_node,
         storage::View view);

  Expand() {}

  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  class ExpandCursor : public Cursor {
   public:
    ExpandCursor(const Expand &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    using InEdgeT = std::remove_reference_t<decltype(*std::declval<VertexAccessor>().InEdges(storage::View::OLD))>;
    using InEdgeIteratorT = decltype(std::declval<InEdgeT>().begin());
    using OutEdgeT = std::remove_reference_t<decltype(*std::declval<VertexAccessor>().OutEdges(storage::View::OLD))>;
    using OutEdgeIteratorT = decltype(std::declval<OutEdgeT>().begin());

    const Expand &self_;
    const UniqueCursorPtr input_cursor_;

    // The iterable over edges and the current edge iterator are referenced via
    // optional because they can not be initialized in the constructor of
    // this class. They are initialized once for each pull from the input.
    std::optional<InEdgeT> in_edges_;
    std::optional<InEdgeIteratorT> in_edges_it_;
    std::optional<OutEdgeT> out_edges_;
    std::optional<OutEdgeIteratorT> out_edges_it_;

    bool InitEdges(Frame &, ExecutionContext &);
  };

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  Symbol input_symbol_;
  memgraph::query::plan::ExpandCommon common_;
  /// State from which the input node should get expanded.
  storage::View view_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Expand>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->input_symbol_ = input_symbol_;
    object->common_ = common_;
    object->view_ = view_;
    return object;
  }
};

struct ExpansionLambda {
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const { return kType; }

  /// Currently expanded edge symbol.
  Symbol inner_edge_symbol;
  /// Currently expanded node symbol.
  Symbol inner_node_symbol;
  /// Expression used in lambda during expansion.
  Expression *expression;

  ExpansionLambda Clone(AstStorage *storage) const {
    ExpansionLambda object;
    object.inner_edge_symbol = inner_edge_symbol;
    object.inner_node_symbol = inner_node_symbol;
    object.expression = expression ? expression->Clone(storage) : nullptr;
    return object;
  }
};

/// Variable-length expansion operator. For a node existing in
/// the frame it expands a variable number of edges and places them
/// (in a list-type TypedValue), as well as the final destination node,
/// on the frame.
///
/// This class does not handle node/edge filtering based on
/// properties, labels and edge types. However, it does handle
/// filtering on existing node / edge. Additionally it handles's
/// edge-uniquess (cyphermorphism) because it's not feasible to do
/// later.
///
/// Filtering on existing means that for a pattern that references
/// an already declared node or edge (for example in
/// MATCH (a) MATCH (a)--(b)),
/// only expansions that match defined equalities are succesfully
/// pulled.
class ExpandVariable : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  ExpandVariable() {}

  /**
   * Creates a variable-length expansion. Most params are forwarded
   * to the @c ExpandCommon constructor, and are documented there.
   *
   * Expansion length bounds are both inclusive (as in Neo's Cypher
   * implementation).
   *
   * @param input Optional logical operator that precedes this one.
   * @param input_symbol Symbol that points to a VertexAccessor in the frame
   *    that expansion should emanate from.
   * @param type - Either Type::DEPTH_FIRST (default variable-length expansion),
   * or Type::BREADTH_FIRST.
   * @param is_reverse Set to `true` if the edges written on frame should expand
   *    from `node_symbol` to `input_symbol`. Opposed to the usual expanding
   *    from `input_symbol` to `node_symbol`.
   * @param lower_bound An optional indicator of the minimum number of edges
   *    that get expanded (inclusive).
   * @param upper_bound An optional indicator of the maximum number of edges
   *    that get expanded (inclusive).
   * @param inner_edge_symbol Like `inner_node_symbol`
   * @param inner_node_symbol For each expansion the node expanded into is
   *    assigned to this symbol so it can be evaluated by the 'where'
   * expression.
   * @param filter_ The filter that must be satisfied for an expansion to
   * succeed. Can use inner(node/edge) symbols. If nullptr, it is ignored.
   */
  ExpandVariable(const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol, Symbol node_symbol,
                 Symbol edge_symbol, EdgeAtom::Type type, EdgeAtom::Direction direction,
                 const std::vector<storage::EdgeTypeId> &edge_types, bool is_reverse, Expression *lower_bound,
                 Expression *upper_bound, bool existing_node, ExpansionLambda filter_lambda,
                 std::optional<ExpansionLambda> weight_lambda, std::optional<Symbol> total_weight);

  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  Symbol input_symbol_;
  memgraph::query::plan::ExpandCommon common_;
  EdgeAtom::Type type_;
  /// True if the path should be written as expanding from node_symbol to input_symbol.
  bool is_reverse_;
  /// Optional lower bound of the variable length expansion, defaults are (1, inf)
  Expression *lower_bound_;
  /// Optional upper bound of the variable length expansion, defaults are (1, inf)
  Expression *upper_bound_;
  memgraph::query::plan::ExpansionLambda filter_lambda_;
  std::optional<memgraph::query::plan::ExpansionLambda> weight_lambda_;
  std::optional<Symbol> total_weight_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<ExpandVariable>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->input_symbol_ = input_symbol_;
    object->common_ = common_;
    object->type_ = type_;
    object->is_reverse_ = is_reverse_;
    object->lower_bound_ = lower_bound_ ? lower_bound_->Clone(storage) : nullptr;
    object->upper_bound_ = upper_bound_ ? upper_bound_->Clone(storage) : nullptr;
    object->filter_lambda_ = filter_lambda_.Clone(storage);
    if (weight_lambda_) {
      memgraph::query::plan::ExpansionLambda value0;
      value0 = (*weight_lambda_).Clone(storage);
      object->weight_lambda_.emplace(std::move(value0));
    } else {
      object->weight_lambda_ = std::nullopt;
    }
    object->total_weight_ = total_weight_;
    return object;
  }

 private:
  // the Cursors are not declared in the header because
  // it's edges_ and edges_it_ are decltyped using a helper function
  // that should be inaccessible (private class function won't compile)
  friend class ExpandVariableCursor;
  friend class ExpandWeightedShortestPathCursor;
  friend class ExpandAllShortestPathCursor;
};

/// Constructs a named path from its elements and places it on the frame.
class ConstructNamedPath : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  ConstructNamedPath() {}
  ConstructNamedPath(const std::shared_ptr<LogicalOperator> &input, Symbol path_symbol,
                     const std::vector<Symbol> &path_elements)
      : input_(input), path_symbol_(path_symbol), path_elements_(path_elements) {}
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  Symbol path_symbol_;
  std::vector<Symbol> path_elements_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<ConstructNamedPath>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->path_symbol_ = path_symbol_;
    object->path_elements_ = path_elements_;
    return object;
  }
};

/// Filter whose Pull returns true only when the given expression
/// evaluates into true.
///
/// The given expression is assumed to return either NULL (treated as false) or
/// a boolean value.
class Filter : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Filter() {}

  Filter(const std::shared_ptr<LogicalOperator> &input_,
         const std::vector<std::shared_ptr<LogicalOperator>> &pattern_filters_, Expression *expression_);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  std::vector<std::shared_ptr<memgraph::query::plan::LogicalOperator>> pattern_filters_;
  Expression *expression_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Filter>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->pattern_filters_.resize(pattern_filters_.size());
    for (auto i1 = 0; i1 < pattern_filters_.size(); ++i1) {
      object->pattern_filters_[i1] = pattern_filters_[i1] ? pattern_filters_[i1]->Clone(storage) : nullptr;
    }
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    return object;
  }

 private:
  class FilterCursor : public Cursor {
   public:
    FilterCursor(const Filter &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const Filter &self_;
    const UniqueCursorPtr input_cursor_;
    const std::vector<UniqueCursorPtr> pattern_filter_cursors_;
  };
};

/// A logical operator that places an arbitrary number
/// of named expressions on the frame (the logical operator
/// for the RETURN clause).
///
/// Supports optional input. When the input is provided,
/// it is Pulled from and the Produce succeeds once for
/// every input Pull (typically a MATCH/RETURN query).
/// When the input is not provided (typically a standalone
/// RETURN clause) the Produce's pull succeeds exactly once.
class Produce : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Produce() {}

  Produce(const std::shared_ptr<LogicalOperator> &input, const std::vector<NamedExpression *> &named_expressions);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  std::vector<NamedExpression *> named_expressions_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Produce>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->named_expressions_.resize(named_expressions_.size());
    for (auto i2 = 0; i2 < named_expressions_.size(); ++i2) {
      object->named_expressions_[i2] = named_expressions_[i2] ? named_expressions_[i2]->Clone(storage) : nullptr;
    }
    return object;
  }

 private:
  class ProduceCursor : public Cursor {
   public:
    ProduceCursor(const Produce &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const Produce &self_;
    const UniqueCursorPtr input_cursor_;
  };
};

/// Operator for deleting vertices and edges.
///
/// Has a flag for using DETACH DELETE when deleting vertices.
class Delete : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Delete() {}

  Delete(const std::shared_ptr<LogicalOperator> &input_, const std::vector<Expression *> &expressions, bool detach_);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  std::vector<Expression *> expressions_;
  /// Whether the vertex should be detached before deletion. If not detached,
  ///            and has connections, an error is raised when deleting edges.
  bool detach_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Delete>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->expressions_.resize(expressions_.size());
    for (auto i3 = 0; i3 < expressions_.size(); ++i3) {
      object->expressions_[i3] = expressions_[i3] ? expressions_[i3]->Clone(storage) : nullptr;
    }
    object->detach_ = detach_;
    return object;
  }

 private:
  class DeleteCursor : public Cursor {
   public:
    DeleteCursor(const Delete &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const Delete &self_;
    const UniqueCursorPtr input_cursor_;
  };
};

/// Logical operator for setting a single property on a single vertex or edge.
///
/// The property value is an expression that must evaluate to some type that
/// can be stored (a TypedValue that can be converted to PropertyValue).
class SetProperty : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  SetProperty() {}

  SetProperty(const std::shared_ptr<LogicalOperator> &input, storage::PropertyId property, PropertyLookup *lhs,
              Expression *rhs);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  storage::PropertyId property_;
  PropertyLookup *lhs_;
  Expression *rhs_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<SetProperty>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->property_ = property_;
    object->lhs_ = lhs_ ? lhs_->Clone(storage) : nullptr;
    object->rhs_ = rhs_ ? rhs_->Clone(storage) : nullptr;
    return object;
  }

 private:
  class SetPropertyCursor : public Cursor {
   public:
    SetPropertyCursor(const SetProperty &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const SetProperty &self_;
    const UniqueCursorPtr input_cursor_;
  };
};

/// Logical operator for setting the whole property set on a vertex or an edge.
///
/// The value being set is an expression that must evaluate to a vertex, edge or
/// map (literal or parameter).
///
/// Supports setting (replacing the whole properties set with another) and
/// updating.
class SetProperties : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  /// Defines how setting the properties works.
  ///
  /// @c UPDATE means that the current property set is augmented with additional
  /// ones (existing props of the same name are replaced), while @c REPLACE means
  /// that the old properties are discarded and replaced with new ones.
  enum class Op { UPDATE, REPLACE };

  SetProperties() {}

  SetProperties(const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol, Expression *rhs, Op op);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  Symbol input_symbol_;
  Expression *rhs_;
  memgraph::query::plan::SetProperties::Op op_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<SetProperties>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->input_symbol_ = input_symbol_;
    object->rhs_ = rhs_ ? rhs_->Clone(storage) : nullptr;
    object->op_ = op_;
    return object;
  }

 private:
  class SetPropertiesCursor : public Cursor {
   public:
    SetPropertiesCursor(const SetProperties &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const SetProperties &self_;
    const UniqueCursorPtr input_cursor_;
  };
};

/// Logical operator for setting an arbitrary number of labels on a Vertex.
///
/// It does NOT remove labels that are already set on that Vertex.
class SetLabels : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  SetLabels() {}

  SetLabels(const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol,
            const std::vector<storage::LabelId> &labels);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  Symbol input_symbol_;
  std::vector<storage::LabelId> labels_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<SetLabels>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->input_symbol_ = input_symbol_;
    object->labels_ = labels_;
    return object;
  }

 private:
  class SetLabelsCursor : public Cursor {
   public:
    SetLabelsCursor(const SetLabels &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const SetLabels &self_;
    const UniqueCursorPtr input_cursor_;
  };
};

/// Logical operator for removing a property from an edge or a vertex.
class RemoveProperty : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  RemoveProperty() {}

  RemoveProperty(const std::shared_ptr<LogicalOperator> &input, storage::PropertyId property, PropertyLookup *lhs);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  storage::PropertyId property_;
  PropertyLookup *lhs_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<RemoveProperty>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->property_ = property_;
    object->lhs_ = lhs_ ? lhs_->Clone(storage) : nullptr;
    return object;
  }

 private:
  class RemovePropertyCursor : public Cursor {
   public:
    RemovePropertyCursor(const RemoveProperty &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const RemoveProperty &self_;
    const UniqueCursorPtr input_cursor_;
  };
};

/// Logical operator for removing an arbitrary number of labels on a Vertex.
///
/// If a label does not exist on a Vertex, nothing happens.
class RemoveLabels : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  RemoveLabels() {}

  RemoveLabels(const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol,
               const std::vector<storage::LabelId> &labels);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  Symbol input_symbol_;
  std::vector<storage::LabelId> labels_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<RemoveLabels>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->input_symbol_ = input_symbol_;
    object->labels_ = labels_;
    return object;
  }

 private:
  class RemoveLabelsCursor : public Cursor {
   public:
    RemoveLabelsCursor(const RemoveLabels &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const RemoveLabels &self_;
    const UniqueCursorPtr input_cursor_;
  };
};

/// Filter whose Pull returns true only when the given expand_symbol frame
/// value (the latest expansion) is not equal to any of the previous_symbols frame
/// values.
///
/// Used for implementing Cyphermorphism.
/// Isomorphism is vertex-uniqueness. It means that two different vertices in a
/// pattern can not map to the same data vertex.
/// Cyphermorphism is edge-uniqueness (the above explanation applies). By default
/// Neo4j uses Cyphermorphism (that's where the name stems from, it is not a valid
/// graph-theory term).
///
/// Supports variable-length-edges (uniqueness comparisons between edges and an
/// edge lists).
class EdgeUniquenessFilter : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  EdgeUniquenessFilter() {}

  EdgeUniquenessFilter(const std::shared_ptr<LogicalOperator> &input, Symbol expand_symbol,
                       const std::vector<Symbol> &previous_symbols);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  Symbol expand_symbol_;
  std::vector<Symbol> previous_symbols_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<EdgeUniquenessFilter>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->expand_symbol_ = expand_symbol_;
    object->previous_symbols_ = previous_symbols_;
    return object;
  }

 private:
  class EdgeUniquenessFilterCursor : public Cursor {
   public:
    EdgeUniquenessFilterCursor(const EdgeUniquenessFilter &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const EdgeUniquenessFilter &self_;
    const UniqueCursorPtr input_cursor_;
  };
};

/// Pulls everything from the input and discards it.
///
/// On the first Pull from this operator's Cursor the input Cursor will be Pulled
/// until it is empty. The results won't be accumulated in the temporary cache.
///
/// This technique is used for ensuring that the cursor has been exhausted after
/// a WriteHandleClause. A typical use case is a `MATCH--SET` query with RETURN statement
/// missing.
/// @param input Input @c LogicalOperator.
class EmptyResult : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  EmptyResult() {}

  EmptyResult(const std::shared_ptr<LogicalOperator> &input);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<EmptyResult>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    return object;
  }
};

/// Pulls everything from the input before passing it through.
/// Optionally advances the command after accumulation and before emitting.
///
/// On the first Pull from this operator's Cursor the input Cursor will be Pulled
/// until it is empty. The results will be accumulated in the temporary cache. Once
/// the input Cursor is empty, this operator's Cursor will start returning cached
/// stuff from its Pull.
///
/// This technique is used for ensuring all the operations from the
/// previous logical operator have been performed before exposing data
/// to the next. A typical use case is a `MATCH--SET--RETURN`
/// query in which every SET iteration must be performed before
/// RETURN starts iterating (see Memgraph Wiki for detailed reasoning).
///
/// IMPORTANT: This operator does not cache all the results but only those
/// elements from the frame whose symbols (frame positions) it was given.
/// All other frame positions will contain undefined junk after this
/// operator has executed, and should not be used.
///
/// This operator can also advance the command after the accumulation and
/// before emitting. If the command gets advanced, every value that
/// has been cached will be reconstructed before Pull returns.
///
/// @param input Input @c LogicalOperator.
/// @param symbols A vector of Symbols that need to be accumulated
///  and exposed to the next op.
class Accumulate : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Accumulate() {}

  Accumulate(const std::shared_ptr<LogicalOperator> &input, const std::vector<Symbol> &symbols,
             bool advance_command = false);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  std::vector<Symbol> symbols_;
  bool advance_command_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Accumulate>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->symbols_ = symbols_;
    object->advance_command_ = advance_command_;
    return object;
  }
};

/// Performs an arbitrary number of aggregations of data
/// from the given input grouped by the given criteria.
///
/// Aggregations are defined by triples that define
/// (input data expression, type of aggregation, output symbol).
/// Input data is grouped based on the given set of named
/// expressions. Grouping is done on unique values.
///
/// IMPORTANT:
/// Operators taking their input from an aggregation are only
/// allowed to use frame values that are either aggregation
/// outputs or group-by named-expressions. All other frame
/// elements are in an undefined state after aggregation.
class Aggregate : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  /// An aggregation element, contains:
  ///        (input data expression, key expression - only used in COLLECT_MAP, type of
  ///        aggregation, output symbol).
  struct Element {
    static const utils::TypeInfo kType;
    const utils::TypeInfo &GetTypeInfo() const { return kType; }

    Expression *value;
    Expression *key;
    Aggregation::Op op;
    Symbol output_sym;
    bool distinct{false};

    Element Clone(AstStorage *storage) const {
      Element object;
      object.value = value ? value->Clone(storage) : nullptr;
      object.key = key ? key->Clone(storage) : nullptr;
      object.op = op;
      object.output_sym = output_sym;
      object.distinct = distinct;
      return object;
    }
  };

  Aggregate() = default;
  Aggregate(const std::shared_ptr<LogicalOperator> &input, const std::vector<Element> &aggregations,
            const std::vector<Expression *> &group_by, const std::vector<Symbol> &remember);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  std::vector<memgraph::query::plan::Aggregate::Element> aggregations_;
  std::vector<Expression *> group_by_;
  std::vector<Symbol> remember_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Aggregate>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->aggregations_.resize(aggregations_.size());
    for (auto i4 = 0; i4 < aggregations_.size(); ++i4) {
      object->aggregations_[i4] = aggregations_[i4].Clone(storage);
    }
    object->group_by_.resize(group_by_.size());
    for (auto i5 = 0; i5 < group_by_.size(); ++i5) {
      object->group_by_[i5] = group_by_[i5] ? group_by_[i5]->Clone(storage) : nullptr;
    }
    object->remember_ = remember_;
    return object;
  }
};

/// Skips a number of Pulls from the input op.
///
/// The given expression determines how many Pulls from the input
/// should be skipped (ignored).
/// All other successful Pulls from the
/// input are simply passed through.
///
/// The given expression is evaluated after the first Pull from
/// the input, and only once. Neo does not allow this expression
/// to contain identifiers, and neither does Memgraph, but this
/// operator's implementation does not expect this.
class Skip : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Skip() {}

  Skip(const std::shared_ptr<LogicalOperator> &input, Expression *expression);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  Expression *expression_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Skip>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    return object;
  }

 private:
  class SkipCursor : public Cursor {
   public:
    SkipCursor(const Skip &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const Skip &self_;
    const UniqueCursorPtr input_cursor_;
    // init to_skip_ to -1, indicating
    // that it's still unknown (input has not been Pulled yet)
    int64_t to_skip_{-1};
    int64_t skipped_{0};
  };
};

/// Applies the pattern filter by putting the value of the input cursor to the frame.
class EvaluatePatternFilter : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  EvaluatePatternFilter() {}

  EvaluatePatternFilter(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  Symbol output_symbol_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<EvaluatePatternFilter>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->output_symbol_ = output_symbol_;
    return object;
  }

 private:
  class EvaluatePatternFilterCursor : public Cursor {
   public:
    EvaluatePatternFilterCursor(const EvaluatePatternFilter &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const EvaluatePatternFilter &self_;
    UniqueCursorPtr input_cursor_;
  };
};

/// Limits the number of Pulls from the input op.
///
/// The given expression determines how many
/// input Pulls should be passed through. The input is not
/// Pulled once this limit is reached. Note that this has
/// implications: the out-of-bounds input Pulls are never
/// evaluated.
///
/// The limit expression must NOT use anything from the
/// Frame. It is evaluated before the first Pull from the
/// input. This is consistent with Neo (they don't allow
/// identifiers in limit expressions), and it's necessary
/// when limit evaluates to 0 (because 0 Pulls from the
/// input should be performed).
class Limit : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Limit() {}

  Limit(const std::shared_ptr<LogicalOperator> &input, Expression *expression);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  Expression *expression_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Limit>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    return object;
  }

 private:
  class LimitCursor : public Cursor {
   public:
    LimitCursor(const Limit &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const Limit &self_;
    UniqueCursorPtr input_cursor_;
    // init limit_ to -1, indicating
    // that it's still unknown (Cursor has not been Pulled yet)
    int64_t limit_{-1};
    int64_t pulled_{0};
  };
};

/// Logical operator for ordering (sorting) results.
///
/// Sorts the input rows based on an arbitrary number of
/// Expressions. Ascending or descending ordering can be chosen
/// for each independently (not providing enough orderings
/// results in a runtime error).
///
/// For each row an arbitrary number of Frame elements can be
/// remembered. Only these elements (defined by their Symbols)
/// are valid for usage after the OrderBy operator.
class OrderBy : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  OrderBy() {}

  OrderBy(const std::shared_ptr<LogicalOperator> &input, const std::vector<SortItem> &order_by,
          const std::vector<Symbol> &output_symbols);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  TypedValueVectorCompare compare_;
  std::vector<Expression *> order_by_;
  std::vector<Symbol> output_symbols_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<OrderBy>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->compare_ = compare_;
    object->order_by_.resize(order_by_.size());
    for (auto i6 = 0; i6 < order_by_.size(); ++i6) {
      object->order_by_[i6] = order_by_[i6] ? order_by_[i6]->Clone(storage) : nullptr;
    }
    object->output_symbols_ = output_symbols_;
    return object;
  }
};

/// Merge operator. For every sucessful Pull from the
/// input operator a Pull from the merge_match is attempted. All
/// successfull Pulls from the merge_match are passed on as output.
/// If merge_match Pull does not yield any elements, a single Pull
/// from the merge_create op is performed.
///
/// The input logical op is optional. If false (nullptr)
/// it will be replaced by a Once op.
///
/// For an argumentation of this implementation see the wiki
/// documentation.
class Merge : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Merge() {}

  Merge(const std::shared_ptr<LogicalOperator> &input, const std::shared_ptr<LogicalOperator> &merge_match,
        const std::shared_ptr<LogicalOperator> &merge_create);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  // TODO: Consider whether we want to treat Merge as having single input. It
  // makes sense that we do, because other branches are executed depending on
  // the input.
  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  std::shared_ptr<memgraph::query::plan::LogicalOperator> merge_match_;
  std::shared_ptr<memgraph::query::plan::LogicalOperator> merge_create_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Merge>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->merge_match_ = merge_match_ ? merge_match_->Clone(storage) : nullptr;
    object->merge_create_ = merge_create_ ? merge_create_->Clone(storage) : nullptr;
    return object;
  }

 private:
  class MergeCursor : public Cursor {
   public:
    MergeCursor(const Merge &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const UniqueCursorPtr input_cursor_;
    const UniqueCursorPtr merge_match_cursor_;
    const UniqueCursorPtr merge_create_cursor_;

    // indicates if the next Pull from this cursor
    // should perform a pull from input_cursor_
    // this is true when:
    //  - first Pulling from this cursor
    //  - previous Pull from this cursor exhausted the merge_match_cursor
    bool pull_input_{true};
  };
};

/// Optional operator. Used for optional match. For every
/// successful Pull from the input branch a Pull from the optional
/// branch is attempted (and Pulled from till exhausted). If zero
/// Pulls succeed from the optional branch, the Optional operator
/// sets the optional symbols to TypedValue::Null on the Frame
/// and returns true, once.
class Optional : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Optional() {}

  Optional(const std::shared_ptr<LogicalOperator> &input, const std::shared_ptr<LogicalOperator> &optional,
           const std::vector<Symbol> &optional_symbols);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  std::shared_ptr<memgraph::query::plan::LogicalOperator> optional_;
  std::vector<Symbol> optional_symbols_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Optional>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->optional_ = optional_ ? optional_->Clone(storage) : nullptr;
    object->optional_symbols_ = optional_symbols_;
    return object;
  }

 private:
  class OptionalCursor : public Cursor {
   public:
    OptionalCursor(const Optional &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const Optional &self_;
    const UniqueCursorPtr input_cursor_;
    const UniqueCursorPtr optional_cursor_;
    // indicates if the next Pull from this cursor should
    // perform a Pull from the input_cursor_
    // this is true when:
    //  - first pulling from this Cursor
    //  - previous Pull from this cursor exhausted the optional_cursor_
    bool pull_input_{true};
  };
};

/// Takes a list TypedValue as it's input and yields each
/// element as it's output.
///
/// Input is optional (unwind can be the first clause in a query).
class Unwind : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Unwind() {}

  Unwind(const std::shared_ptr<LogicalOperator> &input, Expression *input_expression_, Symbol output_symbol);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  Expression *input_expression_;
  Symbol output_symbol_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Unwind>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->input_expression_ = input_expression_ ? input_expression_->Clone(storage) : nullptr;
    object->output_symbol_ = output_symbol_;
    return object;
  }
};

/// Ensures that only distinct rows are yielded.
/// This implementation accepts a vector of Symbols
/// which define a row. Only those Symbols are valid
/// for use in operators following Distinct.
///
/// This implementation maintains input ordering.
class Distinct : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Distinct() {}

  Distinct(const std::shared_ptr<LogicalOperator> &input, const std::vector<Symbol> &value_symbols);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  std::vector<Symbol> value_symbols_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Distinct>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->value_symbols_ = value_symbols_;
    return object;
  }
};

/// A logical operator that applies UNION operator on inputs and places the
/// result on the frame.
///
/// This operator takes two inputs, a vector of symbols for the result, and vectors
/// of symbols used by each of the inputs.
class Union : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Union() {}

  Union(const std::shared_ptr<LogicalOperator> &left_op, const std::shared_ptr<LogicalOperator> &right_op,
        const std::vector<Symbol> &union_symbols, const std::vector<Symbol> &left_symbols,
        const std::vector<Symbol> &right_symbols);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override;
  std::shared_ptr<LogicalOperator> input() const override;
  void set_input(std::shared_ptr<LogicalOperator>) override;

  std::shared_ptr<memgraph::query::plan::LogicalOperator> left_op_;
  std::shared_ptr<memgraph::query::plan::LogicalOperator> right_op_;
  std::vector<Symbol> union_symbols_;
  std::vector<Symbol> left_symbols_;
  std::vector<Symbol> right_symbols_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Union>();
    object->left_op_ = left_op_ ? left_op_->Clone(storage) : nullptr;
    object->right_op_ = right_op_ ? right_op_->Clone(storage) : nullptr;
    object->union_symbols_ = union_symbols_;
    object->left_symbols_ = left_symbols_;
    object->right_symbols_ = right_symbols_;
    return object;
  }

 private:
  class UnionCursor : public Cursor {
   public:
    UnionCursor(const Union &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const Union &self_;
    const UniqueCursorPtr left_cursor_, right_cursor_;
  };
};

/// Operator for producing a Cartesian product from 2 input branches
class Cartesian : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Cartesian() {}
  /** Construct the operator with left input branch and right input branch. */
  Cartesian(const std::shared_ptr<LogicalOperator> &left_op, const std::vector<Symbol> &left_symbols,
            const std::shared_ptr<LogicalOperator> &right_op, const std::vector<Symbol> &right_symbols)
      : left_op_(left_op), left_symbols_(left_symbols), right_op_(right_op), right_symbols_(right_symbols) {}

  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override;
  std::shared_ptr<LogicalOperator> input() const override;
  void set_input(std::shared_ptr<LogicalOperator>) override;

  std::shared_ptr<memgraph::query::plan::LogicalOperator> left_op_;
  std::vector<Symbol> left_symbols_;
  std::shared_ptr<memgraph::query::plan::LogicalOperator> right_op_;
  std::vector<Symbol> right_symbols_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Cartesian>();
    object->left_op_ = left_op_ ? left_op_->Clone(storage) : nullptr;
    object->left_symbols_ = left_symbols_;
    object->right_op_ = right_op_ ? right_op_->Clone(storage) : nullptr;
    object->right_symbols_ = right_symbols_;
    return object;
  }
};

/// An operator that outputs a table, producing a single row on each pull
class OutputTable : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  OutputTable() {}
  OutputTable(std::vector<Symbol> output_symbols,
              std::function<std::vector<std::vector<TypedValue>>(Frame *, ExecutionContext *)> callback);
  OutputTable(std::vector<Symbol> output_symbols, std::vector<std::vector<TypedValue>> rows);

  bool Accept(HierarchicalLogicalOperatorVisitor &) override {
    LOG_FATAL("OutputTable operator should not be visited!");
  }

  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> OutputSymbols(const SymbolTable &) const override { return output_symbols_; }
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override { return output_symbols_; }

  bool HasSingleInput() const override;
  std::shared_ptr<LogicalOperator> input() const override;
  void set_input(std::shared_ptr<LogicalOperator> input) override;

  std::vector<Symbol> output_symbols_;
  std::function<std::vector<std::vector<TypedValue>>(Frame *, ExecutionContext *)> callback_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<OutputTable>();
    object->output_symbols_ = output_symbols_;
    object->callback_ = callback_;
    return object;
  }
};

/// An operator that outputs a table, producing a single row on each pull.
/// This class is different from @c OutputTable in that its callback doesn't fetch all rows
/// at once. Instead, each call of the callback should return a single row of the table.
class OutputTableStream : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  OutputTableStream() {}
  OutputTableStream(std::vector<Symbol> output_symbols,
                    std::function<std::optional<std::vector<TypedValue>>(Frame *, ExecutionContext *)> callback);

  bool Accept(HierarchicalLogicalOperatorVisitor &) override {
    LOG_FATAL("OutputTableStream operator should not be visited!");
  }

  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> OutputSymbols(const SymbolTable &) const override { return output_symbols_; }
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override { return output_symbols_; }

  bool HasSingleInput() const override;
  std::shared_ptr<LogicalOperator> input() const override;
  void set_input(std::shared_ptr<LogicalOperator> input) override;

  std::vector<Symbol> output_symbols_;
  std::function<std::optional<std::vector<TypedValue>>(Frame *, ExecutionContext *)> callback_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<OutputTableStream>();
    object->output_symbols_ = output_symbols_;
    object->callback_ = callback_;
    return object;
  }
};

class CallProcedure : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  CallProcedure() = default;
  CallProcedure(std::shared_ptr<LogicalOperator> input, std::string name, std::vector<Expression *> arguments,
                std::vector<std::string> fields, std::vector<Symbol> symbols, Expression *memory_limit,
                size_t memory_scale, bool is_write);

  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  static void IncrementCounter(const std::string &procedure_name);
  static std::unordered_map<std::string, int64_t> GetAndResetCounters();

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  std::string procedure_name_;
  std::vector<Expression *> arguments_;
  std::vector<std::string> result_fields_;
  std::vector<Symbol> result_symbols_;
  Expression *memory_limit_{nullptr};
  size_t memory_scale_{1024U};
  bool is_write_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<CallProcedure>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->procedure_name_ = procedure_name_;
    object->arguments_.resize(arguments_.size());
    for (auto i7 = 0; i7 < arguments_.size(); ++i7) {
      object->arguments_[i7] = arguments_[i7] ? arguments_[i7]->Clone(storage) : nullptr;
    }
    object->result_fields_ = result_fields_;
    object->result_symbols_ = result_symbols_;
    object->memory_limit_ = memory_limit_ ? memory_limit_->Clone(storage) : nullptr;
    object->memory_scale_ = memory_scale_;
    object->is_write_ = is_write_;
    return object;
  }

 private:
  inline static utils::Synchronized<std::unordered_map<std::string, int64_t>, utils::SpinLock> procedure_counters_;
};

class LoadCsv : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  LoadCsv() = default;
  LoadCsv(std::shared_ptr<LogicalOperator> input, Expression *file, bool with_header, bool ignore_bad,
          Expression *delimiter, Expression *quote, Symbol row_var);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  Expression *file_;
  bool with_header_;
  bool ignore_bad_;
  Expression *delimiter_{nullptr};
  Expression *quote_{nullptr};
  Symbol row_var_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<LoadCsv>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->file_ = file_ ? file_->Clone(storage) : nullptr;
    object->with_header_ = with_header_;
    object->ignore_bad_ = ignore_bad_;
    object->delimiter_ = delimiter_ ? delimiter_->Clone(storage) : nullptr;
    object->quote_ = quote_ ? quote_->Clone(storage) : nullptr;
    object->row_var_ = row_var_;
    return object;
  }
};

/// Iterates over a collection of elements and applies one or more update
/// clauses.
///
class Foreach : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Foreach() = default;
  Foreach(std::shared_ptr<LogicalOperator> input, std::shared_ptr<LogicalOperator> updates, Expression *named_expr,
          Symbol loop_variable_symbol);

  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;
  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = std::move(input); }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  std::shared_ptr<memgraph::query::plan::LogicalOperator> update_clauses_;
  Expression *expression_;
  Symbol loop_variable_symbol_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Foreach>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->update_clauses_ = update_clauses_ ? update_clauses_->Clone(storage) : nullptr;
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    object->loop_variable_symbol_ = loop_variable_symbol_;
    return object;
  }
};

/// Applies symbols from both output branches.
class Apply : public memgraph::query::plan::LogicalOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Apply() {}

  Apply(const std::shared_ptr<LogicalOperator> input, const std::shared_ptr<LogicalOperator> subquery,
        bool subquery_has_return);
  bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
  UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
  std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

  bool HasSingleInput() const override { return true; }
  std::shared_ptr<LogicalOperator> input() const override { return input_; }
  void set_input(std::shared_ptr<LogicalOperator> input) override { input_ = input; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  std::shared_ptr<memgraph::query::plan::LogicalOperator> subquery_;
  bool subquery_has_return_;

  std::unique_ptr<LogicalOperator> Clone(AstStorage *storage) const override {
    auto object = std::make_unique<Apply>();
    object->input_ = input_ ? input_->Clone(storage) : nullptr;
    object->subquery_ = subquery_ ? subquery_->Clone(storage) : nullptr;
    object->subquery_has_return_ = subquery_has_return_;
    return object;
  }

 private:
  class ApplyCursor : public Cursor {
   public:
    ApplyCursor(const Apply &, utils::MemoryResource *);
    bool Pull(Frame &, ExecutionContext &) override;
    void Shutdown() override;
    void Reset() override;

   private:
    const Apply &self_;
    UniqueCursorPtr input_;
    UniqueCursorPtr subquery_;
    bool pull_input_{true};
    bool subquery_has_return_{true};
  };
};

}  // namespace plan
}  // namespace query
}  // namespace memgraph
