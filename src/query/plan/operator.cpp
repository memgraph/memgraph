// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/plan/operator.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <cppitertools/chain.hpp>
#include <cppitertools/imap.hpp>
#include "memory/query_memory_control.hpp"
#include "query/common.hpp"
#include "spdlog/spdlog.h"

#include "csv/parsing.hpp"
#include "flags/experimental.hpp"
#include "license/license.hpp"
#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/graph.hpp"
#include "query/interpret/eval.hpp"
#include "query/path.hpp"
#include "query/plan/scoped_profile.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "query/procedure/module.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/point_iterator.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/property_value_utils.hpp"
#include "storage/v2/storage_error.hpp"
#include "storage/v2/view.hpp"
#include "utils/algorithm.hpp"
#include "utils/event_counter.hpp"
#include "utils/exceptions.hpp"
#include "utils/fnv.hpp"
#include "utils/java_string_formatter.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/message.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/pmr/deque.hpp"
#include "utils/pmr/list.hpp"
#include "utils/pmr/unordered_map.hpp"
#include "utils/pmr/unordered_set.hpp"
#include "utils/pmr/vector.hpp"
#include "utils/query_memory_tracker.hpp"
#include "utils/readable_size.hpp"
#include "utils/tag.hpp"
#include "utils/temporal.hpp"
#include "vertex_accessor.hpp"

namespace r = ranges;
namespace rv = r::views;

// macro for the default implementation of LogicalOperator::Accept
// that accepts the visitor and visits it's input_ operator
// NOLINTNEXTLINE
#define ACCEPT_WITH_INPUT(class_name)                                                            \
  bool class_name::Accept(HierarchicalLogicalOperatorVisitor &visitor) {                         \
    if (visitor.PreVisit(*this)) {                                                               \
      if (input_ == nullptr) {                                                                   \
        throw QueryRuntimeException(                                                             \
            "The query couldn't be executed due to the unexpected null value in " #class_name    \
            " operator. To learn more about operators visit https://memgr.ph/query-operators!"); \
      }                                                                                          \
      input_->Accept(visitor);                                                                   \
    }                                                                                            \
    return visitor.PostVisit(*this);                                                             \
  }

#define WITHOUT_SINGLE_INPUT(class_name)                         \
  bool class_name::HasSingleInput() const { return false; }      \
  std::shared_ptr<LogicalOperator> class_name::input() const {   \
    LOG_FATAL("Operator " #class_name " has no single input!");  \
  }                                                              \
  void class_name::set_input(std::shared_ptr<LogicalOperator>) { \
    LOG_FATAL("Operator " #class_name " has no single input!");  \
  }

namespace memgraph::metrics {
extern const Event OnceOperator;
extern const Event CreateNodeOperator;
extern const Event CreateExpandOperator;
extern const Event ScanAllOperator;
extern const Event ScanAllByLabelOperator;
extern const Event ScanAllByLabelPropertiesOperator;
extern const Event ScanAllByIdOperator;
extern const Event ScanAllByEdgeOperator;
extern const Event ScanAllByEdgeTypeOperator;
extern const Event ScanAllByEdgeTypePropertyOperator;
extern const Event ScanAllByEdgeTypePropertyValueOperator;
extern const Event ScanAllByEdgeTypePropertyRangeOperator;
extern const Event ScanAllByEdgePropertyOperator;
extern const Event ScanAllByEdgePropertyValueOperator;
extern const Event ScanAllByEdgePropertyRangeOperator;
extern const Event ScanAllByEdgeIdOperator;
extern const Event ScanAllByPointDistanceOperator;
extern const Event ScanAllByPointWithinbboxOperator;
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
extern const Event EmptyResultOperator;
extern const Event EvaluatePatternFilterOperator;
extern const Event ApplyOperator;
extern const Event IndexedJoinOperator;
extern const Event HashJoinOperator;
extern const Event RollUpApplyOperator;
extern const Event PeriodicCommitOperator;
extern const Event PeriodicSubqueryOperator;
extern const Event SetNestedPropertyOperator;
extern const Event RemoveNestedPropertyOperator;
}  // namespace memgraph::metrics

namespace memgraph::query::plan {

using OOMExceptionEnabler = utils::MemoryTracker::OutOfMemoryExceptionEnabler;

ExpressionRange::ExpressionRange(ExpressionRange const &other, AstStorage &storage)
    : type_{other.type_},
      lower_{other.lower_
                 ? std::make_optional(utils::Bound(other.lower_->value()->Clone(&storage), other.lower_->type()))
                 : std::nullopt},
      upper_{other.upper_
                 ? std::make_optional(utils::Bound(other.upper_->value()->Clone(&storage), other.upper_->type()))
                 : std::nullopt} {}

auto ExpressionRange::Equal(Expression *value) -> ExpressionRange {
  // Only store lower bound, Evaluate will only use the lower bound
  return {Type::EQUAL, utils::MakeBoundInclusive(value), std::nullopt};
}

auto ExpressionRange::RegexMatch() -> ExpressionRange { return {Type::REGEX_MATCH, std::nullopt, std::nullopt}; }

auto ExpressionRange::Range(std::optional<utils::Bound<Expression *>> lower,
                            std::optional<utils::Bound<Expression *>> upper) -> ExpressionRange {
  return {Type::RANGE, std::move(lower), std::move(upper)};
}

auto ExpressionRange::IsNotNull() -> ExpressionRange { return {Type::IS_NOT_NULL, std::nullopt, std::nullopt}; }

auto ExpressionRange::Evaluate(ExpressionEvaluator &evaluator) const -> storage::PropertyValueRange {
  auto const to_bounded_property_value = [&](auto &value) -> std::optional<utils::Bound<storage::PropertyValue>> {
    if (value == std::nullopt) {
      return std::nullopt;
    } else {
      auto const typed_value = value->value()->Accept(evaluator);
      if (!typed_value.IsPropertyValue()) {
        throw QueryRuntimeException("'{}' cannot be used as a property value.", typed_value.type());
      }
      return utils::Bound{typed_value.ToPropertyValue(evaluator.GetNameIdMapper()), value->type()};
    }
  };

  switch (type_) {
    case Type::EQUAL:
    case Type::IN: {
      auto bounded_property_value = to_bounded_property_value(lower_);
      return storage::PropertyValueRange::Bounded(bounded_property_value, bounded_property_value);
    }

    case Type::REGEX_MATCH: {
      auto empty_string = utils::MakeBoundInclusive(storage::PropertyValue(""));
      auto upper_bound = storage::UpperBoundForType(storage::PropertyValueType::String);
      return storage::PropertyValueRange::Bounded(std::move(empty_string), std::move(upper_bound));
    }

    case Type::RANGE: {
      auto lower_bound = to_bounded_property_value(lower_);
      auto upper_bound = to_bounded_property_value(upper_);

      // When scanning a range, the bounds must be the same type
      if (lower_bound && upper_bound && !AreComparableTypes(lower_bound->value().type(), upper_bound->value().type())) {
        return storage::PropertyValueRange::Invalid(*lower_bound, *upper_bound);
      }

      // InMemoryLabelPropertyIndex::Iterable is responsible to make sure an unset lower/upper
      // bound will be limitted to the same type as the other bound
      return storage::PropertyValueRange::Bounded(lower_bound, upper_bound);
    }

    case Type::IS_NOT_NULL: {
      return storage::PropertyValueRange::IsNotNull();
    }
  }
}

std::optional<storage::ExternalPropertyValue> ConstExternalPropertyValue(const Expression *expression,
                                                                         Parameters const &parameters) {
  if (auto *literal = utils::Downcast<const PrimitiveLiteral>(expression)) {
    return literal->value_;
  } else if (auto *param_lookup = utils::Downcast<const ParameterLookup>(expression)) {
    return parameters.AtTokenPosition(param_lookup->token_position_);
  }
  return std::nullopt;
}

auto ExpressionRange::ResolveAtPlantime(Parameters const &params, storage::NameIdMapper *name_id_mapper) const
    -> std::optional<storage::PropertyValueRange> {
  struct UnknownAtPlanTime {};

  using obpv = std::optional<utils::Bound<storage::PropertyValue>>;

  auto const to_bounded_property_value = [&](auto &value) -> std::variant<UnknownAtPlanTime, obpv> {
    if (value == std::nullopt) {
      return std::nullopt;
    } else {
      auto intermediate_property_value = ConstExternalPropertyValue(value->value(), params);
      if (intermediate_property_value) {
        auto property_value = storage::ToPropertyValue(*intermediate_property_value, name_id_mapper);
        return utils::Bound{std::move(property_value), value->type()};
      } else {
        return UnknownAtPlanTime{};
      }
    }
  };

  switch (type_) {
    case Type::EQUAL:
    case Type::IN: {
      auto bounded_property_value = to_bounded_property_value(lower_);
      if (std::holds_alternative<UnknownAtPlanTime>(bounded_property_value)) return std::nullopt;
      return storage::PropertyValueRange::Bounded(std::get<obpv>(bounded_property_value),
                                                  std::get<obpv>(bounded_property_value));
    }

    case Type::REGEX_MATCH: {
      auto empty_string = utils::MakeBoundInclusive(storage::PropertyValue(""));
      auto upper_bound = storage::UpperBoundForType(storage::PropertyValueType::String);
      return storage::PropertyValueRange::Bounded(std::move(empty_string), std::move(upper_bound));
    }

    case Type::RANGE: {
      auto maybe_lower_bound = to_bounded_property_value(lower_);
      if (std::holds_alternative<UnknownAtPlanTime>(maybe_lower_bound)) return std::nullopt;
      auto maybe_upper_bound = to_bounded_property_value(upper_);
      if (std::holds_alternative<UnknownAtPlanTime>(maybe_upper_bound)) return std::nullopt;

      auto lower_bound = std::move(std::get<obpv>(maybe_lower_bound));
      auto upper_bound = std::move(std::get<obpv>(maybe_upper_bound));

      // When scanning a range, the bounds must be the same type
      if (lower_bound && upper_bound && !AreComparableTypes(lower_bound->value().type(), upper_bound->value().type())) {
        return storage::PropertyValueRange::Invalid(*lower_bound, *upper_bound);
      }

      // InMemoryLabelPropertyIndex::Iterable is responsible to make sure an unset lower/upper
      // bound will be limitted to the same type as the other bound
      return storage::PropertyValueRange::Bounded(lower_bound, upper_bound);
    }

    case Type::IS_NOT_NULL: {
      return storage::PropertyValueRange::IsNotNull();
    }
  }
}

ExpressionRange::ExpressionRange(Type type, std::optional<utils::Bound<Expression *>> lower,
                                 std::optional<utils::Bound<Expression *>> upper)
    : type_{type}, lower_{std::move(lower)}, upper_{std::move(upper)} {}

namespace {
template <typename>
constexpr auto kAlwaysFalse = false;

void HandlePeriodicCommitError(const storage::StorageManipulationError &error) {
  std::visit(
      []<typename T>(const T & /* unused */) {
        using ErrorType = std::remove_cvref_t<T>;
        if constexpr (std::is_same_v<ErrorType, storage::SyncReplicationError>) {
          spdlog::warn(
              "PeriodicCommit warning: At least one SYNC replica has not confirmed the "
              "commit.");
        } else if constexpr (std::is_same_v<ErrorType, storage::StrictSyncReplicationError>) {
          spdlog::warn(
              "PeriodicCommit warning: At least one STRICT_SYNC replica has not confirmed committing last transaction. "
              "Transaction will be aborted on all instances.");
        } else if constexpr (std::is_same_v<ErrorType, storage::ConstraintViolation>) {
          throw QueryException(
              "PeriodicCommit failed: Unable to commit due to constraint "
              "violation.");
        } else if constexpr (std::is_same_v<ErrorType, storage::SerializationError>) {
          throw QueryException("PeriodicCommit failed: Unable to commit due to serialization error.");
        } else if constexpr (std::is_same_v<ErrorType, storage::PersistenceError>) {
          throw QueryException("PeriodicCommit failed: Unable to commit due to persistence error.");
        } else if constexpr (std::is_same_v<ErrorType, storage::ReplicaShouldNotWriteError>) {
          throw QueryException("PeriodicCommit failed: Queries on replica shouldn't write.");
        } else {
          static_assert(kAlwaysFalse<T>, "Missing type from variant visitor");
        }
      },
      error);
}

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

// Checking abort is a cheap check but is still doing atomic
// reads. Reducing the frequency of those reads will reduce their
// impact on performance for the expected (non-abort) case.
thread_local auto maybe_check_abort = utils::ResettableCounter{20};

inline void AbortCheck(ExecutionContext const &context) {
  if (!maybe_check_abort()) return;

  if (auto const reason = context.stopping_context.MustAbort(); reason != AbortReason::NO_ABORT)
    throw HintedAbortError(reason);
}

std::vector<storage::LabelId> EvaluateLabels(const std::vector<StorageLabelType> &labels,
                                             ExpressionEvaluator &evaluator, DbAccessor *dba) {
  std::vector<storage::LabelId> result;
  result.reserve(labels.size());
  for (const auto &label : labels) {
    if (const auto *label_atom = std::get_if<storage::LabelId>(&label)) {
      result.emplace_back(*label_atom);
    } else {
      const auto value = std::get<Expression *>(label)->Accept(evaluator);
      if (value.IsString()) {
        result.emplace_back(dba->NameToLabel(value.ValueString()));
      } else if (value.IsList()) {
        for (const auto &label : value.ValueList()) {
          result.emplace_back(dba->NameToLabel(label.ValueString()));
        }
      } else {
        throw QueryRuntimeException(
            fmt::format("Expected to evaluate labels of String or List[String] type, got {}.", value.type()));
      }
    }
  }
  return result;
}

storage::EdgeTypeId EvaluateEdgeType(const StorageEdgeType &edge_type, ExpressionEvaluator &evaluator,
                                     DbAccessor *dba) {
  if (const auto *edge_type_id = std::get_if<storage::EdgeTypeId>(&edge_type)) {
    return *edge_type_id;
  }

  return dba->NameToEdgeType(std::get<Expression *>(edge_type)->Accept(evaluator).ValueString());
}

}  // namespace

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define SCOPED_PROFILE_OP(name)                                                                    \
  std::optional<ScopedProfile> profile =                                                           \
      context.is_profile_query                                                                     \
          ? std::optional<ScopedProfile>(std::in_place, ComputeProfilingKey(this), name, &context) \
          : std::nullopt;
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define SCOPED_PROFILE_OP_BY_REF(ref)                                                                                  \
  std::optional<ScopedProfile> profile =                                                                               \
      context.is_profile_query ? std::optional<ScopedProfile>(std::in_place, ComputeProfilingKey(this), ref, &context) \
                               : std::nullopt;

bool Once::OnceCursor::Pull(Frame &, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP("Once");

  AbortCheck(context);

  if (!did_pull_) {
    did_pull_ = true;
    return true;
  }
  return false;
}

UniqueCursorPtr Once::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::OnceOperator);

  return MakeUniqueCursorPtr<OnceCursor>(mem);
}

WITHOUT_SINGLE_INPUT(Once);

std::unique_ptr<LogicalOperator> Once::Clone(AstStorage *storage) const {
  auto object = std::make_unique<Once>();
  object->symbols_ = symbols_;
  return object;
}

void Once::OnceCursor::Shutdown() {}

void Once::OnceCursor::Reset() { did_pull_ = false; }

CreateNode::CreateNode(const std::shared_ptr<LogicalOperator> &input, NodeCreationInfo node_info)
    : input_(input ? input : std::make_shared<Once>()), node_info_(std::move(node_info)) {}

// Creates a vertex on this GraphDb. Returns a reference to vertex placed on the
// frame.
VertexAccessor &CreateLocalVertex(const NodeCreationInfo &node_info, Frame *frame, ExecutionContext &context,
                                  std::vector<storage::LabelId> &labels, ExpressionEvaluator &evaluator) {
  auto &dba = *context.db_accessor;
  auto new_node = dba.InsertVertex();
  context.execution_stats[ExecutionStats::Key::CREATED_NODES] += 1;
  for (const auto &label : labels) {
    auto maybe_error = std::invoke([&] { return new_node.AddLabel(label); });
    if (maybe_error.HasError()) {
      switch (maybe_error.GetError()) {
        case storage::Error::SERIALIZATION_ERROR:
          throw TransactionSerializationException();
        case storage::Error::DELETED_OBJECT:
          throw QueryRuntimeException("Trying to set a label on a deleted node.");
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::PROPERTIES_DISABLED:
        case storage::Error::NONEXISTENT_OBJECT:
          throw QueryRuntimeException("Unexpected error when setting a label.");
      }
    }
    context.execution_stats[ExecutionStats::Key::CREATED_LABELS] += 1;
  }
  // TODO: PropsSetChecked allocates a PropertyValue, make it use context.memory
  // when we update PropertyValue with custom allocator.
  std::map<storage::PropertyId, storage::PropertyValue> properties;
  if (const auto *node_info_properties = std::get_if<PropertiesMapList>(&node_info.properties)) {
    for (const auto &[key, value_expression] : *node_info_properties) {
      auto typed_value = value_expression->Accept(evaluator);
      properties.emplace(key,
                         typed_value.ToPropertyValue(context.db_accessor->GetStorageAccessor()->GetNameIdMapper()));
    }
  } else {
    auto property_map = evaluator.Visit(*std::get<ParameterLookup *>(node_info.properties));
    for (const auto &[key, value] : property_map.ValueMap()) {
      properties.emplace(dba.NameToProperty(key),
                         value.ToPropertyValue(context.db_accessor->GetStorageAccessor()->GetNameIdMapper()));
    }
  }
  if (context.evaluation_context.scope.in_merge) {
    for (const auto &[k, v] : properties) {
      if (v.IsNull()) {
        throw QueryRuntimeException(fmt::format("Can't have null literal properties inside merge ({}.{})!",
                                                node_info.symbol.name(), dba.PropertyToName(k)));
      }
    }
  }

  MultiPropsInitChecked(&new_node, properties);

  (*frame)[node_info.symbol] = new_node;
  return (*frame)[node_info.symbol].ValueVertex();
}

ACCEPT_WITH_INPUT(CreateNode)

UniqueCursorPtr CreateNode::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::CreateNodeOperator);

  return MakeUniqueCursorPtr<CreateNodeCursor>(mem, *this, mem);
}

std::vector<Symbol> CreateNode::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(node_info_.symbol);
  return symbols;
}

std::unique_ptr<LogicalOperator> CreateNode::Clone(AstStorage *storage) const {
  auto object = std::make_unique<CreateNode>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->node_info_ = node_info_.Clone(storage);
  return object;
}

CreateNode::CreateNodeCursor::CreateNodeCursor(const CreateNode &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self.input_->MakeCursor(mem)) {}

bool CreateNode::CreateNodeCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP("CreateNode");

  AbortCheck(context);

  ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                storage::View::NEW, nullptr, &context.number_of_hops);

  if (input_cursor_->Pull(frame, context)) {
    // we have to resolve the labels before we can check for permissions
    auto labels = EvaluateLabels(self_.node_info_.labels, evaluator, context.db_accessor);

#ifdef MG_ENTERPRISE
    if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
        !context.auth_checker->Has(labels, memgraph::query::AuthQuery::FineGrainedPrivilege::CREATE_DELETE)) {
      throw QueryRuntimeException("Vertex not created due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
    }
#endif

    auto created_vertex = CreateLocalVertex(self_.node_info_, &frame, context, labels, evaluator);
    if (context.trigger_context_collector) {
      context.trigger_context_collector->RegisterCreatedObject(created_vertex);
    }
    return true;
  }

  return false;
}

void CreateNode::CreateNodeCursor::Shutdown() { input_cursor_->Shutdown(); }

void CreateNode::CreateNodeCursor::Reset() { input_cursor_->Reset(); }

CreateExpand::CreateExpand(NodeCreationInfo node_info, EdgeCreationInfo edge_info,
                           const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol, bool existing_node)
    : node_info_(std::move(node_info)),
      edge_info_(std::move(edge_info)),
      input_(input ? input : std::make_shared<Once>()),
      input_symbol_(std::move(input_symbol)),
      existing_node_(existing_node) {}

ACCEPT_WITH_INPUT(CreateExpand)

UniqueCursorPtr CreateExpand::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::CreateExpandOperator);

  return MakeUniqueCursorPtr<CreateExpandCursor>(mem, *this, mem);
}

std::vector<Symbol> CreateExpand::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(node_info_.symbol);
  symbols.emplace_back(edge_info_.symbol);
  return symbols;
}

std::string CreateExpand::ToString() const {
  const auto *maybe_edge_type_id = std::get_if<storage::EdgeTypeId>(&edge_info_.edge_type);
  const bool is_expansion_static = maybe_edge_type_id != nullptr;
  return fmt::format("{} ({}){}[{}:{}]{}({})", "CreateExpand", input_symbol_.name(),
                     edge_info_.direction == query::EdgeAtom::Direction::IN ? "<-" : "-", edge_info_.symbol.name(),
                     is_expansion_static ? dba_->EdgeTypeToName(*maybe_edge_type_id) : "<DYNAMIC>",
                     edge_info_.direction == query::EdgeAtom::Direction::OUT ? "->" : "-", node_info_.symbol.name());
}

std::unique_ptr<LogicalOperator> CreateExpand::Clone(AstStorage *storage) const {
  auto object = std::make_unique<CreateExpand>();
  object->node_info_ = node_info_.Clone(storage);
  object->edge_info_ = edge_info_.Clone(storage);
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->input_symbol_ = input_symbol_;
  object->existing_node_ = existing_node_;
  return object;
}

CreateExpand::CreateExpandCursor::CreateExpandCursor(const CreateExpand &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self.input_->MakeCursor(mem)) {}

namespace {

EdgeAccessor CreateEdge(const EdgeCreationInfo &edge_info, const storage::EdgeTypeId edge_type_id, DbAccessor *dba,
                        VertexAccessor *from, VertexAccessor *to, Frame *frame, ExecutionContext &context,
                        ExpressionEvaluator *evaluator) {
  auto maybe_edge = dba->InsertEdge(from, to, edge_type_id);
  if (maybe_edge.HasValue()) {
    auto &edge = *maybe_edge;
    std::map<storage::PropertyId, storage::PropertyValue> properties;
    if (const auto *edge_info_properties = std::get_if<PropertiesMapList>(&edge_info.properties)) {
      for (const auto &[key, value_expression] : *edge_info_properties) {
        auto typed_value = value_expression->Accept(*evaluator);
        properties.emplace(key,
                           typed_value.ToPropertyValue(context.db_accessor->GetStorageAccessor()->GetNameIdMapper()));
      }
    } else {
      auto property_map = evaluator->Visit(*std::get<ParameterLookup *>(edge_info.properties));
      for (const auto &[key, value] : property_map.ValueMap()) {
        properties.emplace(dba->NameToProperty(key),
                           value.ToPropertyValue(context.db_accessor->GetStorageAccessor()->GetNameIdMapper()));
      }
    }
    if (context.evaluation_context.scope.in_merge) {
      for (const auto &[k, v] : properties) {
        if (v.IsNull()) {
          throw QueryRuntimeException(fmt::format("Can't have null literal properties inside merge ({}.{})!",
                                                  edge_info.symbol.name(), dba->PropertyToName(k)));
        }
      }
    }
    if (!properties.empty()) MultiPropsInitChecked(&edge, properties);

    (*frame)[edge_info.symbol] = edge;
  } else {
    switch (maybe_edge.GetError()) {
      case storage::Error::SERIALIZATION_ERROR:
        throw TransactionSerializationException();
      case storage::Error::DELETED_OBJECT:
        throw QueryRuntimeException("Trying to create an edge on a deleted node.");
      case storage::Error::VERTEX_HAS_EDGES:
      case storage::Error::PROPERTIES_DISABLED:
      case storage::Error::NONEXISTENT_OBJECT:
        throw QueryRuntimeException("Unexpected error when creating an edge.");
    }
  }

  return *maybe_edge;
}

}  // namespace

bool CreateExpand::CreateExpandCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP_BY_REF(self_);

  AbortCheck(context);

  if (!input_cursor_->Pull(frame, context)) return false;
  ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                storage::View::NEW, nullptr, &context.number_of_hops);
  auto labels = EvaluateLabels(self_.node_info_.labels, evaluator, context.db_accessor);
  auto edge_type = EvaluateEdgeType(self_.edge_info_.edge_type, evaluator, context.db_accessor);

#ifdef MG_ENTERPRISE
  if (license::global_license_checker.IsEnterpriseValidFast()) {
    const auto fine_grained_permission = self_.existing_node_
                                             ? memgraph::query::AuthQuery::FineGrainedPrivilege::UPDATE

                                             : memgraph::query::AuthQuery::FineGrainedPrivilege::CREATE_DELETE;

    if (context.auth_checker &&
        !(context.auth_checker->Has(edge_type, memgraph::query::AuthQuery::FineGrainedPrivilege::CREATE_DELETE) &&
          context.auth_checker->Has(labels, fine_grained_permission))) {
      throw QueryRuntimeException("Edge not created due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
    }
  }
#endif
  // get the origin vertex
  TypedValue &vertex_value = frame[self_.input_symbol_];
  ExpectType(self_.input_symbol_, vertex_value, TypedValue::Type::Vertex);
  auto &v1 = vertex_value.ValueVertex();

  // get the destination vertex (possibly an existing node)
  auto &v2 = OtherVertex(frame, context, labels, evaluator);

  // create an edge between the two nodes
  auto *dba = context.db_accessor;

  auto created_edge = [&] {
    switch (self_.edge_info_.direction) {
      case EdgeAtom::Direction::IN:
        return CreateEdge(self_.edge_info_, edge_type, dba, &v2, &v1, &frame, context, &evaluator);
      case EdgeAtom::Direction::OUT:
      // in the case of an undirected CreateExpand we choose an arbitrary
      // direction. this is used in the MERGE clause
      // it is not allowed in the CREATE clause, and the semantic
      // checker needs to ensure it doesn't reach this point
      case EdgeAtom::Direction::BOTH:
        return CreateEdge(self_.edge_info_, edge_type, dba, &v1, &v2, &frame, context, &evaluator);
    }
  }();

  context.execution_stats[ExecutionStats::Key::CREATED_EDGES] += 1;
  if (context.trigger_context_collector) {
    context.trigger_context_collector->RegisterCreatedObject(created_edge);
  }

  return true;
}

void CreateExpand::CreateExpandCursor::Shutdown() { input_cursor_->Shutdown(); }

void CreateExpand::CreateExpandCursor::Reset() { input_cursor_->Reset(); }

VertexAccessor &CreateExpand::CreateExpandCursor::OtherVertex(Frame &frame, ExecutionContext &context,
                                                              std::vector<storage::LabelId> &labels,
                                                              ExpressionEvaluator &evaluator) {
  if (self_.existing_node_) {
    TypedValue &dest_node_value = frame[self_.node_info_.symbol];
    ExpectType(self_.node_info_.symbol, dest_node_value, TypedValue::Type::Vertex);
    return dest_node_value.ValueVertex();
  } else {
    auto &created_vertex = CreateLocalVertex(self_.node_info_, &frame, context, labels, evaluator);
    if (context.trigger_context_collector) {
      context.trigger_context_collector->RegisterCreatedObject(created_vertex);
    }
    return created_vertex;
  }
}

template <class TVerticesFun>
class ScanAllCursor : public Cursor {
 public:
  explicit ScanAllCursor(const ScanAll &self, Symbol output_symbol, UniqueCursorPtr input_cursor, storage::View view,
                         TVerticesFun get_vertices, const char *op_name)
      : self_(self),
        output_symbol_(std::move(output_symbol)),
        input_cursor_(std::move(input_cursor)),
        view_(view),
        get_vertices_(std::move(get_vertices)),
        op_name_(op_name) {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP_BY_REF(self_);

    AbortCheck(context);

    while (!vertices_ || vertices_it_.value() == vertices_end_it_.value()) {
      if (!input_cursor_->Pull(frame, context)) return false;
      // We need a getter function, because in case of exhausting a lazy
      // iterable, we cannot simply reset it by calling begin().
      auto next_vertices = get_vertices_(frame, context);
      if (!next_vertices) continue;
      vertices_ = std::move(next_vertices);
      vertices_it_.emplace(vertices_.value().begin());
      vertices_end_it_.emplace(vertices_.value().end());
    }
#ifdef MG_ENTERPRISE
    if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker && !FindNextVertex(context)) {
      return false;
    }
#endif

    frame[output_symbol_] = *vertices_it_.value();
    ++vertices_it_.value();
    return true;
  }

#ifdef MG_ENTERPRISE
  bool FindNextVertex(const ExecutionContext &context) {
    while (vertices_it_.value() != vertices_end_it_.value()) {
      if (context.auth_checker->Has(*vertices_it_.value(), view_,
                                    memgraph::query::AuthQuery::FineGrainedPrivilege::READ)) {
        return true;
      }
      ++vertices_it_.value();
    }
    return false;
  }
#endif

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    vertices_ = std::nullopt;
    vertices_it_ = std::nullopt;
    vertices_end_it_ = std::nullopt;
  }

 private:
  const ScanAll &self_;
  const Symbol output_symbol_;
  const UniqueCursorPtr input_cursor_;
  storage::View view_;
  TVerticesFun get_vertices_;
  std::optional<typename std::result_of<TVerticesFun(Frame &, ExecutionContext &)>::type::value_type> vertices_;
  std::optional<decltype(vertices_.value().begin())> vertices_it_;
  std::optional<decltype(vertices_.value().end())> vertices_end_it_;
  const char *op_name_;
};
template <typename TEdgesFun>
class ScanAllByEdgeCursor : public Cursor {
 public:
  explicit ScanAllByEdgeCursor(const ScanAllByEdge &self, UniqueCursorPtr input_cursor, storage::View view,
                               TEdgesFun get_edges, const char *op_name)
      : self_(self),
        input_cursor_(std::move(input_cursor)),
        view_(view),
        get_edges_(std::move(get_edges)),
        op_name_(op_name) {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP_BY_REF(self_);

    AbortCheck(context);

    while (!edges_ || edges_it_.value() == edges_end_it_.value()) {
      if (!input_cursor_->Pull(frame, context)) return false;
      auto next_edges = get_edges_(frame, context);
      if (!next_edges) continue;

      edges_.emplace(std::move(next_edges.value()));
      edges_it_.emplace(edges_.value().begin());
      edges_end_it_.emplace(edges_.value().end());
    }

    auto output_expansion = [this, &frame](const EdgeAccessor &edge, bool reverse) {
      frame[self_.common_.edge_symbol] = edge;
      if (!reverse) {
        frame[self_.common_.node1_symbol] = edge.From();
        frame[self_.common_.node2_symbol] = edge.To();
      } else {
        frame[self_.common_.node1_symbol] = edge.To();
        frame[self_.common_.node2_symbol] = edge.From();
      }
    };

    const EdgeAccessor edge = *edges_it_.value();
    frame[self_.common_.edge_symbol] = edge;
    if (self_.common_.direction == EdgeAtom::Direction::OUT) {
      output_expansion(edge, false);
    } else if (self_.common_.direction == EdgeAtom::Direction::IN) {
      output_expansion(edge, true);
    } else {
      // both, need to output the edge twice
      if (!do_reverse_output_) {
        output_expansion(edge, false);
        do_reverse_output_ = true;
        return true;
      }
      output_expansion(edge, true);
    }

    do_reverse_output_ = false;
    ++edges_it_.value();
    return true;
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    edges_ = std::nullopt;
    edges_it_ = std::nullopt;
    edges_end_it_ = std::nullopt;
    do_reverse_output_ = false;
  }

 private:
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
  const ScanAllByEdge &self_;
  const UniqueCursorPtr input_cursor_;
  storage::View view_;
  TEdgesFun get_edges_;

  std::optional<typename std::result_of<TEdgesFun(Frame &, ExecutionContext &)>::type::value_type> edges_;
  std::optional<decltype(edges_.value().begin())> edges_it_;
  std::optional<decltype(edges_.value().end())> edges_end_it_;
  const char *op_name_;
  bool do_reverse_output_{false};
};

ScanAll::ScanAll(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol, storage::View view)
    : input_(input ? input : std::make_shared<Once>()), output_symbol_(std::move(output_symbol)), view_(view) {}

ACCEPT_WITH_INPUT(ScanAll)

UniqueCursorPtr ScanAll::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ScanAllOperator);

  auto vertices = [this](Frame &, ExecutionContext &context) {
    auto *db = context.db_accessor;
    return std::make_optional(db->Vertices(view_));
  };
  return MakeUniqueCursorPtr<ScanAllCursor<decltype(vertices)>>(mem, *this, output_symbol_, input_->MakeCursor(mem),
                                                                view_, std::move(vertices), "ScanAll");
}

std::vector<Symbol> ScanAll::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(output_symbol_);
  return symbols;
}

std::string ScanAll::ToString() const { return fmt::format("ScanAll ({})", output_symbol_.name()); }

std::unique_ptr<LogicalOperator> ScanAll::Clone(AstStorage *storage) const {
  auto object = std::make_unique<ScanAll>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->output_symbol_ = output_symbol_;
  object->view_ = view_;
  return object;
}

ScanAllByLabel::ScanAllByLabel(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol,
                               storage::LabelId label, storage::View view)
    : ScanAll(input, output_symbol, view), label_(label) {}

ACCEPT_WITH_INPUT(ScanAllByLabel)

UniqueCursorPtr ScanAllByLabel::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ScanAllByLabelOperator);

  auto vertices = [this](Frame &, ExecutionContext &context) {
    auto *db = context.db_accessor;
    return std::make_optional(db->Vertices(view_, label_));
  };
  return MakeUniqueCursorPtr<ScanAllCursor<decltype(vertices)>>(mem, *this, output_symbol_, input_->MakeCursor(mem),
                                                                view_, std::move(vertices), "ScanAllByLabel");
}

std::string ScanAllByLabel::ToString() const {
  return fmt::format("ScanAllByLabel ({} :{})", output_symbol_.name(), dba_->LabelToName(label_));
}

std::unique_ptr<LogicalOperator> ScanAllByLabel::Clone(AstStorage *storage) const {
  auto object = std::make_unique<ScanAllByLabel>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->output_symbol_ = output_symbol_;
  object->view_ = view_;
  object->label_ = label_;
  return object;
}

ScanAllByEdge::ScanAllByEdge(const std::shared_ptr<LogicalOperator> &input, Symbol edge_symbol, Symbol node1_symbol,
                             Symbol node2_symbol, EdgeAtom::Direction direction,
                             const std::vector<storage::EdgeTypeId> &edge_types, storage::View view)
    : ScanAll(input, edge_symbol, view), common_{edge_symbol, node1_symbol, node2_symbol, direction, edge_types} {}

ACCEPT_WITH_INPUT(ScanAllByEdge)

UniqueCursorPtr ScanAllByEdge::MakeCursor(utils::MemoryResource * /*mem*/) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ScanAllByEdgeOperator);

  throw utils::NotYetImplemented("Sequential scan over edges!");
}

std::vector<Symbol> ScanAllByEdge::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(common_.edge_symbol);
  symbols.emplace_back(common_.node1_symbol);
  symbols.emplace_back(common_.node2_symbol);
  return symbols;
}

std::string ScanAllByEdge::ToString() const {
  return fmt::format(
      "ScanAllByEdge ({}){}[{}{}]{}({})", common_.node1_symbol.name(),
      common_.direction == query::EdgeAtom::Direction::IN ? "<-" : "-", common_.edge_symbol.name(),
      utils::IterableToString(common_.edge_types, "|",
                              [this](const auto &edge_type) { return ":" + dba_->EdgeTypeToName(edge_type); }),
      common_.direction == query::EdgeAtom::Direction::OUT ? "->" : "-", common_.node2_symbol.name());
}

std::unique_ptr<LogicalOperator> ScanAllByEdge::Clone(AstStorage *storage) const {
  auto object = std::make_unique<ScanAllByEdge>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->common_ = common_;
  object->view_ = view_;
  return object;
}

ScanAllByEdgeType::ScanAllByEdgeType(const std::shared_ptr<LogicalOperator> &input, Symbol edge_symbol,
                                     Symbol node1_symbol, Symbol node2_symbol, EdgeAtom::Direction direction,
                                     storage::EdgeTypeId edge_type, storage::View view)
    : ScanAllByEdge(input, edge_symbol, node1_symbol, node2_symbol, direction, {edge_type}, view) {}

ACCEPT_WITH_INPUT(ScanAllByEdgeType)

UniqueCursorPtr ScanAllByEdgeType::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ScanAllByEdgeTypeOperator);

  auto edges = [this](Frame &, ExecutionContext &context) {
    auto *db = context.db_accessor;
    return std::make_optional(db->Edges(view_, common_.edge_types[0]));
  };

  return MakeUniqueCursorPtr<ScanAllByEdgeCursor<decltype(edges)>>(mem, *this, input_->MakeCursor(mem), view_,
                                                                   std::move(edges), "ScanAllByEdgeType");
}

std::string ScanAllByEdgeType::ToString() const {
  return fmt::format(
      "ScanAllByEdgeType ({}){}[{}{}]{}({})", common_.node1_symbol.name(),
      common_.direction == query::EdgeAtom::Direction::IN ? "<-" : "-", common_.edge_symbol.name(),
      utils::IterableToString(common_.edge_types, "|",
                              [this](const auto &edge_type) { return ":" + dba_->EdgeTypeToName(edge_type); }),
      common_.direction == query::EdgeAtom::Direction::OUT ? "->" : "-", common_.node2_symbol.name());
}

std::unique_ptr<LogicalOperator> ScanAllByEdgeType::Clone(AstStorage *storage) const {
  auto object = std::make_unique<ScanAllByEdgeType>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->common_ = common_;
  object->view_ = view_;
  return object;
}

ScanAllByEdgeTypeProperty::ScanAllByEdgeTypeProperty(const std::shared_ptr<LogicalOperator> &input, Symbol edge_symbol,
                                                     Symbol node1_symbol, Symbol node2_symbol,
                                                     EdgeAtom::Direction direction, storage::EdgeTypeId edge_type,
                                                     storage::PropertyId property, storage::View view)
    : ScanAllByEdge(input, edge_symbol, node1_symbol, node2_symbol, direction, {edge_type}, view),
      property_(property) {}

ACCEPT_WITH_INPUT(ScanAllByEdgeTypeProperty)

UniqueCursorPtr ScanAllByEdgeTypeProperty::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ScanAllByEdgeTypePropertyOperator);

  const auto get_edges = [this](Frame &, ExecutionContext &context) {
    auto *db = context.db_accessor;
    return std::make_optional(db->Edges(view_, common_.edge_types[0], property_));
  };

  return MakeUniqueCursorPtr<ScanAllByEdgeCursor<decltype(get_edges)>>(
      mem, *this, input_->MakeCursor(mem), view_, std::move(get_edges), "ScanAllByEdgeTypeProperty");
}

std::string ScanAllByEdgeTypeProperty::ToString() const {
  return fmt::format(
      "ScanAllByEdgeTypeProperty ({0}){1}[{2}{3} {{{4}}}]{5}({6})", common_.node1_symbol.name(),
      common_.direction == query::EdgeAtom::Direction::IN ? "<-" : "-", common_.edge_symbol.name(),
      utils::IterableToString(common_.edge_types, "|",
                              [this](const auto &edge_type) { return ":" + dba_->EdgeTypeToName(edge_type); }),
      dba_->PropertyToName(property_), common_.direction == query::EdgeAtom::Direction::OUT ? "->" : "-",
      common_.node2_symbol.name());
}

std::unique_ptr<LogicalOperator> ScanAllByEdgeTypeProperty::Clone(AstStorage *storage) const {
  auto object = std::make_unique<ScanAllByEdgeTypeProperty>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->common_ = common_;
  object->view_ = view_;
  object->property_ = property_;
  return object;
}

ScanAllByEdgeTypePropertyValue::ScanAllByEdgeTypePropertyValue(const std::shared_ptr<LogicalOperator> &input,
                                                               Symbol edge_symbol, Symbol node1_symbol,
                                                               Symbol node2_symbol, EdgeAtom::Direction direction,
                                                               storage::EdgeTypeId edge_type,
                                                               storage::PropertyId property, Expression *expression,
                                                               storage::View view)
    : ScanAllByEdge(input, edge_symbol, node1_symbol, node2_symbol, direction, {edge_type}, view),
      property_(property),
      expression_(expression) {}

ACCEPT_WITH_INPUT(ScanAllByEdgeTypePropertyValue)

UniqueCursorPtr ScanAllByEdgeTypePropertyValue::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ScanAllByEdgeTypePropertyValueOperator);

  const auto get_edges = [this](Frame &frame, ExecutionContext &context)
      -> std::optional<decltype(context.db_accessor->Edges(view_, common_.edge_types[0], property_,
                                                           storage::PropertyValue()))> {
    auto *db = context.db_accessor;
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor, view_,
                                  nullptr, &context.number_of_hops);
    auto value = expression_->Accept(evaluator);
    if (value.IsNull()) return std::nullopt;
    if (!value.IsPropertyValue()) {
      throw QueryRuntimeException("'{}' cannot be used as a property value.", value.type());
    }
    return std::make_optional(
        db->Edges(view_, common_.edge_types[0], property_,
                  value.ToPropertyValue(context.db_accessor->GetStorageAccessor()->GetNameIdMapper())));
  };

  return MakeUniqueCursorPtr<ScanAllByEdgeCursor<decltype(get_edges)>>(
      mem, *this, input_->MakeCursor(mem), view_, std::move(get_edges), "ScanAllByEdgeTypePropertyValue");
}

std::string ScanAllByEdgeTypePropertyValue::ToString() const {
  return fmt::format(
      "ScanAllByEdgeTypePropertyValue ({0}){1}[{2}{3} {{{4}}}]{5}({6})", common_.node1_symbol.name(),
      common_.direction == query::EdgeAtom::Direction::IN ? "<-" : "-", common_.edge_symbol.name(),
      utils::IterableToString(common_.edge_types, "|",
                              [this](const auto &edge_type) { return ":" + dba_->EdgeTypeToName(edge_type); }),
      dba_->PropertyToName(property_), common_.direction == query::EdgeAtom::Direction::OUT ? "->" : "-",
      common_.node2_symbol.name());
}

std::unique_ptr<LogicalOperator> ScanAllByEdgeTypePropertyValue::Clone(AstStorage *storage) const {
  auto object = std::make_unique<ScanAllByEdgeTypePropertyValue>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->common_ = common_;
  object->view_ = view_;
  object->property_ = property_;
  object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
  return object;
}

ScanAllByEdgeTypePropertyRange::ScanAllByEdgeTypePropertyRange(
    const std::shared_ptr<LogicalOperator> &input, Symbol edge_symbol, Symbol node1_symbol, Symbol node2_symbol,
    EdgeAtom::Direction direction, storage::EdgeTypeId edge_type, storage::PropertyId property,
    std::optional<Bound> lower_bound, std::optional<Bound> upper_bound, storage::View view)
    : ScanAllByEdge(input, edge_symbol, node1_symbol, node2_symbol, direction, {edge_type}, view),
      property_(property),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound) {}

ACCEPT_WITH_INPUT(ScanAllByEdgeTypePropertyRange)

namespace {
std::optional<utils::Bound<storage::PropertyValue>> TryConvertToBound(std::optional<utils::Bound<Expression *>> bound,
                                                                      ExpressionEvaluator &evaluator) {
  if (!bound) return std::nullopt;
  const auto &value = bound->value()->Accept(evaluator);
  try {
    const auto &property_value = value.ToPropertyValue(evaluator.GetNameIdMapper());
    switch (property_value.type()) {
      case storage::PropertyValue::Type::Bool:
      case storage::PropertyValue::Type::List:
      case storage::PropertyValue::Type::Map:
      case storage::PropertyValue::Type::Enum:
      case storage::PropertyValueType::Point2d:
      case storage::PropertyValueType::Point3d:
        // Prevent indexed lookup with something that would fail if we did
        // the original filter with `operator<`. Note, for some reason,
        // Cypher does not support comparing boolean values.
        throw QueryRuntimeException("Range operator does not provide comparison methods for type {}.", value.type());
      case storage::PropertyValue::Type::Null:
      case storage::PropertyValue::Type::Int:
      case storage::PropertyValue::Type::Double:
      case storage::PropertyValue::Type::String:
      case storage::PropertyValue::Type::TemporalData:
      case storage::PropertyValue::Type::ZonedTemporalData:
        return std::make_optional(utils::Bound<storage::PropertyValue>(property_value, bound->type()));
    }
  } catch (const TypedValueException &) {
    throw QueryRuntimeException("'{}' cannot be used as a property value.", value.type());
  }
}
}  // namespace

UniqueCursorPtr ScanAllByEdgeTypePropertyRange::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ScanAllByEdgeTypePropertyRangeOperator);

  const auto get_edges = [this](Frame &frame, ExecutionContext &context)
      -> std::optional<decltype(context.db_accessor->Edges(view_, common_.edge_types[0], property_, std::nullopt,
                                                           std::nullopt))> {
    auto *db = context.db_accessor;
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor, view_,
                                  nullptr, &context.number_of_hops);

    auto maybe_lower = TryConvertToBound(lower_bound_, evaluator);
    auto maybe_upper = TryConvertToBound(upper_bound_, evaluator);

    // If any bound is null, then the comparison would result in nulls. This
    // is treated as not satisfying the filter, so return no vertices.
    if (maybe_lower && maybe_lower->value().IsNull()) return std::nullopt;
    if (maybe_upper && maybe_upper->value().IsNull()) return std::nullopt;

    return std::make_optional(db->Edges(view_, common_.edge_types[0], property_, maybe_lower, maybe_upper));
  };

  return MakeUniqueCursorPtr<ScanAllByEdgeCursor<decltype(get_edges)>>(
      mem, *this, input_->MakeCursor(mem), view_, std::move(get_edges), "ScanAllByEdgeTypePropertyRange");
}

std::string ScanAllByEdgeTypePropertyRange::ToString() const {
  return fmt::format(
      "ScanAllByEdgeTypePropertyRange ({0}){1}[{2}{3} {{{4}}}]{5}({6})", common_.node1_symbol.name(),
      common_.direction == query::EdgeAtom::Direction::IN ? "<-" : "-", common_.edge_symbol.name(),
      utils::IterableToString(common_.edge_types, "|",
                              [this](const auto &edge_type) { return ":" + dba_->EdgeTypeToName(edge_type); }),
      dba_->PropertyToName(property_), common_.direction == query::EdgeAtom::Direction::OUT ? "->" : "-",
      common_.node2_symbol.name());
}

std::unique_ptr<LogicalOperator> ScanAllByEdgeTypePropertyRange::Clone(AstStorage *storage) const {
  auto object = std::make_unique<ScanAllByEdgeTypePropertyRange>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->common_ = common_;
  object->view_ = view_;
  object->property_ = property_;
  if (lower_bound_) {
    object->lower_bound_.emplace(
        utils::Bound<Expression *>(lower_bound_->value()->Clone(storage), lower_bound_->type()));
  }
  if (upper_bound_) {
    object->upper_bound_.emplace(
        utils::Bound<Expression *>(upper_bound_->value()->Clone(storage), upper_bound_->type()));
  }
  return object;
}

ScanAllByEdgeProperty::ScanAllByEdgeProperty(const std::shared_ptr<LogicalOperator> &input, Symbol edge_symbol,
                                             Symbol node1_symbol, Symbol node2_symbol, EdgeAtom::Direction direction,
                                             storage::PropertyId property, storage::View view)
    : ScanAllByEdge(input, edge_symbol, node1_symbol, node2_symbol, direction, {}, view), property_(property) {}

ACCEPT_WITH_INPUT(ScanAllByEdgeProperty)

UniqueCursorPtr ScanAllByEdgeProperty::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ScanAllByEdgePropertyOperator);

  const auto get_edges = [this](Frame &, ExecutionContext &context) {
    auto *db = context.db_accessor;
    return std::make_optional(db->Edges(view_, property_));
  };

  return MakeUniqueCursorPtr<ScanAllByEdgeCursor<decltype(get_edges)>>(mem, *this, input_->MakeCursor(mem), view_,
                                                                       std::move(get_edges), "ScanAllByEdgeProperty");
}

std::string ScanAllByEdgeProperty::ToString() const {
  return fmt::format("ScanAllByEdgeProperty ({0}){1}[{2} {{{3}}}]{4}({5})", common_.node1_symbol.name(),
                     common_.direction == query::EdgeAtom::Direction::IN ? "<-" : "-", common_.edge_symbol.name(),
                     dba_->PropertyToName(property_), common_.direction == query::EdgeAtom::Direction::OUT ? "->" : "-",
                     common_.node2_symbol.name());
}

std::unique_ptr<LogicalOperator> ScanAllByEdgeProperty::Clone(AstStorage *storage) const {
  auto object = std::make_unique<ScanAllByEdgeProperty>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->common_ = common_;
  object->view_ = view_;
  object->property_ = property_;
  return object;
}

ScanAllByEdgePropertyValue::ScanAllByEdgePropertyValue(const std::shared_ptr<LogicalOperator> &input,
                                                       Symbol edge_symbol, Symbol node1_symbol, Symbol node2_symbol,
                                                       EdgeAtom::Direction direction, storage::PropertyId property,
                                                       Expression *expression, storage::View view)
    : ScanAllByEdge(input, edge_symbol, node1_symbol, node2_symbol, direction, {}, view),
      property_(property),
      expression_(expression) {}

ACCEPT_WITH_INPUT(ScanAllByEdgePropertyValue)

UniqueCursorPtr ScanAllByEdgePropertyValue::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ScanAllByEdgePropertyValueOperator);

  const auto get_edges = [this](Frame &frame, ExecutionContext &context)
      -> std::optional<decltype(context.db_accessor->Edges(view_, common_.edge_types[0], property_,
                                                           storage::PropertyValue()))> {
    auto *db = context.db_accessor;
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor, view_,
                                  nullptr, &context.number_of_hops);
    auto value = expression_->Accept(evaluator);
    if (value.IsNull()) return std::nullopt;
    if (!value.IsPropertyValue()) {
      throw QueryRuntimeException("'{}' cannot be used as a property value.", value.type());
    }
    return std::make_optional(db->Edges(
        view_, property_, value.ToPropertyValue(context.db_accessor->GetStorageAccessor()->GetNameIdMapper())));
  };

  return MakeUniqueCursorPtr<ScanAllByEdgeCursor<decltype(get_edges)>>(
      mem, *this, input_->MakeCursor(mem), view_, std::move(get_edges), "ScanAllByEdgePropertyValue");
}

std::string ScanAllByEdgePropertyValue::ToString() const {
  return fmt::format("ScanAllByEdgePropertyValue ({0}){1}[{2} {{{3}}}]{4}({5})", common_.node1_symbol.name(),
                     common_.direction == query::EdgeAtom::Direction::IN ? "<-" : "-", common_.edge_symbol.name(),
                     dba_->PropertyToName(property_), common_.direction == query::EdgeAtom::Direction::OUT ? "->" : "-",
                     common_.node2_symbol.name());
}

std::unique_ptr<LogicalOperator> ScanAllByEdgePropertyValue::Clone(AstStorage *storage) const {
  auto object = std::make_unique<ScanAllByEdgePropertyValue>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->common_ = common_;
  object->view_ = view_;
  object->property_ = property_;
  object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
  return object;
}

ScanAllByEdgePropertyRange::ScanAllByEdgePropertyRange(const std::shared_ptr<LogicalOperator> &input,
                                                       Symbol edge_symbol, Symbol node1_symbol, Symbol node2_symbol,
                                                       EdgeAtom::Direction direction, storage::PropertyId property,
                                                       std::optional<Bound> lower_bound,
                                                       std::optional<Bound> upper_bound, storage::View view)
    : ScanAllByEdge(input, edge_symbol, node1_symbol, node2_symbol, direction, {}, view),
      property_(property),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound) {}

ACCEPT_WITH_INPUT(ScanAllByEdgePropertyRange)

UniqueCursorPtr ScanAllByEdgePropertyRange::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ScanAllByEdgePropertyRangeOperator);

  const auto get_edges = [this](Frame &frame, ExecutionContext &context)
      -> std::optional<decltype(context.db_accessor->Edges(view_, common_.edge_types[0], property_, std::nullopt,
                                                           std::nullopt))> {
    auto *db = context.db_accessor;
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor, view_,
                                  nullptr, &context.number_of_hops);

    auto maybe_lower = TryConvertToBound(lower_bound_, evaluator);
    auto maybe_upper = TryConvertToBound(upper_bound_, evaluator);

    // If any bound is null, then the comparison would result in nulls. This
    // is treated as not satisfying the filter, so return no vertices.
    if (maybe_lower && maybe_lower->value().IsNull()) return std::nullopt;
    if (maybe_upper && maybe_upper->value().IsNull()) return std::nullopt;

    return std::make_optional(db->Edges(view_, property_, maybe_lower, maybe_upper));
  };

  return MakeUniqueCursorPtr<ScanAllByEdgeCursor<decltype(get_edges)>>(
      mem, *this, input_->MakeCursor(mem), view_, std::move(get_edges), "ScanAllByEdgePropertyRange");
}

std::string ScanAllByEdgePropertyRange::ToString() const {
  return fmt::format("ScanAllByEdgePropertyRange ({0}){1}[{2} {{{3}}}]{4}({5})", common_.node1_symbol.name(),
                     common_.direction == query::EdgeAtom::Direction::IN ? "<-" : "-", common_.edge_symbol.name(),
                     dba_->PropertyToName(property_), common_.direction == query::EdgeAtom::Direction::OUT ? "->" : "-",
                     common_.node2_symbol.name());
}

std::unique_ptr<LogicalOperator> ScanAllByEdgePropertyRange::Clone(AstStorage *storage) const {
  auto object = std::make_unique<ScanAllByEdgePropertyRange>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->common_ = common_;
  object->view_ = view_;
  object->property_ = property_;
  if (lower_bound_) {
    object->lower_bound_.emplace(
        utils::Bound<Expression *>(lower_bound_->value()->Clone(storage), lower_bound_->type()));
  }
  if (upper_bound_) {
    object->upper_bound_.emplace(
        utils::Bound<Expression *>(upper_bound_->value()->Clone(storage), upper_bound_->type()));
  }
  return object;
}

ScanAllByLabelProperties::ScanAllByLabelProperties(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol,
                                                   storage::LabelId label,
                                                   std::vector<storage::PropertyPath> properties,
                                                   std::vector<ExpressionRange> expression_ranges, storage::View view)
    : ScanAll(input, output_symbol, view),
      label_(label),
      properties_(std::move(properties)),
      expression_ranges_(std::move(expression_ranges)) {
  DMG_ASSERT(!properties_.empty(), "Properties are not optional.");
  DMG_ASSERT(!expression_ranges_.empty(), "Expressions are not optional.");
}

ACCEPT_WITH_INPUT(ScanAllByLabelProperties)

UniqueCursorPtr ScanAllByLabelProperties::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ScanAllByLabelPropertiesOperator);

  auto vertices = [this](Frame &frame, ExecutionContext &context)
      -> std::optional<decltype(context.db_accessor->Vertices(view_, label_, properties_,
                                                              std::span<storage::PropertyValueRange>{}))> {
    auto *db = context.db_accessor;
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor, view_,
                                  nullptr, &context.number_of_hops);

    auto to_property_value_range = [&](auto &&expression_range) { return expression_range.Evaluate(evaluator); };
    auto prop_value_ranges = expression_ranges_ | rv::transform(to_property_value_range) | ranges::to_vector;

    auto const bound_is_null = [](auto &&range) {
      return (range.lower_ && range.lower_->value().IsNull()) || (range.upper_ && range.upper_->value().IsNull());
    };

    // If either upper or lower bounds are `null`, then nothing can satisy the
    // filter.
    if (ranges::any_of(prop_value_ranges, bound_is_null)) {
      return std::nullopt;
    }

    return std::make_optional(db->Vertices(view_, label_, properties_, prop_value_ranges));
  };
  return MakeUniqueCursorPtr<ScanAllCursor<decltype(vertices)>>(mem, *this, output_symbol_, input_->MakeCursor(mem),
                                                                view_, std::move(vertices), "ScanAllByLabelProperties");
}

std::string ScanAllByLabelProperties::ToString() const {
  // TODO: better diagnostics...info about expression_ranges_?
  auto const property_names = properties_ | rv::transform([&](storage::PropertyPath const &property_path) {
                                return storage::ToString(property_path, dba_);
                              }) |
                              ranges::to_vector;
  auto const properties_stringified = utils::Join(property_names, ", ");
  return fmt::format("ScanAllByLabelProperties ({0} :{1} {{{2}}})", output_symbol_.name(), dba_->LabelToName(label_),
                     properties_stringified);
}

std::unique_ptr<LogicalOperator> ScanAllByLabelProperties::Clone(AstStorage *storage) const {
  auto object = std::make_unique<ScanAllByLabelProperties>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->output_symbol_ = output_symbol_;
  object->view_ = view_;
  object->label_ = label_;
  object->properties_ = properties_;
  object->expression_ranges_ = expression_ranges_ |
                               rv::transform([&](auto &&expr) { return ExpressionRange(expr, *storage); }) |
                               ranges::to_vector;
  return object;
}

ScanAllById::ScanAllById(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol, Expression *expression,
                         storage::View view)
    : ScanAll(input, output_symbol, view), expression_(expression) {
  MG_ASSERT(expression);
}

ACCEPT_WITH_INPUT(ScanAllById)

UniqueCursorPtr ScanAllById::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ScanAllByIdOperator);

  auto vertices = [this](Frame &frame, ExecutionContext &context) -> std::optional<std::vector<VertexAccessor>> {
    auto *db = context.db_accessor;
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor, view_,
                                  nullptr, &context.number_of_hops);
    auto value = expression_->Accept(evaluator);
    if (!value.IsNumeric()) return std::nullopt;
    int64_t id = value.IsInt() ? value.ValueInt() : value.ValueDouble();
    if (value.IsDouble() && id != value.ValueDouble()) return std::nullopt;
    auto maybe_vertex = db->FindVertex(storage::Gid::FromInt(id), view_);
    if (!maybe_vertex) return std::nullopt;
    return std::vector<VertexAccessor>{*maybe_vertex};
  };
  return MakeUniqueCursorPtr<ScanAllCursor<decltype(vertices)>>(mem, *this, output_symbol_, input_->MakeCursor(mem),
                                                                view_, std::move(vertices), "ScanAllById");
}

std::string ScanAllById::ToString() const { return fmt::format("ScanAllById ({})", output_symbol_.name()); }

std::unique_ptr<LogicalOperator> ScanAllById::Clone(AstStorage *storage) const {
  auto object = std::make_unique<ScanAllById>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->output_symbol_ = output_symbol_;
  object->view_ = view_;
  object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
  return object;
}

ScanAllByEdgeId::ScanAllByEdgeId(const std::shared_ptr<LogicalOperator> &input, Symbol edge_symbol, Symbol node1_symbol,
                                 Symbol node2_symbol, EdgeAtom::Direction direction, Expression *expression,
                                 storage::View view)
    : ScanAllByEdge(input, edge_symbol, node1_symbol, node2_symbol, direction, {}, view), expression_(expression) {
  MG_ASSERT(expression);
}

ACCEPT_WITH_INPUT(ScanAllByEdgeId)

UniqueCursorPtr ScanAllByEdgeId::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ScanAllByEdgeIdOperator);

  auto edges = [this](Frame &frame, ExecutionContext &context) -> std::optional<std::vector<EdgeAccessor>> {
    auto *db = context.db_accessor;
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor, view_,
                                  nullptr, &context.number_of_hops);
    auto value = expression_->Accept(evaluator);
    if (!value.IsNumeric()) return std::nullopt;
    int64_t id = value.IsInt() ? value.ValueInt() : value.ValueDouble();
    if (value.IsDouble() && id != value.ValueDouble()) return std::nullopt;
    auto maybe_edge = db->FindEdge(storage::Gid::FromInt(id), view_);
    if (!maybe_edge) return std::nullopt;
    return std::vector<EdgeAccessor>{*maybe_edge};
  };
  return MakeUniqueCursorPtr<ScanAllByEdgeCursor<decltype(edges)>>(mem, *this, input_->MakeCursor(mem), view_,
                                                                   std::move(edges), "ScanAllByEdgeId");
}

std::string ScanAllByEdgeId::ToString() const {
  return fmt::format("ScanAllByEdgeId ({})", common_.edge_symbol.name());
}

std::unique_ptr<LogicalOperator> ScanAllByEdgeId::Clone(AstStorage *storage) const {
  auto object = std::make_unique<ScanAllByEdgeId>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->common_ = common_;
  object->view_ = view_;
  object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
  return object;
}

namespace {
bool CheckExistingNode(const VertexAccessor &new_node, const Symbol &existing_node_sym, Frame &frame) {
  const TypedValue &existing_node = frame[existing_node_sym];
  if (existing_node.IsNull()) return false;
  ExpectType(existing_node_sym, existing_node, TypedValue::Type::Vertex);
  return existing_node.ValueVertex() == new_node;
}

template <class TEdgesResult>
auto UnwrapEdgesResult(storage::Result<TEdgesResult> &&result) {
  if (result.HasError()) {
    switch (result.GetError()) {
      case storage::Error::DELETED_OBJECT:
        throw QueryRuntimeException("Trying to get relationships of a deleted node.");
      case storage::Error::NONEXISTENT_OBJECT:
        throw query::QueryRuntimeException("Trying to get relationships from a node that doesn't exist.");
      case storage::Error::VERTEX_HAS_EDGES:
      case storage::Error::SERIALIZATION_ERROR:
      case storage::Error::PROPERTIES_DISABLED:
        throw QueryRuntimeException("Unexpected error when accessing relationships.");
    }
  }
  return std::move(*result);
}

}  // namespace

Expand::Expand(const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol, Symbol node_symbol,
               Symbol edge_symbol, EdgeAtom::Direction direction, const std::vector<storage::EdgeTypeId> &edge_types,
               bool existing_node, storage::View view)
    : input_(input ? input : std::make_shared<Once>()),
      input_symbol_(std::move(input_symbol)),
      common_{node_symbol, edge_symbol, direction, edge_types, existing_node},
      view_(view) {}

ACCEPT_WITH_INPUT(Expand)

UniqueCursorPtr Expand::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ExpandOperator);

  return MakeUniqueCursorPtr<ExpandCursor>(mem, *this, mem);
}

std::vector<Symbol> Expand::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(common_.node_symbol);
  symbols.emplace_back(common_.edge_symbol);
  return symbols;
}

std::string Expand::ToString() const {
  return fmt::format(
      "Expand ({}){}[{}{}]{}({})", input_symbol_.name(),
      common_.direction == query::EdgeAtom::Direction::IN ? "<-" : "-", common_.edge_symbol.name(),
      utils::IterableToString(common_.edge_types, "|",
                              [this](const auto &edge_type) { return ":" + dba_->EdgeTypeToName(edge_type); }),
      common_.direction == query::EdgeAtom::Direction::OUT ? "->" : "-", common_.node_symbol.name());
}

std::unique_ptr<LogicalOperator> Expand::Clone(AstStorage *storage) const {
  auto object = std::make_unique<Expand>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->input_symbol_ = input_symbol_;
  object->common_ = common_;
  object->view_ = view_;
  return object;
}

Expand::ExpandCursor::ExpandCursor(const Expand &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self.input_->MakeCursor(mem)) {}

Expand::ExpandCursor::ExpandCursor(const Expand &self, int64_t input_degree, int64_t existing_node_degree,
                                   utils::MemoryResource *mem)
    : self_(self),
      input_cursor_(self.input_->MakeCursor(mem)),
      prev_input_degree_(input_degree),
      prev_existing_degree_(existing_node_degree) {}

bool Expand::ExpandCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP_BY_REF(self_);

  // A helper function for expanding a node from an edge.
  auto pull_node = [this, &frame]<EdgeAtom::Direction direction>(const EdgeAccessor &new_edge,
                                                                 utils::tag_value<direction>) {
    if (self_.common_.existing_node) return;
    if constexpr (direction == EdgeAtom::Direction::IN) {
      frame[self_.common_.node_symbol] = new_edge.From();
    } else if constexpr (direction == EdgeAtom::Direction::OUT) {
      frame[self_.common_.node_symbol] = new_edge.To();
    } else {
      LOG_FATAL("Must indicate exact expansion direction here");
    }
  };

  while (true) {
    AbortCheck(context);
    // attempt to get a value from the incoming edges
    if (in_edges_ && *in_edges_it_ != in_edges_->end()) {
      auto edge = *(*in_edges_it_)++;
#ifdef MG_ENTERPRISE
      if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
          !(context.auth_checker->Has(edge, memgraph::query::AuthQuery::FineGrainedPrivilege::READ) &&
            context.auth_checker->Has(edge.From(), self_.view_,
                                      memgraph::query::AuthQuery::FineGrainedPrivilege::READ))) {
        continue;
      }
#endif

      frame[self_.common_.edge_symbol] = edge;
      pull_node(edge, utils::tag_v<EdgeAtom::Direction::IN>);
      return true;
    }

    // attempt to get a value from the outgoing edges
    if (out_edges_ && *out_edges_it_ != out_edges_->end()) {
      auto edge = *(*out_edges_it_)++;
      // when expanding in EdgeAtom::Direction::BOTH directions
      // we should do only one expansion for cycles, and it was
      // already done in the block above
      if (self_.common_.direction == EdgeAtom::Direction::BOTH && edge.IsCycle()) continue;
#ifdef MG_ENTERPRISE
      if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
          !(context.auth_checker->Has(edge, memgraph::query::AuthQuery::FineGrainedPrivilege::READ) &&
            context.auth_checker->Has(edge.To(), self_.view_,
                                      memgraph::query::AuthQuery::FineGrainedPrivilege::READ))) {
        continue;
      }
#endif
      frame[self_.common_.edge_symbol] = edge;
      pull_node(edge, utils::tag_v<EdgeAtom::Direction::OUT>);
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
  in_edges_ = std::nullopt;
  in_edges_it_ = std::nullopt;
  out_edges_ = std::nullopt;
  out_edges_it_ = std::nullopt;
}

ExpansionInfo Expand::ExpandCursor::GetExpansionInfo(Frame &frame) {
  TypedValue &vertex_value = frame[self_.input_symbol_];

  if (vertex_value.IsNull()) {
    return ExpansionInfo{};
  }

  ExpectType(self_.input_symbol_, vertex_value, TypedValue::Type::Vertex);
  auto &vertex = vertex_value.ValueVertex();

  auto direction = self_.common_.direction;
  if (!self_.common_.existing_node) {
    return ExpansionInfo{.input_node = vertex, .direction = direction};
  }

  TypedValue &existing_node = frame[self_.common_.node_symbol];

  if (existing_node.IsNull()) {
    return ExpansionInfo{.input_node = vertex, .direction = direction};
  }

  ExpectType(self_.common_.node_symbol, existing_node, TypedValue::Type::Vertex);

  auto &existing_vertex = existing_node.ValueVertex();

  // -1 and -1 -> normal expansion
  // -1 and expanded -> can't happen
  // expanded and -1 -> reverse
  // expanded and expanded -> see if can reverse
  if ((prev_input_degree_ == -1 && prev_existing_degree_ == -1) || prev_input_degree_ < prev_existing_degree_) {
    return ExpansionInfo{.input_node = vertex, .direction = direction, .existing_node = existing_vertex};
  }

  auto new_direction = direction;
  switch (new_direction) {
    case EdgeAtom::Direction::IN:
      new_direction = EdgeAtom::Direction::OUT;
      break;
    case EdgeAtom::Direction::OUT:
      new_direction = EdgeAtom::Direction::IN;
      break;
    default:
      new_direction = EdgeAtom::Direction::BOTH;
      break;
  }

  return ExpansionInfo{
      .input_node = existing_vertex, .direction = new_direction, .existing_node = vertex, .reversed = true};
}

bool Expand::ExpandCursor::InitEdges(Frame &frame, ExecutionContext &context) {
  // Input Vertex could be null if it is created by a failed optional match. In
  // those cases we skip that input pull and continue with the next.
  while (true) {
    if (!input_cursor_->Pull(frame, context)) return false;

    if (context.hops_limit.IsLimitReached()) return false;

    expansion_info_ = GetExpansionInfo(frame);

    if (!expansion_info_.input_node) {
      continue;
    }

    auto vertex = *expansion_info_.input_node;
    auto direction = expansion_info_.direction;

    int64_t num_expanded_first = -1;
    if (direction == EdgeAtom::Direction::IN || direction == EdgeAtom::Direction::BOTH) {
      if (self_.common_.existing_node) {
        if (expansion_info_.existing_node) {
          auto existing_node = *expansion_info_.existing_node;

          auto edges_result = UnwrapEdgesResult(
              vertex.InEdges(self_.view_, self_.common_.edge_types, existing_node, &context.hops_limit));
          context.number_of_hops += edges_result.expanded_count;
          in_edges_.emplace(std::move(edges_result.edges));
          num_expanded_first = edges_result.expanded_count;
        }
      } else {
        auto edges_result =
            UnwrapEdgesResult(vertex.InEdges(self_.view_, self_.common_.edge_types, &context.hops_limit));
        context.number_of_hops += edges_result.expanded_count;
        in_edges_.emplace(std::move(edges_result.edges));
        num_expanded_first = edges_result.expanded_count;
      }
      if (in_edges_) {
        in_edges_it_.emplace(in_edges_->begin());
      }
    }

    int64_t num_expanded_second = -1;
    if (direction == EdgeAtom::Direction::OUT || direction == EdgeAtom::Direction::BOTH) {
      if (self_.common_.existing_node) {
        if (expansion_info_.existing_node) {
          auto existing_node = *expansion_info_.existing_node;
          auto edges_result = UnwrapEdgesResult(
              vertex.OutEdges(self_.view_, self_.common_.edge_types, existing_node, &context.hops_limit));
          context.number_of_hops += edges_result.expanded_count;
          out_edges_.emplace(std::move(edges_result.edges));
          num_expanded_second = edges_result.expanded_count;
        }
      } else {
        auto edges_result =
            UnwrapEdgesResult(vertex.OutEdges(self_.view_, self_.common_.edge_types, &context.hops_limit));
        context.number_of_hops += edges_result.expanded_count;
        out_edges_.emplace(std::move(edges_result.edges));
        num_expanded_second = edges_result.expanded_count;
      }
      if (out_edges_) {
        out_edges_it_.emplace(out_edges_->begin());
      }
    }

    if (!expansion_info_.existing_node) {
      return true;
    }

    num_expanded_first = num_expanded_first == -1 ? 0 : num_expanded_first;
    num_expanded_second = num_expanded_second == -1 ? 0 : num_expanded_second;
    int64_t total_expanded_edges = num_expanded_first + num_expanded_second;

    if (!expansion_info_.reversed) {
      prev_input_degree_ = total_expanded_edges;
    } else {
      prev_existing_degree_ = total_expanded_edges;
    }

    return true;
  }
}

ExpandVariable::ExpandVariable(const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol, Symbol node_symbol,
                               Symbol edge_symbol, EdgeAtom::Type type, EdgeAtom::Direction direction,
                               const std::vector<storage::EdgeTypeId> &edge_types, bool is_reverse,
                               Expression *lower_bound, Expression *upper_bound, bool existing_node,
                               ExpansionLambda filter_lambda, std::optional<ExpansionLambda> weight_lambda,
                               std::optional<Symbol> total_weight, Expression *limit)
    : input_(input ? input : std::make_shared<Once>()),
      input_symbol_(std::move(input_symbol)),
      common_{node_symbol, edge_symbol, direction, edge_types, existing_node},
      type_(type),
      is_reverse_(is_reverse),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      filter_lambda_(std::move(filter_lambda)),
      weight_lambda_(std::move(weight_lambda)),
      total_weight_(std::move(total_weight)),
      limit_(limit) {
  DMG_ASSERT(type_ == EdgeAtom::Type::DEPTH_FIRST || type_ == EdgeAtom::Type::BREADTH_FIRST ||
                 type_ == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH || type_ == EdgeAtom::Type::ALL_SHORTEST_PATHS ||
                 type_ == EdgeAtom::Type::KSHORTEST,
             "ExpandVariable can only be used with breadth first, depth first, "
             "weighted shortest path, all shortest paths or bfs all paths type");
  DMG_ASSERT(!(type_ == EdgeAtom::Type::BREADTH_FIRST && is_reverse), "Breadth first expansion can't be reversed");
  DMG_ASSERT(type_ == EdgeAtom::Type::KSHORTEST || limit_ == nullptr,
             "Limit is only supported for KSHORTEST path expansion");
}

ACCEPT_WITH_INPUT(ExpandVariable)

std::vector<Symbol> ExpandVariable::ModifiedSymbols(const SymbolTable &table) const {
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
 * @param memory - Used to allocate the result.
 * @return See above.
 */
auto ExpandFromVertex(const VertexAccessor &vertex, EdgeAtom::Direction direction,
                      const std::vector<storage::EdgeTypeId> &edge_types, utils::MemoryResource *memory,
                      ExecutionContext *context) {
  // wraps an EdgeAccessor into a pair <accessor, direction>
  auto wrapper = [](EdgeAtom::Direction direction, auto &&edges) {
    return iter::imap([direction](const auto &edge) { return std::make_pair(edge, direction); },
                      std::forward<decltype(edges)>(edges));
  };

  storage::View view = storage::View::OLD;
  utils::pmr::vector<decltype(wrapper(direction, vertex.InEdges(view, edge_types).GetValue().edges))> chain_elements(
      memory);

  if (direction != EdgeAtom::Direction::OUT) {
    auto edges_result = UnwrapEdgesResult(vertex.InEdges(view, edge_types, &context->hops_limit));
    context->number_of_hops += edges_result.expanded_count;
    if (!edges_result.edges.empty()) {
      chain_elements.emplace_back(wrapper(EdgeAtom::Direction::IN, std::move(edges_result.edges)));
    }
  }

  if (direction != EdgeAtom::Direction::IN) {
    auto edges_result = UnwrapEdgesResult(vertex.OutEdges(view, edge_types, &context->hops_limit));
    context->number_of_hops += edges_result.expanded_count;
    if (!edges_result.edges.empty()) {
      chain_elements.emplace_back(wrapper(EdgeAtom::Direction::OUT, std::move(edges_result.edges)));
    }
  }

  // TODO: Investigate whether itertools perform heap allocation?
  return iter::chain.from_iterable(std::move(chain_elements));
}

}  // namespace

class ExpandVariableCursor : public Cursor {
 public:
  ExpandVariableCursor(const ExpandVariable &self, utils::MemoryResource *mem)
      : self_(self), input_cursor_(self.input_->MakeCursor(mem)), edges_(mem), edges_it_(mem) {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP_BY_REF(self_);

    AbortCheck(context);

    while (true) {
      if (Expand(frame, context)) return true;

      if (PullInput(frame, context)) {
        // if lower bound is zero we also yield empty paths
        if (lower_bound_ == 0) {
          auto &start_vertex = frame[self_.input_symbol_].ValueVertex();
          if (!self_.common_.existing_node) {
            frame[self_.common_.node_symbol] = start_vertex;
            return true;
          }
          if (CheckExistingNode(start_vertex, self_.common_.node_symbol, frame)) {
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
  const UniqueCursorPtr input_cursor_;
  // bounds. in the cursor they are not optional but set to
  // default values if missing in the ExpandVariable operator
  int64_t upper_bound_{-1};
  int64_t lower_bound_{-1};

  // a stack of edge iterables corresponding to the level/depth of
  // the expansion currently being Pulled
  using ExpandEdges =
      decltype(ExpandFromVertex(std::declval<VertexAccessor>(), EdgeAtom::Direction::IN, self_.common_.edge_types,
                                utils::NewDeleteResource(), std::declval<ExecutionContext *>()));

  utils::pmr::vector<ExpandEdges> edges_;
  // an iterator indicating the position in the corresponding edges_ element
  utils::pmr::vector<decltype(edges_.begin()->begin())> edges_it_;

  /**
   * Helper function that Pulls from the input vertex and
   * makes iteration over it's edges possible.
   *
   * @return If the Pull succeeded. If not, this VariableExpandCursor
   * is exhausted.
   */
  bool PullInput(Frame &frame, ExecutionContext &context) {
    // Input Vertex could be null if it is created by a failed optional match.
    // In those cases we skip that input pull and continue with the next.
    while (true) {
      AbortCheck(context);
      if (!input_cursor_->Pull(frame, context)) return false;

      if (context.hops_limit.IsLimitReached()) return false;

      TypedValue &vertex_value = frame[self_.input_symbol_];

      // Null check due to possible failed optional match.
      if (vertex_value.IsNull()) continue;

      ExpectType(self_.input_symbol_, vertex_value, TypedValue::Type::Vertex);
      auto &vertex = vertex_value.ValueVertex();

      // Evaluate the upper and lower bounds.
      ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                    storage::View::OLD, nullptr, &context.number_of_hops);
      auto calc_bound = [&evaluator](auto &bound) {
        auto value = EvaluateInt(evaluator, bound, "Variable expansion bound");
        if (value < 0) throw QueryRuntimeException("Variable expansion bound must be a non-negative integer.");
        return value;
      };

      lower_bound_ = self_.lower_bound_ ? calc_bound(self_.lower_bound_) : 1;
      upper_bound_ = self_.upper_bound_ ? calc_bound(self_.upper_bound_) : std::numeric_limits<int64_t>::max();

      if (upper_bound_ > 0) {
        auto *memory = edges_.get_allocator().resource();
        edges_.emplace_back(
            ExpandFromVertex(vertex, self_.common_.direction, self_.common_.edge_types, memory, &context));
        edges_it_.emplace_back(edges_.back().begin());
      }

      if (self_.filter_lambda_.accumulated_path_symbol) {
        // Add initial vertex of path to the accumulated path
        frame[self_.filter_lambda_.accumulated_path_symbol.value()] = Path(vertex);
      }

      // reset the frame value to an empty edge list
      if (frame[self_.common_.edge_symbol].IsList()) {
        // Preserve the list capacity if possible
        frame[self_.common_.edge_symbol].ValueList().clear();
      } else {
        auto *pull_memory = context.evaluation_context.memory;
        frame[self_.common_.edge_symbol] = TypedValue::TVector(pull_memory);
      }

      return true;
    }
  }

  // Helper function for appending an edge to the list on the frame.
  void AppendEdge(const EdgeAccessor &new_edge, utils::pmr::vector<TypedValue> *edges_on_frame) {
    // We are placing an edge on the frame. It is possible that there already
    // exists an edge on the frame for this level. If so first remove it.
    DMG_ASSERT(edges_.size() > 0, "Edges are empty");
    if (self_.is_reverse_) {
      // TODO: This is innefficient, we should look into replacing
      // vector with something else for TypedValue::List.
      size_t diff = edges_on_frame->size() - std::min(edges_on_frame->size(), edges_.size() - 1U);
      if (diff > 0U) edges_on_frame->erase(edges_on_frame->begin(), edges_on_frame->begin() + diff);
      edges_on_frame->emplace(edges_on_frame->begin(), new_edge);
    } else {
      edges_on_frame->resize(std::min(edges_on_frame->size(), edges_.size() - 1U));
      edges_on_frame->emplace_back(new_edge);
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
  bool Expand(Frame &frame, ExecutionContext &context) {
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                  storage::View::OLD, nullptr, &context.number_of_hops);
    // Some expansions might not be valid due to edge uniqueness and
    // existing_node criterions, so expand in a loop until either the input
    // vertex is exhausted or a valid variable-length expansion is available.
    while (true) {
      AbortCheck(context);
      // pop from the stack while there is stuff to pop and the current
      // level is exhausted
      while (!edges_.empty() && edges_it_.back() == edges_.back().end()) {
        edges_.pop_back();
        edges_it_.pop_back();
      }

      // check if we exhausted everything, if so return false
      if (edges_.empty()) return false;

      // we use this a lot
      auto &edges_on_frame = frame[self_.common_.edge_symbol].ValueList();

      // it is possible that edges_on_frame does not contain as many
      // elements as edges_ due to edge-uniqueness (when a whole layer
      // gets exhausted but no edges are valid). for that reason only
      // pop from edges_on_frame if they contain enough elements
      if (self_.is_reverse_) {
        auto diff = edges_on_frame.size() - std::min(edges_on_frame.size(), edges_.size());
        if (diff > 0) {
          edges_on_frame.erase(edges_on_frame.begin(), edges_on_frame.begin() + diff);
        }
      } else {
        edges_on_frame.resize(std::min(edges_on_frame.size(), edges_.size()));
      }

      // if we are here, we have a valid stack,
      // get the edge, increase the relevant iterator
      auto current_edge = *edges_it_.back()++;
      // Check edge-uniqueness.
      bool found_existing =
          std::any_of(edges_on_frame.begin(), edges_on_frame.end(),
                      [&current_edge](const TypedValue &edge) { return current_edge.first == edge.ValueEdge(); });
      if (found_existing) continue;

      VertexAccessor current_vertex =
          current_edge.second == EdgeAtom::Direction::IN ? current_edge.first.From() : current_edge.first.To();
#ifdef MG_ENTERPRISE
      if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
          !(context.auth_checker->Has(current_edge.first, memgraph::query::AuthQuery::FineGrainedPrivilege::READ) &&
            context.auth_checker->Has(current_vertex, storage::View::OLD,
                                      memgraph::query::AuthQuery::FineGrainedPrivilege::READ))) {
        continue;
      }
#endif
      AppendEdge(current_edge.first, &edges_on_frame);

      if (!self_.common_.existing_node) {
        frame[self_.common_.node_symbol] = current_vertex;
      }

      // Skip expanding out of filtered expansion.
      frame[self_.filter_lambda_.inner_edge_symbol] = current_edge.first;
      frame[self_.filter_lambda_.inner_node_symbol] = current_vertex;
      if (self_.filter_lambda_.accumulated_path_symbol) {
        MG_ASSERT(frame[self_.filter_lambda_.accumulated_path_symbol.value()].IsPath(),
                  "Accumulated path must be path");
        Path &accumulated_path = frame[self_.filter_lambda_.accumulated_path_symbol.value()].ValuePath();
        // Shrink the accumulated path including current level if necessary
        while (accumulated_path.size() >= edges_on_frame.size()) {
          accumulated_path.Shrink();
        }
        accumulated_path.Expand(current_edge.first);
        accumulated_path.Expand(current_vertex);
      }
      if (self_.filter_lambda_.expression && !EvaluateFilter(evaluator, self_.filter_lambda_.expression)) continue;

      // we are doing depth-first search, so place the current
      // edge's expansions onto the stack, if we should continue to expand
      if (upper_bound_ > static_cast<int64_t>(edges_.size()) && !context.hops_limit.IsLimitReached()) {
        auto *memory = edges_.get_allocator().resource();
        edges_.emplace_back(
            ExpandFromVertex(current_vertex, self_.common_.direction, self_.common_.edge_types, memory, &context));
        edges_it_.emplace_back(edges_.back().begin());
      }

      if (self_.common_.existing_node && !CheckExistingNode(current_vertex, self_.common_.node_symbol, frame)) continue;

      // We only yield true if we satisfy the lower bound.
      if (static_cast<int64_t>(edges_on_frame.size()) >= lower_bound_) {
        return true;
      }
    }
  }
};

class STShortestPathCursor : public query::plan::Cursor {
 public:
  STShortestPathCursor(const ExpandVariable &self, utils::MemoryResource *mem)
      : self_(self), input_cursor_(self_.input()->MakeCursor(mem)) {
    MG_ASSERT(self_.common_.existing_node,
              "s-t shortest path algorithm should only "
              "be used when `existing_node` flag is "
              "set!");
  }

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP("STShortestPath");

    AbortCheck(context);

    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                  storage::View::OLD, nullptr, &context.number_of_hops);
    while (input_cursor_->Pull(frame, context)) {
      if (context.hops_limit.IsLimitReached()) return false;

      const auto &source_tv = frame[self_.input_symbol_];
      const auto &sink_tv = frame[self_.common_.node_symbol];

      // It is possible that source or sink vertex is Null due to optional
      // matching.
      if (source_tv.IsNull() || sink_tv.IsNull()) continue;

      const auto &source = source_tv.ValueVertex();
      const auto &sink = sink_tv.ValueVertex();

      int64_t lower_bound =
          self_.lower_bound_ ? EvaluateInt(evaluator, self_.lower_bound_, "Min depth in breadth-first expansion") : 1;
      int64_t upper_bound = self_.upper_bound_
                                ? EvaluateInt(evaluator, self_.upper_bound_, "Max depth in breadth-first expansion")
                                : std::numeric_limits<int64_t>::max();

      if (upper_bound < 1 || lower_bound > upper_bound) continue;

      if (FindPath(source, sink, lower_bound, upper_bound, &frame, &evaluator, context)) {
        return true;
      }
    }
    return false;
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override { input_cursor_->Reset(); }

 private:
  const ExpandVariable &self_;
  UniqueCursorPtr input_cursor_;

  using VertexEdgeMapT = utils::pmr::unordered_map<VertexAccessor, std::optional<EdgeAccessor>>;

  void ReconstructPath(const VertexAccessor &midpoint, const VertexEdgeMapT &in_edge, const VertexEdgeMapT &out_edge,
                       Frame *frame, utils::MemoryResource *pull_memory) {
    utils::pmr::vector<TypedValue> result(pull_memory);
    auto last_vertex = midpoint;
    while (true) {
      const auto &last_edge = in_edge.at(last_vertex);
      if (!last_edge) break;
      last_vertex = last_edge->From() == last_vertex ? last_edge->To() : last_edge->From();
      result.emplace_back(*last_edge);
    }
    std::reverse(result.begin(), result.end());
    last_vertex = midpoint;
    while (true) {
      const auto &last_edge = out_edge.at(last_vertex);
      if (!last_edge) break;
      last_vertex = last_edge->From() == last_vertex ? last_edge->To() : last_edge->From();
      result.emplace_back(*last_edge);
    }
    frame->at(self_.common_.edge_symbol) = std::move(result);
  }

  bool ShouldExpand(const VertexAccessor &vertex, const EdgeAccessor &edge, Frame *frame,
                    ExpressionEvaluator *evaluator) {
    if (!self_.filter_lambda_.expression) return true;

    frame->at(self_.filter_lambda_.inner_node_symbol) = vertex;
    frame->at(self_.filter_lambda_.inner_edge_symbol) = edge;

    TypedValue result = self_.filter_lambda_.expression->Accept(*evaluator);
    if (result.IsNull()) return false;
    if (result.IsBool()) return result.ValueBool();

    throw QueryRuntimeException("Expansion condition must evaluate to boolean or null");
  }

  bool FindPath(const VertexAccessor &source, const VertexAccessor &sink, int64_t lower_bound, int64_t upper_bound,
                Frame *frame, ExpressionEvaluator *evaluator, ExecutionContext &context) {
    using utils::Contains;

    if (source == sink) return false;

    // We expand from both directions, both from the source and the sink.
    // Expansions meet at the middle of the path if it exists. This should
    // perform better for real-world like graphs where the expansion front
    // grows exponentially, effectively reducing the exponent by half.

    auto *pull_memory = evaluator->GetMemoryResource();
    // Holds vertices at the current level of expansion from the source
    // (sink).
    utils::pmr::vector<VertexAccessor> source_frontier(pull_memory);
    utils::pmr::vector<VertexAccessor> sink_frontier(pull_memory);

    // Holds vertices we can expand to from `source_frontier`
    // (`sink_frontier`).
    utils::pmr::vector<VertexAccessor> source_next(pull_memory);
    utils::pmr::vector<VertexAccessor> sink_next(pull_memory);

    // Maps each vertex we visited expanding from the source (sink) to the
    // edge used. Necessary for path reconstruction.
    VertexEdgeMapT in_edge(pull_memory);
    VertexEdgeMapT out_edge(pull_memory);

    size_t current_length = 0;

    source_frontier.emplace_back(source);
    in_edge[source] = std::nullopt;
    sink_frontier.emplace_back(sink);
    out_edge[sink] = std::nullopt;

    while (true) {
      AbortCheck(context);
      // Top-down step (expansion from the source).
      ++current_length;
      if (current_length > upper_bound) return false;

      for (const auto &vertex : source_frontier) {
        if (context.hops_limit.IsLimitReached()) break;
        if (self_.common_.direction != EdgeAtom::Direction::IN) {
          auto out_edges_result =
              UnwrapEdgesResult(vertex.OutEdges(storage::View::OLD, self_.common_.edge_types, &context.hops_limit));
          context.number_of_hops += out_edges_result.expanded_count;
          for (const auto &edge : out_edges_result.edges) {
#ifdef MG_ENTERPRISE
            if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
                !(context.auth_checker->Has(edge, memgraph::query::AuthQuery::FineGrainedPrivilege::READ) &&
                  context.auth_checker->Has(edge.To(), storage::View::OLD,
                                            memgraph::query::AuthQuery::FineGrainedPrivilege::READ))) {
              continue;
            }
#endif
            if (ShouldExpand(edge.To(), edge, frame, evaluator) && !Contains(in_edge, edge.To())) {
              in_edge.emplace(edge.To(), edge);
              if (Contains(out_edge, edge.To())) {
                if (current_length >= lower_bound) {
                  ReconstructPath(edge.To(), in_edge, out_edge, frame, pull_memory);
                  return true;
                } else {
                  return false;
                }
              }
              source_next.push_back(edge.To());
            }
          }
        }
        if (self_.common_.direction != EdgeAtom::Direction::OUT) {
          auto in_edges_result =
              UnwrapEdgesResult(vertex.InEdges(storage::View::OLD, self_.common_.edge_types, &context.hops_limit));
          context.number_of_hops += in_edges_result.expanded_count;
          for (const auto &edge : in_edges_result.edges) {
#ifdef MG_ENTERPRISE
            if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
                !(context.auth_checker->Has(edge, memgraph::query::AuthQuery::FineGrainedPrivilege::READ) &&
                  context.auth_checker->Has(edge.From(), storage::View::OLD,
                                            memgraph::query::AuthQuery::FineGrainedPrivilege::READ))) {
              continue;
            }
#endif
            if (ShouldExpand(edge.From(), edge, frame, evaluator) && !Contains(in_edge, edge.From())) {
              in_edge.emplace(edge.From(), edge);
              if (Contains(out_edge, edge.From())) {
                if (current_length >= lower_bound) {
                  ReconstructPath(edge.From(), in_edge, out_edge, frame, pull_memory);
                  return true;
                } else {
                  return false;
                }
              }
              source_next.push_back(edge.From());
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
        if (context.hops_limit.IsLimitReached()) break;
        if (self_.common_.direction != EdgeAtom::Direction::OUT) {
          auto out_edges_result =
              UnwrapEdgesResult(vertex.OutEdges(storage::View::OLD, self_.common_.edge_types, &context.hops_limit));
          context.number_of_hops += out_edges_result.expanded_count;
          for (const auto &edge : out_edges_result.edges) {
#ifdef MG_ENTERPRISE
            if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
                !(context.auth_checker->Has(edge, memgraph::query::AuthQuery::FineGrainedPrivilege::READ) &&
                  context.auth_checker->Has(edge.To(), storage::View::OLD,
                                            memgraph::query::AuthQuery::FineGrainedPrivilege::READ))) {
              continue;
            }
#endif
            if (ShouldExpand(vertex, edge, frame, evaluator) && !Contains(out_edge, edge.To())) {
              out_edge.emplace(edge.To(), edge);
              if (Contains(in_edge, edge.To())) {
                if (current_length >= lower_bound) {
                  ReconstructPath(edge.To(), in_edge, out_edge, frame, pull_memory);
                  return true;
                } else {
                  return false;
                }
              }
              sink_next.push_back(edge.To());
            }
          }
        }
        if (self_.common_.direction != EdgeAtom::Direction::IN) {
          auto in_edges_result =
              UnwrapEdgesResult(vertex.InEdges(storage::View::OLD, self_.common_.edge_types, &context.hops_limit));
          context.number_of_hops += in_edges_result.expanded_count;
          for (const auto &edge : in_edges_result.edges) {
#ifdef MG_ENTERPRISE
            if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
                !(context.auth_checker->Has(edge, memgraph::query::AuthQuery::FineGrainedPrivilege::READ) &&
                  context.auth_checker->Has(edge.From(), storage::View::OLD,
                                            memgraph::query::AuthQuery::FineGrainedPrivilege::READ))) {
              continue;
            }
#endif
            if (ShouldExpand(vertex, edge, frame, evaluator) && !Contains(out_edge, edge.From())) {
              out_edge.emplace(edge.From(), edge);
              if (Contains(in_edge, edge.From())) {
                if (current_length >= lower_bound) {
                  ReconstructPath(edge.From(), in_edge, out_edge, frame, pull_memory);
                  return true;
                } else {
                  return false;
                }
              }
              sink_next.push_back(edge.From());
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
  SingleSourceShortestPathCursor(const ExpandVariable &self, utils::MemoryResource *mem)
      : self_(self),
        input_cursor_(self_.input()->MakeCursor(mem)),
        processed_(mem),
        to_visit_next_(mem),
        to_visit_current_(mem) {
    MG_ASSERT(!self_.common_.existing_node,
              "Single source shortest path algorithm "
              "should not be used when `existing_node` "
              "flag is set, s-t shortest path algorithm "
              "should be used instead!");
  }

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP("SingleSourceShortestPath");

    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                  storage::View::OLD, nullptr, &context.number_of_hops);

    // for the given (edge, vertex) pair checks if they satisfy the
    // "where" condition. if so, places them in the to_visit_ structure.
    auto expand_pair = [this, &evaluator, &frame, &context](EdgeAccessor edge, VertexAccessor vertex) -> bool {
      // if we already processed the given vertex it doesn't get expanded
      if (processed_.find(vertex) != processed_.end()) return false;
#ifdef MG_ENTERPRISE
      if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
          !(context.auth_checker->Has(vertex, storage::View::OLD,
                                      memgraph::query::AuthQuery::FineGrainedPrivilege::READ) &&
            context.auth_checker->Has(edge, memgraph::query::AuthQuery::FineGrainedPrivilege::READ))) {
        return false;
      }
#endif
      frame[self_.filter_lambda_.inner_edge_symbol] = edge;
      frame[self_.filter_lambda_.inner_node_symbol] = vertex;
      std::optional<Path> curr_acc_path = std::nullopt;
      if (self_.filter_lambda_.accumulated_path_symbol) {
        MG_ASSERT(frame[self_.filter_lambda_.accumulated_path_symbol.value()].IsPath(),
                  "Accumulated path must have Path type");
        Path &accumulated_path = frame[self_.filter_lambda_.accumulated_path_symbol.value()].ValuePath();
        accumulated_path.Expand(edge);
        accumulated_path.Expand(vertex);
        curr_acc_path = accumulated_path;
      }

      if (self_.filter_lambda_.expression) {
        TypedValue result = self_.filter_lambda_.expression->Accept(evaluator);
        switch (result.type()) {
          case TypedValue::Type::Null:
            return true;
          case TypedValue::Type::Bool:
            if (!result.ValueBool()) return true;
            break;
          default:
            throw QueryRuntimeException("Expansion condition must evaluate to boolean or null.");
        }
      }
      to_visit_next_.emplace_back(edge, vertex, std::move(curr_acc_path));
      processed_.emplace(vertex, edge);
      return true;
    };

    auto restore_frame_state_after_expansion = [this, &frame](bool was_expanded) {
      if (was_expanded && self_.filter_lambda_.accumulated_path_symbol) {
        frame[self_.filter_lambda_.accumulated_path_symbol.value()].ValuePath().Shrink();
      }
    };

    // populates the to_visit_next_ structure with expansions
    // from the given vertex. skips expansions that don't satisfy
    // the "where" condition.
    auto expand_from_vertex = [this, &expand_pair, &restore_frame_state_after_expansion, &context](const auto &vertex) {
      if (self_.common_.direction != EdgeAtom::Direction::IN) {
        auto out_edges_result =
            UnwrapEdgesResult(vertex.OutEdges(storage::View::OLD, self_.common_.edge_types, &context.hops_limit));
        context.number_of_hops += out_edges_result.expanded_count;
        for (const auto &edge : out_edges_result.edges) {
          bool was_expanded = expand_pair(edge, edge.To());
          restore_frame_state_after_expansion(was_expanded);
        }
      }
      if (self_.common_.direction != EdgeAtom::Direction::OUT) {
        auto in_edges_result =
            UnwrapEdgesResult(vertex.InEdges(storage::View::OLD, self_.common_.edge_types, &context.hops_limit));
        context.number_of_hops += in_edges_result.expanded_count;
        for (const auto &edge : in_edges_result.edges) {
          bool was_expanded = expand_pair(edge, edge.From());
          restore_frame_state_after_expansion(was_expanded);
        }
      }
    };

    // do it all in a loop because we skip some elements
    while (true) {
      AbortCheck(context);
      // if we have nothing to visit on the current depth, switch to next
      if (to_visit_current_.empty()) to_visit_current_.swap(to_visit_next_);

      // if current is still empty, it means both are empty, so pull from
      // input
      if (to_visit_current_.empty()) {
        if (!input_cursor_->Pull(frame, context)) return false;

        if (context.hops_limit.IsLimitReached()) return false;

        to_visit_current_.clear();
        to_visit_next_.clear();
        processed_.clear();

        const auto &vertex_value = frame[self_.input_symbol_];
        // it is possible that the vertex is Null due to optional matching
        if (vertex_value.IsNull()) continue;
        lower_bound_ =
            self_.lower_bound_ ? EvaluateInt(evaluator, self_.lower_bound_, "Min depth in breadth-first expansion") : 1;
        upper_bound_ = self_.upper_bound_
                           ? EvaluateInt(evaluator, self_.upper_bound_, "Max depth in breadth-first expansion")
                           : std::numeric_limits<int64_t>::max();

        if (upper_bound_ < 1 || lower_bound_ > upper_bound_) continue;

        const auto &vertex = vertex_value.ValueVertex();
        processed_.emplace(vertex, std::nullopt);

        if (self_.filter_lambda_.accumulated_path_symbol) {
          // Add initial vertex of path to the accumulated path
          frame[self_.filter_lambda_.accumulated_path_symbol.value()] = Path(vertex);
        }

        expand_from_vertex(vertex);

        // go back to loop start and see if we expanded anything
        continue;
      }

      // take the next expansion from the queue
      auto [curr_edge, curr_vertex, curr_acc_path] = to_visit_current_.back();
      to_visit_current_.pop_back();

      // create the frame value for the edges
      auto *pull_memory = context.evaluation_context.memory;
      utils::pmr::vector<TypedValue> edge_list(pull_memory);
      edge_list.emplace_back(curr_edge);
      auto last_vertex = curr_vertex;
      while (true) {
        const EdgeAccessor &last_edge = edge_list.back().ValueEdge();
        last_vertex = last_edge.From() == last_vertex ? last_edge.To() : last_edge.From();
        // origin_vertex must be in processed
        const auto &previous_edge = processed_.find(last_vertex)->second;
        if (!previous_edge) break;

        edge_list.emplace_back(previous_edge.value());
      }

      // expand only if what we've just expanded is less then max depth
      if (static_cast<int64_t>(edge_list.size()) < upper_bound_) {
        if (self_.filter_lambda_.accumulated_path_symbol) {
          MG_ASSERT(curr_acc_path.has_value(), "Expected non-null accumulated path");
          frame[self_.filter_lambda_.accumulated_path_symbol.value()] = std::move(curr_acc_path.value());
        }
        if (!context.hops_limit.IsLimitReached()) {
          expand_from_vertex(curr_vertex);
        }
      }

      if (static_cast<int64_t>(edge_list.size()) < lower_bound_) continue;

      frame[self_.common_.node_symbol] = curr_vertex;

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
  const UniqueCursorPtr input_cursor_;

  // Depth bounds. Calculated on each pull from the input, the initial value
  // is irrelevant.
  int64_t lower_bound_{-1};
  int64_t upper_bound_{-1};

  // maps vertices to the edge they got expanded from. it is an optional
  // edge because the root does not get expanded from anything.
  // contains visited vertices as well as those scheduled to be visited.
  utils::pmr::unordered_map<VertexAccessor, std::optional<EdgeAccessor>> processed_;
  // edge, vertex we have yet to visit, for current and next depth and their accumulated paths
  utils::pmr::vector<std::tuple<EdgeAccessor, VertexAccessor, std::optional<Path>>> to_visit_next_;
  utils::pmr::vector<std::tuple<EdgeAccessor, VertexAccessor, std::optional<Path>>> to_visit_current_;
};

namespace {

void ValidateWeight(TypedValue current_weight) {
  if (current_weight.IsNull()) {
    return;
  }

  auto value = std::invoke(
      [&](const TypedValue &tv) -> double {
        switch (tv.type()) {
          case TypedValue::Type::Int:
            return static_cast<double>(tv.ValueInt());
          case TypedValue::Type::Double:
            return tv.ValueDouble();
          case TypedValue::Type::Duration:
            return static_cast<double>(tv.ValueDuration().microseconds);
          default:
            throw QueryRuntimeException("Weight must be numeric or a Duration, got {}.", tv.type());
        }
      },
      current_weight);

  if (value < 0.0) {
    throw QueryRuntimeException("Weight must be non-negative, got {}.", value);
  }
}

void ValidateWeightTypes(const TypedValue &lhs, const TypedValue &rhs) {
  if ((lhs.IsNumeric() && rhs.IsNumeric()) || (lhs.IsDuration() && rhs.IsDuration())) [[likely]] {
    return;
  }
  throw QueryRuntimeException(utils::MessageWithLink(
      "All weights should be of the same type, either numeric or a Duration. Please update the weight "
      "expression or the filter expression.",
      "https://memgr.ph/wsp"));
}

TypedValue CalculateNextWeight(const std::optional<memgraph::query::plan::ExpansionLambda> &weight_lambda,
                               const TypedValue &total_weight, ExpressionEvaluator &evaluator) {
  if (!weight_lambda) {
    return {};
  }
  TypedValue current_weight = weight_lambda->expression->Accept(evaluator);
  ValidateWeight(current_weight);
  if (total_weight.IsNull()) {
    return current_weight;
  }
  ValidateWeightTypes(current_weight, total_weight);

  return current_weight + total_weight;
}

}  // namespace

class ExpandWeightedShortestPathCursor : public query::plan::Cursor {
 public:
  ExpandWeightedShortestPathCursor(const ExpandVariable &self, utils::MemoryResource *mem)
      : self_(self),
        input_cursor_(self_.input_->MakeCursor(mem)),
        total_cost_(mem),
        previous_(mem),
        yielded_vertices_(mem),
        pq_(mem) {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP("ExpandWeightedShortestPath");

    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                  storage::View::OLD, nullptr, &context.number_of_hops);

    auto create_state = [this](const VertexAccessor &vertex, int64_t depth) {
      return std::make_pair(vertex, upper_bound_set_ ? depth : 0);
    };

    // For the given (edge, vertex, weight, depth) tuple checks if they
    // satisfy the "where" condition. if so, places them in the priority
    // queue.
    auto expand_pair = [this, &evaluator, &frame, &create_state](const EdgeAccessor &edge, const VertexAccessor &vertex,
                                                                 const TypedValue &total_weight, int64_t depth) {
      frame[self_.weight_lambda_->inner_edge_symbol] = edge;
      frame[self_.weight_lambda_->inner_node_symbol] = vertex;
      TypedValue next_weight = CalculateNextWeight(self_.weight_lambda_, total_weight, evaluator);

      std::optional<Path> curr_acc_path = std::nullopt;
      if (self_.filter_lambda_.expression) {
        frame[self_.filter_lambda_.inner_edge_symbol] = edge;
        frame[self_.filter_lambda_.inner_node_symbol] = vertex;
        if (self_.filter_lambda_.accumulated_path_symbol) {
          MG_ASSERT(frame[self_.filter_lambda_.accumulated_path_symbol.value()].IsPath(),
                    "Accumulated path must be path");
          Path &accumulated_path = frame[self_.filter_lambda_.accumulated_path_symbol.value()].ValuePath();
          accumulated_path.Expand(edge);
          accumulated_path.Expand(vertex);
          curr_acc_path = accumulated_path;

          if (self_.filter_lambda_.accumulated_weight_symbol) {
            frame[self_.filter_lambda_.accumulated_weight_symbol.value()] = next_weight;
          }
        }

        if (!EvaluateFilter(evaluator, self_.filter_lambda_.expression)) return;
      }

      auto next_state = create_state(vertex, depth);

      auto found_it = total_cost_.find(next_state);
      if (found_it != total_cost_.end() && (found_it->second.IsNull() || (found_it->second <= next_weight).ValueBool()))
        return;

      pq_.emplace(next_weight, depth + 1, vertex, edge, curr_acc_path);
    };

    auto restore_frame_state_after_expansion = [this, &frame]() {
      if (self_.filter_lambda_.accumulated_path_symbol) {
        frame[self_.filter_lambda_.accumulated_path_symbol.value()].ValuePath().Shrink();
      }
    };

    // Populates the priority queue structure with expansions
    // from the given vertex. skips expansions that don't satisfy
    // the "where" condition.
    auto expand_from_vertex = [this, &context, &expand_pair, &restore_frame_state_after_expansion](
                                  const VertexAccessor &vertex, const TypedValue &weight, int64_t depth) {
      if (self_.common_.direction != EdgeAtom::Direction::IN) {
        auto out_edges = UnwrapEdgesResult(vertex.OutEdges(storage::View::OLD, self_.common_.edge_types)).edges;
        for (const auto &edge : out_edges) {
#ifdef MG_ENTERPRISE
          if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
              !(context.auth_checker->Has(edge.To(), storage::View::OLD,
                                          memgraph::query::AuthQuery::FineGrainedPrivilege::READ) &&
                context.auth_checker->Has(edge, memgraph::query::AuthQuery::FineGrainedPrivilege::READ))) {
            continue;
          }
#endif
          expand_pair(edge, edge.To(), weight, depth);
          restore_frame_state_after_expansion();
        }
      }
      if (self_.common_.direction != EdgeAtom::Direction::OUT) {
        auto in_edges = UnwrapEdgesResult(vertex.InEdges(storage::View::OLD, self_.common_.edge_types)).edges;
        for (const auto &edge : in_edges) {
#ifdef MG_ENTERPRISE
          if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
              !(context.auth_checker->Has(edge.From(), storage::View::OLD,
                                          memgraph::query::AuthQuery::FineGrainedPrivilege::READ) &&
                context.auth_checker->Has(edge, memgraph::query::AuthQuery::FineGrainedPrivilege::READ))) {
            continue;
          }
#endif
          expand_pair(edge, edge.From(), weight, depth);
          restore_frame_state_after_expansion();
        }
      }
    };

    while (true) {
      AbortCheck(context);
      if (pq_.empty()) {
        if (!input_cursor_->Pull(frame, context)) return false;
        const auto &vertex_value = frame[self_.input_symbol_];
        if (vertex_value.IsNull()) continue;
        auto vertex = vertex_value.ValueVertex();
        if (self_.common_.existing_node) {
          const auto &node = frame[self_.common_.node_symbol];
          // Due to optional matching the existing node could be null.
          // Skip expansion for such nodes.
          if (node.IsNull()) continue;
        }

        std::optional<Path> curr_acc_path;
        if (self_.filter_lambda_.accumulated_path_symbol) {
          // Add initial vertex of path to the accumulated path
          curr_acc_path = Path(vertex);
          frame[self_.filter_lambda_.accumulated_path_symbol.value()] = curr_acc_path.value();
        }
        if (self_.upper_bound_) {
          upper_bound_ = EvaluateInt(evaluator, self_.upper_bound_, "Max depth in weighted shortest path expansion");
          upper_bound_set_ = true;
        } else {
          upper_bound_ = std::numeric_limits<int64_t>::max();
          upper_bound_set_ = false;
        }
        if (upper_bound_ < 1)
          throw QueryRuntimeException(
              "Maximum depth in weighted shortest path expansion must be at "
              "least 1.");

        frame[self_.weight_lambda_->inner_edge_symbol] = TypedValue();
        frame[self_.weight_lambda_->inner_node_symbol] = vertex;
        TypedValue current_weight =
            CalculateNextWeight(self_.weight_lambda_, /* total_weight */ TypedValue(), evaluator);

        // Clear existing data structures.
        previous_.clear();
        total_cost_.clear();
        yielded_vertices_.clear();

        pq_.emplace(current_weight, 0, vertex, std::nullopt, curr_acc_path);
        // We are adding the starting vertex to the set of yielded vertices
        // because we don't want to yield paths that end with the starting
        // vertex.
        yielded_vertices_.insert(vertex);
      }

      while (!pq_.empty()) {
        AbortCheck(context);
        auto [current_weight, current_depth, current_vertex, current_edge, curr_acc_path] = pq_.top();
        pq_.pop();

        auto current_state = create_state(current_vertex, current_depth);

        // Check if the vertex has already been processed.
        if (total_cost_.find(current_state) != total_cost_.end()) {
          continue;
        }
        previous_.emplace(current_state, current_edge);
        total_cost_.emplace(current_state, current_weight);

        // Expand only if what we've just expanded is less than max depth.
        if (current_depth < upper_bound_) {
          if (self_.filter_lambda_.accumulated_path_symbol) {
            frame[self_.filter_lambda_.accumulated_path_symbol.value()] = std::move(curr_acc_path.value());
          }
          expand_from_vertex(current_vertex, current_weight, current_depth);
        }

        // If we yielded a path for a vertex already, make the expansion but
        // don't return the path again.
        if (yielded_vertices_.find(current_vertex) != yielded_vertices_.end()) continue;

        // Reconstruct the path.
        auto last_vertex = current_vertex;
        auto last_depth = current_depth;
        auto *pull_memory = context.evaluation_context.memory;
        utils::pmr::vector<TypedValue> edge_list(pull_memory);
        while (true) {
          // Origin_vertex must be in previous.
          const auto &previous_edge = previous_.find(create_state(last_vertex, last_depth))->second;
          if (!previous_edge) break;
          last_vertex = previous_edge->From() == last_vertex ? previous_edge->To() : previous_edge->From();
          last_depth--;
          edge_list.emplace_back(previous_edge.value());
        }

        // Place destination node on the frame, handle existence flag.
        if (self_.common_.existing_node) {
          const auto &node = frame[self_.common_.node_symbol];
          if ((node != TypedValue(current_vertex, pull_memory)).ValueBool()) {
            continue;
          }
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
  const UniqueCursorPtr input_cursor_;

  // Upper bound on the path length.
  int64_t upper_bound_{-1};
  bool upper_bound_set_{false};

  struct WspStateHash {
    size_t operator()(const std::pair<VertexAccessor, int64_t> &key) const {
      return utils::HashCombine<VertexAccessor, int64_t>{}(key.first, key.second);
    }
  };

  // Maps vertices to weights they got in expansion.
  utils::pmr::unordered_map<std::pair<VertexAccessor, int64_t>, TypedValue, WspStateHash> total_cost_;

  // Maps vertices to edges used to reach them.
  utils::pmr::unordered_map<std::pair<VertexAccessor, int64_t>, std::optional<EdgeAccessor>, WspStateHash> previous_;

  // Keeps track of vertices for which we yielded a path already.
  utils::pmr::unordered_set<VertexAccessor> yielded_vertices_;

  // Priority queue comparator. Keep lowest weight on top of the queue.
  class PriorityQueueComparator {
   public:
    bool operator()(
        const std::tuple<TypedValue, int64_t, VertexAccessor, std::optional<EdgeAccessor>, std::optional<Path>> &lhs,
        const std::tuple<TypedValue, int64_t, VertexAccessor, std::optional<EdgeAccessor>, std::optional<Path>> &rhs) {
      const auto &lhs_weight = std::get<0>(lhs);
      const auto &rhs_weight = std::get<0>(rhs);
      // Null defines minimum value for all types
      if (lhs_weight.IsNull()) {
        return false;
      }

      if (rhs_weight.IsNull()) {
        return true;
      }

      ValidateWeightTypes(lhs_weight, rhs_weight);
      return (lhs_weight > rhs_weight).ValueBool();
    }
  };

  std::priority_queue<std::tuple<TypedValue, int64_t, VertexAccessor, std::optional<EdgeAccessor>, std::optional<Path>>,
                      utils::pmr::vector<std::tuple<TypedValue, int64_t, VertexAccessor, std::optional<EdgeAccessor>,
                                                    std::optional<Path>>>,
                      PriorityQueueComparator>
      pq_;

  void ClearQueue() {
    while (!pq_.empty()) pq_.pop();
  }
};

namespace {

// Numerical error can only happen with doubles
inline bool are_equal(const TypedValue &lhs, const TypedValue &rhs) {
  if (lhs.type() == rhs.type() && lhs.type() != TypedValue::Type::Double) {
    // Either both have to be integers or both have to be durations since we don't allow anything else
    return lhs.IsInt() ? lhs.ValueInt() == rhs.ValueInt() : lhs.ValueDuration() == rhs.ValueDuration();
  }
  // they are both numeric, validated in ValidateWeightTypes
  auto l = lhs.IsDouble() ? lhs.ValueDouble() : static_cast<double>(lhs.ValueInt());
  auto r = rhs.IsDouble() ? rhs.ValueDouble() : static_cast<double>(rhs.ValueInt());
  auto diff = std::abs(l - r);
  if (diff < 1e-12) return true;  // relative comparison doesn't work well if numbers are near zero
  return std::abs(l - r) < std::max(l, r) * 1e-12;
}
}  // namespace

class ExpandAllShortestPathsCursor : public query::plan::Cursor {
 public:
  ExpandAllShortestPathsCursor(const ExpandVariable &self, utils::MemoryResource *mem)
      : self_(self),
        input_cursor_(self_.input_->MakeCursor(mem)),
        cheapest_cost_(mem),
        visited_cost_(mem),
        total_cost_(mem),
        next_edges_(mem),
        traversal_stack_(mem),
        pq_(mem) {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP("ExpandAllShortestPathsCursor");

    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                  storage::View::OLD, nullptr, &context.number_of_hops);

    auto *memory = context.evaluation_context.memory;

    auto create_state = [this](const VertexAccessor &vertex, int64_t depth) {
      return std::make_pair(vertex, upper_bound_set_ ? depth : 0);
    };

    // For the given (edge, direction, weight, depth) tuple checks if they
    // satisfy the "where" condition. if so, places them in the priority
    // queue.
    auto expand_vertex = [this, &evaluator, &frame](const EdgeAccessor &edge, const EdgeAtom::Direction direction,
                                                    const TypedValue &total_weight, int64_t depth) {
      auto const &next_vertex = direction == EdgeAtom::Direction::IN ? edge.From() : edge.To();

      // Evaluate current weight
      frame[self_.weight_lambda_->inner_edge_symbol] = edge;
      frame[self_.weight_lambda_->inner_node_symbol] = next_vertex;
      TypedValue next_weight = CalculateNextWeight(self_.weight_lambda_, total_weight, evaluator);

      // If filter expression exists, evaluate filter
      std::optional<Path> curr_acc_path = std::nullopt;
      if (self_.filter_lambda_.expression) {
        frame[self_.filter_lambda_.inner_edge_symbol] = edge;
        frame[self_.filter_lambda_.inner_node_symbol] = next_vertex;
        if (self_.filter_lambda_.accumulated_path_symbol) {
          MG_ASSERT(frame[self_.filter_lambda_.accumulated_path_symbol.value()].IsPath(),
                    "Accumulated path must be path");
          Path &accumulated_path = frame[self_.filter_lambda_.accumulated_path_symbol.value()].ValuePath();
          accumulated_path.Expand(edge);
          accumulated_path.Expand(next_vertex);
          curr_acc_path = accumulated_path;

          if (self_.filter_lambda_.accumulated_weight_symbol) {
            frame[self_.filter_lambda_.accumulated_weight_symbol.value()] = next_weight;
          }
        }

        if (!EvaluateFilter(evaluator, self_.filter_lambda_.expression)) return;
      }

      auto found_it = visited_cost_.find(next_vertex);
      // Check if the vertex has already been processed.
      if (found_it != visited_cost_.end()) {
        auto &weights = found_it->second;
        bool insert = std::ranges::none_of(weights, [&depth, &next_weight](const auto &entry) {
          auto const &[old_weight, old_depth] = entry;
          return old_depth <= depth && (old_weight < next_weight).ValueBool() && !are_equal(old_weight, next_weight);
        });

        if (!insert) return;
        // They cannot be equal since we checked for that above
        // Its possible that some weights are worse because we update weights at the same time as expanding instead of
        // later when popping from the queue
        std::erase_if(weights, [&next_weight, depth](const std::pair<TypedValue, int64_t> &p) {
          return p.second >= depth && (p.first > next_weight).ValueBool();
        });
        weights.emplace_back(next_weight, depth);

      } else {
        visited_cost_[next_vertex] = {
            std::make_pair(next_weight, depth)};  // TODO (ivan): will this use correct allocator?
      }

      // update cheapest cost to get to the vertex
      auto best_cost = cheapest_cost_.find(next_vertex);
      if (best_cost == cheapest_cost_.end() || best_cost->second.IsNull() ||
          (next_weight < best_cost->second).ValueBool()) {
        cheapest_cost_[next_vertex] = next_weight;
      }

      pq_.emplace(std::move(next_weight), depth + 1, next_vertex, DirectedEdge{edge, direction, next_weight},
                  std::move(curr_acc_path));
    };

    auto restore_frame_state_after_expansion = [this, &frame]() {
      if (self_.filter_lambda_.accumulated_path_symbol) {
        frame[self_.filter_lambda_.accumulated_path_symbol.value()].ValuePath().Shrink();
      }
    };

    // Populates the priority queue structure with expansions
    // from the given vertex. skips expansions that don't satisfy
    // the "where" condition.
    auto expand_from_vertex = [this, &expand_vertex, &context, &restore_frame_state_after_expansion](
                                  const VertexAccessor &vertex, const TypedValue &weight, int64_t depth) {
      if (self_.common_.direction != EdgeAtom::Direction::IN) {
        auto out_edges = UnwrapEdgesResult(vertex.OutEdges(storage::View::OLD, self_.common_.edge_types)).edges;
        for (const auto &edge : out_edges) {
#ifdef MG_ENTERPRISE
          if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
              !(context.auth_checker->Has(edge.To(), storage::View::OLD,
                                          memgraph::query::AuthQuery::FineGrainedPrivilege::READ) &&
                context.auth_checker->Has(edge, memgraph::query::AuthQuery::FineGrainedPrivilege::READ))) {
            continue;
          }
#endif
          expand_vertex(edge, EdgeAtom::Direction::OUT, weight, depth);
          restore_frame_state_after_expansion();
        }
      }
      if (self_.common_.direction != EdgeAtom::Direction::OUT) {
        auto in_edges = UnwrapEdgesResult(vertex.InEdges(storage::View::OLD, self_.common_.edge_types)).edges;
        for (const auto &edge : in_edges) {
#ifdef MG_ENTERPRISE
          if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
              !(context.auth_checker->Has(edge.From(), storage::View::OLD,
                                          memgraph::query::AuthQuery::FineGrainedPrivilege::READ) &&
                context.auth_checker->Has(edge, memgraph::query::AuthQuery::FineGrainedPrivilege::READ))) {
            continue;
          }
#endif
          expand_vertex(edge, EdgeAtom::Direction::IN, weight, depth);
          restore_frame_state_after_expansion();
        }
      }
    };

    std::optional<VertexAccessor> start_vertex;

    auto create_path = [this, &frame, &memory]() {
      auto &current_level = traversal_stack_.back();
      auto &edges_on_frame = frame[self_.common_.edge_symbol].ValueList();

      // Clean out the current stack
      if (current_level.empty()) {
        if (!edges_on_frame.empty()) {
          if (!self_.is_reverse_) {
            edges_on_frame.pop_back();
          } else {
            edges_on_frame.erase(edges_on_frame.begin());
          }
        }
        traversal_stack_.pop_back();
        return false;
      }

      auto [current_edge, current_edge_direction, current_weight] = current_level.back();
      current_level.pop_back();

      // Edges order depends on direction of expansion
      if (!self_.is_reverse_)
        edges_on_frame.emplace_back(current_edge);
      else
        edges_on_frame.emplace(edges_on_frame.begin(), current_edge);

      auto next_vertex = current_edge_direction == EdgeAtom::Direction::IN ? current_edge.From() : current_edge.To();
      frame[self_.total_weight_.value()] = current_weight;

      if (next_edges_.find({next_vertex, traversal_stack_.size()}) != next_edges_.end()) {
        auto [it, inserted] =
            next_edges_.try_emplace({next_vertex, traversal_stack_.size()}, utils::pmr::list<DirectedEdge>(memory));

        // Need to propagate the allocator
        traversal_stack_.emplace_back(utils::pmr::list<DirectedEdge>(it->second, memory));
      } else {
        // Signal the end of iteration
        traversal_stack_.emplace_back(utils::pmr::list<DirectedEdge>(memory));
      }

      auto cheapest_cost = cheapest_cost_.find(next_vertex)->second;
      if ((current_weight > cheapest_cost).ValueBool() && !are_equal(current_weight, cheapest_cost)) return false;

      // Place destination node on the frame, handle existence flag
      if (self_.common_.existing_node) {
        const auto &node = frame[self_.common_.node_symbol];
        ExpectType(self_.common_.node_symbol, node, TypedValue::Type::Vertex);
        if (node.ValueVertex() != next_vertex) return false;
      } else {
        frame[self_.common_.node_symbol] = next_vertex;
      }
      return true;
    };

    auto create_DFS_traversal_tree = [this, &context, &memory, &frame, &create_state, &expand_from_vertex]() {
      while (!pq_.empty()) {
        AbortCheck(context);

        auto [current_weight, current_depth, current_vertex, directed_edge, acc_path] = pq_.top();
        pq_.pop();

        const auto &[current_edge, direction, weight] = directed_edge;
        auto current_state = create_state(current_vertex, current_depth);

        auto position = total_cost_.find(current_state);
        if (position != total_cost_.end()) {
          if ((position->second < current_weight).ValueBool() && !are_equal(position->second, current_weight)) continue;
        } else {
          total_cost_.emplace(current_state, current_weight);
          if (current_depth < upper_bound_) {
            if (self_.filter_lambda_.accumulated_path_symbol) {
              DMG_ASSERT(acc_path.has_value(), "Path must be already filled in AllShortestPath DFS traversals");
              frame[self_.filter_lambda_.accumulated_path_symbol.value()] = std::move(acc_path.value());
            }
            expand_from_vertex(current_vertex, current_weight, current_depth);
          }
        }

        // Searching for a previous vertex in the expansion
        auto prev_vertex = direction == EdgeAtom::Direction::IN ? current_edge.To() : current_edge.From();

        // Update the parent
        if (next_edges_.find({prev_vertex, current_depth - 1}) == next_edges_.end()) {
          next_edges_[{prev_vertex, current_depth - 1}] = utils::pmr::list<DirectedEdge>(memory);
        }
        next_edges_.at({prev_vertex, current_depth - 1}).emplace_back(directed_edge);
      }
    };

    // upper_bound_set is used when storing visited edges, because with an upper bound we also consider suboptimal
    // paths if they are shorter in depth
    if (self_.upper_bound_) {
      upper_bound_ = EvaluateInt(evaluator, self_.upper_bound_, "Max depth in all shortest path expansion");
      upper_bound_set_ = true;
    } else {
      upper_bound_ = std::numeric_limits<int64_t>::max();
      upper_bound_set_ = false;
    }

    // Check if upper bound is valid
    if (upper_bound_ < 1) {
      throw QueryRuntimeException("Maximum depth in all shortest paths expansion must be at least 1.");
    }

    // On first Pull run, traversal stack and priority queue are empty, so we start a pulling stream
    // and create a DFS traversal tree (main part of algorithm). Then we return the first path
    // created from the DFS traversal tree (basically a DFS algorithm).
    // On each subsequent Pull run, paths are created from the traversal stack and returned.
    while (true) {
      // Check if there is an external error.
      AbortCheck(context);

      // The algorithm is run all at once by create_DFS_traversal_tree, after which we
      // traverse the tree iteratively by preserving the traversal state on stack.
      while (!traversal_stack_.empty()) {
        if (create_path()) return true;
      }

      // If priority queue is empty start new pulling stream.
      if (pq_.empty()) {
        // Finish if there is nothing to pull
        if (!input_cursor_->Pull(frame, context)) return false;

        const auto &vertex_value = frame[self_.input_symbol_];
        if (vertex_value.IsNull()) continue;

        start_vertex = vertex_value.ValueVertex();

        if (self_.common_.existing_node) {
          const auto &node = frame[self_.common_.node_symbol];
          // Due to optional matching the existing node could be null.
          // Skip expansion for such nodes.
          if (node.IsNull()) continue;
        }

        // Clear existing data structures.
        visited_cost_.clear();
        cheapest_cost_.clear();
        next_edges_.clear();
        traversal_stack_.clear();
        total_cost_.clear();

        if (self_.filter_lambda_.accumulated_path_symbol) {
          // Add initial vertex of path to the accumulated path
          frame[self_.filter_lambda_.accumulated_path_symbol.value()] = Path(*start_vertex);
        }

        frame[self_.weight_lambda_->inner_edge_symbol] = TypedValue();
        frame[self_.weight_lambda_->inner_node_symbol] = *start_vertex;
        TypedValue current_weight =
            CalculateNextWeight(self_.weight_lambda_, /* total_weight */ TypedValue(), evaluator);

        expand_from_vertex(*start_vertex, current_weight, 0);
        cheapest_cost_[*start_vertex] = 0;
        visited_cost_.emplace(*start_vertex,
                              std::vector<std::pair<TypedValue, int64_t>>{std::make_pair(TypedValue(0, memory), 0)});

        auto new_vector = TypedValue::TVector(memory);
        if (upper_bound_set_ && upper_bound_ > 0) {
          new_vector.reserve(upper_bound_);
        }
        frame[self_.common_.edge_symbol] = std::move(new_vector);
      }

      // Create a DFS traversal tree from the start node
      create_DFS_traversal_tree();

      // DFS traversal tree is create,
      if (start_vertex && next_edges_.find({*start_vertex, 0}) != next_edges_.end()) {
        auto [it, inserted] = next_edges_.try_emplace({*start_vertex, 0}, utils::pmr::list<DirectedEdge>(memory));
        traversal_stack_.emplace_back(utils::pmr::list<DirectedEdge>(it->second, memory));
      }
    }
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    visited_cost_.clear();
    cheapest_cost_.clear();
    next_edges_.clear();
    traversal_stack_.clear();
    total_cost_.clear();
    ClearQueue();
  }

 private:
  const ExpandVariable &self_;
  const UniqueCursorPtr input_cursor_;

  // Upper bound on the path length.
  int64_t upper_bound_{-1};
  bool upper_bound_set_{false};

  struct AspStateHash {
    size_t operator()(const std::pair<VertexAccessor, int64_t> &key) const {
      return utils::HashCombine<VertexAccessor, int64_t>{}(key.first, key.second);
    }
  };

  using DirectedEdge = std::tuple<EdgeAccessor, EdgeAtom::Direction, TypedValue>;
  using NextEdgesState = std::pair<VertexAccessor, int64_t>;
  using VertexState = std::pair<VertexAccessor, int64_t>;
  // Maps vertices to minimum weights they got in expansion.
  utils::pmr::unordered_map<VertexAccessor, TypedValue> cheapest_cost_;
  utils::pmr::unordered_map<VertexAccessor, std::vector<std::pair<TypedValue, int64_t>>> visited_cost_;
  // Maps vertices to weights they got in expansion.
  utils::pmr::unordered_map<NextEdgesState, TypedValue, AspStateHash> total_cost_;
  // Maps the vertex with the potential expansion edge.
  utils::pmr::unordered_map<NextEdgesState, utils::pmr::list<DirectedEdge>, AspStateHash> next_edges_;
  // Stack indicating the traversal level.
  utils::pmr::list<utils::pmr::list<DirectedEdge>> traversal_stack_;

  // Priority queue comparator. Keep lowest weight on top of the queue.
  class PriorityQueueComparator {
   public:
    bool operator()(const std::tuple<TypedValue, int64_t, VertexAccessor, DirectedEdge, std::optional<Path>> &lhs,
                    const std::tuple<TypedValue, int64_t, VertexAccessor, DirectedEdge, std::optional<Path>> &rhs) {
      const auto &lhs_weight = std::get<0>(lhs);
      const auto &rhs_weight = std::get<0>(rhs);
      // Null defines minimum value for all types
      if (lhs_weight.IsNull()) {
        return false;
      }

      if (rhs_weight.IsNull()) {
        return true;
      }

      ValidateWeightTypes(lhs_weight, rhs_weight);
      return (lhs_weight > rhs_weight).ValueBool();
    }
  };

  // Priority queue - core element of the algorithm.
  // Stores: {weight, depth, next vertex, edge and direction}
  std::priority_queue<
      std::tuple<TypedValue, int64_t, VertexAccessor, DirectedEdge, std::optional<Path>>,
      utils::pmr::vector<std::tuple<TypedValue, int64_t, VertexAccessor, DirectedEdge, std::optional<Path>>>,
      PriorityQueueComparator>
      pq_;

  void ClearQueue() {
    while (!pq_.empty()) pq_.pop();
  }
};

// K-Shortest Paths Cursor using lazy-evaluated Yen's algorithm
class KShortestPathsCursor : public Cursor {
 public:
  KShortestPathsCursor(const ExpandVariable &self, utils::MemoryResource *mem)
      : self_(self),
        input_cursor_(self.input_->MakeCursor(mem)),
        shortest_paths_(mem),
        candidate_paths_(mem),
        found_paths_set_(mem),
        current_source_(std::nullopt),
        current_target_(std::nullopt),
        blocked_edges_(mem),
        blocked_vertices_(mem),
        distances_(mem),
        predecessors_(mem) {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP("KShortestPaths");

    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                  storage::View::OLD, nullptr, &context.number_of_hops);

    limit_ = self_.limit_ ? EvaluateInt(evaluator, self_.limit_, "Limit in KSHORTEST path expansion")
                          : std::numeric_limits<int64_t>::max();

    auto push_next_path = [&](Frame &frame, ExpressionEvaluator &evaluator) {
      PushPathToFrame(shortest_paths_[current_path_index_++], &frame, evaluator.GetMemoryResource());
      n_returned_paths_++;
    };

    // Check if we reached the maximum number of paths to return
    if (n_returned_paths_ >= limit_) {
      return false;
    }

    auto unsent_paths_count = [&]() { return shortest_paths_.size() - current_path_index_; };

    // If we have cached shortest paths, return the next one
    if (unsent_paths_count() > 0) {
      push_next_path(frame, evaluator);
      return true;
    }

    // Try to compute the next shortest path for current input
    if (current_input_initialized_ && current_source_.has_value() && current_target_.has_value() &&
        ComputeNextShortestPath(current_source_.value(), current_target_.value(), evaluator, context)) {
      push_next_path(frame, evaluator);
      return true;
    }

    // Need to pull new input
    while (input_cursor_->Pull(frame, context)) {
      AbortCheck(context);
      if (context.hops_limit.IsLimitReached()) return false;

      auto &source_tv = frame[self_.input_symbol_];
      auto &target_tv = frame[self_.common_.node_symbol];

      // It is possible that source or sink vertex is Null due to optional matching.
      if (source_tv.IsNull() || target_tv.IsNull()) continue;

      auto &source_vertex = source_tv.ValueVertex();
      auto &target_vertex = target_tv.ValueVertex();

      // Skip if source and target are the same vertex
      if (source_vertex == target_vertex) continue;

      lower_bound_ = self_.lower_bound_ ? EvaluateInt(evaluator, self_.lower_bound_, "Min depth in expansion") : 1;
      upper_bound_ = self_.upper_bound_ ? EvaluateInt(evaluator, self_.upper_bound_, "Max depth in expansion")
                                        : std::numeric_limits<int64_t>::max();

      // Initialize for this new source-target pair
      current_source_ = source_vertex;
      current_target_ = target_vertex;
      current_input_initialized_ = true;

      if (!InitializeKShortestPaths(source_vertex, target_vertex, evaluator, context)) {
        // If no path found, continue to next input
        continue;
      }

      // Handle lower bound
      auto *last_path = &shortest_paths_.back();
      while (last_path->edges.size() < lower_bound_) {
        current_path_index_ = shortest_paths_.size();
        if (!ComputeNextShortestPath(current_source_.value(), current_target_.value(), evaluator, context)) {
          break;
        }
        last_path = &shortest_paths_.back();
      }

      if (unsent_paths_count() > 0) {
        push_next_path(frame, evaluator);
        return true;
      }
    }

    return false;
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    ResetState();
    current_input_initialized_ = false;
  }

 private:
  struct PathInfo {
    utils::pmr::vector<EdgeAccessor> edges;
    size_t deviation_vertex_index;  // Index where this path deviates from parent

    explicit PathInfo(utils::MemoryResource *mem) : edges(mem), deviation_vertex_index(0) {}

    PathInfo(const utils::pmr::vector<EdgeAccessor> &path_edges, size_t deviation_idx, utils::MemoryResource *mem)
        : edges(path_edges.begin(), path_edges.end(), mem), deviation_vertex_index(deviation_idx) {}
  };

  struct PathComparator {
    bool operator()(const PathInfo &a, const PathInfo &b) const {
      return a.edges.size() > b.edges.size();  // Min-heap: smaller costs have higher priority
    }
  };

  struct EdgeAccessorHash {
    size_t operator()(const EdgeAccessor &edge) const { return std::hash<storage::Gid>{}(edge.Gid()); }
  };

  struct VertexAccessorHash {
    size_t operator()(const VertexAccessor &vertex) const { return std::hash<storage::Gid>{}(vertex.Gid()); }
  };

  struct PathGidsHash {
    size_t operator()(const utils::pmr::vector<storage::Gid> &path_gids) const {
      size_t hash = 0;
      for (const auto &gid : path_gids) {
        hash = utils::HashCombine<size_t, storage::Gid>{}(hash, gid);
      }
      return hash;
    }
  };

  const ExpandVariable &self_;
  UniqueCursorPtr input_cursor_;
  int64_t lower_bound_{1};
  int64_t upper_bound_{std::numeric_limits<int64_t>::max()};
  int64_t limit_{0};
  int64_t n_returned_paths_{0};

  // State for K-shortest paths algorithm
  utils::pmr::vector<PathInfo> shortest_paths_;
  std::priority_queue<PathInfo, utils::pmr::vector<PathInfo>, PathComparator> candidate_paths_;
  utils::pmr::unordered_set<utils::pmr::vector<storage::Gid>, PathGidsHash> found_paths_set_;
  size_t current_path_index_ = 0;

  // Re-entrant state
  bool current_input_initialized_ = false;
  std::optional<VertexAccessor> current_source_;
  std::optional<VertexAccessor> current_target_;

  // Dijkstra's algorithm state (reused for efficiency)
  utils::pmr::unordered_set<EdgeAccessor, EdgeAccessorHash> blocked_edges_;
  utils::pmr::unordered_set<VertexAccessor, VertexAccessorHash> blocked_vertices_;
  utils::pmr::unordered_map<VertexAccessor, double, VertexAccessorHash> distances_;
  utils::pmr::unordered_map<VertexAccessor, EdgeVertexAccessorResult, VertexAccessorHash> in_edges_;
  utils::pmr::unordered_map<VertexAccessor, EdgeVertexAccessorResult, VertexAccessorHash> out_edges_;
  utils::pmr::unordered_map<VertexAccessor, std::optional<EdgeAccessor>, VertexAccessorHash> predecessors_;

  // Bidirectional search state
  using VertexEdgeMapT = utils::pmr::unordered_map<VertexAccessor, std::optional<EdgeAccessor>>;

  bool InitializeKShortestPaths(const VertexAccessor &source, const VertexAccessor &target,
                                ExpressionEvaluator &evaluator, ExecutionContext &context) {
    ResetState();

    // Find the shortest path using Dijkstra's algorithm
    auto shortest_path = ComputeShortestPath(source, target, evaluator, context);
    if (!shortest_path.edges.empty()) {
      shortest_paths_.emplace_back(std::move(shortest_path));
      AddPathToFoundSet(shortest_paths_.back());
      return true;
    }
    return false;
  }

  bool ComputeNextShortestPath(const VertexAccessor &source, const VertexAccessor &target,
                               ExpressionEvaluator &evaluator, ExecutionContext &context) {
    if (shortest_paths_.empty()) return false;

    const auto &last_path = shortest_paths_.back();

    // Generate candidate paths by deviating at each vertex of the last shortest path
    for (size_t i = 0; i < last_path.edges.size(); ++i) {
      GenerateCandidatesFromDeviation(source, target, last_path, i, evaluator, context);
    }

    // Find the best candidate path
    while (!candidate_paths_.empty()) {
      PathInfo candidate = candidate_paths_.top();
      candidate_paths_.pop();
      // Handle upper bound
      if (candidate.edges.size() > upper_bound_) {
        // Next path is too long, stop generating candidates
        return false;
      }
      if (!IsPathInFoundSet(candidate)) {
        shortest_paths_.emplace_back(std::move(candidate));
        AddPathToFoundSet(shortest_paths_.back());
        return true;
      }
    }
    return false;
  }

  void GenerateCandidatesFromDeviation(const VertexAccessor &source, const VertexAccessor &target,
                                       const PathInfo &base_path, size_t deviation_index,
                                       ExpressionEvaluator &evaluator, ExecutionContext &context) {
    // Set up blocked edges and vertices for this deviation
    SetupBlockedElementsForDeviation(source, base_path, deviation_index);

    // Get the deviation vertex
    VertexAccessor deviation_vertex = GetVertexAtIndex(source, base_path, deviation_index);

    // Compute shortest path from deviation vertex to target with blocked elements
    auto spur_path = ComputeShortestPath(deviation_vertex, target, evaluator, context);

    if (!spur_path.edges.empty()) {
      // Combine the root path (up to deviation) with the spur path
      PathInfo candidate_path(evaluator.GetMemoryResource());

      // Add edges from source to deviation vertex
      for (size_t i = 0; i < deviation_index; ++i) {
        candidate_path.edges.push_back(base_path.edges[i]);
      }

      // Add spur path edges
      for (const auto &edge : spur_path.edges) {
        candidate_path.edges.push_back(edge);
      }

      candidate_path.deviation_vertex_index = deviation_index;

      candidate_paths_.push(std::move(candidate_path));
    }
  }

  void SetupBlockedElementsForDeviation(const VertexAccessor &source, const PathInfo &base_path,
                                        size_t deviation_index) {
    blocked_edges_.clear();
    blocked_vertices_.clear();

    // Block the edge at deviation index from all previously found paths that share the same prefix
    for (const auto &path : shortest_paths_) {
      if (deviation_index < path.edges.size()) {
        // Check if the path prefix matches up to deviation index
        bool prefix_matches = true;
        for (size_t i = 0; i < deviation_index; ++i) {
          if (i >= base_path.edges.size() || path.edges[i].Gid() != base_path.edges[i].Gid()) {
            prefix_matches = false;
            break;
          }
        }

        if (prefix_matches) {
          blocked_edges_.insert(path.edges[deviation_index]);
        }
      }
    }

    // Block vertices in the root path (except the deviation vertex)
    VertexAccessor current_vertex = source;
    for (size_t i = 0; i < deviation_index; ++i) {
      blocked_vertices_.insert(current_vertex);
      const auto &edge = base_path.edges[i];
      current_vertex = (edge.From() == current_vertex) ? edge.To() : edge.From();
    }
  }

  static VertexAccessor GetVertexAtIndex(const VertexAccessor &source, const PathInfo &path, size_t index) {
    if (index == 0) return source;

    VertexAccessor current = source;
    for (size_t i = 0; i < index && i < path.edges.size(); ++i) {
      const auto &edge = path.edges[i];
      current = (edge.From() == current) ? edge.To() : edge.From();
    }
    return current;
  }

  static PathInfo ReconstructPath(const VertexAccessor &midpoint, const VertexEdgeMapT &in_edge,
                                  const VertexEdgeMapT &out_edge, utils::MemoryResource *memory) {
    utils::pmr::vector<EdgeAccessor> result(memory);
    VertexAccessor current = midpoint;

    // Reconstruct the path from midpoint to source
    while (in_edge.contains(current)) {
      const auto &edge_opt = in_edge.at(current);
      if (edge_opt.has_value()) {
        const auto &edge = edge_opt.value();
        result.push_back(edge);
        current = (edge.From() == current) ? edge.To() : edge.From();
      } else {
        break;
      }
    }

    // Reverse the path from source to midpoint
    std::reverse(result.begin(), result.end());

    // Reconstruct the path from midpoint to target
    current = midpoint;
    while (out_edge.contains(current)) {
      const auto &edge_opt = out_edge.at(current);
      if (edge_opt.has_value()) {
        const auto &edge = edge_opt.value();
        result.push_back(edge);
        current = (edge.From() == current) ? edge.To() : edge.From();
      } else {
        break;
      }
    }

    return PathInfo(result, 0, memory);
  }

  static constexpr bool kTo = true;
  static constexpr bool kFrom = !kTo;

  template <bool To>
  static bool FineGrainedAccessCheck(const EdgeAccessor &edge, ExecutionContext &context) {
#ifdef MG_ENTERPRISE
    return (!license::global_license_checker.IsEnterpriseValidFast() || !context.auth_checker ||
            (context.auth_checker->Has(edge, memgraph::query::AuthQuery::FineGrainedPrivilege::READ) &&
             context.auth_checker->Has(To == kTo ? edge.To() : edge.From(), storage::View::OLD,
                                       memgraph::query::AuthQuery::FineGrainedPrivilege::READ)));
#else
    (void)edge;
    (void)context;
    return true;
#endif
  }

  template <bool To>
  static bool ShouldExpand(const EdgeAccessor &edge, ExecutionContext &context, const VertexEdgeMapT &edges,
                           const utils::pmr::unordered_set<EdgeAccessor, EdgeAccessorHash> &blocked_edges,
                           const utils::pmr::unordered_set<VertexAccessor, VertexAccessorHash> &blocked_vertices) {
    return FineGrainedAccessCheck<To>(edge, context) && !blocked_edges.contains(edge) &&
           !blocked_vertices.contains(To == kTo ? edge.To() : edge.From()) &&
           !edges.contains(To == kTo ? edge.To() : edge.From());
  }

  PathInfo ComputeShortestPath(const VertexAccessor &source, const VertexAccessor &target,
                               ExpressionEvaluator &evaluator, ExecutionContext &context) {
    using utils::Contains;

    if (source == target) return PathInfo(evaluator.GetMemoryResource());

    // We expand from both directions, both from the source and the target.
    // Expansions meet at the middle of the path if it exists. This should
    // perform better for real-world like graphs where the expansion front
    // grows exponentially, effectively reducing the exponent by half.

    auto *pull_memory = evaluator.GetMemoryResource();
    // Holds vertices at the current level of expansion from the source
    // (target).
    utils::pmr::vector<VertexAccessor> source_frontier(pull_memory);
    utils::pmr::vector<VertexAccessor> target_frontier(pull_memory);

    // Holds vertices we can expand to from `source_frontier`
    // (`target_frontier`).
    utils::pmr::vector<VertexAccessor> source_next(pull_memory);
    utils::pmr::vector<VertexAccessor> target_next(pull_memory);

    // Maps each vertex we visited expanding from the source (target) to the
    // edge used. Necessary for path reconstruction.
    VertexEdgeMapT in_edge(pull_memory);
    VertexEdgeMapT out_edge(pull_memory);

    size_t current_length = 0;

    source_frontier.emplace_back(source);
    in_edge[source] = std::nullopt;
    target_frontier.emplace_back(target);
    out_edge[target] = std::nullopt;

    while (true) {
      AbortCheck(context);
      // Top-down step (expansion from the source).
      ++current_length;
      if (current_length > upper_bound_) return PathInfo(evaluator.GetMemoryResource());

      for (const auto &vertex : source_frontier) {
        if (context.hops_limit.IsLimitReached()) break;
        if (self_.common_.direction != EdgeAtom::Direction::IN) {
          if (!out_edges_.contains(vertex)) {
            auto out_edges_result =
                UnwrapEdgesResult(vertex.OutEdges(storage::View::OLD, self_.common_.edge_types, &context.hops_limit));
            context.number_of_hops += out_edges_result.expanded_count;
            out_edges_.emplace(vertex, out_edges_result);
          }
          for (const auto &edge : out_edges_.at(vertex).edges) {
            if (!ShouldExpand<kTo>(edge, context, in_edge, blocked_edges_, blocked_vertices_)) {
              continue;
            }
            in_edge.emplace(edge.To(), edge);
            if (Contains(out_edge, edge.To())) {
              return ReconstructPath(edge.To(), in_edge, out_edge, evaluator.GetMemoryResource());
            }
            source_next.push_back(edge.To());
          }
        }
        if (self_.common_.direction != EdgeAtom::Direction::OUT) {
          if (!in_edges_.contains(vertex)) {
            auto in_edges_result =
                UnwrapEdgesResult(vertex.InEdges(storage::View::OLD, self_.common_.edge_types, &context.hops_limit));
            context.number_of_hops += in_edges_result.expanded_count;
            in_edges_.emplace(vertex, in_edges_result);
          }
          for (const auto &edge : in_edges_.at(vertex).edges) {
            if (!ShouldExpand<kFrom>(edge, context, in_edge, blocked_edges_, blocked_vertices_)) {
              continue;
            }
            in_edge.emplace(edge.From(), edge);
            if (Contains(out_edge, edge.From())) {
              return ReconstructPath(edge.From(), in_edge, out_edge, evaluator.GetMemoryResource());
            }
            source_next.push_back(edge.From());
          }
        }
      }

      if (source_next.empty()) return PathInfo(evaluator.GetMemoryResource());
      source_frontier.clear();
      std::swap(source_frontier, source_next);

      // Bottom-up step (expansion from the target).
      ++current_length;
      if (current_length > upper_bound_) return PathInfo(evaluator.GetMemoryResource());

      // When expanding from the target we have to be careful which edge
      // endpoint we pass to `should_expand`, because everything is
      // reversed.
      for (const auto &vertex : target_frontier) {
        if (context.hops_limit.IsLimitReached()) break;
        if (self_.common_.direction != EdgeAtom::Direction::OUT) {
          if (!out_edges_.contains(vertex)) {
            auto out_edges_result =
                UnwrapEdgesResult(vertex.OutEdges(storage::View::OLD, self_.common_.edge_types, &context.hops_limit));
            context.number_of_hops += out_edges_result.expanded_count;
            out_edges_.emplace(vertex, out_edges_result);
          }
          for (const auto &edge : out_edges_.at(vertex).edges) {
            if (!ShouldExpand<kTo>(edge, context, out_edge, blocked_edges_, blocked_vertices_)) {
              continue;
            }
            out_edge.emplace(edge.To(), edge);
            if (Contains(in_edge, edge.To())) {
              return ReconstructPath(edge.To(), in_edge, out_edge, evaluator.GetMemoryResource());
            }
            target_next.push_back(edge.To());
          }
        }
        if (self_.common_.direction != EdgeAtom::Direction::IN) {
          if (!in_edges_.contains(vertex)) {
            auto in_edges_result =
                UnwrapEdgesResult(vertex.InEdges(storage::View::OLD, self_.common_.edge_types, &context.hops_limit));
            context.number_of_hops += in_edges_result.expanded_count;
            in_edges_.emplace(vertex, in_edges_result);
          }
          for (const auto &edge : in_edges_.at(vertex).edges) {
            if (!ShouldExpand<kFrom>(edge, context, out_edge, blocked_edges_, blocked_vertices_)) {
              continue;
            }
            out_edge.emplace(edge.From(), edge);
            if (Contains(in_edge, edge.From())) {
              return ReconstructPath(edge.From(), in_edge, out_edge, evaluator.GetMemoryResource());
            }
            target_next.push_back(edge.From());
          }
        }
      }

      if (target_next.empty()) return PathInfo(evaluator.GetMemoryResource());
      target_frontier.clear();
      std::swap(target_frontier, target_next);
    }
  }

  void PushPathToFrame(const PathInfo &path, Frame *frame, utils::MemoryResource *memory) {
    auto edge_list = TypedValue::TVector(memory);
    for (const auto &edge : path.edges) {
      edge_list.emplace_back(edge);
    }
    (*frame)[self_.common_.edge_symbol] = std::move(edge_list);
  }

  bool IsPathInFoundSet(const PathInfo &path) {
    utils::pmr::vector<storage::Gid> path_gids(found_paths_set_.get_allocator());
    for (const auto &edge : path.edges) {
      path_gids.push_back(edge.Gid());
    }
    return found_paths_set_.contains(path_gids);
  }

  void AddPathToFoundSet(const PathInfo &path) {
    utils::pmr::vector<storage::Gid> path_gids(found_paths_set_.get_allocator());
    for (const auto &edge : path.edges) {
      path_gids.push_back(edge.Gid());
    }
    found_paths_set_.insert(std::move(path_gids));
  }

  void ResetState() {
    shortest_paths_.clear();
    while (!candidate_paths_.empty()) candidate_paths_.pop();
    found_paths_set_.clear();
    current_path_index_ = 0;
    blocked_edges_.clear();
    blocked_vertices_.clear();
    distances_.clear();
    predecessors_.clear();
  }
};

UniqueCursorPtr ExpandVariable::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ExpandVariableOperator);

  switch (type_) {
    case EdgeAtom::Type::BREADTH_FIRST:
      if (common_.existing_node) {
        return MakeUniqueCursorPtr<STShortestPathCursor>(mem, *this, mem);
      } else {
        return MakeUniqueCursorPtr<SingleSourceShortestPathCursor>(mem, *this, mem);
      }
    case EdgeAtom::Type::DEPTH_FIRST:
      return MakeUniqueCursorPtr<ExpandVariableCursor>(mem, *this, mem);
    case EdgeAtom::Type::WEIGHTED_SHORTEST_PATH:
      return MakeUniqueCursorPtr<ExpandWeightedShortestPathCursor>(mem, *this, mem);
    case EdgeAtom::Type::ALL_SHORTEST_PATHS:
      return MakeUniqueCursorPtr<ExpandAllShortestPathsCursor>(mem, *this, mem);
    case EdgeAtom::Type::KSHORTEST:
      return MakeUniqueCursorPtr<KShortestPathsCursor>(mem, *this, mem);
    case EdgeAtom::Type::SINGLE:
      LOG_FATAL("ExpandVariable should not be planned for a single expansion!");
  }
}

std::string ExpandVariable::ToString() const {
  return fmt::format(
      "{} ({}){}[{}{}]{}({})", OperatorName(), input_symbol_.name(),
      common_.direction == query::EdgeAtom::Direction::IN ? "<-" : "-", common_.edge_symbol.name(),
      utils::IterableToString(common_.edge_types, "|",
                              [this](const auto &edge_type) { return ":" + dba_->EdgeTypeToName(edge_type); }),
      common_.direction == query::EdgeAtom::Direction::OUT ? "->" : "-", common_.node_symbol.name());
}

std::unique_ptr<LogicalOperator> ExpandVariable::Clone(AstStorage *storage) const {
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

std::string_view ExpandVariable::OperatorName() const {
  using namespace std::string_view_literals;
  using Type = query::EdgeAtom::Type;
  switch (type_) {
    case Type::DEPTH_FIRST:
      return "ExpandVariable"sv;
    case Type::BREADTH_FIRST:
      return (common_.existing_node ? "STShortestPath"sv : "BFSExpand"sv);
    case Type::WEIGHTED_SHORTEST_PATH:
      return "WeightedShortestPath"sv;
    case Type::ALL_SHORTEST_PATHS:
      return "AllShortestPaths"sv;
    case Type::KSHORTEST:
      return "KShortest"sv;
    case Type::SINGLE:
      LOG_FATAL("Unexpected ExpandVariable::type_");
    default:
      LOG_FATAL("Unexpected ExpandVariable::type_");
  }
}

class ConstructNamedPathCursor : public Cursor {
 public:
  ConstructNamedPathCursor(ConstructNamedPath self, utils::MemoryResource *mem)
      : self_(std::move(self)), input_cursor_(self_.input()->MakeCursor(mem)) {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP("ConstructNamedPath");

    AbortCheck(context);

    if (!input_cursor_->Pull(frame, context)) return false;

    auto symbol_it = self_.path_elements_.begin();
    DMG_ASSERT(symbol_it != self_.path_elements_.end(), "Named path must contain at least one node");

    const auto &start_vertex = frame[*symbol_it++];
    auto *pull_memory = context.evaluation_context.memory;
    // In an OPTIONAL MATCH everything could be Null.
    if (start_vertex.IsNull()) {
      frame[self_.path_symbol_] = TypedValue(pull_memory);
      return true;
    }

    DMG_ASSERT(start_vertex.IsVertex(), "First named path element must be a vertex");
    query::Path path(start_vertex.ValueVertex(), pull_memory);

    // If the last path element symbol was for an edge list, then
    // the next symbol is a vertex and it should not append to the path
    // because
    // expansion already did it.
    bool last_was_edge_list = false;

    for (; symbol_it != self_.path_elements_.end(); symbol_it++) {
      const auto &expansion = frame[*symbol_it];
      //  We can have Null (OPTIONAL MATCH), a vertex, an edge, or an edge
      //  list (variable expand or BFS).
      switch (expansion.type()) {
        case TypedValue::Type::Null:
          frame[self_.path_symbol_] = TypedValue(pull_memory);
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
          const auto &edges = expansion.ValueList();
          for (const auto &edge_value : edges) {
            const auto &edge = edge_value.ValueEdge();
            const auto &from = edge.From();
            if (path.vertices().back() == from)
              path.Expand(edge, edge.To());
            else
              path.Expand(edge, from);
          }
          break;
        }
        default:
          LOG_FATAL("Unsupported type in named path construction");

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
  const UniqueCursorPtr input_cursor_;
};

ACCEPT_WITH_INPUT(ConstructNamedPath)

UniqueCursorPtr ConstructNamedPath::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ConstructNamedPathOperator);

  return MakeUniqueCursorPtr<ConstructNamedPathCursor>(mem, *this, mem);
}

std::vector<Symbol> ConstructNamedPath::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(path_symbol_);
  return symbols;
}

std::unique_ptr<LogicalOperator> ConstructNamedPath::Clone(AstStorage *storage) const {
  auto object = std::make_unique<ConstructNamedPath>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->path_symbol_ = path_symbol_;
  object->path_elements_ = path_elements_;
  return object;
}

Filter::Filter(const std::shared_ptr<LogicalOperator> &input,
               const std::vector<std::shared_ptr<LogicalOperator>> &pattern_filters, Expression *expression)
    : input_(input ? input : std::make_shared<Once>()), pattern_filters_(pattern_filters), expression_(expression) {}

Filter::Filter(const std::shared_ptr<LogicalOperator> &input,
               const std::vector<std::shared_ptr<LogicalOperator>> &pattern_filters, Expression *expression,
               Filters all_filters)
    : input_(input ? input : std::make_shared<Once>()),
      pattern_filters_(pattern_filters),
      expression_(expression),
      all_filters_(std::move(all_filters)) {}

bool Filter::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    input_->Accept(visitor);
    for (const auto &pattern_filter : pattern_filters_) {
      pattern_filter->Accept(visitor);
    }
  }
  return visitor.PostVisit(*this);
}

UniqueCursorPtr Filter::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::FilterOperator);

  return MakeUniqueCursorPtr<FilterCursor>(mem, *this, mem);
}

std::vector<Symbol> Filter::ModifiedSymbols(const SymbolTable &table) const { return input_->ModifiedSymbols(table); }

std::unique_ptr<LogicalOperator> Filter::Clone(AstStorage *storage) const {
  auto object = std::make_unique<Filter>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->pattern_filters_.resize(pattern_filters_.size());
  for (auto i1 = 0; i1 < pattern_filters_.size(); ++i1) {
    object->pattern_filters_[i1] = pattern_filters_[i1] ? pattern_filters_[i1]->Clone(storage) : nullptr;
  }
  object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
  return object;
}

std::string Filter::SingleFilterName(FilterInfo const &single_filter) {
  using Type = query::plan::FilterInfo::Type;
  if (single_filter.type == Type::Generic) {
    std::set<std::string, std::less<>> symbol_names;
    for (const auto &symbol : single_filter.used_symbols) {
      symbol_names.insert(symbol.name());
    }
    return fmt::format("Generic {{{}}}",
                       utils::IterableToString(symbol_names, ", ", [](const auto &name) { return name; }));
  } else if (single_filter.type == Type::Id) {
    return fmt::format("id({})", single_filter.id_filter->symbol_.name());
  } else if (single_filter.type == Type::Label) {
    if (single_filter.expression->GetTypeInfo() != LabelsTest::kType) {
      LOG_FATAL("Label filters not using LabelsTest are not supported for query inspection!");
    }
    auto filter_expression = static_cast<LabelsTest *>(single_filter.expression);
    std::set<std::string, std::less<>> AND_label_names;
    for (const auto &label : filter_expression->labels_) {
      AND_label_names.insert(label.name);
    }

    // Generate OR label string only if there are OR labels
    std::string OR_label_string;
    if (!filter_expression->or_labels_.empty()) {
      if (AND_label_names.empty()) {
        // If there is no AND_labels or if there is only one OR_labels vector we
        // don't need parentheses
        OR_label_string =
            filter_expression->or_labels_.size() == 1
                ? utils::IterableToString(filter_expression->or_labels_[0], "|",
                                          [](const auto &label) { return label.name; })
                : utils::IterableToString(filter_expression->or_labels_, ":", [](const auto &label_vec) {
                    return fmt::format(
                        "({})", utils::IterableToString(label_vec, "|", [](const auto &label) { return label.name; }));
                  });
        OR_label_string = fmt::format(":{}", OR_label_string);
      } else {
        OR_label_string = fmt::format(
            ":{}", utils::IterableToString(filter_expression->or_labels_, ":", [](const auto &label_vec) {
              return fmt::format("({})",
                                 utils::IterableToString(label_vec, "|", [](const auto &label) { return label.name; }));
            }));
      }
    }
    std::string AND_label_string;
    if (!AND_label_names.empty()) {
      AND_label_string =
          fmt::format(":{}", utils::IterableToString(AND_label_names, ":", [](const auto &label) { return label; }));
    }

    if (filter_expression->expression_->GetTypeInfo() != Identifier::kType) {
      return fmt::format("({}{})", AND_label_string, OR_label_string);
    }
    auto identifier_expression = static_cast<Identifier *>(filter_expression->expression_);
    return fmt::format("({} {}{})", identifier_expression->name_, AND_label_string, OR_label_string);
  } else if (single_filter.type == Type::Pattern) {
    return "Pattern";
  } else if (single_filter.type == Type::Property) {
    return fmt::format("{{{}.{}}}", single_filter.property_filter->symbol_.name(),
                       single_filter.property_filter->property_ids_);
  } else if (single_filter.type == Type::Point) {
    return fmt::format("{{{}.{}}}", single_filter.point_filter->symbol_.name(),
                       single_filter.point_filter->property_.name);
  } else {
    LOG_FATAL("Unexpected FilterInfo::Type");
  }
}

std::string Filter::ToString() const {
  std::set<std::string, std::less<>> filter_names;
  for (const auto &filter : all_filters_) {
    filter_names.insert(Filter::SingleFilterName(filter));
  }
  return fmt::format("Filter {}", utils::IterableToString(filter_names, ", ", [](const auto &name) { return name; }));
}

static std::vector<UniqueCursorPtr> MakeCursorVector(const std::vector<std::shared_ptr<LogicalOperator>> &ops,
                                                     utils::MemoryResource *mem) {
  std::vector<UniqueCursorPtr> cursors;
  cursors.reserve(ops.size());

  if (!ops.empty()) {
    for (const auto &op : ops) {
      cursors.push_back(op->MakeCursor(mem));
    }
  }

  return cursors;
}

Filter::FilterCursor::FilterCursor(const Filter &self, utils::MemoryResource *mem)
    : self_(self),
      input_cursor_(self_.input_->MakeCursor(mem)),
      pattern_filter_cursors_(MakeCursorVector(self_.pattern_filters_, mem)) {}

bool Filter::FilterCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP_BY_REF(self_);

  AbortCheck(context);

  // Like all filters, newly set values should not affect filtering of old
  // nodes and edges.
  ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                storage::View::OLD, context.frame_change_collector, &context.number_of_hops);
  while (input_cursor_->Pull(frame, context)) {
    for (const auto &pattern_filter_cursor : pattern_filter_cursors_) {
      pattern_filter_cursor->Pull(frame, context);
    }
    if (EvaluateFilter(evaluator, self_.expression_)) return true;
  }
  return false;
}

void Filter::FilterCursor::Shutdown() { input_cursor_->Shutdown(); }

void Filter::FilterCursor::Reset() { input_cursor_->Reset(); }

EvaluatePatternFilter::EvaluatePatternFilter(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol)
    : input_(input), output_symbol_(std::move(output_symbol)) {}

ACCEPT_WITH_INPUT(EvaluatePatternFilter);

UniqueCursorPtr EvaluatePatternFilter::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::EvaluatePatternFilterOperator);

  return MakeUniqueCursorPtr<EvaluatePatternFilterCursor>(mem, *this, mem);
}

EvaluatePatternFilter::EvaluatePatternFilterCursor::EvaluatePatternFilterCursor(const EvaluatePatternFilter &self,
                                                                                utils::MemoryResource *mem)
    : self_(self), input_cursor_(self_.input_->MakeCursor(mem)) {}

std::vector<Symbol> EvaluatePatternFilter::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

std::unique_ptr<LogicalOperator> EvaluatePatternFilter::Clone(AstStorage *storage) const {
  auto object = std::make_unique<EvaluatePatternFilter>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->output_symbol_ = output_symbol_;
  return object;
}

bool EvaluatePatternFilter::EvaluatePatternFilterCursor::Pull(Frame &frame, ExecutionContext &context) {
  SCOPED_PROFILE_OP("EvaluatePatternFilter");

  AbortCheck(context);

  std::function<void(TypedValue *)> function = [&frame, self = this->self_, input_cursor = this->input_cursor_.get(),
                                                &context](TypedValue *return_value) {
    OOMExceptionEnabler oom_exception;
    input_cursor->Reset();

    *return_value = TypedValue(input_cursor->Pull(frame, context), context.evaluation_context.memory);
  };

  frame[self_.output_symbol_] = TypedValue(std::move(function));
  return true;
}

void EvaluatePatternFilter::EvaluatePatternFilterCursor::Shutdown() { input_cursor_->Shutdown(); }

void EvaluatePatternFilter::EvaluatePatternFilterCursor::Reset() { input_cursor_->Reset(); }

Produce::Produce(const std::shared_ptr<LogicalOperator> &input, const std::vector<NamedExpression *> &named_expressions)
    : input_(input ? input : std::make_shared<Once>()), named_expressions_(named_expressions) {}

ACCEPT_WITH_INPUT(Produce)

UniqueCursorPtr Produce::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ProduceOperator);

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

std::unique_ptr<LogicalOperator> Produce::Clone(AstStorage *storage) const {
  auto object = std::make_unique<Produce>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->named_expressions_.resize(named_expressions_.size());
  for (auto i2 = 0; i2 < named_expressions_.size(); ++i2) {
    object->named_expressions_[i2] = named_expressions_[i2] ? named_expressions_[i2]->Clone(storage) : nullptr;
  }
  return object;
}

std::string Produce::ToString() const {
  return fmt::format("Produce {{{}}}",
                     utils::IterableToString(named_expressions_, ", ", [](const auto &nexpr) { return nexpr->name_; }));
}

Produce::ProduceCursor::ProduceCursor(const Produce &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self_.input_->MakeCursor(mem)) {}

bool Produce::ProduceCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP_BY_REF(self_);

  AbortCheck(context);

  if (input_cursor_->Pull(frame, context)) {
    // Produce should always yield the latest results.
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                  storage::View::NEW, context.frame_change_collector, &context.number_of_hops);
    for (auto *named_expr : self_.named_expressions_) {
      if (context.frame_change_collector && context.frame_change_collector->IsKeyTracked(named_expr->name_)) {
        context.frame_change_collector->ResetTrackingValue(named_expr->name_);
      }
      named_expr->Accept(evaluator);
    }
    return true;
  }
  return false;
}

void Produce::ProduceCursor::Shutdown() { input_cursor_->Shutdown(); }

void Produce::ProduceCursor::Reset() { input_cursor_->Reset(); }

Delete::Delete(const std::shared_ptr<LogicalOperator> &input_, const std::vector<Expression *> &expressions,
               bool detach_)
    : input_(input_), expressions_(expressions), detach_(detach_) {}

ACCEPT_WITH_INPUT(Delete)

UniqueCursorPtr Delete::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::DeleteOperator);

  return MakeUniqueCursorPtr<DeleteCursor>(mem, *this, mem);
}

std::vector<Symbol> Delete::ModifiedSymbols(const SymbolTable &table) const { return input_->ModifiedSymbols(table); }

std::unique_ptr<LogicalOperator> Delete::Clone(AstStorage *storage) const {
  auto object = std::make_unique<Delete>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->expressions_.resize(expressions_.size());
  for (auto i3 = 0; i3 < expressions_.size(); ++i3) {
    object->expressions_[i3] = expressions_[i3] ? expressions_[i3]->Clone(storage) : nullptr;
  }
  object->detach_ = detach_;
  object->buffer_size_ = buffer_size_ ? buffer_size_->Clone(storage) : nullptr;

  return object;
}

Delete::DeleteCursor::DeleteCursor(const Delete &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self_.input_->MakeCursor(mem)) {}

void Delete::DeleteCursor::UpdateDeleteBuffer(Frame &frame, ExecutionContext &context) {
  // Delete should get the latest information, this way it is also possible
  // to delete newly added nodes and edges.
  ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                storage::View::NEW, nullptr, &context.number_of_hops);

  auto *pull_memory = context.evaluation_context.memory;
  // collect expressions results so edges can get deleted before vertices
  // this is necessary because an edge that gets deleted could block vertex
  // deletion
  utils::pmr::vector<TypedValue> expression_results(pull_memory);
  expression_results.reserve(self_.expressions_.size());
  for (Expression *expression : self_.expressions_) {
    expression_results.emplace_back(expression->Accept(evaluator));
  }

  auto vertex_auth_checker = [&context](const VertexAccessor &va) -> bool {
#ifdef MG_ENTERPRISE
    return !(license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
             !context.auth_checker->Has(va, storage::View::NEW, query::AuthQuery::FineGrainedPrivilege::CREATE_DELETE));
#else
    return true;
#endif
  };

  auto edge_auth_checker = [&context](const EdgeAccessor &ea) -> bool {
#ifdef MG_ENTERPRISE
    return !(
        license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
        !(context.auth_checker->Has(ea, query::AuthQuery::FineGrainedPrivilege::CREATE_DELETE) &&
          context.auth_checker->Has(ea.To(), storage::View::NEW, query::AuthQuery::FineGrainedPrivilege::UPDATE) &&
          context.auth_checker->Has(ea.From(), storage::View::NEW, query::AuthQuery::FineGrainedPrivilege::UPDATE)));
#else
    return true;
#endif
  };

  for (TypedValue &expression_result : expression_results) {
    AbortCheck(context);
    switch (expression_result.type()) {
      case TypedValue::Type::Vertex: {
        auto va = expression_result.ValueVertex();
        if (vertex_auth_checker(va)) {
          buffer_.nodes.push_back(va);
        } else {
          throw QueryRuntimeException("Vertex not deleted due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
        }
        break;
      }
      case TypedValue::Type::Edge: {
        auto ea = expression_result.ValueEdge();
        if (edge_auth_checker(ea)) {
          buffer_.edges.push_back(ea);
        } else {
          throw QueryRuntimeException("Edge not deleted due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
        }
        break;
      }
      case TypedValue::Type::Path: {
        auto path = expression_result.ValuePath();
#ifdef MG_ENTERPRISE
        auto edges_res = std::any_of(path.edges().cbegin(), path.edges().cend(),
                                     [&edge_auth_checker](const auto &ea) { return !edge_auth_checker(ea); });
        auto vertices_res = std::any_of(path.vertices().cbegin(), path.vertices().cend(),
                                        [&vertex_auth_checker](const auto &va) { return !vertex_auth_checker(va); });

        if (edges_res || vertices_res) {
          throw QueryRuntimeException(
              "Path not deleted due to not having enough permission on all edges and vertices on the path! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
        }
#endif
        buffer_.nodes.insert(buffer_.nodes.begin(), path.vertices().begin(), path.vertices().end());
        buffer_.edges.insert(buffer_.edges.begin(), path.edges().begin(), path.edges().end());
        break;
      }
      case TypedValue::Type::Null:
        break;
      default:
        throw QueryRuntimeException("Edges, vertices and paths can be deleted.");
    }
  }
}

bool Delete::DeleteCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP("Delete");

  AbortCheck(context);

  if (self_.buffer_size_ != nullptr && !buffer_size_.has_value()) [[unlikely]] {
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                  storage::View::OLD, nullptr, &context.number_of_hops);
    buffer_size_ = *EvaluateDeleteBufferSize(evaluator, self_.buffer_size_);
  }

  bool const has_more = input_cursor_->Pull(frame, context);

  if (has_more) {
    UpdateDeleteBuffer(frame, context);
    pulled_++;
  }

  if (!has_more || (buffer_size_.has_value() && pulled_ >= *buffer_size_)) {
    auto &dba = *context.db_accessor;
    auto res = dba.DetachDelete(std::move(buffer_.nodes), std::move(buffer_.edges), self_.detach_);
    if (res.HasError()) {
      switch (res.GetError()) {
        case storage::Error::SERIALIZATION_ERROR:
          throw TransactionSerializationException();
        case storage::Error::VERTEX_HAS_EDGES:
          throw RemoveAttachedVertexException();
        case storage::Error::DELETED_OBJECT:
        case storage::Error::PROPERTIES_DISABLED:
        case storage::Error::NONEXISTENT_OBJECT:
          throw QueryRuntimeException("Unexpected error when deleting a node.");
      }
    }

    if (*res) {
      context.execution_stats[ExecutionStats::Key::DELETED_NODES] += static_cast<int64_t>((*res)->first.size());
      context.execution_stats[ExecutionStats::Key::DELETED_EDGES] += static_cast<int64_t>((*res)->second.size());
    }

    // Update deleted objects for triggers
    if (context.trigger_context_collector && *res) {
      for (const auto &node : (*res)->first) {
        context.trigger_context_collector->RegisterDeletedObject(node);
      }

      if (context.trigger_context_collector->ShouldRegisterDeletedObject<query::EdgeAccessor>()) {
        for (const auto &edge : (*res)->second) {
          context.trigger_context_collector->RegisterDeletedObject(edge);
        }
      }
    }

    pulled_ = 0;
  }

  return has_more;
}

void Delete::DeleteCursor::Shutdown() { input_cursor_->Shutdown(); }

void Delete::DeleteCursor::Reset() {
  input_cursor_->Reset();
  pulled_ = 0;
}

SetProperty::SetProperty(const std::shared_ptr<LogicalOperator> &input, storage::PropertyId property,
                         PropertyLookup *lhs, Expression *rhs)
    : input_(input), property_(property), lhs_(lhs), rhs_(rhs) {}

ACCEPT_WITH_INPUT(SetProperty)

UniqueCursorPtr SetProperty::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::SetPropertyOperator);

  return MakeUniqueCursorPtr<SetPropertyCursor>(mem, *this, mem);
}

std::vector<Symbol> SetProperty::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

std::unique_ptr<LogicalOperator> SetProperty::Clone(AstStorage *storage) const {
  auto object = std::make_unique<SetProperty>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->property_ = property_;
  object->lhs_ = lhs_ ? lhs_->Clone(storage) : nullptr;
  object->rhs_ = rhs_ ? rhs_->Clone(storage) : nullptr;
  return object;
}

SetProperty::SetPropertyCursor::SetPropertyCursor(const SetProperty &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self.input_->MakeCursor(mem)) {}

bool SetProperty::SetPropertyCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP("SetProperty");

  AbortCheck(context);

  if (!input_cursor_->Pull(frame, context)) return false;

  // Set, just like Create needs to see the latest changes.
  ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                storage::View::NEW, nullptr, &context.number_of_hops);
  TypedValue lhs = self_.lhs_->expression_->Accept(evaluator);
  TypedValue rhs = self_.rhs_->Accept(evaluator);

  switch (lhs.type()) {
    case TypedValue::Type::Vertex: {
#ifdef MG_ENTERPRISE
      if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
          !context.auth_checker->Has(lhs.ValueVertex(), storage::View::NEW,
                                     memgraph::query::AuthQuery::FineGrainedPrivilege::UPDATE)) {
        throw QueryRuntimeException("Vertex property not set due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
      }
#endif
      auto old_value = PropsSetChecked(&lhs.ValueVertex(), self_.property_, rhs,
                                       context.db_accessor->GetStorageAccessor()->GetNameIdMapper());
      context.execution_stats[ExecutionStats::Key::UPDATED_PROPERTIES] += 1;
      if (context.trigger_context_collector) {
        // rhs cannot be moved because it was created with the allocator that is only valid during current pull
        context.trigger_context_collector->RegisterSetObjectProperty(
            lhs.ValueVertex(), self_.property_,
            TypedValue{std::move(old_value), context.db_accessor->GetStorageAccessor()->GetNameIdMapper()},
            TypedValue{rhs});
      }
      break;
    }
    case TypedValue::Type::Edge: {
#ifdef MG_ENTERPRISE
      if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
          !context.auth_checker->Has(lhs.ValueEdge(), memgraph::query::AuthQuery::FineGrainedPrivilege::UPDATE)) {
        throw QueryRuntimeException("Edge property not set due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
      }
#endif
      auto old_value = PropsSetChecked(&lhs.ValueEdge(), self_.property_, rhs,
                                       context.db_accessor->GetStorageAccessor()->GetNameIdMapper());
      context.execution_stats[ExecutionStats::Key::UPDATED_PROPERTIES] += 1;
      if (context.trigger_context_collector) {
        // rhs cannot be moved because it was created with the allocator that is only valid
        // during current pull
        context.trigger_context_collector->RegisterSetObjectProperty(
            lhs.ValueEdge(), self_.property_,
            TypedValue{std::move(old_value), context.db_accessor->GetStorageAccessor()->GetNameIdMapper()},
            TypedValue{rhs});
      }
      break;
    }
    case TypedValue::Type::Null:
      // Skip setting properties on Null (can occur in optional match).
      break;
    case TypedValue::Type::Map:
    default:
      throw QueryRuntimeException("Properties can only be set on edges and vertices.");
  }
  return true;
}

void SetProperty::SetPropertyCursor::Shutdown() { input_cursor_->Shutdown(); }

void SetProperty::SetPropertyCursor::Reset() { input_cursor_->Reset(); }

SetNestedProperty::SetNestedProperty(const std::shared_ptr<LogicalOperator> &input,
                                     std::vector<storage::PropertyId> property_path, PropertyLookup *lhs,
                                     Expression *rhs)
    : input_(input), property_path_(std::move(property_path)), lhs_(lhs), rhs_(rhs) {}

ACCEPT_WITH_INPUT(SetNestedProperty)

UniqueCursorPtr SetNestedProperty::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::SetNestedPropertyOperator);

  return MakeUniqueCursorPtr<SetNestedPropertyCursor>(mem, *this, mem);
}

std::vector<Symbol> SetNestedProperty::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

std::unique_ptr<LogicalOperator> SetNestedProperty::Clone(AstStorage *storage) const {
  auto object = std::make_unique<SetNestedProperty>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->property_path_ = property_path_;
  object->lhs_ = lhs_ ? lhs_->Clone(storage) : nullptr;
  object->rhs_ = rhs_ ? rhs_->Clone(storage) : nullptr;
  return object;
}

SetNestedProperty::SetNestedPropertyCursor::SetNestedPropertyCursor(const SetNestedProperty &self,
                                                                    utils::MemoryResource *mem)
    : self_(self), input_cursor_(self.input_->MakeCursor(mem)) {}

bool SetNestedProperty::SetNestedPropertyCursor::Pull(Frame &frame, ExecutionContext &context) {
  const OOMExceptionEnabler oom_exception;
  const SCOPED_PROFILE_OP("SetNestedProperty");

  AbortCheck(context);

  if (!input_cursor_->Pull(frame, context)) return false;
  // Set, just like Create needs to see the latest changes.
  ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                storage::View::NEW);
  TypedValue lhs = self_.lhs_->expression_->Accept(evaluator);
  TypedValue rhs = self_.rhs_->Accept(evaluator);

  auto set_nested_property = [this, &context, &evaluator, &rhs](auto *record) {
    if (self_.lhs_->lookup_mode_ == PropertyLookup::LookupMode::APPEND && !rhs.IsMap()) {
      throw QueryRuntimeException(
          fmt::format("Trying to append to nested property with type {}. Setting of nested property by using the "
                      "append operator (+=) is only allowed if the right expression is of "
                      "type Map!",
                      rhs.type()));
    }

    TypedValue old_value = TypedValue(evaluator.GetProperty(*record, self_.lhs_->property_path_[0]),
                                      evaluator.GetNameIdMapper(), context.evaluation_context.memory);
    if (old_value.IsNull()) {
      old_value = TypedValue(TypedValue::TMap{}, context.evaluation_context.memory);
    } else if (!old_value.IsMap()) {
      throw QueryRuntimeException("Nested property must be of type Map!");
    }

    TypedValue::TMap &old_value_map = old_value.ValueMap();
    // Traverse the property path, creating sub-maps as needed
    TypedValue::TMap *current_map = &old_value_map;
    for (size_t i = 1; i < self_.property_path_.size() - 1; ++i) {
      // This part of the code is traversing through the nested structures, and creating empty maps if necessary
      TypedValue::TString key{context.db_accessor->GetStorageAccessor()->PropertyToName(self_.property_path_[i]),
                              context.evaluation_context.memory};
      auto it = current_map->find(key);
      if (it == current_map->end()) {
        it = current_map->emplace(key, TypedValue(TypedValue::TMap{}, context.evaluation_context.memory)).first;
      } else if (!it->second.IsMap()) {
        throw QueryRuntimeException(
            "Invalid set of nested properties! The property inside the nested structure already exists and is not of "
            "type Map!");
      }
      current_map = &it->second.ValueMap();
    }

    // In the leaf structure, we need to set the property if the method is to replace
    // If the method is to append, then we need a map property as we're adding to the current structure
    TypedValue::TString final_key{
        context.db_accessor->GetStorageAccessor()->PropertyToName(self_.property_path_.back()),
        context.evaluation_context.memory};
    switch (self_.lhs_->lookup_mode_) {
      case PropertyLookup::LookupMode::REPLACE: {
        // We don't care what's the value here, we just override
        (*current_map)[final_key] = rhs;
        break;
      }
      case PropertyLookup::LookupMode::APPEND: {
        // Here we need to check if the left hand side is a map
        // If the map is appended on top level, we skip the part of searching for leaf map as the top map is the leaf
        // one If there is a property path larger than one, we get the leaf map and update that one
        TypedValue::TMap *leaf_map = current_map;
        if (self_.property_path_.size() > 1) {
          auto it = current_map->find(final_key);
          if (it == current_map->end()) {
            it = current_map->emplace(final_key, TypedValue(TypedValue::TMap{}, context.evaluation_context.memory))
                     .first;
          } else if (!it->second.IsMap()) {
            throw QueryRuntimeException(
                "Invalid set of nested properties! The leaf property inside the nested structure already exists and is "
                "not of "
                "type Map!");
          }
          leaf_map = &it->second.ValueMap();
        }
        for (const auto &[k, v] : rhs.ValueMap()) {
          leaf_map->emplace(k, v);
        }
        break;
      }
      default:
        throw QueryRuntimeException("Unknown property lookup mode in set nested property!");
    }

    auto reconstructed_property_value = TypedValue(old_value_map, context.evaluation_context.memory);
    auto old_stored_value = PropsSetChecked(record, self_.property_path_[0], reconstructed_property_value,
                                            context.db_accessor->GetStorageAccessor()->GetNameIdMapper());
    context.execution_stats[ExecutionStats::Key::UPDATED_PROPERTIES] += 1;
    if (context.trigger_context_collector) {
      context.trigger_context_collector->RegisterSetObjectProperty(
          *record, self_.property_path_[0], TypedValue{std::move(old_stored_value), evaluator.GetNameIdMapper()},
          reconstructed_property_value);
    }
  };

  switch (lhs.type()) {
    case TypedValue::Type::Vertex: {
#ifdef MG_ENTERPRISE
      if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
          !context.auth_checker->Has(lhs.ValueVertex(), storage::View::NEW,
                                     memgraph::query::AuthQuery::FineGrainedPrivilege::UPDATE)) {
        throw QueryRuntimeException("Vertex nested property not set due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
      }
#endif
      set_nested_property(&lhs.ValueVertex());
      break;
    }
    case TypedValue::Type::Edge: {
#ifdef MG_ENTERPRISE
      if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
          !context.auth_checker->Has(lhs.ValueEdge(), memgraph::query::AuthQuery::FineGrainedPrivilege::UPDATE)) {
        throw QueryRuntimeException("Edge nested property not set due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
      }
#endif
      set_nested_property(&lhs.ValueEdge());
      break;
    }
    case TypedValue::Type::Null:
      // Skip setting properties on Null (can occur in optional match).
      break;
    case TypedValue::Type::Map:
    default:
      throw QueryRuntimeException("Nested properties can only be set on edges and vertices.");
  }
  return true;
}

void SetNestedProperty::SetNestedPropertyCursor::Shutdown() { input_cursor_->Shutdown(); }

void SetNestedProperty::SetNestedPropertyCursor::Reset() { input_cursor_->Reset(); }

SetProperties::SetProperties(const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol, Expression *rhs, Op op)
    : input_(input), input_symbol_(std::move(input_symbol)), rhs_(rhs), op_(op) {}

ACCEPT_WITH_INPUT(SetProperties)

UniqueCursorPtr SetProperties::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::SetPropertiesOperator);

  return MakeUniqueCursorPtr<SetPropertiesCursor>(mem, *this, mem);
}

std::vector<Symbol> SetProperties::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

std::unique_ptr<LogicalOperator> SetProperties::Clone(AstStorage *storage) const {
  auto object = std::make_unique<SetProperties>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->input_symbol_ = input_symbol_;
  object->rhs_ = rhs_ ? rhs_->Clone(storage) : nullptr;
  object->op_ = op_;
  return object;
}

SetProperties::SetPropertiesCursor::SetPropertiesCursor(const SetProperties &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self.input_->MakeCursor(mem)) {}

namespace {

template <typename T>
concept AccessorWithProperties = requires(T value, storage::PropertyId property_id,
                                          storage::PropertyValue property_value,
                                          std::map<storage::PropertyId, storage::PropertyValue> properties) {
  { value.ClearProperties() } -> std::same_as<storage::Result<std::map<storage::PropertyId, storage::PropertyValue>>>;
  {value.SetProperty(property_id, property_value)};
  {value.UpdateProperties(properties)};
};

/// Helper function that sets the given values on either a Vertex or an Edge.
///
/// @tparam TRecordAccessor Either RecordAccessor<Vertex> or
///     RecordAccessor<Edge>
template <AccessorWithProperties TRecordAccessor>
void SetPropertiesOnRecord(TRecordAccessor *record, const TypedValue &rhs, SetProperties::Op op,
                           ExecutionContext *context,
                           std::unordered_map<std::string, storage::PropertyId> &cached_name_id) {
  using PropertiesMap = std::map<storage::PropertyId, storage::PropertyValue>;
  std::optional<PropertiesMap> old_values;
  const bool should_register_change =
      context->trigger_context_collector &&
      context->trigger_context_collector->ShouldRegisterObjectPropertyChange<TRecordAccessor>();
  if (op == SetProperties::Op::REPLACE) {
    auto maybe_value = record->ClearProperties();
    if (maybe_value.HasError()) {
      switch (maybe_value.GetError()) {
        case storage::Error::DELETED_OBJECT:
          throw QueryRuntimeException("Trying to set properties on a deleted graph element.");
        case storage::Error::SERIALIZATION_ERROR:
          throw TransactionSerializationException();
        case storage::Error::PROPERTIES_DISABLED:
          throw QueryRuntimeException("Can't set property because properties on edges are disabled.");
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::NONEXISTENT_OBJECT:
          throw QueryRuntimeException("Unexpected error when setting properties.");
      }
    }

    if (should_register_change) {
      old_values.emplace(std::move(*maybe_value));
    }
  }

  auto get_props = [](const auto &record) {
    auto maybe_props = record.Properties(storage::View::NEW);
    if (maybe_props.HasError()) {
      switch (maybe_props.GetError()) {
        case storage::Error::DELETED_OBJECT:
          throw QueryRuntimeException("Trying to get properties from a deleted object.");
        case storage::Error::NONEXISTENT_OBJECT:
          throw query::QueryRuntimeException("Trying to get properties from an object that doesn't exist.");
        case storage::Error::SERIALIZATION_ERROR:
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::PROPERTIES_DISABLED:
          throw QueryRuntimeException("Unexpected error when getting properties.");
      }
    }
    return *maybe_props;
  };

  auto register_set_property = [&](auto &&returned_old_value, auto key, auto &&new_value) {
    auto old_value = [&]() -> storage::PropertyValue {
      if (!old_values) {
        return std::forward<decltype(returned_old_value)>(returned_old_value);
      }

      if (auto it = old_values->find(key); it != old_values->end()) {
        return std::move(it->second);
      }

      return {};
    }();

    auto name_id_mapper = context->db_accessor->GetStorageAccessor()->GetNameIdMapper();

    context->trigger_context_collector->RegisterSetObjectProperty(
        *record, key, TypedValue(std::move(old_value), name_id_mapper),
        TypedValue(std::forward<decltype(new_value)>(new_value), name_id_mapper));
  };

  auto update_props = [&, record](PropertiesMap &new_properties) {
    auto updated_properties = UpdatePropertiesChecked(record, new_properties);
    // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
    context->execution_stats[ExecutionStats::Key::UPDATED_PROPERTIES] += new_properties.size();

    if (should_register_change) {
      for (const auto &[id, old_value, new_value] : updated_properties) {
        register_set_property(std::move(old_value), id, std::move(new_value));
      }
    }
  };

  switch (rhs.type()) {
    case TypedValue::Type::Edge: {
      PropertiesMap new_properties = get_props(rhs.ValueEdge());
      update_props(new_properties);
      break;
    }
    case TypedValue::Type::Vertex: {
      PropertiesMap new_properties = get_props(rhs.ValueVertex());
      update_props(new_properties);
      break;
    }
    case TypedValue::Type::Map: {
      PropertiesMap new_properties;
      for (const auto &[string_key, value] : rhs.ValueMap()) {
        storage::PropertyId property_id;
        if (auto it = cached_name_id.find(std::string(string_key)); it != cached_name_id.end()) [[likely]] {
          property_id = it->second;
        } else {
          property_id = context->db_accessor->NameToProperty(string_key);
          cached_name_id.emplace(string_key, property_id);
        }
        new_properties.emplace(property_id,
                               value.ToPropertyValue(context->db_accessor->GetStorageAccessor()->GetNameIdMapper()));
      }
      update_props(new_properties);
      break;
    }
    default:
      throw QueryRuntimeException(
          "Right-hand side in SET expression must be a node, an edge or a "
          "map.");
  }

  if (should_register_change && old_values) {
    // register removed properties
    for (auto &[property_id, property_value] : *old_values) {
      context->trigger_context_collector->RegisterRemovedObjectProperty(
          *record, property_id,
          TypedValue(std::move(property_value), context->db_accessor->GetStorageAccessor()->GetNameIdMapper()));
    }
  }
}

}  // namespace

bool SetProperties::SetPropertiesCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP("SetProperties");

  AbortCheck(context);

  if (!input_cursor_->Pull(frame, context)) return false;

  TypedValue &lhs = frame[self_.input_symbol_];

  // Set, just like Create needs to see the latest changes.
  ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                storage::View::NEW, nullptr, &context.number_of_hops);
  TypedValue rhs = self_.rhs_->Accept(evaluator);

  switch (lhs.type()) {
    case TypedValue::Type::Vertex:
#ifdef MG_ENTERPRISE
      if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
          !context.auth_checker->Has(lhs.ValueVertex(), storage::View::NEW,
                                     memgraph::query::AuthQuery::FineGrainedPrivilege::UPDATE)) {
        throw QueryRuntimeException("Vertex properties not set due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
      }
#endif
      SetPropertiesOnRecord(&lhs.ValueVertex(), rhs, self_.op_, &context, cached_name_id_);

      break;
    case TypedValue::Type::Edge:
#ifdef MG_ENTERPRISE
      if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
          !context.auth_checker->Has(lhs.ValueEdge(), memgraph::query::AuthQuery::FineGrainedPrivilege::UPDATE)) {
        throw QueryRuntimeException("Edge properties not set due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
      }
#endif
      SetPropertiesOnRecord(&lhs.ValueEdge(), rhs, self_.op_, &context, cached_name_id_);
      break;
    case TypedValue::Type::Null:
      // Skip setting properties on Null (can occur in optional match).
      break;
    default:
      throw QueryRuntimeException("Properties can only be set on edges and vertices.");
  }
  return true;
}

void SetProperties::SetPropertiesCursor::Shutdown() { input_cursor_->Shutdown(); }

void SetProperties::SetPropertiesCursor::Reset() { input_cursor_->Reset(); }

SetLabels::SetLabels(const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol,
                     std::vector<StorageLabelType> labels)
    : input_(input), input_symbol_(std::move(input_symbol)), labels_(std::move(labels)) {}

ACCEPT_WITH_INPUT(SetLabels)

UniqueCursorPtr SetLabels::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::SetLabelsOperator);

  return MakeUniqueCursorPtr<SetLabelsCursor>(mem, *this, mem);
}

std::vector<Symbol> SetLabels::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

std::unique_ptr<LogicalOperator> SetLabels::Clone(AstStorage *storage) const {
  auto object = std::make_unique<SetLabels>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->input_symbol_ = input_symbol_;
  object->labels_ = labels_;
  return object;
}

SetLabels::SetLabelsCursor::SetLabelsCursor(const SetLabels &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self.input_->MakeCursor(mem)) {}

bool SetLabels::SetLabelsCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP("SetLabels");

  AbortCheck(context);

  ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                storage::View::NEW, nullptr, &context.number_of_hops);
  if (!input_cursor_->Pull(frame, context)) return false;
  auto labels = EvaluateLabels(self_.labels_, evaluator, context.db_accessor);

#ifdef MG_ENTERPRISE
  if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
      !context.auth_checker->Has(labels, memgraph::query::AuthQuery::FineGrainedPrivilege::CREATE_DELETE)) {
    throw QueryRuntimeException("Couldn't set label due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
  }
#endif

  TypedValue &vertex_value = frame[self_.input_symbol_];
  // Skip setting labels on Null (can occur in optional match).
  if (vertex_value.IsNull()) return true;
  ExpectType(self_.input_symbol_, vertex_value, TypedValue::Type::Vertex);
  auto &vertex = vertex_value.ValueVertex();

#ifdef MG_ENTERPRISE
  if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
      !context.auth_checker->Has(vertex, storage::View::OLD,
                                 memgraph::query::AuthQuery::FineGrainedPrivilege::UPDATE)) {
    throw QueryRuntimeException("Couldn't set label due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
  }
#endif

  for (auto label : labels) {
    auto maybe_value = vertex.AddLabel(label);
    if (maybe_value.HasError()) {
      switch (maybe_value.GetError()) {
        case storage::Error::SERIALIZATION_ERROR:
          throw TransactionSerializationException();
        case storage::Error::DELETED_OBJECT:
          throw QueryRuntimeException("Trying to set a label on a deleted node.");
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::PROPERTIES_DISABLED:
        case storage::Error::NONEXISTENT_OBJECT:
          throw QueryRuntimeException("Unexpected error when setting a label.");
      }
    }

    if (context.trigger_context_collector && *maybe_value) {
      context.trigger_context_collector->RegisterSetVertexLabel(vertex, label);
    }
  }
  return true;
}

void SetLabels::SetLabelsCursor::Shutdown() { input_cursor_->Shutdown(); }

void SetLabels::SetLabelsCursor::Reset() { input_cursor_->Reset(); }

RemoveProperty::RemoveProperty(const std::shared_ptr<LogicalOperator> &input, storage::PropertyId property,
                               PropertyLookup *lhs)
    : input_(input), property_(property), lhs_(lhs) {}

ACCEPT_WITH_INPUT(RemoveProperty)

UniqueCursorPtr RemoveProperty::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::RemovePropertyOperator);

  return MakeUniqueCursorPtr<RemovePropertyCursor>(mem, *this, mem);
}

std::vector<Symbol> RemoveProperty::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

std::unique_ptr<LogicalOperator> RemoveProperty::Clone(AstStorage *storage) const {
  auto object = std::make_unique<RemoveProperty>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->property_ = property_;
  object->lhs_ = lhs_ ? lhs_->Clone(storage) : nullptr;
  return object;
}

RemoveProperty::RemovePropertyCursor::RemovePropertyCursor(const RemoveProperty &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self.input_->MakeCursor(mem)) {}

bool RemoveProperty::RemovePropertyCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP("RemoveProperty");

  AbortCheck(context);

  if (!input_cursor_->Pull(frame, context)) return false;

  // Remove, just like Delete needs to see the latest changes.
  ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                storage::View::NEW, nullptr, &context.number_of_hops);
  TypedValue lhs = self_.lhs_->expression_->Accept(evaluator);

  auto remove_prop = [property = self_.property_, &context](auto *record) {
    auto maybe_old_value = record->RemoveProperty(property);
    if (maybe_old_value.HasError()) {
      switch (maybe_old_value.GetError()) {
        case storage::Error::DELETED_OBJECT:
          throw QueryRuntimeException("Trying to remove a property on a deleted graph element.");
        case storage::Error::SERIALIZATION_ERROR:
          throw TransactionSerializationException();
        case storage::Error::PROPERTIES_DISABLED:
          throw QueryRuntimeException(
              "Can't remove property because properties on edges are "
              "disabled.");
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::NONEXISTENT_OBJECT:
          throw QueryRuntimeException("Unexpected error when removing property.");
      }
    }

    if (context.trigger_context_collector) {
      context.trigger_context_collector->RegisterRemovedObjectProperty(
          *record, property,
          TypedValue(std::move(*maybe_old_value), context.db_accessor->GetStorageAccessor()->GetNameIdMapper()));
    }
  };

  switch (lhs.type()) {
    case TypedValue::Type::Vertex:
#ifdef MG_ENTERPRISE
      if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
          !context.auth_checker->Has(lhs.ValueVertex(), storage::View::NEW,
                                     memgraph::query::AuthQuery::FineGrainedPrivilege::UPDATE)) {
        throw QueryRuntimeException("Vertex property not removed due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
      }
#endif
      remove_prop(&lhs.ValueVertex());

      break;
    case TypedValue::Type::Edge:
#ifdef MG_ENTERPRISE
      if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
          !context.auth_checker->Has(lhs.ValueEdge(), memgraph::query::AuthQuery::FineGrainedPrivilege::UPDATE)) {
        throw QueryRuntimeException("Edge property not removed due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
      }
#endif
      remove_prop(&lhs.ValueEdge());
      break;
    case TypedValue::Type::Null:
      // Skip removing properties on Null (can occur in optional match).
      break;
    default:
      throw QueryRuntimeException("Properties can only be removed from vertices and edges.");
  }
  return true;
}

void RemoveProperty::RemovePropertyCursor::Shutdown() { input_cursor_->Shutdown(); }

void RemoveProperty::RemovePropertyCursor::Reset() { input_cursor_->Reset(); }

RemoveNestedProperty::RemoveNestedProperty(const std::shared_ptr<LogicalOperator> &input,
                                           std::vector<storage::PropertyId> property_path, PropertyLookup *lhs)
    : input_(input), property_path_(std::move(property_path)), lhs_(lhs) {}

ACCEPT_WITH_INPUT(RemoveNestedProperty)

UniqueCursorPtr RemoveNestedProperty::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::RemoveNestedPropertyOperator);

  return MakeUniqueCursorPtr<RemoveNestedPropertyCursor>(mem, *this, mem);
}

std::vector<Symbol> RemoveNestedProperty::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

std::unique_ptr<LogicalOperator> RemoveNestedProperty::Clone(AstStorage *storage) const {
  auto object = std::make_unique<RemoveNestedProperty>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->property_path_ = property_path_;
  object->lhs_ = lhs_ ? lhs_->Clone(storage) : nullptr;
  return object;
}

RemoveNestedProperty::RemoveNestedPropertyCursor::RemoveNestedPropertyCursor(const RemoveNestedProperty &self,
                                                                             utils::MemoryResource *mem)
    : self_(self), input_cursor_(self.input_->MakeCursor(mem)) {}

bool RemoveNestedProperty::RemoveNestedPropertyCursor::Pull(Frame &frame, ExecutionContext &context) {
  const OOMExceptionEnabler oom_exception;
  const SCOPED_PROFILE_OP("RemoveNestedProperty");

  AbortCheck(context);

  if (!input_cursor_->Pull(frame, context)) return false;

  ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                storage::View::NEW, nullptr, &context.number_of_hops);
  TypedValue lhs = self_.lhs_->expression_->Accept(evaluator);

  auto remove_nested_property = [this, &context, &evaluator](auto *record) {
    TypedValue old_value = TypedValue(evaluator.GetProperty(*record, self_.lhs_->property_path_[0]),
                                      evaluator.GetNameIdMapper(), context.evaluation_context.memory);

    if (!old_value.IsMap()) {
      throw QueryRuntimeException("Nested property must be of type Map!");
    }

    TypedValue::TMap &old_value_map = old_value.ValueMap();
    // Traverse the property path, creating sub-maps as needed
    TypedValue::TMap *current_map = &old_value_map;
    for (size_t i = 1; i < self_.property_path_.size() - 1; ++i) {
      // This part of the code is traversing through the nested structures, and creating empty maps if necessary
      TypedValue::TString key{context.db_accessor->GetStorageAccessor()->PropertyToName(self_.property_path_[i]),
                              context.evaluation_context.memory};
      auto it = current_map->find(key);
      if (it == current_map->end()) {
        throw QueryRuntimeException(fmt::format("Nested property '{}' is nonexistent!", key));
      }
      if (!it->second.IsMap()) {
        throw QueryRuntimeException("Nested structure is not of type map!");
      }
      current_map = &it->second.ValueMap();
    }

    const TypedValue::TString final_key{
        context.db_accessor->GetStorageAccessor()->PropertyToName(self_.property_path_.back()),
        context.evaluation_context.memory};
    auto it = current_map->find(final_key);
    if (it != current_map->end()) {
      current_map->erase(it);
    }

    auto reconstructed_property_value = TypedValue(old_value_map, context.evaluation_context.memory);
    auto old_stored_value = PropsSetChecked(record, self_.property_path_[0], reconstructed_property_value,
                                            context.db_accessor->GetStorageAccessor()->GetNameIdMapper());
    context.execution_stats[ExecutionStats::Key::UPDATED_PROPERTIES] += 1;
    if (context.trigger_context_collector) {
      context.trigger_context_collector->RegisterSetObjectProperty(
          *record, self_.property_path_[0], TypedValue{std::move(old_stored_value), evaluator.GetNameIdMapper()},
          reconstructed_property_value);
    }
  };

  switch (lhs.type()) {
    case TypedValue::Type::Vertex: {
#ifdef MG_ENTERPRISE
      if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
          !context.auth_checker->Has(lhs.ValueVertex(), storage::View::NEW,
                                     memgraph::query::AuthQuery::FineGrainedPrivilege::UPDATE)) {
        throw QueryRuntimeException("Vertex nested property not removed due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
      }
#endif
      remove_nested_property(&lhs.ValueVertex());
      break;
    }
    case TypedValue::Type::Edge: {
#ifdef MG_ENTERPRISE
      if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
          !context.auth_checker->Has(lhs.ValueEdge(), memgraph::query::AuthQuery::FineGrainedPrivilege::UPDATE)) {
        throw QueryRuntimeException("Edge nested property not removed due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
      }
#endif
      remove_nested_property(&lhs.ValueEdge());
      break;
    }
    case TypedValue::Type::Null:
      // Skip removing properties on Null (can occur in optional match).
      break;
    default:
      throw QueryRuntimeException("Nested properties can only be removed from vertices and edges.");
  }
  return true;
}

void RemoveNestedProperty::RemoveNestedPropertyCursor::Shutdown() { input_cursor_->Shutdown(); }

void RemoveNestedProperty::RemoveNestedPropertyCursor::Reset() { input_cursor_->Reset(); }

RemoveLabels::RemoveLabels(const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol,
                           std::vector<StorageLabelType> labels)
    : input_(input), input_symbol_(std::move(input_symbol)), labels_(std::move(labels)) {}

ACCEPT_WITH_INPUT(RemoveLabels)

UniqueCursorPtr RemoveLabels::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::RemoveLabelsOperator);

  return MakeUniqueCursorPtr<RemoveLabelsCursor>(mem, *this, mem);
}

std::vector<Symbol> RemoveLabels::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

std::unique_ptr<LogicalOperator> RemoveLabels::Clone(AstStorage *storage) const {
  auto object = std::make_unique<RemoveLabels>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->input_symbol_ = input_symbol_;
  object->labels_ = labels_;
  return object;
}

RemoveLabels::RemoveLabelsCursor::RemoveLabelsCursor(const RemoveLabels &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self.input_->MakeCursor(mem)) {}

bool RemoveLabels::RemoveLabelsCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP("RemoveLabels");

  AbortCheck(context);

  ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                storage::View::NEW, nullptr, &context.number_of_hops);
  if (!input_cursor_->Pull(frame, context)) return false;
  auto labels = EvaluateLabels(self_.labels_, evaluator, context.db_accessor);

#ifdef MG_ENTERPRISE
  if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
      !context.auth_checker->Has(labels, memgraph::query::AuthQuery::FineGrainedPrivilege::CREATE_DELETE)) {
    throw QueryRuntimeException("Couldn't remove label due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
  }
#endif

  TypedValue &vertex_value = frame[self_.input_symbol_];
  // Skip removing labels on Null (can occur in optional match).
  if (vertex_value.IsNull()) return true;
  ExpectType(self_.input_symbol_, vertex_value, TypedValue::Type::Vertex);
  auto &vertex = vertex_value.ValueVertex();

#ifdef MG_ENTERPRISE
  if (license::global_license_checker.IsEnterpriseValidFast() && context.auth_checker &&
      !context.auth_checker->Has(vertex, storage::View::OLD,
                                 memgraph::query::AuthQuery::FineGrainedPrivilege::UPDATE)) {
    throw QueryRuntimeException("Couldn't remove label due to not having enough permission! This error means that the fine grained access control was not correctly set up for the user on this label. Use SHOW PRIVILEGES FOR user_or_role ON CURRENT; to check if you have correct privileges to do operations involving labels. If you do try running SHOW CURRENT DATABASE; to verify you are pointing to correct database.");
  }
#endif

  for (auto label : labels) {
    auto maybe_value = vertex.RemoveLabel(label);
    if (maybe_value.HasError()) {
      switch (maybe_value.GetError()) {
        case storage::Error::SERIALIZATION_ERROR:
          throw TransactionSerializationException();
        case storage::Error::DELETED_OBJECT:
          throw QueryRuntimeException("Trying to remove labels from a deleted node.");
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::PROPERTIES_DISABLED:
        case storage::Error::NONEXISTENT_OBJECT:
          throw QueryRuntimeException("Unexpected error when removing labels from a node.");
      }
    }

    context.execution_stats[ExecutionStats::Key::DELETED_LABELS] += 1;
    if (context.trigger_context_collector && *maybe_value) {
      context.trigger_context_collector->RegisterRemovedVertexLabel(vertex, label);
    }
  }
  return true;
}

void RemoveLabels::RemoveLabelsCursor::Shutdown() { input_cursor_->Shutdown(); }

void RemoveLabels::RemoveLabelsCursor::Reset() { input_cursor_->Reset(); }

EdgeUniquenessFilter::EdgeUniquenessFilter(const std::shared_ptr<LogicalOperator> &input, Symbol expand_symbol,
                                           const std::vector<Symbol> &previous_symbols)
    : input_(input), expand_symbol_(std::move(expand_symbol)), previous_symbols_(previous_symbols) {}

ACCEPT_WITH_INPUT(EdgeUniquenessFilter)

UniqueCursorPtr EdgeUniquenessFilter::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::EdgeUniquenessFilterOperator);

  return MakeUniqueCursorPtr<EdgeUniquenessFilterCursor>(mem, *this, mem);
}

std::vector<Symbol> EdgeUniquenessFilter::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

std::unique_ptr<LogicalOperator> EdgeUniquenessFilter::Clone(AstStorage *storage) const {
  auto object = std::make_unique<EdgeUniquenessFilter>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->expand_symbol_ = expand_symbol_;
  object->previous_symbols_ = previous_symbols_;
  return object;
}

std::string EdgeUniquenessFilter::ToString() const {
  return fmt::format("EdgeUniquenessFilter {{{0} : {1}}}",
                     utils::IterableToString(previous_symbols_, ", ", [](const auto &sym) { return sym.name(); }),
                     expand_symbol_.name());
}

EdgeUniquenessFilter::EdgeUniquenessFilterCursor::EdgeUniquenessFilterCursor(const EdgeUniquenessFilter &self,
                                                                             utils::MemoryResource *mem)
    : self_(self), input_cursor_(self.input_->MakeCursor(mem)) {}

namespace {
/**
 * Returns true if:
 *    - a and b are either edge or edge-list values, and there
 *    is at least one matching edge in the two values
 */
bool ContainsSameEdge(const TypedValue &a, const TypedValue &b) {
  auto compare_to_list = [](const TypedValue &list, const TypedValue &other) {
    for (const TypedValue &list_elem : list.ValueList())
      if (ContainsSameEdge(list_elem, other)) return true;
    return false;
  };

  if (a.type() == TypedValue::Type::List) return compare_to_list(a, b);
  if (b.type() == TypedValue::Type::List) return compare_to_list(b, a);

  return a.ValueEdge() == b.ValueEdge();
}
}  // namespace

bool EdgeUniquenessFilter::EdgeUniquenessFilterCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP("EdgeUniquenessFilter");

  AbortCheck(context);

  auto expansion_ok = [&]() {
    const auto &expand_value = frame[self_.expand_symbol_];
    for (const auto &previous_symbol : self_.previous_symbols_) {
      const auto &previous_value = frame[previous_symbol];
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

void EdgeUniquenessFilter::EdgeUniquenessFilterCursor::Shutdown() { input_cursor_->Shutdown(); }

void EdgeUniquenessFilter::EdgeUniquenessFilterCursor::Reset() { input_cursor_->Reset(); }

EmptyResult::EmptyResult(const std::shared_ptr<LogicalOperator> &input)
    : input_(input ? input : std::make_shared<Once>()) {}

ACCEPT_WITH_INPUT(EmptyResult)

std::vector<Symbol> EmptyResult::OutputSymbols(const SymbolTable &) const {  // NOLINT(hicpp-named-parameter)
  return {};
}

std::vector<Symbol> EmptyResult::ModifiedSymbols(const SymbolTable &) const {  // NOLINT(hicpp-named-parameter)
  return {};
}

class EmptyResultCursor : public Cursor {
 public:
  EmptyResultCursor(const EmptyResult &self, utils::MemoryResource *mem)
      : input_cursor_(self.input_->MakeCursor(mem)) {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    SCOPED_PROFILE_OP("EmptyResult");

    if (!pulled_all_input_) {
      while (input_cursor_->Pull(frame, context)) {
        AbortCheck(context);
      }
      pulled_all_input_ = true;
    }
    return false;
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    pulled_all_input_ = false;
  }

 private:
  const UniqueCursorPtr input_cursor_;
  bool pulled_all_input_{false};
};

UniqueCursorPtr EmptyResult::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::EmptyResultOperator);

  return MakeUniqueCursorPtr<EmptyResultCursor>(mem, *this, mem);
}

std::unique_ptr<LogicalOperator> EmptyResult::Clone(AstStorage *storage) const {
  auto object = std::make_unique<EmptyResult>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  return object;
}

Accumulate::Accumulate(const std::shared_ptr<LogicalOperator> &input, const std::vector<Symbol> &symbols,
                       bool advance_command)
    : input_(input), symbols_(symbols), advance_command_(advance_command) {}

ACCEPT_WITH_INPUT(Accumulate)

std::vector<Symbol> Accumulate::ModifiedSymbols(const SymbolTable &) const { return symbols_; }

class AccumulateCursor : public Cursor {
 public:
  AccumulateCursor(const Accumulate &self, utils::MemoryResource *mem)
      : self_(self), input_cursor_(self.input_->MakeCursor(mem)), cache_(mem) {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP("Accumulate");

    auto &dba = *context.db_accessor;
    // cache all the input
    if (!pulled_all_input_) {
      while (input_cursor_->Pull(frame, context)) {
        utils::pmr::vector<TypedValue> row(cache_.get_allocator().resource());
        row.reserve(self_.symbols_.size());
        for (const Symbol &symbol : self_.symbols_) row.emplace_back(frame[symbol]);
        cache_.emplace_back(std::move(row));
      }
      pulled_all_input_ = true;
      cache_it_ = cache_.begin();

      if (self_.advance_command_) dba.AdvanceCommand();
    }

    AbortCheck(context);
    if (cache_it_ == cache_.end()) return false;
    auto row_it = (cache_it_++)->begin();
    for (const Symbol &symbol : self_.symbols_) {
      if (context.frame_change_collector && context.frame_change_collector->IsKeyTracked(symbol.name())) {
        context.frame_change_collector->ResetTrackingValue(symbol.name());
      }
      frame[symbol] = *row_it++;
    }
    return true;
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    cache_.clear();
    cache_it_ = cache_.begin();
    pulled_all_input_ = false;
  }

 private:
  const Accumulate &self_;
  const UniqueCursorPtr input_cursor_;
  utils::pmr::deque<utils::pmr::vector<TypedValue>> cache_;
  decltype(cache_.begin()) cache_it_ = cache_.begin();
  bool pulled_all_input_{false};
};

UniqueCursorPtr Accumulate::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::AccumulateOperator);

  return MakeUniqueCursorPtr<AccumulateCursor>(mem, *this, mem);
}

std::unique_ptr<LogicalOperator> Accumulate::Clone(AstStorage *storage) const {
  auto object = std::make_unique<Accumulate>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->symbols_ = symbols_;
  object->advance_command_ = advance_command_;
  return object;
}

Aggregate::Aggregate(const std::shared_ptr<LogicalOperator> &input, const std::vector<Aggregate::Element> &aggregations,
                     const std::vector<Expression *> &group_by, const std::vector<Symbol> &remember)
    : input_(input ? input : std::make_shared<Once>()),
      aggregations_(aggregations),
      group_by_(group_by),
      remember_(remember) {}

ACCEPT_WITH_INPUT(Aggregate)

std::vector<Symbol> Aggregate::ModifiedSymbols(const SymbolTable &) const {
  auto symbols = remember_;
  for (const auto &elem : aggregations_) symbols.push_back(elem.output_sym);
  return symbols;
}

namespace {
/** Returns the default TypedValue for an Aggregation element.
 * This value is valid both for returning when where are no inputs
 * to the aggregation op, and for initializing an aggregation result
 * when there are */
TypedValue DefaultAggregationOpValue(const Aggregate::Element &element, utils::MemoryResource *memory) {
  switch (element.op) {
    case Aggregation::Op::MIN:
    case Aggregation::Op::MAX:
    case Aggregation::Op::AVG:
      return TypedValue(memory);
    case Aggregation::Op::COUNT:
    case Aggregation::Op::SUM:
      return TypedValue(0, memory);
    case Aggregation::Op::COLLECT_LIST:
      return TypedValue(TypedValue::TVector(memory));
    case Aggregation::Op::COLLECT_MAP:
      return TypedValue(TypedValue::TMap(memory));
    case Aggregation::Op::PROJECT_PATH:
    case Aggregation::Op::PROJECT_LISTS:
      return TypedValue(query::Graph(memory));
  }
}
}  // namespace

class AggregateCursor : public Cursor {
 public:
  AggregateCursor(const Aggregate &self, utils::MemoryResource *mem)
      : self_(self),
        input_cursor_(self_.input_->MakeCursor(mem)),
        aggregation_(mem),
        reused_group_by_(self.group_by_.size(), mem) {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP_BY_REF(self_);

    AbortCheck(context);

    if (!pulled_all_input_) {
      if (!ProcessAll(&frame, &context) && !self_.group_by_.empty()) return false;
      pulled_all_input_ = true;
      aggregation_it_ = aggregation_.begin();

      if (aggregation_.empty()) {
        auto *pull_memory = context.evaluation_context.memory;
        // place default aggregation values on the frame
        for (const auto &elem : self_.aggregations_) {
          frame[elem.output_sym] = DefaultAggregationOpValue(elem, pull_memory);
          if (context.frame_change_collector && context.frame_change_collector->IsKeyTracked(elem.output_sym.name())) {
            context.frame_change_collector->ResetTrackingValue(elem.output_sym.name());
          }
        }

        // place null as remember values on the frame
        for (const Symbol &remember_sym : self_.remember_) {
          frame[remember_sym] = TypedValue(pull_memory);
          if (context.frame_change_collector && context.frame_change_collector->IsKeyTracked(remember_sym.name())) {
            context.frame_change_collector->ResetTrackingValue(remember_sym.name());
          }
        }
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
    for (const Symbol &remember_sym : self_.remember_) frame[remember_sym] = *remember_values_it++;

    aggregation_it_++;
    return true;
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    aggregation_.clear();
    aggregation_it_ = aggregation_.begin();
    pulled_all_input_ = false;
  }

 private:
  // Data structure for a single aggregation cache.
  // Does NOT include the group-by values since those are a key in the
  // aggregation map. The vectors in an AggregationValue contain one element for
  // each aggregation in this LogicalOp.
  struct AggregationValue {
    explicit AggregationValue(utils::MemoryResource *mem)
        : counts_(mem), values_(mem), remember_(mem), unique_values_(mem) {}

    // how many input rows have been aggregated in respective values_ element so
    // far
    // TODO: The counting value type should be changed to an unsigned type once
    // TypedValue can support signed integer values larger than 64bits so that
    // precision isn't lost.
    utils::pmr::vector<int64_t> counts_;
    // aggregated values. Initially Null (until at least one input row with a
    // valid value gets processed)
    utils::pmr::vector<TypedValue> values_;
    // remember values.
    utils::pmr::vector<TypedValue> remember_;

    using TSet = utils::pmr::unordered_set<TypedValue, TypedValue::Hash, TypedValue::BoolEqual>;

    utils::pmr::vector<TSet> unique_values_;
  };

  const Aggregate &self_;
  const UniqueCursorPtr input_cursor_;
  // storage for aggregated data
  // map key is the vector of group-by values
  // map value is an AggregationValue struct
  utils::pmr::unordered_map<utils::pmr::vector<TypedValue>, AggregationValue,
                            // use FNV collection hashing specialized for a
                            // vector of TypedValues
                            utils::FnvCollection<utils::pmr::vector<TypedValue>, TypedValue, TypedValue::Hash>,
                            // custom equality
                            TypedValueVectorEqual>
      aggregation_;
  // this is a for object reuse, to avoid re-allocating this buffer
  utils::pmr::vector<TypedValue> reused_group_by_;
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
  bool ProcessAll(Frame *frame, ExecutionContext *context) {
    ExpressionEvaluator evaluator(frame, context->symbol_table, context->evaluation_context, context->db_accessor,
                                  storage::View::NEW, nullptr, &context->number_of_hops);

    bool pulled = false;
    while (input_cursor_->Pull(*frame, *context)) {
      ProcessOne(*frame, &evaluator);
      pulled = true;
    }
    if (!pulled) return false;

    // post processing
    for (size_t pos = 0; pos < self_.aggregations_.size(); ++pos) {
      switch (self_.aggregations_[pos].op) {
        case Aggregation::Op::AVG: {
          // calculate AVG aggregations (so far they have only been summed)
          for (auto &kv : aggregation_) {
            AggregationValue &agg_value = kv.second;
            auto count = agg_value.counts_[pos];
            auto *pull_memory = context->evaluation_context.memory;
            if (count > 0) {
              agg_value.values_[pos] = agg_value.values_[pos] / TypedValue(static_cast<double>(count), pull_memory);
            }
          }
          break;
        }
        case Aggregation::Op::COUNT: {
          // Copy counts to be the value
          for (auto &kv : aggregation_) {
            AggregationValue &agg_value = kv.second;
            agg_value.values_[pos] = agg_value.counts_[pos];
          }
          break;
        }
        case Aggregation::Op::MIN:
        case Aggregation::Op::MAX:
        case Aggregation::Op::SUM:
        case Aggregation::Op::COLLECT_LIST:
        case Aggregation::Op::COLLECT_MAP:
        case Aggregation::Op::PROJECT_PATH:
        case Aggregation::Op::PROJECT_LISTS:
          break;
      }
    }
    return true;
  }

  /**
   * Performs a single accumulation.
   */
  void ProcessOne(const Frame &frame, ExpressionEvaluator *evaluator) {
    // Preallocated group_by, since most of the time the aggregation key won't be unique
    reused_group_by_.clear();
    evaluator->ResetPropertyLookupCache();

    // TODO: if self_.group_by_.size() == 0, aggregation_ -> there is only one (becasue we are doing *)
    //       can this be optimised so we don't need to do aggregation_.try_emplace which has a hash cost
    for (Expression *expression : self_.group_by_) {
      reused_group_by_.emplace_back(expression->Accept(*evaluator));
    }
    auto *mem = aggregation_.get_allocator().resource();
    auto res = aggregation_.try_emplace(reused_group_by_, mem);
    auto &agg_value = res.first->second;
    if (res.second /*was newly inserted*/) EnsureInitialized(frame, &agg_value);
    Update(evaluator, &agg_value);
  }

  /** Ensures the new AggregationValue has been initialized. This means
   * that the value vectors are filled with an appropriate number of Nulls,
   * counts are set to 0 and remember values are remembered.
   */
  void EnsureInitialized(const Frame &frame, AggregateCursor::AggregationValue *agg_value) const {
    if (!agg_value->values_.empty()) return;

    const auto num_of_aggregations = self_.aggregations_.size();
    agg_value->values_.reserve(num_of_aggregations);
    agg_value->unique_values_.reserve(num_of_aggregations);

    auto *mem = agg_value->values_.get_allocator().resource();
    for (const auto &agg_elem : self_.aggregations_) {
      agg_value->values_.emplace_back(DefaultAggregationOpValue(agg_elem, mem));
      agg_value->unique_values_.emplace_back(AggregationValue::TSet(mem));
    }
    agg_value->counts_.resize(num_of_aggregations, 0);

    agg_value->remember_.reserve(self_.remember_.size());
    for (const Symbol &remember_sym : self_.remember_) {
      agg_value->remember_.push_back(frame[remember_sym]);
    }
  }

  /** Updates the given AggregationValue with new data. Assumes that
   * the AggregationValue has been initialized */
  void Update(ExpressionEvaluator *evaluator, AggregateCursor::AggregationValue *agg_value) {
    DMG_ASSERT(self_.aggregations_.size() == agg_value->values_.size(),
               "Expected as much AggregationValue.values_ as there are "
               "aggregations.");
    DMG_ASSERT(self_.aggregations_.size() == agg_value->counts_.size(),
               "Expected as much AggregationValue.counts_ as there are "
               "aggregations.");

    auto count_it = agg_value->counts_.begin();
    auto value_it = agg_value->values_.begin();
    auto unique_values_it = agg_value->unique_values_.begin();
    auto agg_elem_it = self_.aggregations_.begin();
    const auto counts_end = agg_value->counts_.end();
    for (; count_it != counts_end; ++count_it, ++value_it, ++unique_values_it, ++agg_elem_it) {
      // COUNT(*) is the only case where input expression is optional
      // handle it here
      auto *input_expr_ptr = agg_elem_it->arg1;
      if (!input_expr_ptr) {
        *count_it += 1;
        // value is deferred to post-processing
        continue;
      }

      TypedValue input_value = input_expr_ptr->Accept(*evaluator);

      // Aggregations skip Null input values.
      if (input_value.IsNull()) continue;
      const auto &agg_op = agg_elem_it->op;
      if (agg_elem_it->distinct) {
        auto insert_result = unique_values_it->insert(input_value);
        if (!insert_result.second) {
          continue;
        }
      }
      *count_it += 1;
      if (*count_it == 1) {
        // first value, nothing to aggregate. check type, set and continue.
        switch (agg_op) {
          case Aggregation::Op::MIN:
          case Aggregation::Op::MAX:
            EnsureOkForMinMax(input_value);
            *value_it = std::move(input_value);
            break;
          case Aggregation::Op::SUM:
          case Aggregation::Op::AVG:
            EnsureOkForAvgSum(input_value);
            *value_it = std::move(input_value);
            break;
          case Aggregation::Op::COUNT:
            // value is deferred to post-processing
            break;
          case Aggregation::Op::COLLECT_LIST:
            value_it->ValueList().push_back(std::move(input_value));
            break;
          case Aggregation::Op::PROJECT_PATH: {
            EnsureOkForProjectPath(input_value);
            value_it->ValueGraph().Expand(input_value.ValuePath());
            break;
          }
          case Aggregation::Op::PROJECT_LISTS: {
            ProjectList(input_value, agg_elem_it->arg2->Accept(*evaluator), value_it->ValueGraph());
            break;
          }
          case Aggregation::Op::COLLECT_MAP:
            auto key = agg_elem_it->arg2->Accept(*evaluator);
            if (key.type() != TypedValue::Type::String) throw QueryRuntimeException("Map key must be a string.");
            value_it->ValueMap().emplace(key.ValueString(), std::move(input_value));
            break;
        }
        continue;
      }

      // aggregation of existing values
      switch (agg_op) {
        case Aggregation::Op::COUNT:
          // value is deferred to post-processing
          break;
        case Aggregation::Op::MIN: {
          EnsureOkForMinMax(input_value);
          try {
            TypedValue comparison_result = input_value < *value_it;
            // since we skip nulls we either have a valid comparison, or
            // an exception was just thrown above
            // safe to assume a bool TypedValue
            if (comparison_result.ValueBool()) *value_it = std::move(input_value);
          } catch (const TypedValueException &) {
            throw QueryRuntimeException("Unable to get MIN of '{}' and '{}'.", input_value.type(), value_it->type());
          }
          break;
        }
        case Aggregation::Op::MAX: {
          //  all comments as for Op::Min
          EnsureOkForMinMax(input_value);
          try {
            TypedValue comparison_result = input_value > *value_it;
            if (comparison_result.ValueBool()) *value_it = std::move(input_value);
          } catch (const TypedValueException &) {
            throw QueryRuntimeException("Unable to get MAX of '{}' and '{}'.", input_value.type(), value_it->type());
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
          value_it->ValueList().push_back(std::move(input_value));
          break;
        case Aggregation::Op::PROJECT_PATH: {
          EnsureOkForProjectPath(input_value);
          value_it->ValueGraph().Expand(input_value.ValuePath());
          break;
        }

        case Aggregation::Op::PROJECT_LISTS: {
          ProjectList(input_value, agg_elem_it->arg2->Accept(*evaluator), value_it->ValueGraph());
          break;
        }
        case Aggregation::Op::COLLECT_MAP:
          auto key = agg_elem_it->arg2->Accept(*evaluator);
          if (key.type() != TypedValue::Type::String) throw QueryRuntimeException("Map key must be a string.");
          value_it->ValueMap().emplace(key.ValueString(), std::move(input_value));
          break;
      }  // end switch over Aggregation::Op enum
    }    // end loop over all aggregations
  }

  /** Project a subgraph from lists of nodes and lists of edges. Any nulls in these lists are ignored.
   */
  static void ProjectList(TypedValue const &arg1, TypedValue const &arg2, Graph &projectedGraph) {
    if (arg1.type() != TypedValue::Type::List || !std::ranges::all_of(arg1.ValueList(), [](TypedValue const &each) {
          return each.type() == TypedValue::Type::Vertex || each.type() == TypedValue::Type::Null;
        })) {
      throw QueryRuntimeException("project() argument 1 must be a list of nodes or nulls.");
    }

    if (arg2.type() != TypedValue::Type::List || !std::ranges::all_of(arg2.ValueList(), [](TypedValue const &each) {
          return each.type() == TypedValue::Type::Edge || each.type() == TypedValue::Type::Null;
        })) {
      throw QueryRuntimeException("project() argument 2 must be a list of relationships or nulls.");
    }

    projectedGraph.Expand(arg1.ValueList(), arg2.ValueList());
  }

  /** Checks if the given TypedValue is legal in MIN and MAX. If not
   * an appropriate exception is thrown. */
  void EnsureOkForMinMax(const TypedValue &value) const {
    switch (value.type()) {
      case TypedValue::Type::Bool:
      case TypedValue::Type::Int:
      case TypedValue::Type::Double:
      case TypedValue::Type::String:
      case TypedValue::Type::Date:
      case TypedValue::Type::LocalTime:
      case TypedValue::Type::LocalDateTime:
      case TypedValue::Type::ZonedDateTime:
        return;
      default:
        throw QueryRuntimeException(
            "Only boolean, numeric, string, and non-duration temporal values are allowed in MIN and MAX "
            "aggregations.");
    }
  }

  /** Checks if the given TypedValue is legal in AVG and SUM. If not
   * an appropriate exception is thrown. */
  void EnsureOkForAvgSum(const TypedValue &value) const {
    switch (value.type()) {
      case TypedValue::Type::Int:
      case TypedValue::Type::Double:
        return;
      default:
        throw QueryRuntimeException("Only numeric values allowed in SUM and AVG aggregations.");
    }
  }

  /** Checks if the given TypedValue is legal in PROJECT_PATH. If not an appropriate exception is thrown. */
  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  void EnsureOkForProjectPath(const TypedValue &value) const {
    switch (value.type()) {
      case TypedValue::Type::Path:
        return;
      default:
        throw QueryRuntimeException("Only path values allowed in PROJECT aggregation.");
    }
  }
};

UniqueCursorPtr Aggregate::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::AggregateOperator);

  return MakeUniqueCursorPtr<AggregateCursor>(mem, *this, mem);
}

std::unique_ptr<LogicalOperator> Aggregate::Clone(AstStorage *storage) const {
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

std::string Aggregate::ToString() const {
  return fmt::format(
      "Aggregate {{{0}}} {{{1}}}",
      utils::IterableToString(aggregations_, ", ", [](const auto &aggr) { return aggr.output_sym.name(); }),
      utils::IterableToString(remember_, ", ", [](const auto &sym) { return sym.name(); }));
}

Skip::Skip(const std::shared_ptr<LogicalOperator> &input, Expression *expression)
    : input_(input), expression_(expression) {}

ACCEPT_WITH_INPUT(Skip)

UniqueCursorPtr Skip::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::SkipOperator);

  return MakeUniqueCursorPtr<SkipCursor>(mem, *this, mem);
}

std::vector<Symbol> Skip::OutputSymbols(const SymbolTable &symbol_table) const {
  // Propagate this to potential Produce.
  return input_->OutputSymbols(symbol_table);
}

std::vector<Symbol> Skip::ModifiedSymbols(const SymbolTable &table) const { return input_->ModifiedSymbols(table); }

std::unique_ptr<LogicalOperator> Skip::Clone(AstStorage *storage) const {
  auto object = std::make_unique<Skip>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
  return object;
}

Skip::SkipCursor::SkipCursor(const Skip &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self_.input_->MakeCursor(mem)) {}

bool Skip::SkipCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP("Skip");

  AbortCheck(context);

  while (input_cursor_->Pull(frame, context)) {
    if (to_skip_ == -1) {
      // First successful pull from the input, evaluate the skip expression.
      // The skip expression doesn't contain identifiers so graph view
      // parameter is not important.
      ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                    storage::View::OLD, nullptr, &context.number_of_hops);
      TypedValue to_skip = self_.expression_->Accept(evaluator);
      if (to_skip.type() != TypedValue::Type::Int)
        throw QueryRuntimeException("Number of elements to skip must be an integer.");

      to_skip_ = to_skip.ValueInt();
      if (to_skip_ < 0) throw QueryRuntimeException("Number of elements to skip must be non-negative.");
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

Limit::Limit(const std::shared_ptr<LogicalOperator> &input, Expression *expression)
    : input_(input), expression_(expression) {}

ACCEPT_WITH_INPUT(Limit)

UniqueCursorPtr Limit::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::LimitOperator);

  return MakeUniqueCursorPtr<LimitCursor>(mem, *this, mem);
}

std::vector<Symbol> Limit::OutputSymbols(const SymbolTable &symbol_table) const {
  // Propagate this to potential Produce.
  return input_->OutputSymbols(symbol_table);
}

std::vector<Symbol> Limit::ModifiedSymbols(const SymbolTable &table) const { return input_->ModifiedSymbols(table); }

std::unique_ptr<LogicalOperator> Limit::Clone(AstStorage *storage) const {
  auto object = std::make_unique<Limit>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
  return object;
}

Limit::LimitCursor::LimitCursor(const Limit &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self_.input_->MakeCursor(mem)) {}

bool Limit::LimitCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP("Limit");

  AbortCheck(context);

  // We need to evaluate the limit expression before the first input Pull
  // because it might be 0 and thereby we shouldn't Pull from input at all.
  // We can do this before Pulling from the input because the limit expression
  // is not allowed to contain any identifiers.
  if (limit_ == -1) {
    // Limit expression doesn't contain identifiers so graph view is not
    // important.
    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                  storage::View::OLD, nullptr, &context.number_of_hops);
    TypedValue limit = self_.expression_->Accept(evaluator);
    if (limit.type() != TypedValue::Type::Int)
      throw QueryRuntimeException("Limit on number of returned elements must be an integer.");

    limit_ = limit.ValueInt();
    if (limit_ < 0) throw QueryRuntimeException("Limit on number of returned elements must be non-negative.");
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

OrderBy::OrderBy(const std::shared_ptr<LogicalOperator> &input, const std::vector<SortItem> &order_by,
                 const std::vector<Symbol> &output_symbols)
    : input_(input), output_symbols_(output_symbols) {
  // split the order_by vector into two vectors of orderings and expressions
  std::vector<OrderedTypedValueCompare> ordering;
  ordering.reserve(order_by.size());
  order_by_.reserve(order_by.size());
  for (const auto &ordering_expression_pair : order_by) {
    ordering.emplace_back(ordering_expression_pair.ordering);
    order_by_.emplace_back(ordering_expression_pair.expression);
  }
  compare_ = TypedValueVectorCompare(std::move(ordering));
}

ACCEPT_WITH_INPUT(OrderBy)

std::vector<Symbol> OrderBy::OutputSymbols(const SymbolTable &symbol_table) const {
  // Propagate this to potential Produce.
  return input_->OutputSymbols(symbol_table);
}

std::vector<Symbol> OrderBy::ModifiedSymbols(const SymbolTable &table) const { return input_->ModifiedSymbols(table); }

class OrderByCursor : public Cursor {
 public:
  OrderByCursor(const OrderBy &self, utils::MemoryResource *mem)
      : self_(self), input_cursor_(self_.input_->MakeCursor(mem)), cache_(mem) {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP_BY_REF(self_);

    if (!did_pull_all_) [[unlikely]] {
      ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                    storage::View::OLD, nullptr, &context.number_of_hops);
      auto *pull_mem = context.evaluation_context.memory;
      auto *query_mem = cache_.get_allocator().resource();

      utils::pmr::vector<utils::pmr::vector<TypedValue>> order_by(pull_mem);  // Not cached, pull memory
      utils::pmr::vector<utils::pmr::vector<TypedValue>> output(query_mem);   // Cached, query memory

      while (input_cursor_->Pull(frame, context)) {
        // collect the order_by elements
        utils::pmr::vector<TypedValue> order_by_elem(pull_mem);
        order_by_elem.reserve(self_.order_by_.size());
        for (auto const &expression_ptr : self_.order_by_) {
          order_by_elem.emplace_back(expression_ptr->Accept(evaluator));
        }
        order_by.emplace_back(std::move(order_by_elem));

        // collect the output elements
        utils::pmr::vector<TypedValue> output_elem(query_mem);
        output_elem.reserve(self_.output_symbols_.size());
        for (const Symbol &output_sym : self_.output_symbols_) {
          output_elem.emplace_back(frame[output_sym]);
        }
        output.emplace_back(std::move(output_elem));
      }

      // sorting with range zip
      // we compare on just the projection of the 1st range (order_by)
      // this will also permute the 2nd range (output)
      ranges::sort(
          rv::zip(order_by, output), self_.compare_.lex_cmp(),
          [](auto const &value) -> auto const & { return std::get<0>(value); });

      // no longer need the order_by terms
      order_by.clear();
      cache_ = std::move(output);

      did_pull_all_ = true;
      cache_it_ = cache_.begin();
    }

    if (cache_it_ == cache_.end()) return false;

    AbortCheck(context);

    // place the output values on the frame
    DMG_ASSERT(self_.output_symbols_.size() == cache_it_->size(),
               "Number of values does not match the number of output symbols "
               "in OrderBy");
    auto output_sym_it = self_.output_symbols_.begin();
    for (TypedValue &output : *cache_it_) {
      if (context.frame_change_collector) {
        context.frame_change_collector->ResetTrackingValue(output_sym_it->name());
      }
      frame[*output_sym_it++] = std::move(output);
    }
    cache_it_++;
    return true;
  }
  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    did_pull_all_ = false;
    cache_.clear();
    cache_it_ = cache_.begin();
  }

 private:
  const OrderBy &self_;
  const UniqueCursorPtr input_cursor_;
  bool did_pull_all_{false};
  // a cache of elements pulled from the input
  // the cache is filled and sorted on first Pull
  utils::pmr::vector<utils::pmr::vector<TypedValue>> cache_;
  // iterator over the cache_, maintains state between Pulls
  decltype(cache_.begin()) cache_it_ = cache_.begin();
};

UniqueCursorPtr OrderBy::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::OrderByOperator);

  return MakeUniqueCursorPtr<OrderByCursor>(mem, *this, mem);
}

std::unique_ptr<LogicalOperator> OrderBy::Clone(AstStorage *storage) const {
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

std::string OrderBy::ToString() const {
  return fmt::format("OrderBy {{{}}}",
                     utils::IterableToString(output_symbols_, ", ", [](const auto &sym) { return sym.name(); }));
}

Merge::Merge(const std::shared_ptr<LogicalOperator> &input, const std::shared_ptr<LogicalOperator> &merge_match,
             const std::shared_ptr<LogicalOperator> &merge_create)
    : input_(input ? input : std::make_shared<Once>()), merge_match_(merge_match), merge_create_(merge_create) {}

bool Merge::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    input_->Accept(visitor) && merge_match_->Accept(visitor) && merge_create_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

UniqueCursorPtr Merge::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::MergeOperator);

  return MakeUniqueCursorPtr<MergeCursor>(mem, *this, mem);
}

std::vector<Symbol> Merge::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  // Match and create branches should have the same symbols, so just take one
  // of them.
  auto my_symbols = merge_match_->OutputSymbols(table);
  symbols.insert(symbols.end(), my_symbols.begin(), my_symbols.end());
  return symbols;
}

std::unique_ptr<LogicalOperator> Merge::Clone(AstStorage *storage) const {
  auto object = std::make_unique<Merge>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->merge_match_ = merge_match_ ? merge_match_->Clone(storage) : nullptr;
  object->merge_create_ = merge_create_ ? merge_create_->Clone(storage) : nullptr;
  return object;
}

Merge::MergeCursor::MergeCursor(const Merge &self, utils::MemoryResource *mem)
    : input_cursor_(self.input_->MakeCursor(mem)),
      merge_match_cursor_(self.merge_match_->MakeCursor(mem)),
      merge_create_cursor_(self.merge_create_->MakeCursor(mem)) {}

bool Merge::MergeCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP("Merge");

  context.evaluation_context.scope.in_merge = true;
  memgraph::utils::OnScopeExit merge_exit([&] { context.evaluation_context.scope.in_merge = false; });

  while (true) {
    AbortCheck(context);
    if (pull_input_) {
      if (input_cursor_->Pull(frame, context)) {
        // after a successful input from the input
        // reset merge_match (it's expand iterators maintain state)
        // and merge_create (could have a Once at the beginning)
        merge_match_cursor_->Reset();
        merge_create_cursor_->Reset();
      } else {
        // input is exhausted, we're done
        return false;
      }
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
        return merge_create_cursor_->Pull(frame, context);
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

Optional::Optional(const std::shared_ptr<LogicalOperator> &input, const std::shared_ptr<LogicalOperator> &optional,
                   const std::vector<Symbol> &optional_symbols)
    : input_(input ? input : std::make_shared<Once>()), optional_(optional), optional_symbols_(optional_symbols) {}

bool Optional::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    input_->Accept(visitor) && optional_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

UniqueCursorPtr Optional::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::OptionalOperator);

  return MakeUniqueCursorPtr<OptionalCursor>(mem, *this, mem);
}

std::vector<Symbol> Optional::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  auto my_symbols = optional_->ModifiedSymbols(table);
  symbols.insert(symbols.end(), my_symbols.begin(), my_symbols.end());
  return symbols;
}

std::unique_ptr<LogicalOperator> Optional::Clone(AstStorage *storage) const {
  auto object = std::make_unique<Optional>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->optional_ = optional_ ? optional_->Clone(storage) : nullptr;
  object->optional_symbols_ = optional_symbols_;
  return object;
}

Optional::OptionalCursor::OptionalCursor(const Optional &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self.input_->MakeCursor(mem)), optional_cursor_(self.optional_->MakeCursor(mem)) {}

bool Optional::OptionalCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP("Optional");

  while (true) {
    AbortCheck(context);
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
        for (const Symbol &sym : self_.optional_symbols_) frame[sym] = TypedValue(context.evaluation_context.memory);
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

Unwind::Unwind(const std::shared_ptr<LogicalOperator> &input, Expression *input_expression, Symbol output_symbol)
    : input_(input ? input : std::make_shared<Once>()),
      input_expression_(input_expression),
      output_symbol_(std::move(output_symbol)) {}

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

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP("Unwind");
    while (true) {
      AbortCheck(context);
      // if we reached the end of our list of values
      // pull from the input
      if (input_value_it_ == input_value_.end()) {
        if (!input_cursor_->Pull(frame, context)) return false;

        // successful pull from input, initialize value and iterator
        ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                      storage::View::OLD);
        TypedValue input_value = self_.input_expression_->Accept(evaluator);
        if (input_value.type() != TypedValue::Type::List)
          throw QueryRuntimeException("Argument of UNWIND must be a list, but '{}' was provided.", input_value.type());
        // Move the evaluted input_value_list to our vector.
        input_value_ = std::move(input_value.ValueList());
        input_value_it_ = input_value_.begin();
      }

      // if we reached the end of our list of values goto back to top
      if (input_value_it_ == input_value_.end()) continue;

      frame[self_.output_symbol_] = std::move(*input_value_it_++);
      if (context.frame_change_collector && context.frame_change_collector->IsKeyTracked(self_.output_symbol_.name_)) {
        context.frame_change_collector->ResetTrackingValue(self_.output_symbol_.name_);
      }
      return true;
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
};

UniqueCursorPtr Unwind::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::UnwindOperator);

  return MakeUniqueCursorPtr<UnwindCursor>(mem, *this, mem);
}

std::unique_ptr<LogicalOperator> Unwind::Clone(AstStorage *storage) const {
  auto object = std::make_unique<Unwind>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->input_expression_ = input_expression_ ? input_expression_->Clone(storage) : nullptr;
  object->output_symbol_ = output_symbol_;
  return object;
}

class DistinctCursor : public Cursor {
 public:
  DistinctCursor(const Distinct &self, utils::MemoryResource *mem)
      : self_(self), input_cursor_(self.input_->MakeCursor(mem)), seen_rows_(mem) {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP("Distinct");

    AbortCheck(context);

    while (true) {
      if (!input_cursor_->Pull(frame, context)) {
        // Nothing left to pull, we can dispose of seen_rows now
        seen_rows_.clear();
        return false;
      }

      utils::pmr::vector<TypedValue> row(seen_rows_.get_allocator().resource());
      row.reserve(self_.value_symbols_.size());

      for (const auto &symbol : self_.value_symbols_) {
        row.emplace_back(frame.at(symbol));
      }

      if (seen_rows_.insert(std::move(row)).second) {
        return true;
      }
    }
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    seen_rows_.clear();
  }

 private:
  const Distinct &self_;
  const UniqueCursorPtr input_cursor_;
  // a set of already seen rows
  utils::pmr::unordered_set<utils::pmr::vector<TypedValue>,
                            // use FNV collection hashing specialized for a
                            // vector of TypedValue
                            utils::FnvCollection<utils::pmr::vector<TypedValue>, TypedValue, TypedValue::Hash>,
                            TypedValueVectorEqual>
      seen_rows_;
};

Distinct::Distinct(const std::shared_ptr<LogicalOperator> &input, const std::vector<Symbol> &value_symbols)
    : input_(input ? input : std::make_shared<Once>()), value_symbols_(value_symbols) {}

ACCEPT_WITH_INPUT(Distinct)

UniqueCursorPtr Distinct::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::DistinctOperator);

  return MakeUniqueCursorPtr<DistinctCursor>(mem, *this, mem);
}

std::vector<Symbol> Distinct::OutputSymbols(const SymbolTable &symbol_table) const {
  // Propagate this to potential Produce.
  return input_->OutputSymbols(symbol_table);
}

std::vector<Symbol> Distinct::ModifiedSymbols(const SymbolTable &table) const { return input_->ModifiedSymbols(table); }

std::unique_ptr<LogicalOperator> Distinct::Clone(AstStorage *storage) const {
  auto object = std::make_unique<Distinct>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->value_symbols_ = value_symbols_;
  return object;
}

Union::Union(const std::shared_ptr<LogicalOperator> &left_op, const std::shared_ptr<LogicalOperator> &right_op,
             const std::vector<Symbol> &union_symbols, const std::vector<Symbol> &left_symbols,
             const std::vector<Symbol> &right_symbols)
    : left_op_(left_op),
      right_op_(right_op),
      union_symbols_(union_symbols),
      left_symbols_(left_symbols),
      right_symbols_(right_symbols) {}

UniqueCursorPtr Union::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::UnionOperator);

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

std::unique_ptr<LogicalOperator> Union::Clone(AstStorage *storage) const {
  auto object = std::make_unique<Union>();
  object->left_op_ = left_op_ ? left_op_->Clone(storage) : nullptr;
  object->right_op_ = right_op_ ? right_op_->Clone(storage) : nullptr;
  object->union_symbols_ = union_symbols_;
  object->left_symbols_ = left_symbols_;
  object->right_symbols_ = right_symbols_;
  return object;
}

std::string Union::ToString() const {
  return fmt::format("Union {{{0} : {1}}}",
                     utils::IterableToString(left_symbols_, ", ", [](const auto &sym) { return sym.name(); }),
                     utils::IterableToString(right_symbols_, ", ", [](const auto &sym) { return sym.name(); }));
}

Union::UnionCursor::UnionCursor(const Union &self, utils::MemoryResource *mem)
    : self_(self), left_cursor_(self.left_op_->MakeCursor(mem)), right_cursor_(self.right_op_->MakeCursor(mem)) {}

bool Union::UnionCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP_BY_REF(self_);

  AbortCheck(context);

  utils::pmr::unordered_map<std::string, TypedValue> results(context.evaluation_context.memory);
  if (left_cursor_->Pull(frame, context)) {
    // collect values from the left child
    for (const auto &output_symbol : self_.left_symbols_) {
      results[output_symbol.name()] = frame[output_symbol];
      if (context.frame_change_collector && context.frame_change_collector->IsKeyTracked(output_symbol.name())) {
        context.frame_change_collector->ResetTrackingValue(output_symbol.name());
      }
    }
  } else if (right_cursor_->Pull(frame, context)) {
    // collect values from the right child
    for (const auto &output_symbol : self_.right_symbols_) {
      results[output_symbol.name()] = frame[output_symbol];
      if (context.frame_change_collector && context.frame_change_collector->IsKeyTracked(output_symbol.name())) {
        context.frame_change_collector->ResetTrackingValue(output_symbol.name());
      }
    }
  } else {
    return false;
  }

  // put collected values on frame under union symbols
  for (const auto &symbol : self_.union_symbols_) {
    frame[symbol] = results[symbol.name()];
    if (context.frame_change_collector && context.frame_change_collector->IsKeyTracked(symbol.name())) {
      context.frame_change_collector->ResetTrackingValue(symbol.name());
    }
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
  CartesianCursor(const Cartesian &self, utils::MemoryResource *mem)
      : self_(self),
        left_op_frames_(mem),
        right_op_frame_(mem),
        left_op_cursor_(self.left_op_->MakeCursor(mem)),
        right_op_cursor_(self_.right_op_->MakeCursor(mem)) {
    MG_ASSERT(left_op_cursor_ != nullptr, "CartesianCursor: Missing left operator cursor.");
    MG_ASSERT(right_op_cursor_ != nullptr, "CartesianCursor: Missing right operator cursor.");
  }

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP_BY_REF(self_);

    if (!cartesian_pull_initialized_) {
      // Pull all left_op frames.
      while (left_op_cursor_->Pull(frame, context)) {
        left_op_frames_.emplace_back(frame.elems().begin(), frame.elems().end());
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

    auto restore_frame = [&frame, &context](const auto &symbols, const auto &restore_from) {
      for (const auto &symbol : symbols) {
        frame[symbol] = restore_from[symbol.position()];
        if (context.frame_change_collector && context.frame_change_collector->IsKeyTracked(symbol.name())) {
          context.frame_change_collector->ResetTrackingValue(symbol.name());
        }
      }
    };

    if (left_op_frames_it_ == left_op_frames_.end()) {
      // Advance right_op_cursor_.
      if (!right_op_cursor_->Pull(frame, context)) return false;

      right_op_frame_.assign(frame.elems().begin(), frame.elems().end());
      left_op_frames_it_ = left_op_frames_.begin();
    } else {
      // Make sure right_op_cursor last pulled results are on frame.
      restore_frame(self_.right_symbols_, right_op_frame_);
    }

    AbortCheck(context);

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
  utils::pmr::vector<utils::pmr::vector<TypedValue>> left_op_frames_;
  utils::pmr::vector<TypedValue> right_op_frame_;
  const UniqueCursorPtr left_op_cursor_;
  const UniqueCursorPtr right_op_cursor_;
  utils::pmr::vector<utils::pmr::vector<TypedValue>>::iterator left_op_frames_it_;
  bool cartesian_pull_initialized_{false};
};

}  // namespace

UniqueCursorPtr Cartesian::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::CartesianOperator);

  return MakeUniqueCursorPtr<CartesianCursor>(mem, *this, mem);
}

std::unique_ptr<LogicalOperator> Cartesian::Clone(AstStorage *storage) const {
  auto object = std::make_unique<Cartesian>();
  object->left_op_ = left_op_ ? left_op_->Clone(storage) : nullptr;
  object->left_symbols_ = left_symbols_;
  object->right_op_ = right_op_ ? right_op_->Clone(storage) : nullptr;
  object->right_symbols_ = right_symbols_;
  return object;
}

OutputTable::OutputTable(std::vector<Symbol> output_symbols, std::vector<std::vector<TypedValue>> rows)
    : output_symbols_(std::move(output_symbols)), callback_([rows](Frame *, ExecutionContext *) { return rows; }) {}

OutputTable::OutputTable(std::vector<Symbol> output_symbols,
                         std::function<std::vector<std::vector<TypedValue>>(Frame *, ExecutionContext *)> callback)
    : output_symbols_(std::move(output_symbols)), callback_(std::move(callback)) {}

WITHOUT_SINGLE_INPUT(OutputTable);

class OutputTableCursor : public Cursor {
 public:
  explicit OutputTableCursor(const OutputTable &self) : self_(self) {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;

    AbortCheck(context);

    if (!pulled_) {
      rows_ = self_.callback_(&frame, &context);
      for (const auto &row : rows_) {
        MG_ASSERT(row.size() == self_.output_symbols_.size(), "Wrong number of columns in row!");
      }
      pulled_ = true;
    }
    if (current_row_ < rows_.size()) {
      for (size_t i = 0; i < self_.output_symbols_.size(); ++i) {
        frame[self_.output_symbols_[i]] = rows_[current_row_][i];
        if (context.frame_change_collector &&
            context.frame_change_collector->IsKeyTracked(self_.output_symbols_[i].name())) {
          context.frame_change_collector->ResetTrackingValue(self_.output_symbols_[i].name());
        }
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

std::unique_ptr<LogicalOperator> OutputTable::Clone(AstStorage *storage) const {
  auto object = std::make_unique<OutputTable>();
  object->output_symbols_ = output_symbols_;
  object->callback_ = callback_;
  return object;
}

OutputTableStream::OutputTableStream(
    std::vector<Symbol> output_symbols,
    std::function<std::optional<std::vector<TypedValue>>(Frame *, ExecutionContext *)> callback)
    : output_symbols_(std::move(output_symbols)), callback_(std::move(callback)) {}

WITHOUT_SINGLE_INPUT(OutputTableStream);

class OutputTableStreamCursor : public Cursor {
 public:
  explicit OutputTableStreamCursor(const OutputTableStream *self) : self_(self) {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;

    AbortCheck(context);

    const auto row = self_->callback_(&frame, &context);
    if (row) {
      MG_ASSERT(row->size() == self_->output_symbols_.size(), "Wrong number of columns in row!");
      for (size_t i = 0; i < self_->output_symbols_.size(); ++i) {
        frame[self_->output_symbols_[i]] = row->at(i);
        if (context.frame_change_collector &&
            context.frame_change_collector->IsKeyTracked(self_->output_symbols_[i].name())) {
          context.frame_change_collector->ResetTrackingValue(self_->output_symbols_[i].name());
        }
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

std::unique_ptr<LogicalOperator> OutputTableStream::Clone(AstStorage *storage) const {
  auto object = std::make_unique<OutputTableStream>();
  object->output_symbols_ = output_symbols_;
  object->callback_ = callback_;
  return object;
}

CallProcedure::CallProcedure(std::shared_ptr<LogicalOperator> input, std::string name, std::vector<Expression *> args,
                             std::vector<std::string> fields, std::vector<Symbol> symbols, Expression *memory_limit,
                             size_t memory_scale, bool is_write, int64_t procedure_id, bool void_procedure)
    : input_(input ? input : std::make_shared<Once>()),
      procedure_name_(std::move(name)),
      arguments_(std::move(args)),
      result_fields_(std::move(fields)),
      result_symbols_(std::move(symbols)),
      memory_limit_(memory_limit),
      memory_scale_(memory_scale),
      is_write_(is_write),
      procedure_id_(procedure_id),
      void_procedure_(void_procedure) {}

ACCEPT_WITH_INPUT(CallProcedure);

std::vector<Symbol> CallProcedure::OutputSymbols(const SymbolTable &) const { return result_symbols_; }

std::vector<Symbol> CallProcedure::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.insert(symbols.end(), result_symbols_.begin(), result_symbols_.end());
  return symbols;
}

void CallProcedure::IncrementCounter(const std::string &procedure_name) {
  procedure_counters_.WithLock([&](auto &counters) { ++counters[procedure_name]; });
}

std::unordered_map<std::string, int64_t> CallProcedure::GetAndResetCounters() {
  auto counters = procedure_counters_.Lock();
  auto ret = std::move(*counters);
  counters->clear();
  return ret;
}

namespace {

void CallCustomProcedure(const std::string_view fully_qualified_procedure_name, const mgp_proc &proc,
                         const std::vector<Expression *> &args, mgp_graph &graph, ExpressionEvaluator *evaluator,
                         utils::MemoryResource *memory, std::optional<size_t> memory_limit, mgp_result *result,
                         int64_t procedure_id, uint64_t transaction_id, const bool call_initializer = false) {
  static_assert(std::uses_allocator_v<mgp_value, utils::Allocator<mgp_value>>,
                "Expected mgp_value to use custom allocator and makes STL "
                "containers aware of that");
  // Build and type check procedure arguments.
  mgp_list proc_args(memory);
  std::vector<TypedValue> args_list;
  args_list.reserve(args.size());
  for (auto *expression : args) {
    args_list.emplace_back(expression->Accept(*evaluator));
  }
  std::optional<query::Graph> subgraph;
  std::optional<query::SubgraphDbAccessor> db_acc;

  if (!args_list.empty() && args_list.front().type() == TypedValue::Type::Graph) {
    auto subgraph_value = args_list.front().ValueGraph();
    subgraph = query::Graph(std::move(subgraph_value), subgraph_value.get_allocator());
    args_list.erase(args_list.begin());

    db_acc = query::SubgraphDbAccessor(*std::get<query::DbAccessor *>(graph.impl), &*subgraph);
    graph.impl = &*db_acc;
  }

  procedure::ValidateArguments(args_list, proc, fully_qualified_procedure_name);
  procedure::ConstructArguments(args_list, proc, proc_args, graph);
  if (call_initializer) {
    MG_ASSERT(proc.initializer);
    mgp_memory initializer_memory{memory};
    proc.initializer.value()(&proc_args, &graph, &initializer_memory);
  }
  if (memory_limit) {
    SPDLOG_INFO("Running '{}' with memory limit of {}", fully_qualified_procedure_name,
                utils::GetReadableSize(*memory_limit));
    // Only allocations which can leak memory are
    // our own mgp object allocations. Jemalloc can track
    // memory correctly, but some memory may not be released
    // immediately, so we want to give user info on leak still
    // considering our allocations
    utils::MemoryTrackingResource memory_tracking_resource{memory, *memory_limit};
    // if we are already tracking, no harm no faul
    // if we are not tracking, we need to start now, with unlimited memory
    // for query, but limited for procedure

    // check if transaction is tracked currently, so we
    // can disable tracking on that arena if it is not
    // once we are done with procedure tracking

#if USE_JEMALLOC
    const bool is_transaction_tracked = memgraph::memory::IsQueryTracked();

    std::unique_ptr<utils::QueryMemoryTracker> tmp_query_tracker{};
    if (!is_transaction_tracked) {
      // start tracking with unlimited limit on query
      // which is same as not being tracked at all
      tmp_query_tracker = std::make_unique<utils::QueryMemoryTracker>();
      tmp_query_tracker->SetQueryLimit(memgraph::memory::UNLIMITED_MEMORY);
      memgraph::memory::StartTrackingCurrentThread(tmp_query_tracker.get());
    }

    // due to mgp_batch_read_proc and mgp_batch_write_proc
    // we can return to execution without exhausting whole
    // memory. Here we need to update tracking
    memgraph::memory::CreateOrContinueProcedureTracking(procedure_id, *memory_limit);

    const utils::OnScopeExit on_scope_exit{[is_transaction_tracked]() {
      memgraph::memory::PauseProcedureTracking();
      if (!is_transaction_tracked) memgraph::memory::StopTrackingCurrentThread();
    }};
#endif

    mgp_memory proc_memory{&memory_tracking_resource};

    // TODO: What about cross library boundary exceptions? OMG C++?! <- should be fine since moving to shared libstd
    proc.cb(&proc_args, &graph, result, &proc_memory);

    auto leaked_bytes = memory_tracking_resource.GetAllocatedBytes();
    if (leaked_bytes > 0U) {
      spdlog::warn("Query procedure '{}' leaked {} *tracked* bytes", fully_qualified_procedure_name, leaked_bytes);
    }
  } else {
    // TODO: Add a tracking MemoryResource without limits, so that we report
    // memory leaks in procedure.
    mgp_memory proc_memory{memory};
    // TODO: What about cross library boundary exceptions? OMG C++?!
    proc.cb(&proc_args, &graph, result, &proc_memory);
  }
}

}  // namespace

class CallProcedureCursor : public Cursor {
  const CallProcedure *self_;
  UniqueCursorPtr input_cursor_;
  mgp_result result_;
  decltype(result_.rows.end()) result_row_it_{result_.rows.end()};
  // Holds the lock on the module so it doesn't get reloaded
  std::shared_ptr<procedure::Module> module_;
  mgp_proc const *proc_{nullptr};
  bool stream_exhausted{true};
  bool call_initializer{false};
  std::optional<std::function<void()>> cleanup_{std::nullopt};

 public:
  CallProcedureCursor(const CallProcedure *self, utils::MemoryResource *mem)
      : self_(self),
        input_cursor_(self_->input_->MakeCursor(mem)),
        // result_ needs to live throughout multiple Pull evaluations, until all
        // rows are produced. We don't use the memory dedicated for QueryExecution (and Frame),
        // but memory dedicated for procedure to wipe result_ and everything allocated in procedure all at once.
        result_(mem) {
    MG_ASSERT(self_->result_fields_.size() == self_->result_symbols_.size(), "Incorrectly constructed CallProcedure");
    auto maybe_found = procedure::FindProcedure(procedure::gModuleRegistry, self_->procedure_name_);
    if (!maybe_found) {
      throw QueryRuntimeException("There is no procedure named '{}'.", self_->procedure_name_);
    }

    // Module lock is held during the whole cursor lifetime
    module_ = std::move(maybe_found->first);
    proc_ = maybe_found->second;

    if (proc_->info.is_write != self_->is_write_) {
      auto get_proc_type_str = [](bool is_write) { return is_write ? "write" : "read"; };
      throw QueryRuntimeException("The procedure named '{}' was a {} procedure, but changed to be a {} procedure.",
                                  self_->procedure_name_, get_proc_type_str(self_->is_write_),
                                  get_proc_type_str(proc_->info.is_write));
    }

    for (size_t i = 0; i < self_->result_fields_.size(); ++i) {
      auto signature_it =
          proc_->results.find(memgraph::utils::pmr::string{self_->result_fields_[i], proc_->results.get_allocator()});
      result_.signature.emplace(
          self_->result_fields_[i],
          ResultsMetadata{signature_it->second.first, signature_it->second.second, static_cast<uint32_t>(i)});
    }
    if (proc_->results.size() == self_->result_fields_.size()) return;
    // Not all results were yielded but they still need to be inserted inside the signature
    uint32_t index = self_->result_fields_.size();
    for (auto const &[name, signature] : proc_->results) {
      if (result_.signature.find(name) == result_.signature.end()) {
        result_.signature.emplace(name, ResultsMetadata{signature.first, signature.second, index++});
      }
    }
  }

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP_BY_REF(*self_);

    AbortCheck(context);

    auto skip_rows_with_deleted_values = [this]() {
      while (result_row_it_ != result_.rows.end() && result_row_it_->has_deleted_values) {
        ++result_row_it_;
      }
    };

    // We need to fetch new procedure results after pulling from input.
    // TODO: Look into openCypher's distinction between procedures returning an
    // empty result set vs procedures which return `void`. We currently don't
    // have procedures registering what they return.
    // This `while` loop will skip over empty results.
    while (result_row_it_ == result_.rows.end()) {
      if (!proc_->info.is_batched) {
        stream_exhausted = true;
      }
      if (stream_exhausted) {
        if (!input_cursor_->Pull(frame, context)) {
          if (proc_->cleanup) {
            proc_->cleanup.value()();
          }
          return false;
        }
        stream_exhausted = false;
        if (proc_->initializer) {
          call_initializer = true;
          MG_ASSERT(proc_->cleanup);
          proc_->cleanup.value()();
        }
      }
      if (!cleanup_ && proc_->cleanup) [[unlikely]] {
        cleanup_.emplace(*proc_->cleanup);
      }
      result_.rows.clear();

      const auto graph_view = proc_->info.is_write ? storage::View::NEW : storage::View::OLD;
      ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                    graph_view, nullptr, &context.number_of_hops);
      result_.is_transactional = storage::IsTransactional(context.db_accessor->GetStorageMode());
      auto *memory = context.evaluation_context.memory;
      auto memory_limit = EvaluateMemoryLimit(evaluator, self_->memory_limit_, self_->memory_scale_);
      auto graph = mgp_graph::WritableGraph(*context.db_accessor, graph_view, context);
      const auto transaction_id = context.db_accessor->GetTransactionId();
      MG_ASSERT(transaction_id.has_value());
      CallCustomProcedure(self_->procedure_name_, *proc_, self_->arguments_, graph, &evaluator, memory, memory_limit,
                          &result_, self_->procedure_id_, transaction_id.value(), call_initializer);

      if (call_initializer) call_initializer = false;

      if (result_.error_msg) {
        memgraph::utils::MemoryTracker::OutOfMemoryExceptionBlocker blocker;
        throw QueryRuntimeException("{}: {}", self_->procedure_name_, *result_.error_msg);
      }
      result_row_it_ = result_.rows.begin();
      if (!result_.is_transactional) {
        skip_rows_with_deleted_values();
      }

      stream_exhausted = result_row_it_ == result_.rows.end();
    }

    // Instead of checking if procedure yielded all required values
    // it is filled with null values on construction. This came as a
    // direct consequence of changing from mgp_result rows from map to vector
    // PRO: this is a lot faster
    // CON: doesn't throw anymore if not all values are present
    // Values are ordered the same as result_fields
    auto &values = result_row_it_->values;
    for (int i = 0; i < self_->result_fields_.size(); ++i) {
      frame[self_->result_symbols_[i]] = std::move(values[i]);
      if (context.frame_change_collector &&
          context.frame_change_collector->IsKeyTracked(self_->result_symbols_[i].name())) {
        context.frame_change_collector->ResetTrackingValue(self_->result_symbols_[i].name());
      }
    }
    ++result_row_it_;
    if (!result_.is_transactional) {
      skip_rows_with_deleted_values();
    }

    return true;
  }

  void Reset() override {
    result_.rows.clear();
    result_row_it_ = result_.rows.begin();
    if (cleanup_) {
      cleanup_.value()();
    }
  }

  void Shutdown() override {
    if (cleanup_) {
      cleanup_.value()();
    }
  }
};

class CallValidateProcedureCursor : public Cursor {
  const CallProcedure *self_;
  UniqueCursorPtr input_cursor_;

 public:
  CallValidateProcedureCursor(const CallProcedure *self, utils::MemoryResource *mem)
      : self_(self), input_cursor_(self_->input_->MakeCursor(mem)) {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP("CallValidateProcedureCursor");

    AbortCheck(context);
    if (!input_cursor_->Pull(frame, context)) {
      return false;
    }

    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                  storage::View::NEW, nullptr, &context.number_of_hops);

    const auto args = self_->arguments_;
    if (args.size() != 3U) {
      throw QueryRuntimeException("'mgps.validate' requires exactly 3 arguments.");
    }

    const auto predicate = args[0]->Accept(evaluator);
    const bool predicate_val = predicate.ValueBool();

    if (predicate_val) [[unlikely]] {
      const auto &message = args[1]->Accept(evaluator);
      const auto &message_args = args[2]->Accept(evaluator);

      using TString = std::remove_cvref_t<decltype(message.ValueString())>;
      using TElement = std::remove_cvref_t<decltype(message_args.ValueList()[0])>;

      utils::JStringFormatter<TString, TElement> formatter;

      try {
        const auto &msg = formatter.FormatString(message.ValueString(), message_args.ValueList());
        throw QueryRuntimeException(msg);
      } catch (const utils::JStringFormatException &e) {
        throw QueryRuntimeException(e.what());
      }
    }

    return true;
  }

  void Reset() override { input_cursor_->Reset(); }

  void Shutdown() override {}
};

UniqueCursorPtr CallProcedure::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::CallProcedureOperator);
  CallProcedure::IncrementCounter(procedure_name_);

  if (void_procedure_) {
    // Currently we do not support Call procedures that do not return
    // anything. This cursor is way too specific, but it provides a workaround
    // to ensure GraphQL compatibility until we start supporting truly void
    // procedures.
    return MakeUniqueCursorPtr<CallValidateProcedureCursor>(mem, this, mem);
  }

  return MakeUniqueCursorPtr<CallProcedureCursor>(mem, this, mem);
}

std::unique_ptr<LogicalOperator> CallProcedure::Clone(AstStorage *storage) const {
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
  object->procedure_id_ = procedure_id_;
  object->void_procedure_ = void_procedure_;
  return object;
}

std::string CallProcedure::ToString() const {
  return fmt::format("CallProcedure<{0}> {{{1}}}", procedure_name_,
                     utils::IterableToString(result_symbols_, ", ", [](const auto &sym) { return sym.name(); }));
}

LoadCsv::LoadCsv(std::shared_ptr<LogicalOperator> input, Expression *file, bool with_header, bool ignore_bad,
                 Expression *delimiter, Expression *quote, Expression *nullif, Symbol row_var)
    : input_(input ? input : (std::make_shared<Once>())),
      file_(file),
      with_header_(with_header),
      ignore_bad_(ignore_bad),
      delimiter_(delimiter),
      quote_(quote),
      nullif_(nullif),
      row_var_(std::move(row_var)) {
  MG_ASSERT(file_, "Something went wrong - '{}' member file_ shouldn't be a nullptr", __func__);
}

ACCEPT_WITH_INPUT(LoadCsv)

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
  return expression ? expression->Accept(*eval) : TypedValue(eval->GetMemoryResource());
}

auto ToOptionalString(ExpressionEvaluator *evaluator, Expression *expression) -> std::optional<utils::pmr::string> {
  auto evaluated_expr = EvaluateOptionalExpression(expression, evaluator);
  if (evaluated_expr.IsString()) {
    return utils::pmr::string(std::move(evaluated_expr).ValueString(), evaluator->GetMemoryResource());
  }
  return std::nullopt;
};

TypedValue CsvRowToTypedList(csv::Reader::Row &row, std::optional<utils::pmr::string> &nullif) {
  auto *mem = row.get_allocator().resource();
  auto typed_columns = utils::pmr::vector<TypedValue>(mem);
  typed_columns.reserve(row.size());
  for (auto &column : row) {
    if (!nullif.has_value() || column != nullif.value()) {
      typed_columns.emplace_back(std::move(column));
    } else {
      typed_columns.emplace_back();
    }
  }
  return {std::move(typed_columns), mem};
}

TypedValue CsvRowToTypedMap(csv::Reader::Row &row, csv::Reader::Header header,
                            std::optional<utils::pmr::string> &nullif) {
  // a valid row has the same number of elements as the header
  auto *mem = row.get_allocator().resource();
  TypedValue::TMap m{mem};
  for (auto i = 0; i < row.size(); ++i) {
    if (!nullif.has_value() || row[i] != nullif.value()) {
      m.emplace(std::move(header[i]), std::move(row[i]));
    } else {
      m.emplace(std::piecewise_construct, std::forward_as_tuple(std::move(header[i])), std::forward_as_tuple());
    }
  }
  return {std::move(m), mem};
}

}  // namespace

class LoadCsvCursor : public Cursor {
  const LoadCsv *self_;
  const UniqueCursorPtr input_cursor_;
  bool did_pull_;
  std::optional<csv::Reader> reader_{};
  std::optional<utils::pmr::string> nullif_;

 public:
  LoadCsvCursor(const LoadCsv *self, utils::MemoryResource *mem)
      : self_(self), input_cursor_(self_->input_->MakeCursor(mem)), did_pull_{false} {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP_BY_REF(*self_);

    AbortCheck(context);

    // ToDo(the-joksim):
    //  - this is an ungodly hack because the pipeline of creating a plan
    //  doesn't allow evaluating the expressions contained in self_->file_,
    //  self_->delimiter_, and self_->quote_ earlier (say, in the interpreter.cpp)
    //  without massacring the code even worse than I did here
    if (UNLIKELY(!reader_)) {
      reader_ = MakeReader(&context.evaluation_context);
      nullif_ = ParseNullif(&context.evaluation_context);
    }

    if (input_cursor_->Pull(frame, context)) {
      if (did_pull_) {
        throw QueryRuntimeException(
            "LOAD CSV can be executed only once, please check if the cardinality of the operator before LOAD CSV "
            "is "
            "1");
      }
      did_pull_ = true;
      reader_->Reset();
    }

    auto row = reader_->GetNextRow(context.evaluation_context.memory);
    if (!row) {
      return false;
    }
    if (!reader_->HasHeader()) {
      frame[self_->row_var_] = CsvRowToTypedList(*row, nullif_);
    } else {
      frame[self_->row_var_] =
          CsvRowToTypedMap(*row, csv::Reader::Header(reader_->GetHeader(), context.evaluation_context.memory), nullif_);
    }
    if (context.frame_change_collector && context.frame_change_collector->IsKeyTracked(self_->row_var_.name())) {
      context.frame_change_collector->ResetTrackingValue(self_->row_var_.name());
    }
    return true;
  }

  void Reset() override { input_cursor_->Reset(); }
  void Shutdown() override { input_cursor_->Shutdown(); }

 private:
  csv::Reader MakeReader(EvaluationContext *eval_context) {
    Frame frame(0);
    SymbolTable symbol_table;
    DbAccessor *dba = nullptr;
    auto evaluator = ExpressionEvaluator(&frame, symbol_table, *eval_context, dba, storage::View::OLD);

    auto maybe_file = ToOptionalString(&evaluator, self_->file_);
    auto maybe_delim = ToOptionalString(&evaluator, self_->delimiter_);
    auto maybe_quote = ToOptionalString(&evaluator, self_->quote_);

    // No need to check if maybe_file is std::nullopt, as the parser makes sure
    // we can't get a nullptr for the 'file_' member in the LoadCsv clause.
    return csv::Reader(
        csv::CsvSource::Create(*maybe_file),
        csv::Reader::Config(self_->with_header_, self_->ignore_bad_, std::move(maybe_delim), std::move(maybe_quote)),
        eval_context->memory);
  }

  std::optional<utils::pmr::string> ParseNullif(EvaluationContext *eval_context) {
    Frame frame(0);
    SymbolTable symbol_table;
    DbAccessor *dba = nullptr;
    auto evaluator = ExpressionEvaluator(&frame, symbol_table, *eval_context, dba, storage::View::OLD);

    return ToOptionalString(&evaluator, self_->nullif_);
  }
};

UniqueCursorPtr LoadCsv::MakeCursor(utils::MemoryResource *mem) const {
  return MakeUniqueCursorPtr<LoadCsvCursor>(mem, this, mem);
}

std::unique_ptr<LogicalOperator> LoadCsv::Clone(AstStorage *storage) const {
  auto object = std::make_unique<LoadCsv>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->file_ = file_ ? file_->Clone(storage) : nullptr;
  object->with_header_ = with_header_;
  object->ignore_bad_ = ignore_bad_;
  object->delimiter_ = delimiter_ ? delimiter_->Clone(storage) : nullptr;
  object->quote_ = quote_ ? quote_->Clone(storage) : nullptr;
  object->nullif_ = nullif_;
  object->row_var_ = row_var_;
  return object;
}

std::string LoadCsv::ToString() const { return fmt::format("LoadCsv {{{}}}", row_var_.name()); };

class ForeachCursor : public Cursor {
 public:
  explicit ForeachCursor(const Foreach &foreach, utils::MemoryResource *mem)
      : loop_variable_symbol_(foreach.loop_variable_symbol_),
        input_(foreach.input_->MakeCursor(mem)),
        updates_(foreach.update_clauses_->MakeCursor(mem)),
        expression(foreach.expression_) {}

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP(op_name_);

    if (!input_->Pull(frame, context)) {
      return false;
    }

    ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                  storage::View::NEW, nullptr, &context.number_of_hops);
    TypedValue expr_result = expression->Accept(evaluator);

    if (expr_result.IsNull()) {
      return true;
    }

    if (!expr_result.IsList()) {
      throw QueryRuntimeException("FOREACH expression must resolve to a list, but got '{}'.", expr_result.type());
    }

    const auto &cache_ = expr_result.ValueList();
    for (const auto &index : cache_) {
      frame[loop_variable_symbol_] = index;
      while (updates_->Pull(frame, context)) {
        AbortCheck(context);
      }
      ResetUpdates();
    }

    return true;
  }

  void Shutdown() override { input_->Shutdown(); }

  void ResetUpdates() { updates_->Reset(); }

  void Reset() override {
    input_->Reset();
    ResetUpdates();
  }

 private:
  const Symbol loop_variable_symbol_;
  const UniqueCursorPtr input_;
  const UniqueCursorPtr updates_;
  Expression *expression;
  const char *op_name_{"Foreach"};
};

Foreach::Foreach(std::shared_ptr<LogicalOperator> input, std::shared_ptr<LogicalOperator> updates, Expression *expr,
                 Symbol loop_variable_symbol)
    : input_(input ? std::move(input) : std::make_shared<Once>()),
      update_clauses_(std::move(updates)),
      expression_(expr),
      loop_variable_symbol_(std::move(loop_variable_symbol)) {}

UniqueCursorPtr Foreach::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ForeachOperator);
  return MakeUniqueCursorPtr<ForeachCursor>(mem, *this, mem);
}

std::vector<Symbol> Foreach::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(loop_variable_symbol_);
  return symbols;
}

bool Foreach::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    input_->Accept(visitor);
    update_clauses_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

std::unique_ptr<LogicalOperator> Foreach::Clone(AstStorage *storage) const {
  auto object = std::make_unique<Foreach>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->update_clauses_ = update_clauses_ ? update_clauses_->Clone(storage) : nullptr;
  object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
  object->loop_variable_symbol_ = loop_variable_symbol_;
  return object;
}

Apply::Apply(const std::shared_ptr<LogicalOperator> input, const std::shared_ptr<LogicalOperator> subquery,
             bool subquery_has_return)
    : input_(input ? input : std::make_shared<Once>()),
      subquery_(subquery),
      subquery_has_return_(subquery_has_return) {}

bool Apply::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    input_->Accept(visitor) && subquery_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

UniqueCursorPtr Apply::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ApplyOperator);

  return MakeUniqueCursorPtr<ApplyCursor>(mem, *this, mem);
}

Apply::ApplyCursor::ApplyCursor(const Apply &self, utils::MemoryResource *mem)
    : self_(self),
      input_(self.input_->MakeCursor(mem)),
      subquery_(self.subquery_->MakeCursor(mem)),
      subquery_has_return_(self.subquery_has_return_) {}

std::vector<Symbol> Apply::ModifiedSymbols(const SymbolTable &table) const {
  // Since Apply is the Cartesian product, modified symbols are combined from
  // both execution branches.
  auto symbols = input_->ModifiedSymbols(table);
  auto subquery_symbols = subquery_->ModifiedSymbols(table);
  symbols.insert(symbols.end(), subquery_symbols.begin(), subquery_symbols.end());
  return symbols;
}

std::unique_ptr<LogicalOperator> Apply::Clone(AstStorage *storage) const {
  auto object = std::make_unique<Apply>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->subquery_ = subquery_ ? subquery_->Clone(storage) : nullptr;
  object->subquery_has_return_ = subquery_has_return_;
  return object;
}

bool Apply::ApplyCursor::Pull(Frame &frame, ExecutionContext &context) {
  OOMExceptionEnabler oom_exception;
  SCOPED_PROFILE_OP("Apply");

  while (true) {
    AbortCheck(context);
    if (pull_input_ && !input_->Pull(frame, context)) {
      return false;
    };

    if (subquery_->Pull(frame, context)) {
      // if successful, next Pull from this should not pull_input_
      pull_input_ = false;
      return true;
    }
    // subquery cursor has been exhausted
    // skip that row
    pull_input_ = true;
    subquery_->Reset();

    // don't skip row if no rows are returned from subquery, return input_ rows
    if (!subquery_has_return_) return true;
  }
}

void Apply::ApplyCursor::Shutdown() {
  input_->Shutdown();
  subquery_->Shutdown();
}

void Apply::ApplyCursor::Reset() {
  input_->Reset();
  subquery_->Reset();
  pull_input_ = true;
}

IndexedJoin::IndexedJoin(const std::shared_ptr<LogicalOperator> main_branch,
                         const std::shared_ptr<LogicalOperator> sub_branch)
    : main_branch_(main_branch ? main_branch : std::make_shared<Once>()), sub_branch_(sub_branch) {}

WITHOUT_SINGLE_INPUT(IndexedJoin);

bool IndexedJoin::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    main_branch_->Accept(visitor) && sub_branch_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

UniqueCursorPtr IndexedJoin::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::IndexedJoinOperator);

  return MakeUniqueCursorPtr<IndexedJoinCursor>(mem, *this, mem);
}

IndexedJoin::IndexedJoinCursor::IndexedJoinCursor(const IndexedJoin &self, utils::MemoryResource *mem)
    : self_(self), main_branch_(self.main_branch_->MakeCursor(mem)), sub_branch_(self.sub_branch_->MakeCursor(mem)) {}

std::vector<Symbol> IndexedJoin::ModifiedSymbols(const SymbolTable &table) const {
  // Since Apply is the Cartesian product, modified symbols are combined from
  // both execution branches.
  auto symbols = main_branch_->ModifiedSymbols(table);
  auto sub_branch_symbols = sub_branch_->ModifiedSymbols(table);
  symbols.insert(symbols.end(), sub_branch_symbols.begin(), sub_branch_symbols.end());
  return symbols;
}

std::unique_ptr<LogicalOperator> IndexedJoin::Clone(AstStorage *storage) const {
  auto object = std::make_unique<IndexedJoin>();
  object->main_branch_ = main_branch_ ? main_branch_->Clone(storage) : nullptr;
  object->sub_branch_ = sub_branch_ ? sub_branch_->Clone(storage) : nullptr;
  return object;
}

bool IndexedJoin::IndexedJoinCursor::Pull(Frame &frame, ExecutionContext &context) {
  SCOPED_PROFILE_OP("IndexedJoin");

  while (true) {
    AbortCheck(context);
    if (pull_input_ && !main_branch_->Pull(frame, context)) {
      return false;
    };

    if (sub_branch_->Pull(frame, context)) {
      // if successful, next Pull from this should not pull_input_
      pull_input_ = false;
      return true;
    }

    // subquery cursor has been exhausted
    // skip that row
    pull_input_ = true;
    sub_branch_->Reset();
  }
}

void IndexedJoin::IndexedJoinCursor::Shutdown() {
  main_branch_->Shutdown();
  sub_branch_->Shutdown();
}

void IndexedJoin::IndexedJoinCursor::Reset() {
  main_branch_->Reset();
  sub_branch_->Reset();
  pull_input_ = true;
}

std::vector<Symbol> HashJoin::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = left_op_->ModifiedSymbols(table);
  auto right = right_op_->ModifiedSymbols(table);
  symbols.insert(symbols.end(), right.begin(), right.end());
  return symbols;
}

bool HashJoin::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    left_op_->Accept(visitor) && right_op_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

WITHOUT_SINGLE_INPUT(HashJoin);

namespace {

class HashJoinCursor : public Cursor {
 public:
  HashJoinCursor(const HashJoin &self, utils::MemoryResource *mem)
      : self_(self),
        left_op_cursor_(self.left_op_->MakeCursor(mem)),
        right_op_cursor_(self_.right_op_->MakeCursor(mem)),
        hashtable_(mem),
        right_op_frame_(mem) {
    MG_ASSERT(left_op_cursor_ != nullptr, "HashJoinCursor: Missing left operator cursor.");
    MG_ASSERT(right_op_cursor_ != nullptr, "HashJoinCursor: Missing right operator cursor.");
  }

  bool Pull(Frame &frame, ExecutionContext &context) override {
    SCOPED_PROFILE_OP("HashJoin");

    AbortCheck(context);

    if (!hash_join_initialized_) {
      InitializeHashJoin(frame, context);
      hash_join_initialized_ = true;
    }

    // If left_op yielded zero results, there is no cartesian product.
    if (hashtable_.empty()) {
      return false;
    }

    auto restore_frame = [&frame, &context](const auto &symbols, const auto &restore_from) {
      for (const auto &symbol : symbols) {
        frame[symbol] = restore_from[symbol.position()];
        if (context.frame_change_collector && context.frame_change_collector->IsKeyTracked(symbol.name())) {
          context.frame_change_collector->ResetTrackingValue(symbol.name());
        }
      }
    };

    if (!common_value_found_) {
      // Pull from the right_op until there's a mergeable frame
      while (true) {
        auto pulled = right_op_cursor_->Pull(frame, context);
        if (!pulled) return false;

        // Check if the join value from the pulled frame is shared with any left frames
        ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                      storage::View::OLD, nullptr, &context.number_of_hops);
        auto right_value = self_.hash_join_condition_->expression2_->Accept(evaluator);
        if (hashtable_.contains(right_value)) {
          // If so, finish pulling for now and proceed to joining the pulled frame
          right_op_frame_.assign(frame.elems().begin(), frame.elems().end());
          common_value_found_ = true;
          common_value = right_value;
          left_op_frame_it_ = hashtable_[common_value].begin();
          break;
        }
      }
    } else {
      // Restore the right frame ahead of restoring the left frame
      restore_frame(self_.right_symbols_, right_op_frame_);
    }

    restore_frame(self_.left_symbols_, *left_op_frame_it_);

    left_op_frame_it_++;
    // When all left frames with the common value have been joined, move on to pulling and joining the next right
    // frame
    if (common_value_found_ && left_op_frame_it_ == hashtable_[common_value].end()) {
      common_value_found_ = false;
    }

    return true;
  }

  void Shutdown() override {
    left_op_cursor_->Shutdown();
    right_op_cursor_->Shutdown();
  }

  void Reset() override {
    left_op_cursor_->Reset();
    right_op_cursor_->Reset();
    hashtable_.clear();
    right_op_frame_.clear();
    left_op_frame_it_ = {};
    hash_join_initialized_ = false;
    common_value_found_ = false;
  }

 private:
  void InitializeHashJoin(Frame &frame, ExecutionContext &context) {
    // Pull all left_op_ frames
    while (left_op_cursor_->Pull(frame, context)) {
      ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                    storage::View::OLD, nullptr, &context.number_of_hops);
      auto left_value = self_.hash_join_condition_->expression1_->Accept(evaluator);
      if (left_value.type() != TypedValue::Type::Null) {
        hashtable_[left_value].emplace_back(frame.elems().begin(), frame.elems().end());
      }
    }
  }

  const HashJoin &self_;
  const UniqueCursorPtr left_op_cursor_;
  const UniqueCursorPtr right_op_cursor_;
  utils::pmr::unordered_map<TypedValue, utils::pmr::vector<utils::pmr::vector<TypedValue>>, TypedValue::Hash,
                            TypedValue::BoolEqual>
      hashtable_;
  utils::pmr::vector<TypedValue> right_op_frame_;
  utils::pmr::vector<utils::pmr::vector<TypedValue>>::iterator left_op_frame_it_;
  bool hash_join_initialized_{false};
  bool common_value_found_{false};
  TypedValue common_value;
};
}  // namespace

UniqueCursorPtr HashJoin::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::HashJoinOperator);
  return MakeUniqueCursorPtr<HashJoinCursor>(mem, *this, mem);
}

std::unique_ptr<LogicalOperator> HashJoin::Clone(AstStorage *storage) const {
  auto object = std::make_unique<HashJoin>();
  object->left_op_ = left_op_ ? left_op_->Clone(storage) : nullptr;
  object->left_symbols_ = left_symbols_;
  object->right_op_ = right_op_ ? right_op_->Clone(storage) : nullptr;
  object->right_symbols_ = right_symbols_;
  object->hash_join_condition_ = hash_join_condition_ ? hash_join_condition_->Clone(storage) : nullptr;
  return object;
}

std::string HashJoin::ToString() const {
  return fmt::format("HashJoin {{{} : {}}}",
                     utils::IterableToString(left_symbols_, ", ", [](const auto &sym) { return sym.name(); }),
                     utils::IterableToString(right_symbols_, ", ", [](const auto &sym) { return sym.name(); }));
}

RollUpApply::RollUpApply(std::shared_ptr<LogicalOperator> &&input,
                         std::shared_ptr<LogicalOperator> &&list_collection_branch,
                         const std::vector<Symbol> &list_collection_symbols, Symbol result_symbol, bool pass_input)
    : input_(std::move(input)),
      list_collection_branch_(std::move(list_collection_branch)),
      result_symbol_(std::move(result_symbol)),
      pass_input_(pass_input) {
  if (list_collection_symbols.size() != 1) {
    throw QueryRuntimeException("RollUpApply: list_collection_symbols must be of size 1! Please contact support.");
  }
  list_collection_symbol_ = list_collection_symbols[0];
}

std::vector<Symbol> RollUpApply::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.push_back(result_symbol_);
  return symbols;
}

bool RollUpApply::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    if (!input_ || !list_collection_branch_) {
      throw utils::NotYetImplemented("One of the branches in pattern comprehension is null! Please contact support.");
    }
    input_->Accept(visitor) && list_collection_branch_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

namespace {

class RollUpApplyCursor : public Cursor {
 public:
  RollUpApplyCursor(const RollUpApply &self, utils::MemoryResource *mem)
      : self_(self),
        input_cursor_(self.input_->MakeCursor(mem)),
        list_collection_cursor_(self_.list_collection_branch_->MakeCursor(mem)) {
    MG_ASSERT(input_cursor_ != nullptr, "RollUpApplyCursor: Missing left operator cursor.");
    MG_ASSERT(list_collection_cursor_ != nullptr, "RollUpApplyCursor: Missing right operator cursor.");
  }

  bool Pull(Frame &frame, ExecutionContext &context) override {
    OOMExceptionEnabler oom_exception;
    SCOPED_PROFILE_OP_BY_REF(self_);

    AbortCheck(context);

    TypedValue result(std::vector<TypedValue>(), context.evaluation_context.memory);
    if (input_cursor_->Pull(frame, context) || self_.pass_input_) {
      while (list_collection_cursor_->Pull(frame, context)) {
        // collect values from the list collection branch
        result.ValueList().emplace_back(frame[self_.list_collection_symbol_]);
      }

      // Clear frame change collector
      if (context.frame_change_collector &&
          context.frame_change_collector->IsKeyTracked(self_.list_collection_symbol_.name())) {
        context.frame_change_collector->ResetTrackingValue(self_.list_collection_symbol_.name());
      }

      frame[self_.result_symbol_] = result;
      // After a successful input from the list_collection_cursor_
      // reset state of cursor because it has to a Once at the beginning
      list_collection_cursor_->Reset();
    } else {
      return false;
    }

    return true;
  }

  void Shutdown() override {
    input_cursor_->Shutdown();
    list_collection_cursor_->Shutdown();
  }

  void Reset() override {
    input_cursor_->Reset();
    list_collection_cursor_->Reset();
  }

 private:
  const RollUpApply &self_;
  const UniqueCursorPtr input_cursor_;
  const UniqueCursorPtr list_collection_cursor_;
};
}  // namespace

UniqueCursorPtr RollUpApply::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::RollUpApplyOperator);
  return MakeUniqueCursorPtr<RollUpApplyCursor>(mem, *this, mem);
}

std::unique_ptr<LogicalOperator> RollUpApply::Clone(AstStorage *storage) const {
  auto object = std::make_unique<RollUpApply>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->list_collection_branch_ = list_collection_branch_ ? list_collection_branch_->Clone(storage) : nullptr;
  object->list_collection_symbol_ = list_collection_symbol_;
  object->result_symbol_ = result_symbol_;
  return object;
}

PeriodicCommit::PeriodicCommit(std::shared_ptr<LogicalOperator> &&input, Expression *commit_frequency)
    : input_(std::move(input)), commit_frequency_(commit_frequency) {}

std::vector<Symbol> PeriodicCommit::ModifiedSymbols(const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

std::vector<Symbol> PeriodicCommit::OutputSymbols(const SymbolTable &symbol_table) const {
  // Propagate this to potential Produce.
  return input_->OutputSymbols(symbol_table);
}

ACCEPT_WITH_INPUT(PeriodicCommit)

namespace {

class PeriodicCommitCursor : public Cursor {
 public:
  PeriodicCommitCursor(const PeriodicCommit &self, utils::MemoryResource *mem)
      : self_(self), input_cursor_(self.input_->MakeCursor(mem)) {
    MG_ASSERT(input_cursor_ != nullptr, "PeriodicCommitCursor: Missing input cursor.");
    MG_ASSERT(self_.commit_frequency_ != nullptr, "Commit frequency should be defined at this point!");
  }

  bool Pull(Frame &frame, ExecutionContext &context) override {
    // NOLINTNEXTLINE(misc-const-correctness)
    OOMExceptionEnabler oom_exception;
    // NOLINTNEXTLINE(misc-const-correctness)
    SCOPED_PROFILE_OP_BY_REF(self_);

    AbortCheck(context);

    if (!commit_frequency_.has_value()) [[unlikely]] {
      ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                    storage::View::OLD, nullptr, &context.number_of_hops);
      commit_frequency_ = *EvaluateCommitFrequency(evaluator, self_.commit_frequency_);
    }

    bool const pull_value = input_cursor_->Pull(frame, context);

    pulled_++;
    utils::BasicResult<storage::StorageManipulationError, void> commit_result;
    if (pulled_ >= commit_frequency_) {
      // do periodic commit since we pulled that many times
      commit_result = context.db_accessor->PeriodicCommit(context.commit_args());
      pulled_ = 0;
    } else if (!pull_value && pulled_ > 0) {
      // do periodic commit for the rest of pulled items
      commit_result = context.db_accessor->PeriodicCommit(context.commit_args());
    }

    if (commit_result.HasError()) {
      HandlePeriodicCommitError(commit_result.GetError());
    }

    return pull_value;
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    commit_frequency_.reset();
    pulled_ = 0;
  }

 private:
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
  const PeriodicCommit &self_;
  const UniqueCursorPtr input_cursor_;
  std::optional<uint64_t> commit_frequency_;
  uint64_t pulled_ = 0;
};
}  // namespace

UniqueCursorPtr PeriodicCommit::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::PeriodicCommitOperator);
  return MakeUniqueCursorPtr<PeriodicCommitCursor>(mem, *this, mem);
}

std::unique_ptr<LogicalOperator> PeriodicCommit::Clone(AstStorage *storage) const {
  auto object = std::make_unique<PeriodicCommit>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->commit_frequency_ = commit_frequency_;
  return object;
}

PeriodicSubquery::PeriodicSubquery(const std::shared_ptr<LogicalOperator> input,
                                   const std::shared_ptr<LogicalOperator> subquery, Expression *commit_frequency,
                                   bool subquery_has_return)
    : input_(input ? input : std::make_shared<Once>()),
      subquery_(subquery),
      commit_frequency_(commit_frequency),
      subquery_has_return_(subquery_has_return) {}

bool PeriodicSubquery::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    input_->Accept(visitor) && subquery_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

std::vector<Symbol> PeriodicSubquery::ModifiedSymbols(const SymbolTable &table) const {
  // Modified symbols are combined from both execution branches.
  auto symbols = input_->ModifiedSymbols(table);
  auto subquery_symbols = subquery_->ModifiedSymbols(table);
  symbols.insert(symbols.end(), subquery_symbols.begin(), subquery_symbols.end());
  return symbols;
}

namespace {
class PeriodicSubqueryCursor : public Cursor {
 public:
  PeriodicSubqueryCursor(const PeriodicSubquery &self, utils::MemoryResource *mem)
      : self_(self),
        input_(self.input_->MakeCursor(mem)),
        subquery_(self.subquery_->MakeCursor(mem)),
        subquery_has_return_(self.subquery_has_return_) {
    MG_ASSERT(self_.commit_frequency_ != nullptr, "Commit frequency should be defined at this point!");
  }

  bool Pull(Frame &frame, ExecutionContext &context) override {
    // NOLINTNEXTLINE(misc-const-correctness)
    OOMExceptionEnabler oom_exception;
    // NOLINTNEXTLINE(misc-const-correctness)
    SCOPED_PROFILE_OP("PeriodicSubquery");

    AbortCheck(context);

    if (!commit_frequency_.has_value()) [[unlikely]] {
      ExpressionEvaluator evaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                    storage::View::OLD, nullptr, &context.number_of_hops);
      commit_frequency_ = *EvaluateCommitFrequency(evaluator, self_.commit_frequency_);
    }

    while (true) {
      if (pull_input_) {
        if (input_->Pull(frame, context)) {
          pulled_++;
        } else {
          if (pulled_ > 0) {
            // do periodic commit for the rest of pulled items
            const auto commit_result = context.db_accessor->PeriodicCommit(context.commit_args());
            if (commit_result.HasError()) {
              HandlePeriodicCommitError(commit_result.GetError());
            }
          }
          return false;
        }
      }

      if (subquery_->Pull(frame, context)) {
        // if successful, next Pull from this should not pull_input_
        pull_input_ = false;
        return true;
      }

      if (pulled_ >= commit_frequency_) {
        // do periodic commit since we pulled that many times
        const auto commit_result = context.db_accessor->PeriodicCommit(context.commit_args());
        if (commit_result.HasError()) {
          HandlePeriodicCommitError(commit_result.GetError());
        }
        pulled_ = 0;
      }

      // subquery cursor has been exhausted
      // skip that row
      pull_input_ = true;
      subquery_->Reset();

      // don't skip row if no rows are returned from subquery, return input_ rows
      if (!subquery_has_return_) return true;
    }
  }

  void Shutdown() override {
    input_->Shutdown();
    subquery_->Shutdown();
  }

  void Reset() override {
    input_->Reset();
    subquery_->Reset();
    pull_input_ = true;
    commit_frequency_.reset();
    pulled_ = 0;
  }

 private:
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
  const PeriodicSubquery &self_;
  UniqueCursorPtr input_;
  UniqueCursorPtr subquery_;
  bool subquery_has_return_{true};
  bool pull_input_{true};
  uint64_t pulled_{0};
  std::optional<uint64_t> commit_frequency_;
};
}  // namespace

UniqueCursorPtr PeriodicSubquery::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::PeriodicSubqueryOperator);

  return MakeUniqueCursorPtr<PeriodicSubqueryCursor>(mem, *this, mem);
}

std::unique_ptr<LogicalOperator> PeriodicSubquery::Clone(AstStorage *storage) const {
  auto object = std::make_unique<PeriodicSubquery>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->subquery_ = subquery_ ? subquery_->Clone(storage) : nullptr;
  object->subquery_has_return_ = subquery_has_return_;
  object->commit_frequency_ = commit_frequency_;
  return object;
}

ScanAllByPointDistance::ScanAllByPointDistance(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol,
                                               storage::LabelId label, storage::PropertyId property,
                                               Expression *cmp_value, Expression *boundary_value,
                                               PointDistanceCondition boundary_condition)
    : ScanAll(input, output_symbol, storage::View::OLD),
      label_(label),
      property_(property),
      cmp_value_{cmp_value},
      boundary_value_{boundary_value},
      boundary_condition_{boundary_condition} {}

ACCEPT_WITH_INPUT(ScanAllByPointDistance)

UniqueCursorPtr ScanAllByPointDistance::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ScanAllByPointDistanceOperator);

  auto vertices = [this](Frame &frame, ExecutionContext &context) -> std::optional<PointIterable> {
    auto evaluator = ExpressionEvaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                         view_, nullptr, &context.number_of_hops);
    auto value = cmp_value_->Accept(evaluator);

    auto crs = GetCRS(value);
    if (!crs) return std::nullopt;

    auto boundary_value = boundary_value_->Accept(evaluator);
    return std::make_optional(
        context.db_accessor->PointVertices(label_, property_, *crs, value, boundary_value, boundary_condition_));
  };
  return MakeUniqueCursorPtr<ScanAllCursor<decltype(vertices)>>(mem, *this, output_symbol_, input_->MakeCursor(mem),
                                                                view_, std::move(vertices), "ScanAllByPointDistance");
}

std::string ScanAllByPointDistance::ToString() const {
  auto const &name = output_symbol_.name();
  auto const &label = dba_->LabelToName(label_);
  auto const &property = dba_->PropertyToName(property_);
  return fmt::format("ScanAllByPointDistance ({0} :{1} {{{2}}})", name, label, property);
}

std::unique_ptr<LogicalOperator> ScanAllByPointDistance::Clone(AstStorage *storage) const {
  auto object = std::make_unique<ScanAllByPointDistance>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->output_symbol_ = output_symbol_;
  object->view_ = view_;
  object->label_ = label_;
  object->property_ = property_;
  object->cmp_value_ = cmp_value_ ? cmp_value_->Clone(storage) : nullptr;
  object->boundary_value_ = boundary_value_ ? boundary_value_->Clone(storage) : nullptr;
  object->boundary_condition_ = boundary_condition_;

  return object;
}

ScanAllByPointWithinbbox::ScanAllByPointWithinbbox(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol,
                                                   storage::LabelId label, storage::PropertyId property,
                                                   Expression *bottom_left, Expression *top_right,
                                                   Expression *boundary_value)
    : ScanAll(input, output_symbol, storage::View::OLD),
      label_(label),
      property_(property),
      bottom_left_{bottom_left},
      top_right_{top_right},
      boundary_value_{boundary_value} {}

ACCEPT_WITH_INPUT(ScanAllByPointWithinbbox)

UniqueCursorPtr ScanAllByPointWithinbbox::MakeCursor(utils::MemoryResource *mem) const {
  memgraph::metrics::IncrementCounter(memgraph::metrics::ScanAllByPointWithinbboxOperator);

  auto vertices = [this](Frame &frame, ExecutionContext &context) -> std::optional<PointIterable> {
    auto evaluator = ExpressionEvaluator(&frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                         view_, nullptr, &context.number_of_hops);
    auto bottom_left_value = bottom_left_->Accept(evaluator);
    auto top_right_value = top_right_->Accept(evaluator);

    auto const crs1 = GetCRS(bottom_left_value);
    auto const crs2 = GetCRS(top_right_value);
    if (!crs1 || !crs2 || crs1 != crs2) return std::nullopt;

    auto boundary_value = boundary_value_->Accept(evaluator);

    if (!boundary_value.IsBool()) {
      throw QueryRuntimeException("point.withinbbox returns a boolean and therefore can only be compared with one.");
    }
    auto boundary_condition = boundary_value.ValueBool() ? WithinBBoxCondition::INSIDE : WithinBBoxCondition::OUTSIDE;

    return std::make_optional(context.db_accessor->PointVertices(label_, property_, *crs1, bottom_left_value,
                                                                 top_right_value, boundary_condition));
  };
  return MakeUniqueCursorPtr<ScanAllCursor<decltype(vertices)>>(mem, *this, output_symbol_, input_->MakeCursor(mem),
                                                                view_, std::move(vertices), "ScanAllByPointWithinbbox");
}

std::string ScanAllByPointWithinbbox::ToString() const {
  auto const &name = output_symbol_.name();
  auto const &label = dba_->LabelToName(label_);
  auto const &property = dba_->PropertyToName(property_);
  return fmt::format("ScanAllByPointWithinbbox ({0} :{1} {{{2}}})", name, label, property);
}

std::unique_ptr<LogicalOperator> ScanAllByPointWithinbbox::Clone(AstStorage *storage) const {
  auto object = std::make_unique<ScanAllByPointWithinbbox>();
  object->input_ = input_ ? input_->Clone(storage) : nullptr;
  object->output_symbol_ = output_symbol_;
  object->view_ = view_;
  object->label_ = label_;
  object->property_ = property_;
  object->bottom_left_ = bottom_left_ ? bottom_left_->Clone(storage) : nullptr;
  object->top_right_ = top_right_ ? top_right_->Clone(storage) : nullptr;
  object->boundary_value_ = boundary_value_ ? boundary_value_->Clone(storage) : nullptr;
  return object;
}

query::plan::NodeCreationInfo query::plan::NodeCreationInfo::Clone(query::AstStorage *storage) const {
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

std::string query::plan::LogicalOperator::ToString() const { return GetTypeInfo().name; }

query::plan::EdgeCreationInfo query::plan::EdgeCreationInfo::Clone(query::AstStorage *storage) const {
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

query::plan::ExpansionLambda query::plan::ExpansionLambda::Clone(query::AstStorage *storage) const {
  ExpansionLambda object;
  object.inner_edge_symbol = inner_edge_symbol;
  object.inner_node_symbol = inner_node_symbol;
  object.expression = expression ? expression->Clone(storage) : nullptr;
  object.accumulated_path_symbol = accumulated_path_symbol;
  object.accumulated_weight_symbol = accumulated_weight_symbol;
  return object;
}

query::plan::Aggregate::Element query::plan::Aggregate::Element::Clone(query::AstStorage *storage) const {
  Element object;
  object.arg1 = arg1 ? arg1->Clone(storage) : nullptr;
  object.arg2 = arg2 ? arg2->Clone(storage) : nullptr;
  object.op = op;
  object.output_sym = output_sym;
  object.distinct = distinct;
  return object;
}

}  // namespace memgraph::query::plan
