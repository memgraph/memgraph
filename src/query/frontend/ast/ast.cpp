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

#include "query/frontend/ast/ast.hpp"
#include "frontend/ast/ast_storage.hpp"
#include "query/frontend/ast/query/aggregation.hpp"
#include "query/frontend/ast/query/auth_query.hpp"
#include "query/frontend/ast/query/exists.hpp"
#include "query/frontend/ast/query/pattern_comprehension.hpp"
#include "storage/v2/id_types.hpp"
#include "utils/typeinfo.hpp"

#include "range/v3/all.hpp"
namespace r = ranges;
namespace rv = r::views;

namespace memgraph {

constexpr utils::TypeInfo query::LabelIx::kType{utils::TypeId::AST_LABELIX, "LabelIx", nullptr};

constexpr utils::TypeInfo query::PropertyIx::kType{utils::TypeId::AST_PROPERTYIX, "PropertyIx", nullptr};

constexpr utils::TypeInfo query::EdgeTypeIx::kType{utils::TypeId::AST_EDGETYPEIX, "EdgeTypeIx", nullptr};

constexpr utils::TypeInfo query::Tree::kType{utils::TypeId::AST_TREE, "Tree", nullptr};

constexpr utils::TypeInfo query::Expression::kType{utils::TypeId::AST_EXPRESSION, "Expression", &query::Tree::kType};

constexpr utils::TypeInfo query::Where::kType{utils::TypeId::AST_WHERE, "Where", &query::Tree::kType};

constexpr utils::TypeInfo query::BinaryOperator::kType{utils::TypeId::AST_BINARY_OPERATOR, "BinaryOperator",
                                                       &query::Expression::kType};

constexpr utils::TypeInfo query::UnaryOperator::kType{utils::TypeId::AST_UNARY_OPERATOR, "UnaryOperator",
                                                      &query::Expression::kType};

constexpr utils::TypeInfo query::OrOperator::kType{utils::TypeId::AST_OR_OPERATOR, "OrOperator",
                                                   &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::XorOperator::kType{utils::TypeId::AST_XOR_OPERATOR, "XorOperator",
                                                    &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::AndOperator::kType{utils::TypeId::AST_AND_OPERATOR, "AndOperator",
                                                    &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::AdditionOperator::kType{utils::TypeId::AST_ADDITION_OPERATOR, "AdditionOperator",
                                                         &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::SubtractionOperator::kType{utils::TypeId::AST_SUBTRACTION_OPERATOR,
                                                            "SubtractionOperator", &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::MultiplicationOperator::kType{utils::TypeId::AST_MULTIPLICATION_OPERATOR,
                                                               "MultiplicationOperator", &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::DivisionOperator::kType{utils::TypeId::AST_DIVISION_OPERATOR, "DivisionOperator",
                                                         &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::ModOperator::kType{utils::TypeId::AST_MOD_OPERATOR, "ModOperator",
                                                    &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::ExponentiationOperator::kType{utils::TypeId::AST_EXPONENTIATION_OPERATOR,
                                                               "ExponentiationOperator", &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::NotEqualOperator::kType{utils::TypeId::AST_NOT_EQUAL_OPERATOR, "NotEqualOperator",
                                                         &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::EqualOperator::kType{utils::TypeId::AST_EQUAL_OPERATOR, "EqualOperator",
                                                      &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::LessOperator::kType{utils::TypeId::AST_LESS_OPERATOR, "LessOperator",
                                                     &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::GreaterOperator::kType{utils::TypeId::AST_GREATER_OPERATOR, "GreaterOperator",
                                                        &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::LessEqualOperator::kType{utils::TypeId::AST_LESS_EQUAL_OPERATOR, "LessEqualOperator",
                                                          &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::GreaterEqualOperator::kType{utils::TypeId::AST_GREATER_EQUAL_OPERATOR,
                                                             "GreaterEqualOperator", &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::RangeOperator::kType{utils::TypeId::AST_RANGE_OPERATOR, "RangeOperator",
                                                      &query::Expression::kType};

constexpr utils::TypeInfo query::InListOperator::kType{utils::TypeId::AST_IN_LIST_OPERATOR, "InListOperator",
                                                       &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::SubscriptOperator::kType{utils::TypeId::AST_SUBSCRIPT_OPERATOR, "SubscriptOperator",
                                                          &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::NotOperator::kType{utils::TypeId::AST_NOT_OPERATOR, "NotOperator",
                                                    &query::UnaryOperator::kType};

constexpr utils::TypeInfo query::UnaryPlusOperator::kType{utils::TypeId::AST_UNARY_PLUS_OPERATOR, "UnaryPlusOperator",
                                                          &query::UnaryOperator::kType};

constexpr utils::TypeInfo query::UnaryMinusOperator::kType{utils::TypeId::AST_UNARY_MINUS_OPERATOR,
                                                           "UnaryMinusOperator", &query::UnaryOperator::kType};

constexpr utils::TypeInfo query::IsNullOperator::kType{utils::TypeId::AST_IS_NULL_OPERATOR, "IsNullOperator",
                                                       &query::UnaryOperator::kType};

constexpr utils::TypeInfo query::Aggregation::kType{utils::TypeId::AST_AGGREGATION, "Aggregation",
                                                    &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::ListSlicingOperator::kType{utils::TypeId::AST_LIST_SLICING_OPERATOR,
                                                            "ListSlicingOperator", &query::Expression::kType};

constexpr utils::TypeInfo query::IfOperator::kType{utils::TypeId::AST_IF_OPERATOR, "IfOperator",
                                                   &query::Expression::kType};

constexpr utils::TypeInfo query::BaseLiteral::kType{utils::TypeId::AST_BASE_LITERAL, "BaseLiteral",
                                                    &query::Expression::kType};

constexpr utils::TypeInfo query::PrimitiveLiteral::kType{utils::TypeId::AST_PRIMITIVE_LITERAL, "PrimitiveLiteral",
                                                         &query::BaseLiteral::kType};

constexpr utils::TypeInfo query::ListLiteral::kType{utils::TypeId::AST_LIST_LITERAL, "ListLiteral",
                                                    &query::BaseLiteral::kType};

constexpr utils::TypeInfo query::MapLiteral::kType{utils::TypeId::AST_MAP_LITERAL, "MapLiteral",
                                                   &query::BaseLiteral::kType};

constexpr utils::TypeInfo query::MapProjectionLiteral::kType{utils::TypeId::AST_MAP_PROJECTION_LITERAL,
                                                             "MapProjectionLiteral", &query::BaseLiteral::kType};

constexpr utils::TypeInfo query::Identifier::kType{utils::TypeId::AST_IDENTIFIER, "Identifier",
                                                   &query::Expression::kType};

constexpr utils::TypeInfo query::PropertyLookup::kType{utils::TypeId::AST_PROPERTY_LOOKUP, "PropertyLookup",
                                                       &query::Expression::kType};

constexpr utils::TypeInfo query::AllPropertiesLookup::kType{utils::TypeId::AST_ALL_PROPERTIES_LOOKUP,
                                                            "AllPropertiesLookup", &query::Expression::kType};

constexpr utils::TypeInfo query::LabelsTest::kType{utils::TypeId::AST_LABELS_TEST, "LabelsTest",
                                                   &query::Expression::kType};

constexpr utils::TypeInfo query::Function::kType{utils::TypeId::AST_FUNCTION, "Function", &query::Expression::kType};

constexpr utils::TypeInfo query::Reduce::kType{utils::TypeId::AST_REDUCE, "Reduce", &query::Expression::kType};

constexpr utils::TypeInfo query::Coalesce::kType{utils::TypeId::AST_COALESCE, "Coalesce", &query::Expression::kType};

constexpr utils::TypeInfo query::Extract::kType{utils::TypeId::AST_EXTRACT, "Extract", &query::Expression::kType};

constexpr utils::TypeInfo query::All::kType{utils::TypeId::AST_ALL, "All", &query::Expression::kType};

constexpr utils::TypeInfo query::Single::kType{utils::TypeId::AST_SINGLE, "Single", &query::Expression::kType};

constexpr utils::TypeInfo query::Any::kType{utils::TypeId::AST_ANY, "Any", &query::Expression::kType};

constexpr utils::TypeInfo query::None::kType{utils::TypeId::AST_NONE, "None", &query::Expression::kType};

constexpr utils::TypeInfo query::ListComprehension::kType{utils::TypeId::AST_LIST_COMPREHENSION, "ListComprehension",
                                                          &query::Expression::kType};

constexpr utils::TypeInfo query::ParameterLookup::kType{utils::TypeId::AST_PARAMETER_LOOKUP, "ParameterLookup",
                                                        &query::Expression::kType};

constexpr utils::TypeInfo query::RegexMatch::kType{utils::TypeId::AST_REGEX_MATCH, "RegexMatch",
                                                   &query::Expression::kType};

constexpr utils::TypeInfo query::NamedExpression::kType{utils::TypeId::AST_NAMED_EXPRESSION, "NamedExpression",
                                                        &query::Tree::kType};

constexpr utils::TypeInfo query::PatternAtom::kType{utils::TypeId::AST_PATTERN_ATOM, "PatternAtom",
                                                    &query::Tree::kType};

constexpr utils::TypeInfo query::NodeAtom::kType{utils::TypeId::AST_NODE_ATOM, "NodeAtom", &query::PatternAtom::kType};

constexpr utils::TypeInfo query::EdgeAtom::Lambda::kType{utils::TypeId::AST_EDGE_ATOM_LAMBDA, "Lambda", nullptr};

constexpr utils::TypeInfo query::EdgeAtom::kType{utils::TypeId::AST_EDGE_ATOM, "EdgeAtom", &query::PatternAtom::kType};

constexpr utils::TypeInfo query::Pattern::kType{utils::TypeId::AST_PATTERN, "Pattern", &query::Tree::kType};

constexpr utils::TypeInfo query::Clause::kType{utils::TypeId::AST_CLAUSE, "Clause", &query::Tree::kType};

constexpr utils::TypeInfo query::SingleQuery::kType{utils::TypeId::AST_SINGLE_QUERY, "SingleQuery",
                                                    &query::Tree::kType};

constexpr utils::TypeInfo query::CypherUnion::kType{utils::TypeId::AST_CYPHER_UNION, "CypherUnion",
                                                    &query::Tree::kType};

constexpr utils::TypeInfo query::Query::kType{utils::TypeId::AST_QUERY, "Query", &query::Tree::kType};

constexpr utils::TypeInfo query::IndexHint::kType{utils::TypeId::AST_INDEX_HINT, "IndexHint", &query::Tree::kType};

query::IndexHint query::IndexHint::Clone(query::AstStorage *storage) const {
  IndexHint object;
  object.index_type_ = index_type_;
  object.label_ix_ = storage->GetLabelIx(label_ix_.name);
  auto clone_path = [&](PropertyIxPath const &path) { return path.Clone(storage); };
  object.property_ixs_ = property_ixs_ | rv::transform(clone_path) | r::to_vector;
  return object;
}

constexpr utils::TypeInfo query::PreQueryDirectives::kType{utils::TypeId::AST_PRE_QUERY_DIRECTIVES,
                                                           "PreQueryDirectives", &query::Tree::kType};

constexpr utils::TypeInfo query::CypherQuery::kType{utils::TypeId::AST_CYPHER_QUERY, "CypherQuery",
                                                    &query::Query::kType};

constexpr utils::TypeInfo query::ExplainQuery::kType{utils::TypeId::AST_EXPLAIN_QUERY, "ExplainQuery",
                                                     &query::Query::kType};

constexpr utils::TypeInfo query::ProfileQuery::kType{utils::TypeId::AST_PROFILE_QUERY, "ProfileQuery",
                                                     &query::Query::kType};

constexpr utils::TypeInfo query::IndexQuery::kType{utils::TypeId::AST_INDEX_QUERY, "IndexQuery", &query::Query::kType};

constexpr utils::TypeInfo query::EdgeIndexQuery::kType{utils::TypeId::AST_EDGE_INDEX_QUERY, "EdgeIndexQuery",
                                                       &query::Query::kType};

constexpr utils::TypeInfo query::PointIndexQuery::kType{utils::TypeId::AST_POINT_INDEX_QUERY, "PointIndexQuery",
                                                        &query::Query::kType};

constexpr utils::TypeInfo query::TextIndexQuery::kType{utils::TypeId::AST_TEXT_INDEX_QUERY, "TextIndexQuery",
                                                       &query::Query::kType};

constexpr utils::TypeInfo query::VectorIndexQuery::kType{utils::TypeId::AST_VECTOR_INDEX_QUERY, "VectorIndexQuery",
                                                         &query::Query::kType};

constexpr utils::TypeInfo query::CreateVectorEdgeIndexQuery::kType{utils::TypeId::AST_VECTOR_EDGE_INDEX_QUERY,
                                                                   "CreateVectorEdgeIndexQuery", &query::Query::kType};

constexpr utils::TypeInfo query::Create::kType{utils::TypeId::AST_CREATE, "Create", &query::Clause::kType};

constexpr utils::TypeInfo query::CallProcedure::kType{utils::TypeId::AST_CALL_PROCEDURE, "CallProcedure",
                                                      &query::Clause::kType};

constexpr utils::TypeInfo query::Match::kType{utils::TypeId::AST_MATCH, "Match", &query::Clause::kType};

constexpr utils::TypeInfo query::SortItem::kType{utils::TypeId::AST_SORT_ITEM, "SortItem", nullptr};

constexpr utils::TypeInfo query::ReturnBody::kType{utils::TypeId::AST_RETURN_BODY, "ReturnBody", nullptr};

constexpr utils::TypeInfo query::Return::kType{utils::TypeId::AST_RETURN, "Return", &query::Clause::kType};

constexpr utils::TypeInfo query::With::kType{utils::TypeId::AST_WITH, "With", &query::Clause::kType};

constexpr utils::TypeInfo query::Delete::kType{utils::TypeId::AST_DELETE, "Delete", &query::Clause::kType};

constexpr utils::TypeInfo query::SetProperty::kType{utils::TypeId::AST_SET_PROPERTY, "SetProperty",
                                                    &query::Clause::kType};

constexpr utils::TypeInfo query::SetProperties::kType{utils::TypeId::AST_SET_PROPERTIES, "SetProperties",
                                                      &query::Clause::kType};

constexpr utils::TypeInfo query::SetLabels::kType{utils::TypeId::AST_SET_LABELS, "SetLabels", &query::Clause::kType};

constexpr utils::TypeInfo query::RemoveProperty::kType{utils::TypeId::AST_REMOVE_PROPERTY, "RemoveProperty",
                                                       &query::Clause::kType};

constexpr utils::TypeInfo query::RemoveLabels::kType{utils::TypeId::AST_REMOVE_LABELS, "RemoveLabels",
                                                     &query::Clause::kType};

constexpr utils::TypeInfo query::Merge::kType{utils::TypeId::AST_MERGE, "Merge", &query::Clause::kType};

constexpr utils::TypeInfo query::Unwind::kType{utils::TypeId::AST_UNWIND, "Unwind", &query::Clause::kType};

constexpr utils::TypeInfo query::AuthQuery::kType{utils::TypeId::AST_AUTH_QUERY, "AuthQuery", &query::Query::kType};

constexpr utils::TypeInfo query::DatabaseInfoQuery::kType{utils::TypeId::AST_DATABASE_INFO_QUERY, "DatabaseInfoQuery",
                                                          &query::Query::kType};

constexpr utils::TypeInfo query::SystemInfoQuery::kType{utils::TypeId::AST_SYSTEM_INFO_QUERY, "SystemInfoQuery",
                                                        &query::Query::kType};

constexpr utils::TypeInfo query::Constraint::kType{utils::TypeId::AST_CONSTRAINT, "Constraint", nullptr};

constexpr utils::TypeInfo query::ConstraintQuery::kType{utils::TypeId::AST_CONSTRAINT_QUERY, "ConstraintQuery",
                                                        &query::Query::kType};

constexpr utils::TypeInfo query::DumpQuery::kType{utils::TypeId::AST_DUMP_QUERY, "DumpQuery", &query::Query::kType};

constexpr utils::TypeInfo query::ReplicationQuery::kType{utils::TypeId::AST_REPLICATION_QUERY, "ReplicationQuery",
                                                         &query::Query::kType};

constexpr utils::TypeInfo query::ReplicationInfoQuery::kType{utils::TypeId::AST_REPLICATION_INFO_QUERY,
                                                             "ReplicationInfoQuery", &query::Query::kType};

constexpr utils::TypeInfo query::CoordinatorQuery::kType{utils::TypeId::AST_COORDINATOR_QUERY, "CoordinatorQuery",
                                                         &query::Query::kType};

constexpr utils::TypeInfo query::DropGraphQuery::kType{utils::TypeId::AST_DROP_GRAPH_QUERY, "DropGraphQuery",
                                                       &query::Query::kType};

constexpr utils::TypeInfo query::LockPathQuery::kType{utils::TypeId::AST_LOCK_PATH_QUERY, "LockPathQuery",
                                                      &query::Query::kType};

constexpr utils::TypeInfo query::LoadCsv::kType{utils::TypeId::AST_LOAD_CSV, "LoadCsv", &query::Clause::kType};

constexpr utils::TypeInfo query::FreeMemoryQuery::kType{utils::TypeId::AST_FREE_MEMORY_QUERY, "FreeMemoryQuery",
                                                        &query::Query::kType};

constexpr utils::TypeInfo query::TriggerQuery::kType{utils::TypeId::AST_TRIGGER_QUERY, "TriggerQuery",
                                                     &query::Query::kType};

constexpr utils::TypeInfo query::IsolationLevelQuery::kType{utils::TypeId::AST_ISOLATION_LEVEL_QUERY,
                                                            "IsolationLevelQuery", &query::Query::kType};

constexpr utils::TypeInfo query::StorageModeQuery::kType{utils::TypeId::AST_STORAGE_MODE_QUERY, "StorageModeQuery",
                                                         &query::Query::kType};

constexpr utils::TypeInfo query::CreateSnapshotQuery::kType{utils::TypeId::AST_CREATE_SNAPSHOT_QUERY,
                                                            "CreateSnapshotQuery", &query::Query::kType};

constexpr utils::TypeInfo query::RecoverSnapshotQuery::kType{utils::TypeId::AST_RECOVER_SNAPSHOT_QUERY,
                                                             "RecoverSnapshotQuery", &query::Query::kType};

constexpr utils::TypeInfo query::ShowSnapshotsQuery::kType{utils::TypeId::AST_SHOW_SNAPSHOTS_QUERY,
                                                           "ShowSnapshotsQuery", &query::Query::kType};

constexpr utils::TypeInfo query::ShowNextSnapshotQuery::kType{utils::TypeId::AST_SHOW_NEXT_SNAPSHOT_QUERY,
                                                              "ShowNextSnapshotQuery", &query::Query::kType};

constexpr utils::TypeInfo query::StreamQuery::kType{utils::TypeId::AST_STREAM_QUERY, "StreamQuery",
                                                    &query::Query::kType};

constexpr utils::TypeInfo query::SettingQuery::kType{utils::TypeId::AST_SETTING_QUERY, "SettingQuery",
                                                     &query::Query::kType};

constexpr utils::TypeInfo query::VersionQuery::kType{utils::TypeId::AST_VERSION_QUERY, "VersionQuery",
                                                     &query::Query::kType};

constexpr utils::TypeInfo query::Foreach::kType{utils::TypeId::AST_FOREACH, "Foreach", &query::Clause::kType};

constexpr utils::TypeInfo query::ShowConfigQuery::kType{utils::TypeId::AST_SHOW_CONFIG_QUERY, "ShowConfigQuery",
                                                        &query::Query::kType};

constexpr utils::TypeInfo query::AnalyzeGraphQuery::kType{utils::TypeId::AST_ANALYZE_GRAPH_QUERY, "AnalyzeGraphQuery",
                                                          &query::Query::kType};

constexpr utils::TypeInfo query::TransactionQueueQuery::kType{utils::TypeId::AST_TRANSACTION_QUEUE_QUERY,
                                                              "TransactionQueueQuery", &query::Query::kType};

constexpr utils::TypeInfo query::Exists::kType{utils::TypeId::AST_EXISTS, "Exists", &query::Expression::kType};

constexpr utils::TypeInfo query::CallSubquery::kType{utils::TypeId::AST_CALL_SUBQUERY, "CallSubquery",
                                                     &query::Clause::kType};

constexpr utils::TypeInfo query::MultiDatabaseQuery::kType{utils::TypeId::AST_MULTI_DATABASE_QUERY,
                                                           "MultiDatabaseQuery", &query::Query::kType};

constexpr utils::TypeInfo query::UseDatabaseQuery::kType{utils::TypeId::AST_USE_DATABASE, "UseDatabaseQuery",
                                                         &query::Query::kType};

constexpr utils::TypeInfo query::ShowDatabaseQuery::kType{utils::TypeId::AST_SHOW_DATABASE, "ShowDatabaseQuery",
                                                          &query::Query::kType};

constexpr utils::TypeInfo query::ShowDatabasesQuery::kType{utils::TypeId::AST_SHOW_DATABASES, "ShowDatabasesQuery",
                                                           &query::Query::kType};

constexpr utils::TypeInfo query::EdgeImportModeQuery::kType{utils::TypeId::AST_EDGE_IMPORT_MODE_QUERY,
                                                            "EdgeImportModeQuery", &query::Query::kType};

constexpr utils::TypeInfo query::PatternComprehension::kType{utils::TypeId::AST_PATTERN_COMPREHENSION,
                                                             "PatternComprehension", &query::Expression::kType};

constexpr utils::TypeInfo query::CreateEnumQuery::kType{utils::TypeId::AST_CREATE_ENUM_QUERY, "CreateEnumQuery",
                                                        &query::Query::kType};

constexpr utils::TypeInfo query::ShowEnumsQuery::kType{utils::TypeId::AST_SHOW_ENUMS_QUERY, "ShowEnumsQuery",
                                                       &query::Query::kType};

constexpr utils::TypeInfo query::EnumValueAccess::kType{utils::TypeId::AST_ENUM_VALUE_ACCESS, "EnumValueAccess",
                                                        &query::Expression::kType};

constexpr utils::TypeInfo query::AlterEnumAddValueQuery::kType{utils::TypeId::AST_ALTER_ENUM_ADD_VALUE_QUERY,
                                                               "AlterEnumAddValueQuery", &query::Query::kType};

constexpr utils::TypeInfo query::AlterEnumUpdateValueQuery::kType{utils::TypeId::AST_ALTER_ENUM_UPDATE_VALUE_QUERY,
                                                                  "AlterEnumUpdateValueQuery", &query::Query::kType};

constexpr utils::TypeInfo query::AlterEnumRemoveValueQuery::kType{utils::TypeId::AST_ALTER_ENUM_REMOVE_VALUE_QUERY,
                                                                  "AlterEnumRemoveValueQuery", &query::Query::kType};

constexpr utils::TypeInfo query::DropEnumQuery::kType{utils::TypeId::AST_DROP_ENUM_QUERY, "DropEnumQuery",
                                                      &query::Query::kType};

constexpr utils::TypeInfo query::ShowSchemaInfoQuery::kType{utils::TypeId::AST_SHOW_SCHEMA_INFO_QUERY,
                                                            "ShowSchemaInfoQuery", &query::Query::kType};

constexpr utils::TypeInfo query::TtlQuery::kType{utils::TypeId::AST_TTL_QUERY, "TtlQuery", &query::Query::kType};

constexpr utils::TypeInfo query::SessionTraceQuery::kType{utils::TypeId::AST_SESSION_TRACE_QUERY, "SessionTraceQuery",
                                                          &query::Query::kType};

namespace query {
DEFINE_VISITABLE(Identifier, ExpressionVisitor<TypedValue>);
DEFINE_VISITABLE(Identifier, ExpressionVisitor<TypedValue *>);
DEFINE_VISITABLE(Identifier, ExpressionVisitor<void>);
DEFINE_VISITABLE(Identifier, HierarchicalTreeVisitor);

DEFINE_VISITABLE(NamedExpression, ExpressionVisitor<TypedValue>);
DEFINE_VISITABLE(NamedExpression, ExpressionVisitor<TypedValue *>);
DEFINE_VISITABLE(NamedExpression, ExpressionVisitor<void>);
bool NamedExpression::Accept(HierarchicalTreeVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    expression_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

DEFINE_VISITABLE(Exists, ExpressionVisitor<TypedValue>);
DEFINE_VISITABLE(Exists, ExpressionVisitor<TypedValue *>);
DEFINE_VISITABLE(Exists, ExpressionVisitor<void>);
bool Exists::Accept(HierarchicalTreeVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    if (HasPattern()) {
      GetPattern()->Accept(visitor);
    } else if (HasSubquery()) {
      GetSubquery()->Accept(visitor);
    }
  }
  return visitor.PostVisit(*this);
}

DEFINE_VISITABLE(PatternComprehension, ExpressionVisitor<TypedValue>);
DEFINE_VISITABLE(PatternComprehension, ExpressionVisitor<TypedValue *>);
DEFINE_VISITABLE(PatternComprehension, ExpressionVisitor<void>);
bool PatternComprehension::Accept(HierarchicalTreeVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    if (variable_) {
      variable_->Accept(visitor);
    }
    pattern_->Accept(visitor);
    if (filter_) {
      filter_->Accept(visitor);
    }
    resultExpr_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

DEFINE_VISITABLE(Aggregation, ExpressionVisitor<TypedValue>);
DEFINE_VISITABLE(Aggregation, ExpressionVisitor<TypedValue *>);
DEFINE_VISITABLE(Aggregation, ExpressionVisitor<void>);

bool Aggregation::Accept(HierarchicalTreeVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    if (expression1_) expression1_->Accept(visitor);
    if (expression2_) expression2_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

Aggregation::Aggregation(Expression *expression1, Expression *expression2, Aggregation::Op op, bool distinct)
    : BinaryOperator(expression1, expression2), op_(op), distinct_(distinct) {
  // COUNT without expression denotes COUNT(*) in cypher.
  DMG_ASSERT(expression1 || op == Aggregation::Op::COUNT, "All aggregations, except COUNT require expression1");
  DMG_ASSERT((expression2 == nullptr) ^ (op == Aggregation::Op::PROJECT_LISTS || op == Aggregation::Op::COLLECT_MAP),
             "expression2 is obligatory in COLLECT_MAP and PROJECT_LISTS, and invalid otherwise");
}

auto PropertyIxPath::Clone(AstStorage *storage) const -> PropertyIxPath {
  auto paths_copy = std::vector<memgraph::query::PropertyIx>{};
  paths_copy.reserve(path.size());
  for (auto const &prop_ix : path) {
    paths_copy.emplace_back(storage->GetPropertyIx(prop_ix.name));
  }
  return PropertyIxPath{std::move(paths_copy)};
}
}  // namespace query

}  // namespace memgraph
