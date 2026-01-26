// Copyright 2026 Memgraph Ltd.
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
#include "query/frontend/ast/query/user_profile.hpp"
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

constexpr utils::TypeInfo query::BinaryOperator::kType{
    .id = utils::TypeId::AST_BINARY_OPERATOR, .name = "BinaryOperator", .superclass = &query::Expression::kType};

constexpr utils::TypeInfo query::UnaryOperator::kType{
    .id = utils::TypeId::AST_UNARY_OPERATOR, .name = "UnaryOperator", .superclass = &query::Expression::kType};

constexpr utils::TypeInfo query::OrOperator::kType{
    .id = utils::TypeId::AST_OR_OPERATOR, .name = "OrOperator", .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::XorOperator::kType{
    .id = utils::TypeId::AST_XOR_OPERATOR, .name = "XorOperator", .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::AndOperator::kType{
    .id = utils::TypeId::AST_AND_OPERATOR, .name = "AndOperator", .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::AdditionOperator::kType{.id = utils::TypeId::AST_ADDITION_OPERATOR,
                                                         .name = "AdditionOperator",
                                                         .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::SubtractionOperator::kType{.id = utils::TypeId::AST_SUBTRACTION_OPERATOR,
                                                            .name = "SubtractionOperator",
                                                            .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::MultiplicationOperator::kType{.id = utils::TypeId::AST_MULTIPLICATION_OPERATOR,
                                                               .name = "MultiplicationOperator",
                                                               .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::DivisionOperator::kType{.id = utils::TypeId::AST_DIVISION_OPERATOR,
                                                         .name = "DivisionOperator",
                                                         .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::ModOperator::kType{
    .id = utils::TypeId::AST_MOD_OPERATOR, .name = "ModOperator", .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::ExponentiationOperator::kType{.id = utils::TypeId::AST_EXPONENTIATION_OPERATOR,
                                                               .name = "ExponentiationOperator",
                                                               .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::NotEqualOperator::kType{.id = utils::TypeId::AST_NOT_EQUAL_OPERATOR,
                                                         .name = "NotEqualOperator",
                                                         .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::EqualOperator::kType{
    .id = utils::TypeId::AST_EQUAL_OPERATOR, .name = "EqualOperator", .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::LessOperator::kType{
    .id = utils::TypeId::AST_LESS_OPERATOR, .name = "LessOperator", .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::GreaterOperator::kType{
    .id = utils::TypeId::AST_GREATER_OPERATOR, .name = "GreaterOperator", .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::LessEqualOperator::kType{.id = utils::TypeId::AST_LESS_EQUAL_OPERATOR,
                                                          .name = "LessEqualOperator",
                                                          .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::GreaterEqualOperator::kType{.id = utils::TypeId::AST_GREATER_EQUAL_OPERATOR,
                                                             .name = "GreaterEqualOperator",
                                                             .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::RangeOperator::kType{
    .id = utils::TypeId::AST_RANGE_OPERATOR, .name = "RangeOperator", .superclass = &query::Expression::kType};

constexpr utils::TypeInfo query::InListOperator::kType{
    .id = utils::TypeId::AST_IN_LIST_OPERATOR, .name = "InListOperator", .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::SubscriptOperator::kType{.id = utils::TypeId::AST_SUBSCRIPT_OPERATOR,
                                                          .name = "SubscriptOperator",
                                                          .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::NotOperator::kType{
    .id = utils::TypeId::AST_NOT_OPERATOR, .name = "NotOperator", .superclass = &query::UnaryOperator::kType};

constexpr utils::TypeInfo query::UnaryPlusOperator::kType{.id = utils::TypeId::AST_UNARY_PLUS_OPERATOR,
                                                          .name = "UnaryPlusOperator",
                                                          .superclass = &query::UnaryOperator::kType};

constexpr utils::TypeInfo query::UnaryMinusOperator::kType{.id = utils::TypeId::AST_UNARY_MINUS_OPERATOR,
                                                           .name = "UnaryMinusOperator",
                                                           .superclass = &query::UnaryOperator::kType};

constexpr utils::TypeInfo query::IsNullOperator::kType{
    .id = utils::TypeId::AST_IS_NULL_OPERATOR, .name = "IsNullOperator", .superclass = &query::UnaryOperator::kType};

constexpr utils::TypeInfo query::Aggregation::kType{
    .id = utils::TypeId::AST_AGGREGATION, .name = "Aggregation", .superclass = &query::BinaryOperator::kType};

constexpr utils::TypeInfo query::ListSlicingOperator::kType{.id = utils::TypeId::AST_LIST_SLICING_OPERATOR,
                                                            .name = "ListSlicingOperator",
                                                            .superclass = &query::Expression::kType};

constexpr utils::TypeInfo query::IfOperator::kType{
    .id = utils::TypeId::AST_IF_OPERATOR, .name = "IfOperator", .superclass = &query::Expression::kType};

constexpr utils::TypeInfo query::BaseLiteral::kType{
    .id = utils::TypeId::AST_BASE_LITERAL, .name = "BaseLiteral", .superclass = &query::Expression::kType};

constexpr utils::TypeInfo query::PrimitiveLiteral::kType{
    .id = utils::TypeId::AST_PRIMITIVE_LITERAL, .name = "PrimitiveLiteral", .superclass = &query::BaseLiteral::kType};

constexpr utils::TypeInfo query::ListLiteral::kType{
    .id = utils::TypeId::AST_LIST_LITERAL, .name = "ListLiteral", .superclass = &query::BaseLiteral::kType};

constexpr utils::TypeInfo query::MapLiteral::kType{
    .id = utils::TypeId::AST_MAP_LITERAL, .name = "MapLiteral", .superclass = &query::BaseLiteral::kType};

constexpr utils::TypeInfo query::MapProjectionLiteral::kType{.id = utils::TypeId::AST_MAP_PROJECTION_LITERAL,
                                                             .name = "MapProjectionLiteral",
                                                             .superclass = &query::BaseLiteral::kType};

constexpr utils::TypeInfo query::Identifier::kType{
    .id = utils::TypeId::AST_IDENTIFIER, .name = "Identifier", .superclass = &query::Expression::kType};

constexpr utils::TypeInfo query::PropertyLookup::kType{
    .id = utils::TypeId::AST_PROPERTY_LOOKUP, .name = "PropertyLookup", .superclass = &query::Expression::kType};

constexpr utils::TypeInfo query::AllPropertiesLookup::kType{.id = utils::TypeId::AST_ALL_PROPERTIES_LOOKUP,
                                                            .name = "AllPropertiesLookup",
                                                            .superclass = &query::Expression::kType};

constexpr utils::TypeInfo query::LabelsTest::kType{
    .id = utils::TypeId::AST_LABELS_TEST, .name = "LabelsTest", .superclass = &query::Expression::kType};

constexpr utils::TypeInfo query::EdgeTypesTest::kType{
    .id = utils::TypeId::AST_EDGETYPES_TEST, .name = "EdgeTypesTest", .superclass = &Expression::kType};

constexpr utils::TypeInfo query::Function::kType{utils::TypeId::AST_FUNCTION, "Function", &query::Expression::kType};

constexpr utils::TypeInfo query::Reduce::kType{utils::TypeId::AST_REDUCE, "Reduce", &query::Expression::kType};

constexpr utils::TypeInfo query::Coalesce::kType{utils::TypeId::AST_COALESCE, "Coalesce", &query::Expression::kType};

constexpr utils::TypeInfo query::Extract::kType{utils::TypeId::AST_EXTRACT, "Extract", &query::Expression::kType};

constexpr utils::TypeInfo query::All::kType{utils::TypeId::AST_ALL, "All", &query::Expression::kType};

constexpr utils::TypeInfo query::Single::kType{utils::TypeId::AST_SINGLE, "Single", &query::Expression::kType};

constexpr utils::TypeInfo query::Any::kType{utils::TypeId::AST_ANY, "Any", &query::Expression::kType};

constexpr utils::TypeInfo query::None::kType{utils::TypeId::AST_NONE, "None", &query::Expression::kType};

constexpr utils::TypeInfo query::ListComprehension::kType{
    .id = utils::TypeId::AST_LIST_COMPREHENSION, .name = "ListComprehension", .superclass = &query::Expression::kType};

constexpr utils::TypeInfo query::ParameterLookup::kType{
    .id = utils::TypeId::AST_PARAMETER_LOOKUP, .name = "ParameterLookup", .superclass = &query::Expression::kType};

constexpr utils::TypeInfo query::RegexMatch::kType{
    .id = utils::TypeId::AST_REGEX_MATCH, .name = "RegexMatch", .superclass = &query::Expression::kType};

constexpr utils::TypeInfo query::NamedExpression::kType{
    .id = utils::TypeId::AST_NAMED_EXPRESSION, .name = "NamedExpression", .superclass = &query::Tree::kType};

constexpr utils::TypeInfo query::PatternAtom::kType{
    .id = utils::TypeId::AST_PATTERN_ATOM, .name = "PatternAtom", .superclass = &query::Tree::kType};

constexpr utils::TypeInfo query::NodeAtom::kType{utils::TypeId::AST_NODE_ATOM, "NodeAtom", &query::PatternAtom::kType};

constexpr utils::TypeInfo query::EdgeAtom::Lambda::kType{utils::TypeId::AST_EDGE_ATOM_LAMBDA, "Lambda", nullptr};

constexpr utils::TypeInfo query::EdgeAtom::kType{utils::TypeId::AST_EDGE_ATOM, "EdgeAtom", &query::PatternAtom::kType};

constexpr utils::TypeInfo query::Pattern::kType{utils::TypeId::AST_PATTERN, "Pattern", &query::Tree::kType};

constexpr utils::TypeInfo query::Clause::kType{utils::TypeId::AST_CLAUSE, "Clause", &query::Tree::kType};

constexpr utils::TypeInfo query::SingleQuery::kType{
    .id = utils::TypeId::AST_SINGLE_QUERY, .name = "SingleQuery", .superclass = &query::Tree::kType};

constexpr utils::TypeInfo query::CypherUnion::kType{
    .id = utils::TypeId::AST_CYPHER_UNION, .name = "CypherUnion", .superclass = &query::Tree::kType};

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

constexpr utils::TypeInfo query::PreQueryDirectives::kType{
    .id = utils::TypeId::AST_PRE_QUERY_DIRECTIVES, .name = "PreQueryDirectives", .superclass = &query::Tree::kType};

constexpr utils::TypeInfo query::CypherQuery::kType{
    .id = utils::TypeId::AST_CYPHER_QUERY, .name = "CypherQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::ExplainQuery::kType{
    .id = utils::TypeId::AST_EXPLAIN_QUERY, .name = "ExplainQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::ProfileQuery::kType{
    .id = utils::TypeId::AST_PROFILE_QUERY, .name = "ProfileQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::IndexQuery::kType{utils::TypeId::AST_INDEX_QUERY, "IndexQuery", &query::Query::kType};

constexpr utils::TypeInfo query::EdgeIndexQuery::kType{
    .id = utils::TypeId::AST_EDGE_INDEX_QUERY, .name = "EdgeIndexQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::PointIndexQuery::kType{
    .id = utils::TypeId::AST_POINT_INDEX_QUERY, .name = "PointIndexQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::TextIndexQuery::kType{
    .id = utils::TypeId::AST_TEXT_INDEX_QUERY, .name = "TextIndexQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::CreateTextEdgeIndexQuery::kType{.id = utils::TypeId::AST_CREATE_TEXT_EDGE_INDEX_QUERY,
                                                                 .name = "CreateTextEdgeIndexQuery",
                                                                 .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::VectorIndexQuery::kType{
    .id = utils::TypeId::AST_VECTOR_INDEX_QUERY, .name = "VectorIndexQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::CreateVectorEdgeIndexQuery::kType{
    .id = utils::TypeId::AST_CREATE_VECTOR_EDGE_INDEX_QUERY,
    .name = "CreateVectorEdgeIndexQuery",
    .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::Create::kType{utils::TypeId::AST_CREATE, "Create", &query::Clause::kType};

constexpr utils::TypeInfo query::CallProcedure::kType{
    .id = utils::TypeId::AST_CALL_PROCEDURE, .name = "CallProcedure", .superclass = &query::Clause::kType};

constexpr utils::TypeInfo query::Match::kType{utils::TypeId::AST_MATCH, "Match", &query::Clause::kType};

constexpr utils::TypeInfo query::SortItem::kType{utils::TypeId::AST_SORT_ITEM, "SortItem", nullptr};

constexpr utils::TypeInfo query::ReturnBody::kType{utils::TypeId::AST_RETURN_BODY, "ReturnBody", nullptr};

constexpr utils::TypeInfo query::Return::kType{utils::TypeId::AST_RETURN, "Return", &query::Clause::kType};

constexpr utils::TypeInfo query::With::kType{utils::TypeId::AST_WITH, "With", &query::Clause::kType};

constexpr utils::TypeInfo query::Delete::kType{utils::TypeId::AST_DELETE, "Delete", &query::Clause::kType};

constexpr utils::TypeInfo query::SetProperty::kType{
    .id = utils::TypeId::AST_SET_PROPERTY, .name = "SetProperty", .superclass = &query::Clause::kType};

constexpr utils::TypeInfo query::SetProperties::kType{
    .id = utils::TypeId::AST_SET_PROPERTIES, .name = "SetProperties", .superclass = &query::Clause::kType};

constexpr utils::TypeInfo query::SetLabels::kType{utils::TypeId::AST_SET_LABELS, "SetLabels", &query::Clause::kType};

constexpr utils::TypeInfo query::RemoveProperty::kType{
    .id = utils::TypeId::AST_REMOVE_PROPERTY, .name = "RemoveProperty", .superclass = &query::Clause::kType};

constexpr utils::TypeInfo query::RemoveLabels::kType{
    .id = utils::TypeId::AST_REMOVE_LABELS, .name = "RemoveLabels", .superclass = &query::Clause::kType};

constexpr utils::TypeInfo query::Merge::kType{utils::TypeId::AST_MERGE, "Merge", &query::Clause::kType};

constexpr utils::TypeInfo query::Unwind::kType{utils::TypeId::AST_UNWIND, "Unwind", &query::Clause::kType};

constexpr utils::TypeInfo query::AuthQuery::kType{utils::TypeId::AST_AUTH_QUERY, "AuthQuery", &query::Query::kType};

constexpr utils::TypeInfo query::DatabaseInfoQuery::kType{
    .id = utils::TypeId::AST_DATABASE_INFO_QUERY, .name = "DatabaseInfoQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::SystemInfoQuery::kType{
    .id = utils::TypeId::AST_SYSTEM_INFO_QUERY, .name = "SystemInfoQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::Constraint::kType{utils::TypeId::AST_CONSTRAINT, "Constraint", nullptr};

constexpr utils::TypeInfo query::ConstraintQuery::kType{
    .id = utils::TypeId::AST_CONSTRAINT_QUERY, .name = "ConstraintQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::DumpQuery::kType{utils::TypeId::AST_DUMP_QUERY, "DumpQuery", &query::Query::kType};

constexpr utils::TypeInfo query::ReplicationQuery::kType{
    .id = utils::TypeId::AST_REPLICATION_QUERY, .name = "ReplicationQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::ReplicationInfoQuery::kType{.id = utils::TypeId::AST_REPLICATION_INFO_QUERY,
                                                             .name = "ReplicationInfoQuery",
                                                             .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::CoordinatorQuery::kType{
    .id = utils::TypeId::AST_COORDINATOR_QUERY, .name = "CoordinatorQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::DropAllIndexesQuery::kType{
    .id = utils::TypeId::AST_DROP_ALL_INDEXES_QUERY, .name = "DropAllIndexesQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::DropAllConstraintsQuery::kType{.id = utils::TypeId::AST_DROP_ALL_CONSTRAINTS_QUERY,
                                                                .name = "DropAllConstraintsQuery",
                                                                .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::DropGraphQuery::kType{
    .id = utils::TypeId::AST_DROP_GRAPH_QUERY, .name = "DropGraphQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::LockPathQuery::kType{
    .id = utils::TypeId::AST_LOCK_PATH_QUERY, .name = "LockPathQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::LoadCsv::kType{utils::TypeId::AST_LOAD_CSV, "LoadCsv", &query::Clause::kType};

constexpr utils::TypeInfo query::LoadParquet::kType{
    .id = utils::TypeId::AST_LOAD_PARQUET, .name = "LoadParquet", .superclass = &Clause::kType};

constexpr utils::TypeInfo query::LoadJsonl::kType{
    .id = utils::TypeId::AST_LOAD_JSONL, .name = "LoadJsonl", .superclass = &Clause::kType};

constexpr utils::TypeInfo query::FreeMemoryQuery::kType{
    .id = utils::TypeId::AST_FREE_MEMORY_QUERY, .name = "FreeMemoryQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::TriggerQuery::kType{
    .id = utils::TypeId::AST_TRIGGER_QUERY, .name = "TriggerQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::IsolationLevelQuery::kType{
    .id = utils::TypeId::AST_ISOLATION_LEVEL_QUERY, .name = "IsolationLevelQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::StorageModeQuery::kType{
    .id = utils::TypeId::AST_STORAGE_MODE_QUERY, .name = "StorageModeQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::CreateSnapshotQuery::kType{
    .id = utils::TypeId::AST_CREATE_SNAPSHOT_QUERY, .name = "CreateSnapshotQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::RecoverSnapshotQuery::kType{.id = utils::TypeId::AST_RECOVER_SNAPSHOT_QUERY,
                                                             .name = "RecoverSnapshotQuery",
                                                             .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::ShowSnapshotsQuery::kType{
    .id = utils::TypeId::AST_SHOW_SNAPSHOTS_QUERY, .name = "ShowSnapshotsQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::ShowNextSnapshotQuery::kType{.id = utils::TypeId::AST_SHOW_NEXT_SNAPSHOT_QUERY,
                                                              .name = "ShowNextSnapshotQuery",
                                                              .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::StreamQuery::kType{
    .id = utils::TypeId::AST_STREAM_QUERY, .name = "StreamQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::SettingQuery::kType{
    .id = utils::TypeId::AST_SETTING_QUERY, .name = "SettingQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::VersionQuery::kType{
    .id = utils::TypeId::AST_VERSION_QUERY, .name = "VersionQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::Foreach::kType{utils::TypeId::AST_FOREACH, "Foreach", &query::Clause::kType};

constexpr utils::TypeInfo query::ShowConfigQuery::kType{
    .id = utils::TypeId::AST_SHOW_CONFIG_QUERY, .name = "ShowConfigQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::AnalyzeGraphQuery::kType{
    .id = utils::TypeId::AST_ANALYZE_GRAPH_QUERY, .name = "AnalyzeGraphQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::TransactionQueueQuery::kType{.id = utils::TypeId::AST_TRANSACTION_QUEUE_QUERY,
                                                              .name = "TransactionQueueQuery",
                                                              .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::Exists::kType{utils::TypeId::AST_EXISTS, "Exists", &query::Expression::kType};

constexpr utils::TypeInfo query::CallSubquery::kType{
    .id = utils::TypeId::AST_CALL_SUBQUERY, .name = "CallSubquery", .superclass = &query::Clause::kType};

constexpr utils::TypeInfo query::MultiDatabaseQuery::kType{
    .id = utils::TypeId::AST_MULTI_DATABASE_QUERY, .name = "MultiDatabaseQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::UseDatabaseQuery::kType{
    .id = utils::TypeId::AST_USE_DATABASE, .name = "UseDatabaseQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::ShowDatabaseQuery::kType{
    .id = utils::TypeId::AST_SHOW_DATABASE, .name = "ShowDatabaseQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::ShowDatabasesQuery::kType{
    .id = utils::TypeId::AST_SHOW_DATABASES, .name = "ShowDatabasesQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::EdgeImportModeQuery::kType{
    .id = utils::TypeId::AST_EDGE_IMPORT_MODE_QUERY, .name = "EdgeImportModeQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::PatternComprehension::kType{.id = utils::TypeId::AST_PATTERN_COMPREHENSION,
                                                             .name = "PatternComprehension",
                                                             .superclass = &query::Expression::kType};

constexpr utils::TypeInfo query::CreateEnumQuery::kType{
    .id = utils::TypeId::AST_CREATE_ENUM_QUERY, .name = "CreateEnumQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::ShowEnumsQuery::kType{
    .id = utils::TypeId::AST_SHOW_ENUMS_QUERY, .name = "ShowEnumsQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::EnumValueAccess::kType{
    .id = utils::TypeId::AST_ENUM_VALUE_ACCESS, .name = "EnumValueAccess", .superclass = &query::Expression::kType};

constexpr utils::TypeInfo query::AlterEnumAddValueQuery::kType{.id = utils::TypeId::AST_ALTER_ENUM_ADD_VALUE_QUERY,
                                                               .name = "AlterEnumAddValueQuery",
                                                               .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::AlterEnumUpdateValueQuery::kType{
    .id = utils::TypeId::AST_ALTER_ENUM_UPDATE_VALUE_QUERY,
    .name = "AlterEnumUpdateValueQuery",
    .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::AlterEnumRemoveValueQuery::kType{
    .id = utils::TypeId::AST_ALTER_ENUM_REMOVE_VALUE_QUERY,
    .name = "AlterEnumRemoveValueQuery",
    .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::DropEnumQuery::kType{
    .id = utils::TypeId::AST_DROP_ENUM_QUERY, .name = "DropEnumQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::ShowSchemaInfoQuery::kType{
    .id = utils::TypeId::AST_SHOW_SCHEMA_INFO_QUERY, .name = "ShowSchemaInfoQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::TtlQuery::kType{utils::TypeId::AST_TTL_QUERY, "TtlQuery", &query::Query::kType};

constexpr utils::TypeInfo query::SessionTraceQuery::kType{
    .id = utils::TypeId::AST_SESSION_TRACE_QUERY, .name = "SessionTraceQuery", .superclass = &query::Query::kType};

constexpr utils::TypeInfo query::UserProfileQuery::kType{
    .id = utils::TypeId::AST_USER_PROFILE_QUERY, .name = "UserProfileQuery", .superclass = &query::Query::kType};

namespace query {
DEFINE_VISITABLE(Identifier, ExpressionVisitor<TypedValue>);
DEFINE_VISITABLE(Identifier, ExpressionVisitor<TypedValue *>);
DEFINE_VISITABLE(Identifier, ExpressionVisitor<TypedValue const *>);
DEFINE_VISITABLE(Identifier, ExpressionVisitor<void>);
DEFINE_VISITABLE(Identifier, HierarchicalTreeVisitor);

DEFINE_VISITABLE(NamedExpression, ExpressionVisitor<TypedValue>);
DEFINE_VISITABLE(NamedExpression, ExpressionVisitor<TypedValue *>);
DEFINE_VISITABLE(NamedExpression, ExpressionVisitor<TypedValue const *>);
DEFINE_VISITABLE(NamedExpression, ExpressionVisitor<void>);

bool NamedExpression::Accept(HierarchicalTreeVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    expression_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

DEFINE_VISITABLE(Exists, ExpressionVisitor<TypedValue>);
DEFINE_VISITABLE(Exists, ExpressionVisitor<TypedValue *>);
DEFINE_VISITABLE(Exists, ExpressionVisitor<TypedValue const *>);
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
DEFINE_VISITABLE(PatternComprehension, ExpressionVisitor<TypedValue const *>);
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
DEFINE_VISITABLE(Aggregation, ExpressionVisitor<TypedValue const *>);
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
