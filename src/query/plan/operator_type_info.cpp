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

#include "query/plan/operator.hpp"

namespace memgraph {

constexpr utils::TypeInfo query::plan::LogicalOperator::kType{
    .id = utils::TypeId::LOGICAL_OPERATOR, .name = "LogicalOperator", .superclass = nullptr};

constexpr utils::TypeInfo query::plan::Once::kType{utils::TypeId::ONCE, "Once", &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::NodeCreationInfo::kType{
    .id = utils::TypeId::NODE_CREATION_INFO, .name = "NodeCreationInfo", .superclass = nullptr};

constexpr utils::TypeInfo query::plan::CreateNode::kType{
    .id = utils::TypeId::CREATE_NODE, .name = "CreateNode", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::EdgeCreationInfo::kType{
    .id = utils::TypeId::EDGE_CREATION_INFO, .name = "EdgeCreationInfo", .superclass = nullptr};

constexpr utils::TypeInfo query::plan::CreateExpand::kType{
    .id = utils::TypeId::CREATE_EXPAND, .name = "CreateExpand", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::ScanAll::kType{
    .id = utils::TypeId::SCAN_ALL, .name = "ScanAll", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::ScanAllByLabel::kType{
    .id = utils::TypeId::SCAN_ALL_BY_LABEL, .name = "ScanAllByLabel", .superclass = &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllByLabelProperties::kType{
    utils::TypeId::SCAN_ALL_BY_LABEL_PROPERTIES, "ScanAllByLabelProperties", &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllById::kType{
    .id = utils::TypeId::SCAN_ALL_BY_ID, .name = "ScanAllById", .superclass = &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllByEdge::kType{
    .id = utils::TypeId::SCAN_ALL_BY_EDGE, .name = "ScanAllByEdge", .superclass = &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllByEdgeType::kType{.id = utils::TypeId::SCAN_ALL_BY_EDGE_TYPE,
                                                                .name = "ScanAllByEdgeType",
                                                                .superclass = &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllByEdgeTypeProperty::kType{
    utils::TypeId::SCAN_ALL_BY_EDGE_TYPE_PROPERTY, "ScanAllByEdgeTypeProperty", &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllByEdgeTypePropertyValue::kType{
    utils::TypeId::SCAN_ALL_BY_EDGE_TYPE_PROPERTY_VALUE,
    "ScanAllByEdgeTypePropertyValue",
    &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllByEdgeTypePropertyRange::kType{
    utils::TypeId::SCAN_ALL_BY_EDGE_TYPE_PROPERTY_RANGE,
    "ScanAllByEdgeTypePropertyRange",
    &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllByEdgeProperty::kType{
    utils::TypeId::SCAN_ALL_BY_EDGE_PROPERTY, "ScanAllByEdgeProperty", &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllByEdgePropertyValue::kType{
    utils::TypeId::SCAN_ALL_BY_EDGE_PROPERTY_VALUE, "ScanAllByEdgePropertyValue", &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllByEdgePropertyRange::kType{
    utils::TypeId::SCAN_ALL_BY_EDGE_PROPERTY_RANGE, "ScanAllByEdgePropertyRange", &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllByEdgeId::kType{
    .id = utils::TypeId::SCAN_ALL_BY_ID, .name = "ScanAllByEdgeId", .superclass = &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ExpandCommon::kType{utils::TypeId::EXPAND_COMMON, "ExpandCommon", nullptr};
constexpr utils::TypeInfo query::plan::ScanByEdgeCommon::kType{
    .id = utils::TypeId::SCAN_BY_EDGE_COMMON, .name = "ScanByEdgeCommon", .superclass = nullptr};

constexpr utils::TypeInfo query::plan::Expand::kType{
    .id = utils::TypeId::EXPAND, .name = "Expand", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::ExpansionLambda::kType{
    .id = utils::TypeId::EXPANSION_LAMBDA, .name = "ExpansionLambda", .superclass = nullptr};

constexpr utils::TypeInfo query::plan::ExpandVariable::kType{
    .id = utils::TypeId::EXPAND_VARIABLE, .name = "ExpandVariable", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::ConstructNamedPath::kType{
    utils::TypeId::CONSTRUCT_NAMED_PATH, "ConstructNamedPath", &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Filter::kType{
    .id = utils::TypeId::FILTER, .name = "Filter", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Produce::kType{
    .id = utils::TypeId::PRODUCE, .name = "Produce", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Delete::kType{
    .id = utils::TypeId::DELETE, .name = "Delete", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::SetProperty::kType{
    .id = utils::TypeId::SET_PROPERTY, .name = "SetProperty", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::SetProperties::kType{
    .id = utils::TypeId::SET_PROPERTIES, .name = "SetProperties", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::SetLabels::kType{
    .id = utils::TypeId::SET_LABELS, .name = "SetLabels", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::RemoveProperty::kType{
    .id = utils::TypeId::REMOVE_PROPERTY, .name = "RemoveProperty", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::RemoveLabels::kType{
    .id = utils::TypeId::REMOVE_LABELS, .name = "RemoveLabels", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::EdgeUniquenessFilter::kType{
    utils::TypeId::EDGE_UNIQUENESS_FILTER, "EdgeUniquenessFilter", &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::EmptyResult::kType{
    .id = utils::TypeId::EMPTY_RESULT, .name = "EmptyResult", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Accumulate::kType{
    .id = utils::TypeId::ACCUMULATE, .name = "Accumulate", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Aggregate::Element::kType{utils::TypeId::AGGREGATE_ELEMENT, "Element", nullptr};

constexpr utils::TypeInfo query::plan::Aggregate::kType{
    .id = utils::TypeId::AGGREGATE, .name = "Aggregate", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Skip::kType{utils::TypeId::SKIP, "Skip", &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::EvaluatePatternFilter::kType{
    utils::TypeId::EVALUATE_PATTERN_FILTER, "EvaluatePatternFilter", &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Limit::kType{
    .id = utils::TypeId::LIMIT, .name = "Limit", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::OrderBy::kType{
    .id = utils::TypeId::ORDERBY, .name = "OrderBy", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Merge::kType{
    .id = utils::TypeId::MERGE, .name = "Merge", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Optional::kType{
    .id = utils::TypeId::OPTIONAL, .name = "Optional", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Unwind::kType{
    .id = utils::TypeId::UNWIND, .name = "Unwind", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Distinct::kType{
    .id = utils::TypeId::DISTINCT, .name = "Distinct", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Union::kType{
    .id = utils::TypeId::UNION, .name = "Union", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Cartesian::kType{
    .id = utils::TypeId::CARTESIAN, .name = "Cartesian", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::OutputTable::kType{
    .id = utils::TypeId::OUTPUT_TABLE, .name = "OutputTable", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::OutputTableStream::kType{.id = utils::TypeId::OUTPUT_TABLE_STREAM,
                                                                .name = "OutputTableStream",
                                                                .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::CallProcedure::kType{
    .id = utils::TypeId::CALL_PROCEDURE, .name = "CallProcedure", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::LoadCsv::kType{
    .id = utils::TypeId::LOAD_CSV, .name = "LoadCsv", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::LoadParquet::kType{
    .id = utils::TypeId::LOAD_PARQUET, .name = "LoadParquet", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::LoadJsonl::kType{
    .id = utils::TypeId::LOAD_JSONL, .name = "LoadJsonl", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Foreach::kType{
    .id = utils::TypeId::FOREACH, .name = "Foreach", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Apply::kType{
    .id = utils::TypeId::APPLY, .name = "Apply", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::IndexedJoin::kType{
    .id = utils::TypeId::INDEXED_JOIN, .name = "IndexedJoin", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::HashJoin::kType{
    .id = utils::TypeId::HASH_JOIN, .name = "HashJoin", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::RollUpApply::kType{
    .id = utils::TypeId::ROLLUP_APPLY, .name = "RollUpApply", .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::PeriodicCommit::kType{
    .id = utils::TypeId::PERIODIC_COMMIT, .name = "PeriodicCommit", .superclass = &query::plan::LogicalOperator::kType};
constexpr utils::TypeInfo query::plan::PeriodicSubquery::kType{.id = utils::TypeId::PERIODIC_SUBQUERY,
                                                               .name = "PeriodicSubquery",
                                                               .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::SetNestedProperty::kType{.id = utils::TypeId::SET_NESTED_PROPERTY,
                                                                .name = "SetNestedProperty",
                                                                .superclass = &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::RemoveNestedProperty::kType{
    utils::TypeId::REMOVE_NESTED_PROPERTY, "RemoveNestedProperty", &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::ScanAllByPointDistance::kType{
    utils::TypeId::SCAN_ALL_BY_POINT_DISTANCE, "ScanAllByPointDistance", &query::plan::ScanAllByPointDistance::kType};

constexpr utils::TypeInfo query::plan::ScanAllByPointWithinbbox::kType{utils::TypeId::SCAN_ALL_BY_POINT_WITHINBBOX,
                                                                       "ScanAllByPointWithinbbox",
                                                                       &query::plan::ScanAllByPointWithinbbox::kType};
}  // namespace memgraph
