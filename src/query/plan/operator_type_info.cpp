// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cstdint>

#include "query/plan/operator.hpp"

namespace memgraph {

constexpr utils::TypeInfo query::plan::LogicalOperator::kType{utils::TypeId::LOGICAL_OPERATOR, "LogicalOperator",
                                                              nullptr};

constexpr utils::TypeInfo query::plan::Once::kType{utils::TypeId::ONCE, "Once", &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::NodeCreationInfo::kType{utils::TypeId::NODE_CREATION_INFO, "NodeCreationInfo",
                                                               nullptr};

constexpr utils::TypeInfo query::plan::CreateNode::kType{utils::TypeId::CREATE_NODE, "CreateNode",
                                                         &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::EdgeCreationInfo::kType{utils::TypeId::EDGE_CREATION_INFO, "EdgeCreationInfo",
                                                               nullptr};

constexpr utils::TypeInfo query::plan::CreateExpand::kType{utils::TypeId::CREATE_EXPAND, "CreateExpand",
                                                           &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::ScanAll::kType{utils::TypeId::SCAN_ALL, "ScanAll",
                                                      &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::ScanAllByLabel::kType{utils::TypeId::SCAN_ALL_BY_LABEL, "ScanAllByLabel",
                                                             &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllByLabelPropertyRange::kType{
    utils::TypeId::SCAN_ALL_BY_LABEL_PROPERTY_RANGE, "ScanAllByLabelPropertyRange", &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllByLabelPropertyValue::kType{
    utils::TypeId::SCAN_ALL_BY_LABEL_PROPERTY_VALUE, "ScanAllByLabelPropertyValue", &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllByLabelProperty::kType{
    utils::TypeId::SCAN_ALL_BY_LABEL_PROPERTY, "ScanAllByLabelProperty", &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllById::kType{utils::TypeId::SCAN_ALL_BY_ID, "ScanAllById",
                                                          &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllByEdgeType::kType{utils::TypeId::SCAN_ALL_BY_EDGE_TYPE,
                                                                "ScanAllByEdgeType", &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllByEdgeTypeProperty::kType{
    utils::TypeId::SCAN_ALL_BY_EDGE_TYPE_PROPERTY, "ScanAllByEdgeTypeProperty", &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ScanAllByEdgeId::kType{utils::TypeId::SCAN_ALL_BY_ID, "ScanAllByEdgeId",
                                                              &query::plan::ScanAll::kType};

constexpr utils::TypeInfo query::plan::ExpandCommon::kType{utils::TypeId::EXPAND_COMMON, "ExpandCommon", nullptr};

constexpr utils::TypeInfo query::plan::Expand::kType{utils::TypeId::EXPAND, "Expand",
                                                     &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::ExpansionLambda::kType{utils::TypeId::EXPANSION_LAMBDA, "ExpansionLambda",
                                                              nullptr};

constexpr utils::TypeInfo query::plan::ExpandVariable::kType{utils::TypeId::EXPAND_VARIABLE, "ExpandVariable",
                                                             &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::ConstructNamedPath::kType{
    utils::TypeId::CONSTRUCT_NAMED_PATH, "ConstructNamedPath", &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Filter::kType{utils::TypeId::FILTER, "Filter",
                                                     &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Produce::kType{utils::TypeId::PRODUCE, "Produce",
                                                      &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Delete::kType{utils::TypeId::DELETE, "Delete",
                                                     &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::SetProperty::kType{utils::TypeId::SET_PROPERTY, "SetProperty",
                                                          &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::SetProperties::kType{utils::TypeId::SET_PROPERTIES, "SetProperties",
                                                            &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::SetLabels::kType{utils::TypeId::SET_LABELS, "SetLabels",
                                                        &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::RemoveProperty::kType{utils::TypeId::REMOVE_PROPERTY, "RemoveProperty",
                                                             &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::RemoveLabels::kType{utils::TypeId::REMOVE_LABELS, "RemoveLabels",
                                                           &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::EdgeUniquenessFilter::kType{
    utils::TypeId::EDGE_UNIQUENESS_FILTER, "EdgeUniquenessFilter", &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::EmptyResult::kType{utils::TypeId::EMPTY_RESULT, "EmptyResult",
                                                          &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Accumulate::kType{utils::TypeId::ACCUMULATE, "Accumulate",
                                                         &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Aggregate::Element::kType{utils::TypeId::AGGREGATE_ELEMENT, "Element", nullptr};

constexpr utils::TypeInfo query::plan::Aggregate::kType{utils::TypeId::AGGREGATE, "Aggregate",
                                                        &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Skip::kType{utils::TypeId::SKIP, "Skip", &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::EvaluatePatternFilter::kType{
    utils::TypeId::EVALUATE_PATTERN_FILTER, "EvaluatePatternFilter", &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Limit::kType{utils::TypeId::LIMIT, "Limit",
                                                    &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::OrderBy::kType{utils::TypeId::ORDERBY, "OrderBy",
                                                      &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Merge::kType{utils::TypeId::MERGE, "Merge",
                                                    &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Optional::kType{utils::TypeId::OPTIONAL, "Optional",
                                                       &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Unwind::kType{utils::TypeId::UNWIND, "Unwind",
                                                     &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Distinct::kType{utils::TypeId::DISTINCT, "Distinct",
                                                       &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Union::kType{utils::TypeId::UNION, "Union",
                                                    &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Cartesian::kType{utils::TypeId::CARTESIAN, "Cartesian",
                                                        &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::OutputTable::kType{utils::TypeId::OUTPUT_TABLE, "OutputTable",
                                                          &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::OutputTableStream::kType{utils::TypeId::OUTPUT_TABLE_STREAM, "OutputTableStream",
                                                                &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::CallProcedure::kType{utils::TypeId::CALL_PROCEDURE, "CallProcedure",
                                                            &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::LoadCsv::kType{utils::TypeId::LOAD_CSV, "LoadCsv",
                                                      &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Foreach::kType{utils::TypeId::FOREACH, "Foreach",
                                                      &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::Apply::kType{utils::TypeId::APPLY, "Apply",
                                                    &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::IndexedJoin::kType{utils::TypeId::INDEXED_JOIN, "IndexedJoin",
                                                          &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::HashJoin::kType{utils::TypeId::HASH_JOIN, "HashJoin",
                                                       &query::plan::LogicalOperator::kType};

constexpr utils::TypeInfo query::plan::RollUpApply::kType{utils::TypeId::ROLLUP_APPLY, "RollUpApply",
                                                          &query::plan::LogicalOperator::kType};
}  // namespace memgraph
