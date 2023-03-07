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

#include "query/plan/operator.hpp"

const memgraph::utils::TypeInfo memgraph::query::plan::LogicalOperator::kType{0x120A36D770B69648ULL, "LogicalOperator",
                                                                              nullptr};

const memgraph::utils::TypeInfo memgraph::query::plan::Once::kType{0x3EB442A780E68928ULL, "Once",
                                                                   &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::NodeCreationInfo::kType{0x604890DC54DB8354ULL,
                                                                               "NodeCreationInfo", nullptr};

const memgraph::utils::TypeInfo memgraph::query::plan::CreateNode::kType{
    0x7135BC99F7B5F5D9ULL, "CreateNode", &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::EdgeCreationInfo::kType{0xE2CAA9FC5E7E68A9ULL,
                                                                               "EdgeCreationInfo", nullptr};

const memgraph::utils::TypeInfo memgraph::query::plan::CreateExpand::kType{
    0x7AD15EB2814C4695ULL, "CreateExpand", &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::ScanAll::kType{0xF3819240BF187BD7ULL, "ScanAll",
                                                                      &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::ScanAllByLabel::kType{0xCF94B907AD871638ULL, "ScanAllByLabel",
                                                                             &memgraph::query::plan::ScanAll::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::ScanAllByLabelPropertyRange::kType{
    0x97F16CF4B6AE0C6EULL, "ScanAllByLabelPropertyRange", &memgraph::query::plan::ScanAll::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::ScanAllByLabelPropertyValue::kType{
    0x4D039372268E9ACEULL, "ScanAllByLabelPropertyValue", &memgraph::query::plan::ScanAll::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::ScanAllByLabelProperty::kType{
    0x5309B180D61D8001ULL, "ScanAllByLabelProperty", &memgraph::query::plan::ScanAll::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::ScanAllById::kType{0x147CDC409BA2CE8FULL, "ScanAllById",
                                                                          &memgraph::query::plan::ScanAll::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::ExpandCommon::kType{0x12E660673BBE87B4ULL, "ExpandCommon",
                                                                           nullptr};

const memgraph::utils::TypeInfo memgraph::query::plan::Expand::kType{0x1AF4D38DA08674E5ULL, "Expand",
                                                                     &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::ExpansionLambda::kType{0x5A3EAB7454AF4CD5ULL, "ExpansionLambda",
                                                                              nullptr};

const memgraph::utils::TypeInfo memgraph::query::plan::ExpandVariable::kType{
    0xE83B5FFF179C31E5ULL, "ExpandVariable", &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::ConstructNamedPath::kType{
    0x2CFE3EE6AD7DB62CULL, "ConstructNamedPath", &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::Filter::kType{0x88C480C382E4E833ULL, "Filter",
                                                                     &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::Produce::kType{0x16258130466DDAB3ULL, "Produce",
                                                                      &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::Delete::kType{0x5C2A17AE39AC6236ULL, "Delete",
                                                                     &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::SetProperty::kType{
    0xC5911748F534C656ULL, "SetProperty", &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::SetProperties::kType{
    0x4A4749864FD186DEULL, "SetProperties", &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::SetLabels::kType{0x9F44A2778C769DC4ULL, "SetLabels",
                                                                        &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::RemoveProperty::kType{
    0x590B51DABC6742CCULL, "RemoveProperty", &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::RemoveLabels::kType{
    0x51A8823DAE6CC45EULL, "RemoveLabels", &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::EdgeUniquenessFilter::kType{
    0x13960CD62FBD3D22ULL, "EdgeUniquenessFilter", &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::EmptyResult::kType{
    0x3AC47389B3EE61B3ULL, "EmptyResult", &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::Accumulate::kType{
    0x4FD5DC6A1550A35FULL, "Accumulate", &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::Aggregate::Element::kType{0xA2816A06AF49C170ULL, "Element",
                                                                                 nullptr};

const memgraph::utils::TypeInfo memgraph::query::plan::Aggregate::kType{0x275C506C760A066CULL, "Aggregate",
                                                                        &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::Skip::kType{0x85B81ECB8F0197AEULL, "Skip",
                                                                   &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::EvaluatePatternFilter::kType{
    0xF77BC145A4C54D06ULL, "EvaluatePatternFilter", &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::Limit::kType{0x79C67A0E22B61400ULL, "Limit",
                                                                    &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::OrderBy::kType{0x3A28CE2977BA392EULL, "OrderBy",
                                                                      &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::Merge::kType{0x814A58A452867E1BULL, "Merge",
                                                                    &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::Optional::kType{0xABD7FF12F7D7653DULL, "Optional",
                                                                       &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::Unwind::kType{0x9656479F3650BAACULL, "Unwind",
                                                                     &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::Distinct::kType{0xF88871E7D3EF59D9ULL, "Distinct",
                                                                       &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::Union::kType{0x986F73BF9DB2ED98ULL, "Union",
                                                                    &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::Cartesian::kType{0x495ADE66700E6A59ULL, "Cartesian",
                                                                        &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::OutputTable::kType{
    0xAFA63A11801A216CULL, "OutputTable", &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::OutputTableStream::kType{
    0x562CFD1DB41726B4ULL, "OutputTableStream", &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::CallProcedure::kType{
    0x600DBFC7086E51EEULL, "CallProcedure", &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::LoadCsv::kType{0x59DB756153BD0A87ULL, "LoadCsv",
                                                                      &memgraph::query::plan::LogicalOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::plan::Foreach::kType{0x43D38A2F943425ULL, "Foreach",
                                                                      &memgraph::query::plan::LogicalOperator::kType};
