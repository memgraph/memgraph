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

#include "query/frontend/ast/ast.hpp"

const memgraph::utils::TypeInfo memgraph::query::LabelIx::kType{0x11DA5C011A9F1309ULL, "LabelIx", nullptr};

const memgraph::utils::TypeInfo memgraph::query::PropertyIx::kType{0xCEA5DC57324EA35EULL, "PropertyIx", nullptr};

const memgraph::utils::TypeInfo memgraph::query::EdgeTypeIx::kType{0x53FD4D0015B2805AULL, "EdgeTypeIx", nullptr};

const memgraph::utils::TypeInfo memgraph::query::Tree::kType{0x4344888F6660DFD6ULL, "Tree", nullptr};

const memgraph::utils::TypeInfo memgraph::query::Expression::kType{0xDCBF69BE8668F528ULL, "Expression",
                                                                   &memgraph::query::Tree::kType};

const memgraph::utils::TypeInfo memgraph::query::Where::kType{0x62792B21ED499AC1ULL, "Where",
                                                              &memgraph::query::Tree::kType};

const memgraph::utils::TypeInfo memgraph::query::BinaryOperator::kType{0x5B2E430CE6B0429BULL, "BinaryOperator",
                                                                       &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::UnaryOperator::kType{0x8031CA2870755F9DULL, "UnaryOperator",
                                                                      &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::OrOperator::kType{0x1BCDF6D4561D227ULL, "OrOperator",
                                                                   &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::XorOperator::kType{0x5AB64F8056E4891BULL, "XorOperator",
                                                                    &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::AndOperator::kType{0x62BDE5D339401E7BULL, "AndOperator",
                                                                    &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::AdditionOperator::kType{0x52D17561301C1D42ULL, "AdditionOperator",
                                                                         &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::SubtractionOperator::kType{0xFCF5A1BA84B0C90ULL, "SubtractionOperator",
                                                                            &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::MultiplicationOperator::kType{
    0x6ADE43AB6C2A3530ULL, "MultiplicationOperator", &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::DivisionOperator::kType{0x2D7BB196E9571D8DULL, "DivisionOperator",
                                                                         &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::ModOperator::kType{0x149CAB96EB3C4EULL, "ModOperator",
                                                                    &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::NotEqualOperator::kType{0xF13556B8D0A0D025ULL, "NotEqualOperator",
                                                                         &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::EqualOperator::kType{0xAC4646D2975CC11EULL, "EqualOperator",
                                                                      &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::LessOperator::kType{0xC1B0BF810466E2C3ULL, "LessOperator",
                                                                     &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::GreaterOperator::kType{0xFC38BB8B1B6D2C68ULL, "GreaterOperator",
                                                                        &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::LessEqualOperator::kType{0xE000F643E2DF0FB1ULL, "LessEqualOperator",
                                                                          &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::GreaterEqualOperator::kType{
    0x6089DF79F16D1540ULL, "GreaterEqualOperator", &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::InListOperator::kType{0x2F4A2CAE8729D9E9ULL, "InListOperator",
                                                                       &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::SubscriptOperator::kType{0xBF71EC9E9CCDBF3BULL, "SubscriptOperator",
                                                                          &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::NotOperator::kType{0x70CBC2CC602C6887ULL, "NotOperator",
                                                                    &memgraph::query::UnaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::UnaryPlusOperator::kType{0xB755AEAE5F1F6CDULL, "UnaryPlusOperator",
                                                                          &memgraph::query::UnaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::UnaryMinusOperator::kType{0x25CD9E3AEC4E13EFULL, "UnaryMinusOperator",
                                                                           &memgraph::query::UnaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::IsNullOperator::kType{0xD108479FB6CC864FULL, "IsNullOperator",
                                                                       &memgraph::query::UnaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::Aggregation::kType{0xF951EC4E42B59308ULL, "Aggregation",
                                                                    &memgraph::query::BinaryOperator::kType};

const memgraph::utils::TypeInfo memgraph::query::ListSlicingOperator::kType{
    0x2C8C4A3A0E7D0847ULL, "ListSlicingOperator", &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::IfOperator::kType{0xF546776DD9D906F5ULL, "IfOperator",
                                                                   &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::BaseLiteral::kType{0x62D38804A2693BAEULL, "BaseLiteral",
                                                                    &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::PrimitiveLiteral::kType{0x2B217BAA686D53DEULL, "PrimitiveLiteral",
                                                                         &memgraph::query::BaseLiteral::kType};

const memgraph::utils::TypeInfo memgraph::query::ListLiteral::kType{0x4DE40DC42B658AADULL, "ListLiteral",
                                                                    &memgraph::query::BaseLiteral::kType};

const memgraph::utils::TypeInfo memgraph::query::MapLiteral::kType{0xACF06A81158A01ABULL, "MapLiteral",
                                                                   &memgraph::query::BaseLiteral::kType};

const memgraph::utils::TypeInfo memgraph::query::Identifier::kType{0xA56BF6F2F344DB9ULL, "Identifier",
                                                                   &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::PropertyLookup::kType{0xDFCE26AF96CECA59ULL, "PropertyLookup",
                                                                       &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::LabelsTest::kType{0xEA448992A167401ULL, "LabelsTest",
                                                                   &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::Function::kType{0xE32DA253EF62E16EULL, "Function",
                                                                 &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::Reduce::kType{0xA7575119A971A100ULL, "Reduce",
                                                               &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::Coalesce::kType{0x6016F400F741CEADULL, "Coalesce",
                                                                 &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::Extract::kType{0xE7F77C95B95D4C0FULL, "Extract",
                                                                &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::All::kType{0x522ED151337F2FE1ULL, "All",
                                                            &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::Single::kType{0xC2501281959170C2ULL, "Single",
                                                               &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::Any::kType{0x5235C051338531ACULL, "Any",
                                                            &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::None::kType{0x109A79BFBFFEBEC0ULL, "None",
                                                             &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::ParameterLookup::kType{0xF509150171270893ULL, "ParameterLookup",
                                                                        &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::RegexMatch::kType{0x62835B5DCBB47696ULL, "RegexMatch",
                                                                   &memgraph::query::Expression::kType};

const memgraph::utils::TypeInfo memgraph::query::NamedExpression::kType{0xB5DC8CE8B571C741ULL, "NamedExpression",
                                                                        &memgraph::query::Tree::kType};

const memgraph::utils::TypeInfo memgraph::query::PatternAtom::kType{0xB0919DE94D286B89ULL, "PatternAtom",
                                                                    &memgraph::query::Tree::kType};

const memgraph::utils::TypeInfo memgraph::query::NodeAtom::kType{0xDC95C7EFA10C7DDBULL, "NodeAtom",
                                                                 &memgraph::query::PatternAtom::kType};

const memgraph::utils::TypeInfo memgraph::query::EdgeAtom::Lambda::kType{0x5EAAAC14AEAEF225ULL, "Lambda", nullptr};

const memgraph::utils::TypeInfo memgraph::query::EdgeAtom::kType{0xD3F077F262CEA052ULL, "EdgeAtom",
                                                                 &memgraph::query::PatternAtom::kType};

const memgraph::utils::TypeInfo memgraph::query::Pattern::kType{0x15E5FD256974140ULL, "Pattern",
                                                                &memgraph::query::Tree::kType};

const memgraph::utils::TypeInfo memgraph::query::Clause::kType{0xB52EEC9FBB2853D7ULL, "Clause",
                                                               &memgraph::query::Tree::kType};

const memgraph::utils::TypeInfo memgraph::query::SingleQuery::kType{0xB6E28A944CD9182EULL, "SingleQuery",
                                                                    &memgraph::query::Tree::kType};

const memgraph::utils::TypeInfo memgraph::query::CypherUnion::kType{0xC387DFABFE1FB730ULL, "CypherUnion",
                                                                    &memgraph::query::Tree::kType};

const memgraph::utils::TypeInfo memgraph::query::Query::kType{0xE280D65C1EB115C6ULL, "Query",
                                                              &memgraph::query::Tree::kType};

const memgraph::utils::TypeInfo memgraph::query::CypherQuery::kType{0x2FD066553FCC9DDFULL, "CypherQuery",
                                                                    &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::ExplainQuery::kType{0xF3EFEB69DEB1186FULL, "ExplainQuery",
                                                                     &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::ProfileQuery::kType{0x24B61FC5A0AFB7FDULL, "ProfileQuery",
                                                                     &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::IndexQuery::kType{0x3C1D8D92B9312C12ULL, "IndexQuery",
                                                                   &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::Create::kType{0xACE3D09D645BF60EULL, "Create",
                                                               &memgraph::query::Clause::kType};

const memgraph::utils::TypeInfo memgraph::query::CallProcedure::kType{0x560339D48ED2F44FULL, "CallProcedure",
                                                                      &memgraph::query::Clause::kType};

const memgraph::utils::TypeInfo memgraph::query::Match::kType{0x6CBF9A7D0E8C766BULL, "Match",
                                                              &memgraph::query::Clause::kType};

const memgraph::utils::TypeInfo memgraph::query::SortItem::kType{0xE1AF9E8C71B734A1ULL, "SortItem", nullptr};

const memgraph::utils::TypeInfo memgraph::query::ReturnBody::kType{0x110F1E75DDAE9CD0ULL, "ReturnBody", nullptr};

const memgraph::utils::TypeInfo memgraph::query::Return::kType{0x118F7AA21E55A934ULL, "Return",
                                                               &memgraph::query::Clause::kType};

const memgraph::utils::TypeInfo memgraph::query::With::kType{0xF2376847777F0C6ULL, "With",
                                                             &memgraph::query::Clause::kType};

const memgraph::utils::TypeInfo memgraph::query::Delete::kType{0x7B875C0362AD99C5ULL, "Delete",
                                                               &memgraph::query::Clause::kType};

const memgraph::utils::TypeInfo memgraph::query::SetProperty::kType{0x3DD61ACA20CC820FULL, "SetProperty",
                                                                    &memgraph::query::Clause::kType};

const memgraph::utils::TypeInfo memgraph::query::SetProperties::kType{0x6D89C20D4D481E67ULL, "SetProperties",
                                                                      &memgraph::query::Clause::kType};

const memgraph::utils::TypeInfo memgraph::query::SetLabels::kType{0x1CB79B18DC5683A9ULL, "SetLabels",
                                                                  &memgraph::query::Clause::kType};

const memgraph::utils::TypeInfo memgraph::query::RemoveProperty::kType{0x7E6FE9C800A545CBULL, "RemoveProperty",
                                                                       &memgraph::query::Clause::kType};

const memgraph::utils::TypeInfo memgraph::query::RemoveLabels::kType{0x66EE62F4E97784E5ULL, "RemoveLabels",
                                                                     &memgraph::query::Clause::kType};

const memgraph::utils::TypeInfo memgraph::query::Merge::kType{0x7495D75E46539F62ULL, "Merge",
                                                              &memgraph::query::Clause::kType};

const memgraph::utils::TypeInfo memgraph::query::Unwind::kType{0x4645FD88B37C7C53ULL, "Unwind",
                                                               &memgraph::query::Clause::kType};

const memgraph::utils::TypeInfo memgraph::query::AuthQuery::kType{0xADFB12C266132854ULL, "AuthQuery",
                                                                  &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::InfoQuery::kType{0x67C04B7AE20004AEULL, "InfoQuery",
                                                                  &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::Constraint::kType{0x4040168D5B6D2EA9ULL, "Constraint", nullptr};

const memgraph::utils::TypeInfo memgraph::query::ConstraintQuery::kType{0x9B55D3642FB9765FULL, "ConstraintQuery",
                                                                        &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::DumpQuery::kType{0x4BFE2FB4AB02B7A4ULL, "DumpQuery",
                                                                  &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::ReplicationQuery::kType{0xF8C44201464F8970ULL, "ReplicationQuery",
                                                                         &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::LockPathQuery::kType{0x2F7DE35B8A3A91EAULL, "LockPathQuery",
                                                                      &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::LoadCsv::kType{0xCAABB7895328BB12ULL, "LoadCsv",
                                                                &memgraph::query::Clause::kType};

const memgraph::utils::TypeInfo memgraph::query::FreeMemoryQuery::kType{0x6159878D41E2ABE1ULL, "FreeMemoryQuery",
                                                                        &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::TriggerQuery::kType{0x9F83F333E4C5C606ULL, "TriggerQuery",
                                                                     &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::IsolationLevelQuery::kType{0xC1DB3EFA523DE20ULL, "IsolationLevelQuery",
                                                                            &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::CreateSnapshotQuery::kType{
    0x2BB6237F3AC564BEULL, "CreateSnapshotQuery", &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::StreamQuery::kType{0xC541BC25300676AEULL, "StreamQuery",
                                                                    &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::SettingQuery::kType{0xB9454814207B2A5AULL, "SettingQuery",
                                                                     &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::VersionQuery::kType{0x5DAC03A5A2BE0CAULL, "VersionQuery",
                                                                     &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::Foreach::kType{0xF22E7972BF35BA14ULL, "Foreach",
                                                                &memgraph::query::Clause::kType};

const memgraph::utils::TypeInfo memgraph::query::ShowConfigQuery::kType{0x3E7D0B5FDE8211DBULL, "ShowConfigQuery",
                                                                        &memgraph::query::Query::kType};

const memgraph::utils::TypeInfo memgraph::query::Exists::kType{0x5F492EE46988DD42ULL, "Exists",
                                                               &memgraph::query::Expression::kType};
