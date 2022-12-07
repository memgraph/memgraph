// Copyright 2022 Memgraph Ltd.
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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "query/v2/plan/operator.hpp"
#include "query/v2/request_router.hpp"

namespace memgraph {
class MockedRequestRouter : public query::v2::RequestRouterInterface {
 public:
  MOCK_METHOD1(ScanVertices, std::vector<VertexAccessor>(std::optional<std::string> label));
  MOCK_METHOD1(CreateVertices, std::vector<msgs::CreateVerticesResponse>(std::vector<msgs::NewVertex>));
  MOCK_METHOD1(ExpandOne, std::vector<msgs::ExpandOneResultRow>(msgs::ExpandOneRequest));
  MOCK_METHOD1(CreateExpand, std::vector<msgs::CreateExpandResponse>(std::vector<msgs::NewExpand>));
  MOCK_METHOD1(GetProperties, std::vector<msgs::GetPropertiesResultRow>(msgs::GetPropertiesRequest));
  MOCK_METHOD0(StartTransaction, void());
  MOCK_METHOD0(Commit, void());

  MOCK_CONST_METHOD1(NameToEdgeType, storage::v3::EdgeTypeId(const std::string &));
  MOCK_CONST_METHOD1(NameToProperty, storage::v3::PropertyId(const std::string &));
  MOCK_CONST_METHOD1(NameToLabel, storage::v3::LabelId(const std::string &));
  MOCK_CONST_METHOD1(LabelToName, storage::v3::LabelId(const std::string &));
  MOCK_CONST_METHOD1(PropertyToName, const std::string &(storage::v3::PropertyId));
  MOCK_CONST_METHOD1(LabelToName, const std::string &(storage::v3::LabelId label));
  MOCK_CONST_METHOD1(EdgeTypeToName, const std::string &(storage::v3::EdgeTypeId type));
  MOCK_CONST_METHOD1(MaybeNameToProperty, std::optional<storage::v3::PropertyId>(const std::string &));
  MOCK_CONST_METHOD1(MaybeNameToEdgeType, std::optional<storage::v3::EdgeTypeId>(const std::string &));
  MOCK_CONST_METHOD1(MaybeNameToLabel, std::optional<storage::v3::LabelId>(const std::string &));
  MOCK_CONST_METHOD1(IsPrimaryLabel, bool(storage::v3::LabelId));
  MOCK_CONST_METHOD2(IsPrimaryKey, bool(storage::v3::LabelId, storage::v3::PropertyId));
};

class MockedLogicalOperator : query::v2::plan::LogicalOperator {
 public:
  MOCK_CONST_METHOD1(MakeCursor, query::v2::plan::UniqueCursorPtr(utils::MemoryResource *));
  MOCK_CONST_METHOD1(OutputSymbols, std::vector<expr::Symbol>(const expr::SymbolTable &));
  MOCK_CONST_METHOD1(ModifiedSymbols, std::vector<expr::Symbol>(const expr::SymbolTable &));
  MOCK_CONST_METHOD0(HasSingleInput, bool());
  MOCK_CONST_METHOD0(input, std::shared_ptr<LogicalOperator>());
  MOCK_METHOD1(set_input, void(std::shared_ptr<LogicalOperator>));
  MOCK_CONST_METHOD1(Clone, std::unique_ptr<LogicalOperator>(query::v2::AstStorage *storage));
};

class MockedCursor : memgraph::query::v2::plan::Cursor {
 public:
  MOCK_METHOD2(Pull, bool(query::v2::Frame &, expr::ExecutionContext &));
  MOCK_METHOD2(PullMultiple, void(query::v2::MultiFrame &, expr::ExecutionContext &));
  MOCK_METHOD0(Reset, void());
  MOCK_METHOD0(Shutdown, void());
};

}  // namespace memgraph
