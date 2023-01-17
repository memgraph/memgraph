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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "query/v2/common.hpp"
#include "query/v2/context.hpp"
#include "query/v2/plan/operator.hpp"
#include "query/v2/request_router.hpp"

namespace memgraph::query::v2::tests {
class MockedRequestRouter : public RequestRouterInterface {
 public:
  MOCK_METHOD(std::vector<VertexAccessor>, ScanVertices, (std::optional<std::string> label));
  MOCK_METHOD(std::vector<msgs::CreateVerticesResponse>, CreateVertices, (std::vector<msgs::NewVertex>));
  MOCK_METHOD(std::vector<msgs::ExpandOneResultRow>, ExpandOne, (msgs::ExpandOneRequest));
  MOCK_METHOD(std::vector<msgs::CreateExpandResponse>, CreateExpand, (std::vector<msgs::NewExpand>));
  MOCK_METHOD(std::vector<msgs::GetPropertiesResultRow>, GetProperties, (msgs::GetPropertiesRequest));
  MOCK_METHOD(void, StartTransaction, ());
  MOCK_METHOD(void, Commit, ());

  MOCK_METHOD(storage::v3::EdgeTypeId, NameToEdgeType, (const std::string &), (const));
  MOCK_METHOD(storage::v3::PropertyId, NameToProperty, (const std::string &), (const));
  MOCK_METHOD(storage::v3::LabelId, NameToLabel, (const std::string &), (const));
  MOCK_METHOD(storage::v3::LabelId, LabelToName, (const std::string &), (const));
  MOCK_METHOD(const std::string &, PropertyToName, (storage::v3::PropertyId), (const));
  MOCK_METHOD(const std::string &, LabelToName, (storage::v3::LabelId label), (const));
  MOCK_METHOD(const std::string &, EdgeTypeToName, (storage::v3::EdgeTypeId type), (const));
  MOCK_METHOD(std::optional<storage::v3::PropertyId>, MaybeNameToProperty, (const std::string &), (const));
  MOCK_METHOD(std::optional<storage::v3::EdgeTypeId>, MaybeNameToEdgeType, (const std::string &), (const));
  MOCK_METHOD(std::optional<storage::v3::LabelId>, MaybeNameToLabel, (const std::string &), (const));
  MOCK_METHOD(bool, IsPrimaryLabel, (storage::v3::LabelId), (const));
  MOCK_METHOD(bool, IsPrimaryKey, (storage::v3::LabelId, storage::v3::PropertyId), (const));
};

class MockedLogicalOperator : public plan::LogicalOperator {
 public:
  MOCK_METHOD(plan::UniqueCursorPtr, MakeCursor, (utils::MemoryResource *), (const));
  MOCK_METHOD(std::vector<expr::Symbol>, ModifiedSymbols, (const expr::SymbolTable &), (const));
  MOCK_METHOD(bool, HasSingleInput, (), (const));
  MOCK_METHOD(std::shared_ptr<LogicalOperator>, input, (), (const));
  MOCK_METHOD(void, set_input, (std::shared_ptr<LogicalOperator>));
  MOCK_METHOD(std::unique_ptr<LogicalOperator>, Clone, (AstStorage * storage), (const));
  MOCK_METHOD(bool, Accept, (plan::HierarchicalLogicalOperatorVisitor & visitor));
};

class MockedCursor : public plan::Cursor {
 public:
  MOCK_METHOD(bool, Pull, (Frame &, expr::ExecutionContext &));
  MOCK_METHOD(void, PullMultiple, (MultiFrame &, expr::ExecutionContext &));
  MOCK_METHOD(void, Reset, ());
  MOCK_METHOD(void, Shutdown, ());
};

inline expr::ExecutionContext MakeContext(const expr::AstStorage &storage, const expr::SymbolTable &symbol_table,
                                          RequestRouterInterface *router, IdAllocator *id_alloc) {
  expr::ExecutionContext context;
  context.symbol_table = symbol_table;
  context.evaluation_context.properties = NamesToProperties(storage.properties_, router);
  context.evaluation_context.labels = NamesToLabels(storage.labels_, router);
  context.edge_ids_alloc = id_alloc;
  context.request_router = router;
  return context;
}

inline MockedLogicalOperator &BaseToMock(plan::LogicalOperator &op) {
  return dynamic_cast<MockedLogicalOperator &>(op);
}

inline MockedCursor &BaseToMock(plan::Cursor &cursor) { return dynamic_cast<MockedCursor &>(cursor); }

}  // namespace memgraph::query::v2::tests
