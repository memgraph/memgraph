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

#include "mock_helpers.hpp"

#include "query/v2/bindings/frame.hpp"
#include "query/v2/bindings/symbol_table.hpp"
#include "query/v2/common.hpp"
#include "query/v2/context.hpp"
#include "query/v2/frontend/ast/ast.hpp"
#include "query/v2/plan/operator.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/shard.hpp"
#include "utils/memory.hpp"

namespace memgraph::query::v2 {
MultiFrame CreateMultiFrame(const size_t max_pos) {
  static constexpr size_t frame_size = 100;
  MultiFrame multi_frame(max_pos, frame_size, utils::NewDeleteResource());
  auto frames_populator = multi_frame.GetInvalidFramesPopulator();
  for (auto &frame : frames_populator) {
    frame.MakeValid();
  }

  return multi_frame;
}

TEST(CreateNodeTest, CreateNodeCursor) {
  using testing::_;
  using testing::Return;

  AstStorage ast;
  SymbolTable symbol_table;

  plan::NodeCreationInfo node;
  plan::EdgeCreationInfo edge;
  edge.edge_type = msgs::EdgeTypeId::FromUint(1);
  edge.direction = EdgeAtom::Direction::IN;
  auto id_alloc = IdAllocator(0, 100);

  node.symbol = symbol_table.CreateSymbol("n", true);
  node.labels.push_back(msgs::LabelId::FromUint(2));
  auto literal = PrimitiveLiteral();
  literal.value_ = TypedValue(static_cast<int64_t>(200));
  auto p = plan::PropertiesMapList{};
  p.push_back(std::make_pair(msgs::PropertyId::FromUint(2), &literal));
  node.properties.emplace<0>(std::move(p));

  auto once_cur = plan::MakeUniqueCursorPtr<MockedCursor>(utils::NewDeleteResource());
  EXPECT_CALL(BaseToMock(once_cur.get()), PullMultiple(_, _)).Times(1);

  std::shared_ptr<plan::LogicalOperator> once_op = std::make_shared<MockedLogicalOperator>();
  EXPECT_CALL(BaseToMock(once_op.get()), MakeCursor(_)).Times(1).WillOnce(Return(std::move(once_cur)));

  auto create_expand = plan::CreateNode(once_op, node);
  auto cursor = create_expand.MakeCursor(utils::NewDeleteResource());
  MockedRequestRouter router;
  EXPECT_CALL(router, CreateVertices(testing::_))
      .Times(1)
      .WillOnce(::testing::Return(std::vector<msgs::CreateVerticesResponse>{}));
  EXPECT_CALL(router, IsPrimaryKey(testing::_, testing::_)).WillRepeatedly(::testing::Return(true));
  auto context = MakeContext(ast, symbol_table, &router, &id_alloc);
  auto multi_frame = CreateMultiFrame(context.symbol_table.max_position());
  cursor->PullMultiple(multi_frame, context);
}
}  // namespace memgraph::query::v2
