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

#include "gmock/gmock.h"
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

namespace memgraph::query::v2::tests {
MultiFrame CreateMultiFrame(const size_t max_pos) {
  static constexpr size_t frame_size = 100;
  MultiFrame multi_frame(max_pos, frame_size, utils::NewDeleteResource());

  return multi_frame;
}

TEST(CreateNodeTest, CreateNodeCursor) {
  using testing::_;
  using testing::IsEmpty;
  using testing::Return;

  AstStorage ast;
  SymbolTable symbol_table;

  plan::NodeCreationInfo node;
  auto id_alloc = IdAllocator(0, 100);

  node.symbol = symbol_table.CreateSymbol("n", true);
  const auto primary_label_id = msgs::LabelId::FromUint(2);
  node.labels.push_back(primary_label_id);
  auto literal = PrimitiveLiteral();
  literal.value_ = TypedValue(static_cast<int64_t>(200));
  auto p = plan::PropertiesMapList{};
  p.push_back(std::make_pair(msgs::PropertyId::FromUint(3), &literal));
  node.properties.emplace<0>(std::move(p));

  auto once_op = std::make_shared<plan::Once>();

  auto create_expand = plan::CreateNode(once_op, node);
  auto cursor = create_expand.MakeCursor(utils::NewDeleteResource());
  MockedRequestRouter router;
  EXPECT_CALL(router, CreateVertices(_)).Times(1).WillOnce(Return(std::vector<msgs::CreateVerticesResponse>{}));
  EXPECT_CALL(router, IsPrimaryLabel(_)).WillRepeatedly(Return(true));
  EXPECT_CALL(router, IsPrimaryKey(_, _)).WillRepeatedly(Return(true));
  auto context = MakeContext(ast, symbol_table, &router, &id_alloc);
  auto multi_frame = CreateMultiFrame(context.symbol_table.max_position());
  cursor->PullMultiple(multi_frame, context);

  auto frames = multi_frame.GetValidFramesReader();
  auto number_of_valid_frames = 0;
  for (auto &frame : frames) {
    ++number_of_valid_frames;
    EXPECT_EQ(frame[node.symbol].IsVertex(), true);
    const auto &n = frame[node.symbol].ValueVertex();
    EXPECT_THAT(n.Labels(), IsEmpty());
    EXPECT_EQ(n.PrimaryLabel(), primary_label_id);
    // TODO(antaljanosbenjamin): Check primary key
  }
  EXPECT_EQ(number_of_valid_frames, 1);

  auto invalid_frames = multi_frame.GetInvalidFramesPopulator();
  auto number_of_invalid_frames = std::distance(invalid_frames.begin(), invalid_frames.end());
  EXPECT_EQ(number_of_invalid_frames, 99);
}
}  // namespace memgraph::query::v2::tests
