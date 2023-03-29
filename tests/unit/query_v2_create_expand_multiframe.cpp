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

#include <memory>
#include "mock_helpers.hpp"

#include "query/v2/bindings/frame.hpp"
#include "query/v2/bindings/symbol_table.hpp"
#include "query/v2/common.hpp"
#include "query/v2/context.hpp"
#include "query/v2/plan/operator.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/shard.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"

namespace memgraph::query::v2::tests {

MultiFrame CreateMultiFrame(const size_t max_pos, const Symbol &src, const Symbol &dst, MockedRequestRouter *router) {
  static constexpr size_t number_of_frames = 100;
  MultiFrame multi_frame(max_pos, number_of_frames, utils::NewDeleteResource());
  auto frames_populator = multi_frame.GetInvalidFramesPopulator();
  size_t i = 0;
  for (auto &frame : frames_populator) {
    auto &src_acc = frame.At(src);
    auto &dst_acc = frame.At(dst);
    auto v1 = msgs::Vertex{.id = {{msgs::LabelId::FromUint(1)}, {msgs::Value(static_cast<int64_t>(i++))}}};
    auto v2 = msgs::Vertex{.id = {{msgs::LabelId::FromUint(1)}, {msgs::Value(static_cast<int64_t>(i++))}}};
    std::map<msgs::PropertyId, msgs::Value> mp;
    src_acc = TypedValue(query::v2::accessors::VertexAccessor(v1, mp, router));
    dst_acc = TypedValue(query::v2::accessors::VertexAccessor(v2, mp, router));
  }

  multi_frame.MakeAllFramesInvalid();

  return multi_frame;
}

TEST(CreateExpandTest, Cursor) {
  using testing::_;
  using testing::Return;

  AstStorage ast;
  SymbolTable symbol_table;

  plan::NodeCreationInfo node;
  plan::EdgeCreationInfo edge;
  edge.edge_type = msgs::EdgeTypeId::FromUint(1);
  edge.direction = EdgeAtom::Direction::IN;
  edge.symbol = symbol_table.CreateSymbol("e", true);
  auto id_alloc = IdAllocator(0, 100);

  const auto &src = symbol_table.CreateSymbol("n", true);
  node.symbol = symbol_table.CreateSymbol("u", true);

  auto once_op = std::make_shared<plan::Once>();

  auto create_expand = plan::CreateExpand(node, edge, once_op, src, true);
  auto cursor = create_expand.MakeCursor(utils::NewDeleteResource());

  MockedRequestRouter router;
  EXPECT_CALL(router, CreateExpand(_))
      .Times(1)
      .WillOnce(Return(std::vector<msgs::CreateExpandResponse>{msgs::CreateExpandResponse{}}));
  auto context = MakeContext(ast, symbol_table, &router, &id_alloc);
  auto multi_frame = CreateMultiFrame(context.symbol_table.max_position(), src, node.symbol, &router);
  cursor->PullMultiple(multi_frame, context);

  auto frames = multi_frame.GetValidFramesReader();
  auto number_of_valid_frames = 0;
  for (auto &frame : frames) {
    ++number_of_valid_frames;
    EXPECT_EQ(frame[edge.symbol].IsEdge(), true);
    const auto &e = frame[edge.symbol].ValueEdge();
    EXPECT_EQ(e.EdgeType(), edge.edge_type);
  }
  EXPECT_EQ(number_of_valid_frames, 1);

  auto invalid_frames = multi_frame.GetInvalidFramesPopulator();
  auto number_of_invalid_frames = std::distance(invalid_frames.begin(), invalid_frames.end());
  EXPECT_EQ(number_of_invalid_frames, 99);
}

}  // namespace memgraph::query::v2::tests
