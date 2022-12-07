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
#include "query/v2/plan/operator.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/shard.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"

using namespace memgraph::query::v2;
using namespace memgraph::query::v2::plan;
namespace memgraph {
class TestTemplate : public testing::Test {
 protected:
  void SetUp() override {}
};

ExecutionContext MakeContext(const AstStorage &storage, const SymbolTable &symbol_table, RequestRouterInterface *router,
                             IdAllocator *id_alloc) {
  ExecutionContext context;
  context.symbol_table = symbol_table;
  context.evaluation_context.properties = NamesToProperties(storage.properties_, router);
  context.evaluation_context.labels = NamesToLabels(storage.labels_, router);
  context.edge_ids_alloc = id_alloc;
  context.request_router = router;
  return context;
}

MultiFrame CreateMultiFrame(const size_t max_pos, const Symbol &src, const Symbol &dst, MockedRequestRouter *router) {
  static constexpr size_t frame_size = 100;
  MultiFrame multi_frame(max_pos, frame_size, utils::NewDeleteResource());
  auto frames_populator = multi_frame.GetInvalidFramesPopulator();
  size_t i = 0;
  for (auto &frame : frames_populator) {
    frame.MakeValid();
    auto &src_acc = frame.at(src);
    auto &dst_acc = frame.at(dst);
    auto v1 = msgs::Vertex{.id = {{msgs::LabelId::FromUint(1)}, {msgs::Value(static_cast<int64_t>(i++))}}};
    auto v2 = msgs::Vertex{.id = {{msgs::LabelId::FromUint(1)}, {msgs::Value(static_cast<int64_t>(i++))}}};
    std::map<msgs::PropertyId, msgs::Value> mp;
    src_acc = TypedValue(query::v2::accessors::VertexAccessor(v1, mp, router));
    dst_acc = TypedValue(query::v2::accessors::VertexAccessor(v2, mp, router));
  }

  return multi_frame;
}

TEST_F(TestTemplate, CreateExpand) {
  MockedRequestRouter router;

  AstStorage ast;
  SymbolTable symbol_table;

  query::v2::plan::NodeCreationInfo node;
  query::v2::plan::EdgeCreationInfo edge;
  edge.edge_type = msgs::EdgeTypeId::FromUint(1);
  edge.direction = EdgeAtom::Direction::IN;
  auto id_alloc = IdAllocator(0, 100);

  const auto &src = symbol_table.CreateSymbol("n", true);
  node.symbol = symbol_table.CreateSymbol("u", true);

  auto create_expand = query::v2::plan::CreateExpand(node, edge, nullptr, src, true);
  auto cursor = create_expand.MakeCursor(utils::NewDeleteResource());

  EXPECT_CALL(router, CreateExpand(testing::_))
      .Times(1)
      .WillOnce(::testing::Return(std::vector<msgs::CreateExpandResponse>{}));
  auto context = MakeContext(ast, symbol_table, &router, &id_alloc);
  auto multi_frame = CreateMultiFrame(context.symbol_table.max_position(), src, node.symbol, &router);
  cursor->PullMultiple(multi_frame, context);
}
}  // namespace memgraph
