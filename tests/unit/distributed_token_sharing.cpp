#include "distributed_common.hpp"

#include <memory>
#include <thread>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"

DECLARE_bool(dynamic_graph_partitioner_enabled);
DECLARE_int32(dgp_max_batch_size);

using namespace distributed;
using namespace database;

class TokenSharingTest : public DistributedGraphDbTest {
  void SetUp() override {
    FLAGS_dynamic_graph_partitioner_enabled = true;
    FLAGS_dgp_max_batch_size = 1;
    DistributedGraphDbTest::SetUp();
  }
};

TEST_F(TokenSharingTest, Integration) {
  auto vb = InsertVertex(worker(1));
  for (int i = 0; i < 100; ++i) {
    auto v = InsertVertex(master());
    InsertEdge(vb, v, "edge");
  }
  std::this_thread::sleep_for(std::chrono::seconds(3));
  // Migrate at least something from or to here
  EXPECT_NE(VertexCount(master()), 100);
}
