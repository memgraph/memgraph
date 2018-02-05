#include <gtest/gtest.h>

#include "database/graph_db_accessor.hpp"

#include "distributed_common.hpp"

TEST_F(DistributedGraphDbTest, RemoteUpdateLocalVisibility) {
  database::GraphDbAccessor dba_tx1{worker1()};
  auto v = dba_tx1.InsertVertex();
  auto v_ga = v.GlobalAddress();
  dba_tx1.Commit();

  database::GraphDbAccessor dba_tx2_w2{worker2()};
  v = VertexAccessor(v_ga, dba_tx2_w2);
  ASSERT_FALSE(v.address().is_local());
  auto label = dba_tx2_w2.Label("l");
  EXPECT_FALSE(v.has_label(label));
  v.add_label(label);
  v.SwitchNew();
  EXPECT_TRUE(v.has_label(label));
  v.SwitchOld();
  EXPECT_FALSE(v.has_label(label));

  // In the same transaction on the owning worker there is no label.
  database::GraphDbAccessor dba_tx2_w1{worker1(), dba_tx2_w2.transaction_id()};
  v = VertexAccessor(v_ga, dba_tx2_w1);
  v.SwitchOld();
  EXPECT_FALSE(v.has_label(label));
  v.SwitchNew();
  EXPECT_FALSE(v.has_label(label));
}
