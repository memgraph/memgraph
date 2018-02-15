#include <gtest/gtest.h>

#include "database/graph_db_accessor.hpp"
#include "distributed/remote_updates_rpc_server.hpp"

#include "distributed_common.hpp"

class DistributedUpdateTest : public DistributedGraphDbTest {
 protected:
  std::unique_ptr<database::GraphDbAccessor> dba1;
  std::unique_ptr<database::GraphDbAccessor> dba2;
  storage::Label label;
  std::unique_ptr<VertexAccessor> v1_dba1;
  std::unique_ptr<VertexAccessor> v1_dba2;

  void SetUp() override {
    DistributedGraphDbTest::SetUp();

    database::GraphDbAccessor dba_tx1{worker(1)};
    auto v = dba_tx1.InsertVertex();
    auto v_ga = v.GlobalAddress();
    dba_tx1.Commit();

    dba1 = std::make_unique<database::GraphDbAccessor>(worker(1));
    dba2 = std::make_unique<database::GraphDbAccessor>(worker(2),
                                                       dba1->transaction_id());

    v1_dba1 = std::make_unique<VertexAccessor>(v_ga, *dba1);
    v1_dba2 = std::make_unique<VertexAccessor>(v_ga, *dba2);
    ASSERT_FALSE(v1_dba2->address().is_local());
    label = dba1->Label("l");
    v1_dba2->add_label(label);
  }

  void TearDown() override {
    dba2 = nullptr;
    dba1 = nullptr;
    DistributedGraphDbTest::TearDown();
  }
};

#define EXPECT_LABEL(var, old_result, new_result) \
  {                                               \
    var->SwitchOld();                             \
    EXPECT_EQ(var->has_label(label), old_result); \
    var->SwitchNew();                             \
    EXPECT_EQ(var->has_label(label), new_result); \
  }

TEST_F(DistributedUpdateTest, RemoteUpdateLocalOnly) {
  EXPECT_LABEL(v1_dba2, false, true);
  EXPECT_LABEL(v1_dba1, false, false);
}

TEST_F(DistributedUpdateTest, RemoteUpdateApply) {
  EXPECT_LABEL(v1_dba1, false, false);
  worker(1).remote_updates_server().Apply(dba1->transaction_id());
  EXPECT_LABEL(v1_dba1, false, true);
}

#undef EXPECT_LABEL
