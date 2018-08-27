#include "gtest/gtest.h"

#include "distributed/plan_dispatcher.hpp"
#include "distributed_common.hpp"
#include "query/context.hpp"
#include "query/plan/distributed_ops.hpp"
#include "query/interpret/frame.hpp"

class DistributedReset : public DistributedGraphDbTest {
 protected:
  DistributedReset() : DistributedGraphDbTest("reset") {}
};

TEST_F(DistributedReset, ResetTest) {
  query::SymbolTable symbol_table;
  auto once = std::make_shared<query::plan::Once>();
  auto pull_remote = std::make_shared<query::plan::PullRemote>(
      once, 42, std::vector<query::Symbol>());
  master().plan_dispatcher().DispatchPlan(42, once, symbol_table);
  auto dba = master().Access();
  query::Frame frame(0);
  query::Context context(*dba);
  auto pull_remote_cursor = pull_remote->MakeCursor(*dba);

  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(pull_remote_cursor->Pull(frame, context));
  }
  EXPECT_FALSE(pull_remote_cursor->Pull(frame, context));

  pull_remote_cursor->Reset();

  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(pull_remote_cursor->Pull(frame, context));
  }
  EXPECT_FALSE(pull_remote_cursor->Pull(frame, context));
}
