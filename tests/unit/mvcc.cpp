#include <vector>
#include "gtest/gtest.h"

#include "mvcc/record.hpp"
#include "mvcc/version.hpp"
#include "mvcc/version_list.hpp"
#include "transactions/engine_single_node.hpp"
#include "transactions/transaction.hpp"
#include "utils/thread/sync.hpp"

#include "mvcc_gc_common.hpp"

TEST(MVCC, Deadlock) {
  tx::SingleNodeEngine engine;

  auto t0 = engine.Begin();
  mvcc::VersionList<Prop> version_list1(*t0, 0, 0);
  mvcc::VersionList<Prop> version_list2(*t0, 1, 1);
  engine.Commit(*t0);

  auto t1 = engine.Begin();
  auto t2 = engine.Begin();

  version_list1.update(*t1);
  version_list2.update(*t2);
  EXPECT_THROW(version_list1.update(*t2), utils::LockTimeoutException);
}

// TODO Gleich: move this test to mvcc_gc???
// check that we don't delete records when we re-link
TEST(MVCC, UpdateDontDelete) {
  std::atomic<int> count{0};
  {
    tx::SingleNodeEngine engine;
    auto t1 = engine.Begin();
    mvcc::VersionList<DestrCountRec> version_list(*t1, 0, 0, count);
    engine.Commit(*t1);

    auto t2 = engine.Begin();
    version_list.update(*t2);
    engine.Abort(*t2);
    EXPECT_EQ(count, 0);

    auto t3 = engine.Begin();

    // Update re-links the node and shouldn't clear it yet.
    version_list.update(*t3);
    EXPECT_EQ(count, 0);

    // TODO Gleich: why don't we also test that remove doesn't delete?
    engine.Commit(*t3);
  }
  EXPECT_EQ(count, 3);
}

// Check that we get the oldest record.
TEST(MVCC, Oldest) {
  tx::SingleNodeEngine engine;
  auto t1 = engine.Begin();
  mvcc::VersionList<Prop> version_list(*t1, 0, 0);
  auto first = version_list.Oldest();
  EXPECT_NE(first, nullptr);
  // TODO Gleich: no need to do 10 checks of the same thing
  for (int i = 0; i < 10; ++i) {
    engine.Advance(t1->id_);
    version_list.update(*t1);
    EXPECT_EQ(version_list.Oldest(), first);
  }
  // TODO Gleich: what about remove?
  // TODO Gleich: here it might make sense to write a concurrent test
  //    since these ops rely heavily on linkage atomicity?
}

// TODO Gleich: perhaps some concurrent VersionList::find tests?
