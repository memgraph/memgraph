#include <vector>
#include "gtest/gtest.h"

#include "mvcc/record.hpp"
#include "mvcc/version.hpp"
#include "mvcc/version_list.hpp"
#include "threading/sync/lock_timeout_exception.hpp"
#include "transactions/engine.hpp"
#include "transactions/transaction.cpp"

#include "mvcc_gc_common.hpp"

class TestClass : public mvcc::Record<TestClass> {};

TEST(MVCC, Deadlock) {
  tx::Engine engine;

  auto t0 = engine.Begin();
  mvcc::VersionList<TestClass> version_list1(*t0);
  mvcc::VersionList<TestClass> version_list2(*t0);
  t0->Commit();

  auto t1 = engine.Begin();
  auto t2 = engine.Begin();

  version_list1.update(*t1);
  version_list2.update(*t2);
  EXPECT_THROW(version_list1.update(*t2), LockTimeoutException);
}

// TODO Gleich: move this test to mvcc_gc???
// check that we don't delete records when we re-link
TEST(MVCC, UpdateDontDelete) {
  std::atomic<int> count{0};
  {
    tx::Engine engine;
    auto t1 = engine.Begin();
    mvcc::VersionList<DestrCountRec> version_list(*t1, count);
    t1->Commit();

    auto t2 = engine.Begin();
    version_list.update(*t2);
    t2->Abort();
    EXPECT_EQ(count, 0);

    auto t3 = engine.Begin();

    // Update re-links the node and shouldn't clear it yet.
    version_list.update(*t3);
    EXPECT_EQ(count, 0);

    // TODO Gleich: why don't we also test that remove doesn't delete?
    t3->Commit();
  }
  EXPECT_EQ(count, 3);
}

// Check that we get the oldest record.
TEST(MVCC, Oldest) {
  tx::Engine engine;
  auto t1 = engine.Begin();
  mvcc::VersionList<TestClass> version_list(*t1);
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
