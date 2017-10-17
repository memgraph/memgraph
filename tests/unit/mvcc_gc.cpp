
#include <chrono>
#include <memory>
#include <thread>

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "data_structures/concurrent/skiplist.hpp"
#include "mvcc/record.hpp"
#include "mvcc/version_list.hpp"
#include "storage/garbage_collector.hpp"
#include "storage/vertex.hpp"
#include "transactions/engine.hpp"

#include "mvcc_gc_common.hpp"

class MvccGcTest : public ::testing::Test {
 protected:
  tx::Engine engine;

 private:
  tx::Transaction *t0 = engine.Begin();

 protected:
  std::atomic<int> record_destruction_count{0};
  mvcc::VersionList<DestrCountRec> version_list{*t0, record_destruction_count};
  std::vector<tx::Transaction *> transactions{t0};

  void SetUp() override { t0->Commit(); }

  void MakeUpdates(int update_count, bool commit) {
    for (int i = 0; i < update_count; i++) {
      auto t = engine.Begin();
      version_list.update(*t);
      if (commit)
        t->Commit();
      else
        t->Abort();
    }
  }

  auto GcDeleted(tx::Transaction *latest = nullptr) {
    return version_list.GcDeleted(GcSnapshot(engine, latest), engine);
  }
};

TEST_F(MvccGcTest, RemoveAndAbort) {
  auto t = engine.Begin();
  version_list.remove(version_list.find(*t), *t);
  t->Abort();
  auto ret = GcDeleted();
  EXPECT_EQ(ret.first, false);
  EXPECT_EQ(ret.second, nullptr);
  EXPECT_EQ(record_destruction_count, 0);
}

TEST_F(MvccGcTest, UpdateAndAbort) {
  MakeUpdates(1, false);
  auto ret = GcDeleted();
  EXPECT_EQ(ret.first, false);
  EXPECT_EQ(ret.second, nullptr);
  EXPECT_EQ(record_destruction_count, 0);

  MakeUpdates(3, false);
  ret = GcDeleted();
  EXPECT_EQ(ret.first, false);
  EXPECT_EQ(ret.second, nullptr);
  EXPECT_EQ(record_destruction_count, 0);
}

TEST_F(MvccGcTest, RemoveAndCommit) {
  auto t = engine.Begin();
  version_list.remove(version_list.find(*t), *t);
  t->Commit();
  auto ret = GcDeleted();
  EXPECT_EQ(ret.first, true);
  EXPECT_NE(ret.second, nullptr);
  delete ret.second;
  EXPECT_EQ(record_destruction_count, 1);
}

TEST_F(MvccGcTest, UpdateAndCommit) {
  MakeUpdates(4, true);
  auto ret = GcDeleted();
  EXPECT_EQ(ret.first, false);
  EXPECT_NE(ret.second, nullptr);
  delete ret.second;
  EXPECT_EQ(record_destruction_count, 4);
}

TEST_F(MvccGcTest, OldestTransactionSnapshot) {
  // this test validates that we can't delete
  // a record that has been expired by a transaction (t1)
  // committed before GC starts (when t2 is oldest),
  // if t1 is in t2's snapshot.
  // this is because there could exist transcation t3
  // that also has t1 in it's snapshot, and consequently
  // does not see the expiration and sees the record
  auto t1 = engine.Begin();
  auto t2 = engine.Begin();
  version_list.remove(version_list.find(*t1), *t1);
  t1->Commit();

  auto ret = GcDeleted(t2);
  EXPECT_EQ(ret.first, false);
  EXPECT_EQ(ret.second, nullptr);
  EXPECT_EQ(record_destruction_count, 0);
}

/**
 * Test integration of garbage collector with MVCC GC. Delete version lists
 * which are
 * empty (not visible from any future transaction) from the skiplist.
 */
TEST(GarbageCollector, GcClean) {
  SkipList<mvcc::VersionList<DestrCountRec> *> skiplist;
  tx::Engine engine;
  DeferredDeleter<DestrCountRec> deleter;
  DeferredDeleter<mvcc::VersionList<DestrCountRec>> vlist_deleter;
  GarbageCollector<DestrCountRec> gc(skiplist, deleter, vlist_deleter);

  // create a version list in transaction t1
  auto t1 = engine.Begin();
  std::atomic<int> record_destruction_count{0};
  auto vl = new mvcc::VersionList<DestrCountRec>(*t1, record_destruction_count);
  auto access = skiplist.access();
  access.insert(vl);
  t1->Commit();

  // run garbage collection that has nothing co collect
  gc.Run(GcSnapshot(engine, nullptr), engine);
  EXPECT_EQ(deleter.Count(), 0);
  EXPECT_EQ(vlist_deleter.Count(), 0);
  EXPECT_EQ(record_destruction_count, 0);

  // delete the only record in the version-list in transaction t2
  auto t2 = engine.Begin();
  vl->remove(vl->find(*t2), *t2);
  t2->Commit();
  gc.Run(GcSnapshot(engine, nullptr), engine);

  // check that we destroyed the record
  EXPECT_EQ(deleter.Count(), 1);
  deleter.FreeExpiredObjects(engine.Count() + 1);
  EXPECT_EQ(deleter.Count(), 0);
  EXPECT_EQ(record_destruction_count, 1);

  // check that we destroyed the version list
  EXPECT_EQ(vlist_deleter.Count(), 1);
  vlist_deleter.FreeExpiredObjects(engine.Count() + 1);
  EXPECT_EQ(vlist_deleter.Count(), 0);

  EXPECT_EQ(access.size(), 0U);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}
