#include <chrono>
#include <memory>
#include <thread>

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "mvcc/single_node/record.hpp"
#include "mvcc/single_node/version_list.hpp"
#include "storage/single_node/garbage_collector.hpp"
#include "storage/single_node/vertex.hpp"
#include "transactions/single_node/engine.hpp"

#include "mvcc_gc_common.hpp"

class MvccGcTest : public ::testing::Test {
 protected:
  tx::Engine engine;

 private:
  tx::Transaction *t0 = engine.Begin();

 protected:
  std::atomic<int> record_destruction_count{0};
  mvcc::VersionList<DestrCountRec> version_list{*t0, 0, 0,
                                                record_destruction_count};
  std::vector<tx::Transaction *> transactions{t0};

  void SetUp() override { engine.Commit(*t0); }

  void MakeUpdates(int update_count, bool commit) {
    for (int i = 0; i < update_count; i++) {
      auto t = engine.Begin();
      version_list.update(*t);
      if (commit)
        engine.Commit(*t);
      else
        engine.Abort(*t);
    }
  }

  auto GcDeleted(tx::Transaction *latest = nullptr) {
    return version_list.GcDeleted(GcSnapshot(engine, latest), engine);
  }
};

TEST_F(MvccGcTest, RemoveAndAbort) {
  auto t = engine.Begin();
  version_list.remove(version_list.find(*t), *t);
  engine.Abort(*t);
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
  engine.Commit(*t);
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
  engine.Commit(*t1);

  auto ret = GcDeleted(t2);
  EXPECT_EQ(ret.first, false);
  EXPECT_EQ(ret.second, nullptr);
  EXPECT_EQ(record_destruction_count, 0);
}

/**
 * Test integration of garbage collector with MVCC GC. Delete version lists
 * which are empty (not visible from any future transaction) from the skiplist.
 */
TEST(GarbageCollector, GcClean) {
  ConcurrentMap<int64_t, mvcc::VersionList<DestrCountRec> *> collection;
  tx::Engine engine;
  DeferredDeleter<DestrCountRec> deleter;
  DeferredDeleter<mvcc::VersionList<DestrCountRec>> vlist_deleter;
  GarbageCollector<decltype(collection), DestrCountRec> gc(collection, deleter,
                                                           vlist_deleter);

  // create a version list in transaction t1
  auto t1 = engine.Begin();
  std::atomic<int> record_destruction_count{0};
  auto vl =
      new mvcc::VersionList<DestrCountRec>(*t1, 0, 0, record_destruction_count);
  auto access = collection.access();
  access.insert(0, vl);
  engine.Commit(*t1);

  // run garbage collection that has nothing co collect
  gc.Run(GcSnapshot(engine, nullptr), engine);
  EXPECT_EQ(deleter.Count(), 0);
  EXPECT_EQ(vlist_deleter.Count(), 0);
  EXPECT_EQ(record_destruction_count, 0);

  // delete the only record in the version-list in transaction t2
  auto t2 = engine.Begin();
  vl->remove(vl->find(*t2), *t2);
  engine.Commit(*t2);
  gc.Run(GcSnapshot(engine, nullptr), engine);

  // check that we destroyed the record
  EXPECT_EQ(deleter.Count(), 1);
  deleter.FreeExpiredObjects(3);
  EXPECT_EQ(deleter.Count(), 0);
  EXPECT_EQ(record_destruction_count, 1);

  // check that we destroyed the version list
  EXPECT_EQ(vlist_deleter.Count(), 1);
  vlist_deleter.FreeExpiredObjects(3);
  EXPECT_EQ(vlist_deleter.Count(), 0);

  EXPECT_EQ(access.size(), 0U);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}
