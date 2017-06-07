#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <chrono>
#include <memory>
#include <thread>

#include "data_structures/concurrent/skiplist.hpp"
#include "logging/logger.hpp"
#include "logging/streams/stdout.hpp"
#include "mvcc/record.hpp"
#include "mvcc/version_list.hpp"
#include "storage/garbage_collector.hpp"
#include "storage/vertex.hpp"
#include "transactions/engine.hpp"

#include "gc_common.hpp"

/**
 * Test will the mvcc gc delete records inside the version list because they
 * are not longer visible.
 */
TEST(VersionList, GcDeleted) {
  tx::Engine engine;

  // create a version_list with one record
  std::vector<uint64_t> ids;
  auto t1 = engine.begin();
  std::atomic<int> count{0};
  mvcc::VersionList<PropCount> version_list(*t1, count);
  ids.push_back(t1->id);
  t1->commit();

  // create some updates
  const int UPDATES = 10;
  for (int i = 0; i < UPDATES; ++i) {
    auto t2 = engine.begin();
    ids.push_back(t2->id);
    version_list.update(*t2);
    t2->commit();
  }

  // deleting with the first transaction does nothing
  {
    auto ret = version_list.GcDeleted(ids[0], engine);
    EXPECT_EQ(ret.first, false);
    EXPECT_EQ(ret.second, nullptr);
  }

  // deleting with the last transaction + 1 deletes
  // everything except the last update
  {
    auto ret = version_list.GcDeleted(ids.back() + 1, engine);
    EXPECT_EQ(ret.first, false);
    EXPECT_NE(ret.second, nullptr);
    delete ret.second;
    EXPECT_EQ(count, UPDATES);
  }

  // remove and abort, nothing gets deleted
  {
    auto t = engine.begin();
    version_list.remove(*t);
    auto id = t->id + 1;
    t->abort();
    auto ret = version_list.GcDeleted(id, engine);
    EXPECT_EQ(ret.first, false);
    EXPECT_EQ(ret.second, nullptr);
  }

  // update and abort, nothing gets deleted
  {
    auto t = engine.begin();
    version_list.update(*t);
    auto id = t->id + 1;
    t->abort();
    auto ret = version_list.GcDeleted(id, engine);
    EXPECT_EQ(ret.first, false);
    EXPECT_EQ(ret.second, nullptr);
  }

  // remove and commit, everything gets deleted
  {
    auto t = engine.begin();
    version_list.remove(*t);
    auto id = t->id + 1;
    t->commit();
    auto ret = version_list.GcDeleted(id, engine);
    EXPECT_EQ(ret.first, true);
    EXPECT_NE(ret.second, nullptr);
    delete ret.second;
    EXPECT_EQ(count, UPDATES + 2);
  }
}

/**
 * Test integration of garbage collector with MVCC GC. Delete mvcc's which are
 * empty (not visible from any future transaction) from the skiplist.
 */
TEST(GarbageCollector, GcClean) {
  SkipList<mvcc::VersionList<PropCount> *> skiplist;
  tx::Engine engine;
  DeferredDeleter<PropCount> deleter;
  DeferredDeleter<mvcc::VersionList<PropCount>> vlist_deleter;
  GarbageCollector<PropCount> gc(skiplist, deleter, vlist_deleter);

  auto t1 = engine.begin();
  std::atomic<int> count{0};
  auto vl = new mvcc::VersionList<PropCount>(*t1, count);

  auto access = skiplist.access();
  access.insert(vl);
  gc.Run(Id(2), engine);
  t1->commit();

  auto t2 = engine.begin();
  vl->remove(*t2);
  t2->commit();
  gc.Run(Id(3), engine);

  EXPECT_EQ(deleter.Count(), 1);
  deleter.FreeExpiredObjects(engine.count() + 1);
  EXPECT_EQ(deleter.Count(), 0);
  EXPECT_EQ(count, 1);

  EXPECT_EQ(vlist_deleter.Count(), 1);
  vlist_deleter.FreeExpiredObjects(engine.count() + 1);
  EXPECT_EQ(vlist_deleter.Count(), 0);

  EXPECT_EQ(access.size(), (size_t)0);
}

int main(int argc, char **argv) {
  ::logging::init_async();
  ::logging::log->pipe(std::make_unique<Stdout>());
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
