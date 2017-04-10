#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <chrono>
#include <memory>

#include "config/config.hpp"
#include "data_structures/concurrent/skiplist.hpp"
#include "logging/logger.hpp"
#include "logging/streams/stdout.hpp"
#include "mvcc/record.hpp"
#include "mvcc/version_list.hpp"
#include "storage/garbage_collector.hpp"
#include "storage/vertex.hpp"
#include "transactions/engine.hpp"

/**
 * Class which takes an atomic variable to count number of destructor calls (to
 * test if GC is actually deleting records).
 */
class Prop : public mvcc::Record<Prop> {
 public:
  Prop(std::atomic<int> &count) : count_(count) {}
  ~Prop() { ++count_; }

 private:
  std::atomic<int> &count_;
};

/**
 * Test will the mvcc gc delete records inside the version list because they
 * are not longer visible.
 */
TEST(VersionList, GcDeleted) {
  const int UPDATES = 10;
  tx::Engine engine;
  std::vector<uint64_t> ids;
  auto t1 = engine.begin();
  std::atomic<int> count{0};
  mvcc::VersionList<Prop> version_list(*t1, count);
  ids.push_back(t1->id);
  t1->commit();

  for (int i = 0; i < UPDATES; ++i) {
    auto t2 = engine.begin();
    ids.push_back(t2->id);
    version_list.update(*t2);
    t2->commit();
  }

  EXPECT_EQ(version_list.GcDeleted(ids[0]), false);
  EXPECT_EQ(count, 0);
  EXPECT_EQ(version_list.GcDeleted(ids.back() + 1), false);
  EXPECT_EQ(count, UPDATES);

  auto tl = engine.begin();
  version_list.remove(*tl);
  EXPECT_EQ(version_list.GcDeleted(tl->id + 1), true);
  EXPECT_EQ(count, UPDATES + 1);
  tl->commit();
}

/**
 * Test integration of garbage collector with MVCC GC. Delete mvcc's which are
 * empty (not visible from any future transaction) from the skiplist.
 */
TEST(GarbageCollector, GcClean) {
  SkipList<mvcc::VersionList<Prop> *> skiplist;
  tx::Engine engine;
  GarbageCollector<Prop> gc(&skiplist, &engine);

  auto t1 = engine.begin();
  std::atomic<int> count;
  auto vl = new mvcc::VersionList<Prop>(*t1, count);

  auto access = skiplist.access();
  access.insert(vl);
  gc.Run();
  t1->commit();
  gc.Run();

  auto t2 = engine.begin();
  EXPECT_EQ(vl->remove(*t2), true);
  t2->commit();

  gc.Run();
  EXPECT_EQ(access.size(), (size_t)0);
}

int main(int argc, char **argv) {
  ::logging::init_async();
  ::logging::log->pipe(std::make_unique<Stdout>());
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
