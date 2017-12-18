#include <chrono>
#include <memory>
#include <thread>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "data_structures/concurrent/skiplist_gc.hpp"

/**
 * FakeItem class which increments a variable in the destructor.
 * Used to keep track of the number of destroyed elements in GC.
 */
class FakeItem {
 public:
  FakeItem(std::atomic<int> &count, int value) : count(count), value(value) {}
  ~FakeItem() { count.fetch_add(1); }

  bool operator<(const FakeItem &item) const {
    return this->value < item.value;
  }
  bool operator>(const FakeItem &item) const {
    return this->value > item.value;
  }

  static void destroy(FakeItem *item) { delete item; }

 private:
  std::atomic<int> &count;
  int value;
};

DECLARE_int32(skiplist_gc_interval);

TEST(SkipListGC, CreateNewAccessors) {
  FLAGS_skiplist_gc_interval = -1;
  SkipListGC<FakeItem> gc;
  auto &accessor1 = gc.CreateNewAccessor();
  auto &accessor2 = gc.CreateNewAccessor();
  auto &accessor3 = gc.CreateNewAccessor();

  EXPECT_EQ(accessor1.id_, 1);
  EXPECT_EQ(accessor2.id_, 2);
  EXPECT_EQ(accessor3.id_, 3);

  accessor1.alive_ = false;
  accessor2.alive_ = false;
  accessor3.alive_ = false;
}

TEST(SkipListGC, DeleteItem) {
  FLAGS_skiplist_gc_interval = -1;
  SkipListGC<FakeItem> gc;
  auto &accessor1 = gc.CreateNewAccessor();
  std::atomic<int> count{0};
  auto item1 = new FakeItem(count, 1);
  gc.Collect(item1);

  // Kill the accesssor
  accessor1.alive_ = false;
  gc.GarbageCollect();
  EXPECT_EQ(count, 1);
}

TEST(SkipListGC, DontDeleteItem) {
  FLAGS_skiplist_gc_interval = -1;
  SkipListGC<FakeItem> gc;
  auto &accessor1 = gc.CreateNewAccessor();
  auto &accessor2 = gc.CreateNewAccessor();
  std::atomic<int> count{0};
  auto item1 = new FakeItem(count, 1);
  gc.Collect(item1);

  // Kill the accesssor
  accessor2.alive_ = false;

  // Nothing deleted because accessor1 is blocking.
  gc.GarbageCollect();
  EXPECT_EQ(count, 0);

  // Accessor 1 is not blocking anymore.
  accessor1.alive_ = false;
  gc.GarbageCollect();
  EXPECT_EQ(count, 1);
}

TEST(SkipListGC, Destructor) {
  FLAGS_skiplist_gc_interval = -1;
  std::atomic<int> count{0};
  auto item1 = new FakeItem(count, 1);
  {
    SkipListGC<FakeItem> gc;
    gc.Collect(item1);
    EXPECT_EQ(count, 0);
  }
  EXPECT_EQ(count, 1);
}

TEST(SkipListGC, MultipleDeletes) {
  FLAGS_skiplist_gc_interval = -1;
  SkipListGC<FakeItem> gc;
  std::atomic<int> count{0};
  auto &accessor1 = gc.CreateNewAccessor();
  auto item1 = new FakeItem(count, 1);
  gc.Collect(item1);

  auto &accessor2 = gc.CreateNewAccessor();
  auto item2 = new FakeItem(count, 1);
  gc.Collect(item2);

  auto &accessor3 = gc.CreateNewAccessor();
  auto item3 = new FakeItem(count, 1);
  gc.Collect(item3);

  accessor1.alive_ = false;
  accessor2.alive_ = false;
  gc.GarbageCollect();
  EXPECT_EQ(count, 2);

  accessor3.alive_ = false;
  gc.GarbageCollect();
  EXPECT_EQ(count, 3);
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
