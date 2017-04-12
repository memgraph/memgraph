#include "gtest/gtest.h"

#include <chrono>
#include <memory>
#include <thread>

#include "data_structures/concurrent/skiplist.hpp"
#include "logging/streams/stderr.hpp"

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

 private:
  std::atomic<int> &count;
  int value;
};

TEST(SkipListGC, TripleScopeGC) {
  SkipList<FakeItem> skiplist;
  std::atomic<int> count{0};
  auto item = FakeItem(count, 1);
  {
    auto access_1 = skiplist.access();
    {
      auto access_2 = skiplist.access();
      {
        auto access_3 = skiplist.access();
        access_1.insert(item);  // add with 1
        access_2.remove(item);  // remove with 2
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        EXPECT_EQ(count, 0);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      EXPECT_EQ(count, 0);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(count, 0);
  }  // scope end - GC called
  for (int i = 0; i < 10; ++i) {
    if (count != 0) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  EXPECT_EQ(count, 1);
}

TEST(SkipListGC, BlockedGCNoGC) {
  SkipList<FakeItem> skiplist;
  std::atomic<int> count{0};
  auto item = FakeItem(count, 1);
  auto blocking_access = skiplist.access();
  {
    auto access = skiplist.access();
    access.insert(item);
    access.remove(item);
  }  // scope end - GC still isn't called because of blocking_access
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(count, 0);
}

TEST(SkipListGC, NotInScopeGC) {
  SkipList<FakeItem> skiplist;
  std::atomic<int> count{0};
  auto item = FakeItem(count, 1);
  {
    auto access = skiplist.access();
    access.insert(item);
    access.remove(item);
  }  // scope end - GC called
  for (int i = 0; i < 10; ++i) {
    if (count != 0) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  EXPECT_EQ(count, 1);
}

TEST(SkipListGC, StillInScopeNoGC) {
  SkipList<FakeItem> skiplist;
  std::atomic<int> count{0};
  auto item = FakeItem(count, 1);
  auto access = skiplist.access();
  access.insert(item);
  access.remove(item);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(count, 0);
}

int main(int argc, char **argv) {
  logging::init_sync();
  logging::log->pipe(std::make_unique<Stderr>());
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
