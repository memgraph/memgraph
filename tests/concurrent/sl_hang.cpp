#include "gtest/gtest.h"

#include <thread>
#include <vector>

#include "data_structures/concurrent/skiplist.hpp"

// Try to provoke find_or_larger to hang. This happened before and caused
// Jenkins to stop responding. It is hard to recreate deterministically and this
// is the best we can do without doing friend_tests or refactoring skiplist.
TEST(SkipList, HangDuringFindOrLarger) {
  std::vector<std::thread> threads;
  SkipList<int> skiplist;
  const int num_of_threads = 8;
  const int iter = 100000;
  for (int i = 0; i < num_of_threads; ++i) {
    threads.emplace_back([&iter, &skiplist]() {
      auto accessor = skiplist.access();
      for (int i = 0; i < iter; ++i) accessor.insert(rand() % 3);
    });
    threads.emplace_back([&iter, &skiplist]() {
      auto accessor = skiplist.access();
      for (int i = 0; i < iter; ++i) accessor.remove(rand() % 3);
    });
    threads.emplace_back([&iter, &skiplist]() {
      auto accessor = skiplist.access();
      for (int i = 0; i < iter; ++i)
        accessor.find_or_larger(rand() % 3);
    });
  }
  for (auto &thread : threads) thread.join();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
