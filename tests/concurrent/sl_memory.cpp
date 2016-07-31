#include "common.h"

#define THREADS_NO 8

constexpr size_t elements = 2e6;

// Test for simple memory leaks
int main()
{
  memory_check(THREADS_NO, [] {
    skiplist_t skiplist;

    auto futures = run<size_t>(THREADS_NO, skiplist, [](auto acc, auto index) {
      for (size_t i = 0; i < elements; i++) {
        acc.insert(i, index);
      }
      return index;
    });
    collect(futures);
    check_size(skiplist.access(), elements);
  });
}
