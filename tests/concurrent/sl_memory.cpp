#include "common.hpp"

constexpr size_t THREADS_NO = std::min(max_no_threads, 8);
constexpr size_t elements = 2e6;

/**
 * Put elements number of elements in the skiplist per each thread and see
 * is there any memory leak
 */
int main(int, char **argv) {
  google::InitGoogleLogging(argv[0]);
  map_t skiplist;

  auto futures = run<size_t>(THREADS_NO, skiplist, [](auto acc, auto index) {
    for (size_t i = 0; i < elements; i++) {
      acc.insert(i, index);
    }
    return index;
  });
  collect(futures);

  auto accessor = skiplist.access();
  check_size(accessor, elements);
}
