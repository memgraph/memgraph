#include <iostream>

#include <thread>
#include <vector>

#include "utils/assert.hpp"
#include "utils/measure_time.hpp"
#include "utils/memory/allocator.hpp"
#include "utils/memory/maker.hpp"

struct TestStructure {
  TestStructure(int a, int b, int c, int d) : a(a), b(b), c(c), d(d) {}
  int a, b, c, d;
};

void test_classic(int N) {
  TestStructure** xs = new TestStructure*[N];
  for (int i = 0; i < N; ++i) xs[i] = new TestStructure(i, i, i, i);
  for (int i = 0; i < N; ++i) delete xs[i];
  delete[] xs;
}

void test_fast(int N) {
  TestStructure** xs = new TestStructure*[N];
  for (int i = 0; i < N; ++i) xs[i] = makeme<TestStructure>(i, i, i, i);
  for (int i = 0; i < N; ++i) delete xs[i];
  delete[] xs;
}

int main(void) {
  constexpr int n_threads = 32;
  constexpr int N = 80000000 / n_threads;

  auto elapsed_classic = utils::measure_time<std::chrono::milliseconds>([&]() {
    std::vector<std::thread> threads;
    for (int i = 0; i < n_threads; ++i)
      threads.push_back(std::thread(test_classic, N));
    for (auto& thread : threads) {
      thread.join();
    }
  });
  std::cout << "Classic (new): " << elapsed_classic << "ms" << std::endl;

  auto elapsed_fast = utils::measure_time<std::chrono::milliseconds>([&]() {
    std::vector<std::thread> threads;
    for (int i = 0; i < n_threads; ++i)
      threads.push_back(std::thread(test_fast, N));
    for (auto& thread : threads) {
      thread.join();
    }
  });
  std::cout << "Fast (fast allocator): " << elapsed_fast << "ms" << std::endl;
  permanent_assert(elapsed_fast < elapsed_classic,
                   "Custom fast allocator "
                   "has to perform faster on simple array allocation");

  return 0;
}
