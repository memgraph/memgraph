#include <thread>
#include <vector>

#include "transactions/engine.hpp"
#include "utils/assert.hpp"

int main() {
  // (try to) test correctness of the transaction life cycle
  constexpr int THREADS = 16;
  constexpr int TRANSACTIONS = 10;

  tx::Engine engine;
  std::vector<uint64_t> sums;

  sums.resize(THREADS);

  auto f = [&engine, &sums](int idx, int n) {
    uint64_t sum = 0;

    for (int i = 0; i < n; ++i) {
      auto& t = engine.begin();
      sum += t.id;
      engine.commit(t);
    }

    sums[idx] = sum;
  };

  std::vector<std::thread> threads;

  for (int i = 0; i < THREADS; ++i)
    threads.push_back(std::thread(f, i, TRANSACTIONS));

  for (auto& thread : threads) thread.join();

  uint64_t sum_computed = 0;

  for (int i = 0; i < THREADS; ++i) sum_computed += sums[i];

  uint64_t sum_actual = 0;
  for (uint64_t i = 2; i <= THREADS * TRANSACTIONS + 1; ++i) sum_actual += i;
  // the range is strange because the first transaction gets transaction id 2

  std::cout << sum_computed << " " << sum_actual << std::endl;
  permanent_assert(sum_computed == sum_actual, "sums have to be the same");
}
