#include <chrono>
#include <mutex>
#include <random>
#include <thread>

#include "threading/sync/futex.hpp"
#include "utils/assert.hpp"

Futex futex;
int x = 0;

/**
 * @param thread id
 */
void test_lock(int) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 1000);

  // TODO: create long running test
  for (int i = 0; i < 5; ++i) {
    {
      std::unique_lock<Futex> guard(futex);
      x++;
      std::this_thread::sleep_for(std::chrono::milliseconds(dis(gen)));
      CHECK(x == 1) << "Other thread shouldn't be able to "
                       "change the value of x";
      x--;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(dis(gen)));
  }
}

int main(void) {
  constexpr int N = 16;
  std::vector<std::thread> threads;

  for (int i = 0; i < N; ++i) threads.push_back(std::thread(test_lock, i));

  for (auto& thread : threads) {
    thread.join();
  }

  return 0;
}
