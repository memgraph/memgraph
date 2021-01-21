#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

#include "utils/spin_lock.hpp"

int x = 0;
utils::SpinLock lock;

void test_lock() {
  using namespace std::literals;

  {
    std::unique_lock<utils::SpinLock> guard(lock);
    x++;

    std::this_thread::sleep_for(25ms);

    MG_ASSERT(x < 2,
              "x always has to be less than 2 (other "
              "threads shouldn't be able to change the x simultaneously");
    x--;
  }
}

int main() {
  constexpr int N = 16;
  std::vector<std::thread> threads;

  for (int i = 0; i < N; ++i) threads.push_back(std::thread(test_lock));

  for (auto &thread : threads) {
    thread.join();
  }

  return 0;
}
