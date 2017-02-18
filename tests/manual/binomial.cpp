/* Plots the distribution histogram of the fast_binomial algorithm
 * (spoiler alert: it's pleasingly (1/2)^N all the way :D)
 */
#include <array>
#include <atomic>
#include <iomanip>
#include <iostream>
#include <thread>

#include <sys/ioctl.h>
#include <unistd.h>

#include "utils/random/fast_binomial.hpp"

static constexpr unsigned B = 24;
static thread_local FastBinomial<B> rnd;

static constexpr unsigned M = 4;
static constexpr size_t N = 1ULL << 34;
static constexpr size_t per_thread_iters = N / M;

std::array<std::atomic<uint64_t>, B> buckets;

void generate() {
  for (size_t i = 0; i < per_thread_iters; ++i) buckets[rnd() - 1].fetch_add(1);
}

int main(void) {
  struct winsize w;
  ioctl(STDOUT_FILENO, TIOCGWINSZ, &w);

  auto bar_len = w.ws_col - 20;

  std::array<std::thread, M> threads;

  for (auto& bucket : buckets) bucket.store(0);

  for (auto& t : threads) t = std::thread([]() { generate(); });

  for (auto& t : threads) t.join();

  auto max = std::accumulate(
      buckets.begin(), buckets.end(), (uint64_t)0,
      [](auto& acc, auto& x) { return std::max(acc, x.load()); });

  std::cout << std::fixed;

  for (size_t i = 0; i < buckets.size(); ++i) {
    auto x = buckets[i].load();
    auto rel = bar_len * x / max;

    std::cout << std::setw(2) << i + 1 << " ";

    for (size_t i = 0; i < rel; ++i) std::cout << "=";

    std::cout << " " << 100 * (double)x / N << "%" << std::endl;
  }

  return 0;
}
