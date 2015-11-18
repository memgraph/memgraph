/* Plots the distribution histogram of the xorshift algorithm
 * (spoiler alert: it's pleasingly uniform all the way :D)
 */
#include <iostream>
#include <array>
#include <atomic>
#include <thread>

#include <sys/ioctl.h>
#include <unistd.h>

#include "utils/random/xorshift128plus.hpp"

static thread_local Xorshift128plus rnd;
static constexpr unsigned B = 1 << 10;
static constexpr uint64_t K = (uint64_t)(-1) / B;

static constexpr unsigned M = 4;
static constexpr size_t N = 1ULL << 34;
static constexpr size_t per_thread_iters = N / M;

std::array<std::atomic<unsigned>, B> buckets;

void generate()
{
    for(size_t i = 0; i < per_thread_iters; ++i)
        buckets[rnd() / K].fetch_add(1);
}

int main(void)
{
    struct winsize w;
    ioctl(STDOUT_FILENO, TIOCGWINSZ, &w);

    auto bar_len = w.ws_col - 20;

    std::array<std::thread, M> threads;

    for(auto& bucket : buckets)
        bucket.store(0);

    for(auto& t : threads)
        t = std::thread([]() { generate(); });

    for(auto& t : threads)
        t.join();

    auto max = std::accumulate(buckets.begin(), buckets.end(), 0u,
        [](auto& acc, auto& x) { return std::max(acc, x.load()); });

    std::cout << std::fixed;

    for(auto& bucket : buckets)
    {
        auto x = bucket.load();
        auto rel = bar_len * x / max;

        for(size_t i = 0; i < rel; ++i)
            std::cout << "=";

        std::cout << " " << 100.0 * x / N * B - 100 << "%" << std::endl;
    }

    return 0;
}
