#pragma once

#include <chrono>
#include <ratio>
#include <utility>

#define time_now() std::chrono::high_resolution_clock::now()

template <typename DurationUnit = std::chrono::nanoseconds>
auto to_duration(const std::chrono::duration<long, std::nano> &delta)
{
    return std::chrono::duration_cast<DurationUnit>(delta).count();
}

template <typename DurationUnit, typename F, typename... Args>
auto timer(F func, Args &&... args)
{
    auto start_time = time_now();
    func(std::forward<Args>(args)...);
    return to_duration<DurationUnit>(time_now() - start_time);
}
