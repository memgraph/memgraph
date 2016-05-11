#pragma once

#include <utility>
#include <chrono>

using time_point_t = std::chrono::high_resolution_clock::time_point;

#define duration(a) \
    std::chrono::duration_cast<std::chrono::nanoseconds>(a).count()

#define time_now() std::chrono::high_resolution_clock::now()

template<typename F, typename... Args>
double timer(F func, Args&&... args)
{
    time_point_t start_time = time_now();
    func(std::forward<Args>(args)...);
    return duration(time_now() - start_time);
}
