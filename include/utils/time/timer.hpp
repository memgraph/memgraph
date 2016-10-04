#pragma once

#include <chrono>
#include <ratio>
#include <utility>

#define time_now() std::chrono::high_resolution_clock::now()

using ns = std::chrono::nanoseconds;
using ms = std::chrono::milliseconds;

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

// idea from Optimized C++ Kurt Guntheroth 2016
// TODO: make more modular and easier to use
class Stopwatch
{
public:
    Stopwatch() : start(std::chrono::system_clock::time_point::min()) {}

    void Clear()
    {
        start = std::chrono::system_clock::time_point::min();
    }

    bool IsStarted() const
    {
        return (start != std::chrono::system_clock::time_point::min());
    }

    void Start()
    {
        start = std::chrono::system_clock::now();
    }

    unsigned long GetMs()
    {
        if (IsStarted())
        {
            std::chrono::system_clock::duration diff;
            diff = std::chrono::system_clock::now() - start;
            return (unsigned) (std::chrono::duration_cast<ms>(diff).count());
        }

        return 0;
    }

    ~Stopwatch()
    {
        std::cout << "Time: " << GetMs() << "ms" << std::endl;
    }

private:
    std::chrono::system_clock::time_point start;

};
