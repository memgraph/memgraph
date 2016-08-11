#include <iostream>
#include <chrono>

#include "logging/default.cpp"
#include "utils/timer/timer.hpp"

using namespace std::chrono_literals;

Timer::sptr create_test_timer(int64_t counter)
{
    return std::make_shared<Timer>(
        counter, [](){ logging::info("Timer timeout"); }
    );
}

int main(void)
{
    TimerScheduler<TimerSet, std::chrono::seconds> timer_scheduler;
    timer_scheduler.run();
    for (int64_t i = 1; i <= 3; ++i) {
        timer_scheduler.add(create_test_timer(i));
    }
    std::this_thread::sleep_for(4s);
    timer_scheduler.add(create_test_timer(1));
    std::this_thread::sleep_for(2s);
    timer_scheduler.stop();
    return 0;
}
