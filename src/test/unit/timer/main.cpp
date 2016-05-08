#include <iostream>
#include <chrono>

// #define NOT_LOG_INFO
// compile this with: c++ -std=c++1y -o a.out -I../../../ -pthread main.cpp

#include "utils/log/logger.hpp"
#include "utils/timer/timer.hpp"

using namespace std::chrono_literals;

Timer::sptr create_test_timer(int64_t counter)
{
    return std::make_shared<Timer>(
        counter, [](){ LOG_INFO("Timer timeout"); }
    );
}

int main(void)
{
    TimerScheduler<TimerSet, std::chrono::seconds> timer_scheduler;
    timer_scheduler.run();
    for (int64_t i = 1; i <= 3; ++i) {
        timer_scheduler.add(create_test_timer(i));
    }
    std::this_thread::sleep_for(10s);
    timer_scheduler.add(create_test_timer(1));
    return 0;
}
