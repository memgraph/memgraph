#include <iostream>
#include <chrono>

#include "gtest/gtest.h"

#include "logging/default.cpp"
#include "utils/timer/timer.hpp"
#include "utils/assert.hpp"

using namespace std::chrono_literals;

/**
 * Creates a test timer which will log timeout message at the timeout event.
 *
 * @param counter how many time units the timer has to wait
 *
 * @return shared pointer to a timer
 */
Timer::sptr create_test_timer(int64_t counter)
{
    return std::make_shared<Timer>(
        counter, [](){ logging::info("Timer timeout"); }
    );
}

TEST(TimerSchedulerTest, TimerSchedulerExecution)
{
    // initialize the timer
    TimerScheduler<TimerSet, std::chrono::seconds> timer_scheduler;

    // run the timer
    timer_scheduler.run();

    // add a couple of test timers
    for (int64_t i = 1; i <= 3; ++i) {
        timer_scheduler.add(create_test_timer(i));
    }

    // wait for that timers
    std::this_thread::sleep_for(4s);

    ASSERT_EQ(timer_scheduler.size(), 0);

    // add another test timer
    timer_scheduler.add(create_test_timer(1));

    // wait for another timer
    std::this_thread::sleep_for(2s);
    
    // the test is done
    timer_scheduler.stop();

    ASSERT_EQ(timer_scheduler.size(), 0);
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
