#include <fstream>
#include <thread>

#include "gtest/gtest.h"
#include "logging/default.cpp"
#include "utils/fswatcher.hpp"
#include "utils/signals/handler.hpp"
#include "utils/stacktrace/log.hpp"
#include "utils/terminate_handler.hpp"

using namespace std::chrono_literals;
using namespace utils;

fs::path working_dir = "../data";
fs::path filename    = "test.txt";
fs::path test_path   = working_dir / filename;

void create_delete_loop(int iterations, ms action_delta)
{
    for (int i = 0; i < iterations; ++i)
    {
        // create test file
        std::ofstream outfile(test_path);
        outfile.close();
        std::this_thread::sleep_for(action_delta);

        // remove test file
        fs::remove(test_path);
        std::this_thread::sleep_for(action_delta);
    }
}

void modify_loop(int iterations, ms action_delta)
{
    // create test file
    std::ofstream outfile(test_path);
    outfile.close();
    std::this_thread::sleep_for(action_delta);

    // append TEST multiple times
    for (int i = 0; i < iterations; ++i)
    {
        outfile.open(test_path, std::ios_base::app);
        outfile << "TEST" << i;
        outfile.close();
        std::this_thread::sleep_for(action_delta);
    }

    // remove test file
    fs::remove(test_path);
    std::this_thread::sleep_for(action_delta);
}

TEST(FSWatcherTest, CreateDeleteLoop)
{
    FSWatcher watcher;

    // parameters
    int iterations  = 10;
    int created_no  = 0;
    int deleted_no  = 0;

    // time distance between two actions should be big enough otherwise
    // one event will hide another one
    ms action_delta = watcher.check_interval() * 3;

    // watchers
    watcher.watch(WatchDescriptor(working_dir, FSEventType::Created),
                  [&](FSEvent) {});
    watcher.watch(WatchDescriptor(working_dir, FSEventType::Deleted),
                  [&](FSEvent) {});
    // above watchers should be ignored
    watcher.watch(WatchDescriptor(working_dir, FSEventType::All),
                  [&](FSEvent event) {
                      if (event.type == FSEventType::Created) created_no++;
                      if (event.type == FSEventType::Deleted) deleted_no++;
                  });

    ASSERT_EQ(watcher.size(), 1);

    create_delete_loop(iterations, action_delta);
    ASSERT_EQ(created_no, iterations);
    ASSERT_EQ(deleted_no, iterations);

    watcher.unwatchAll();
    ASSERT_EQ(watcher.size(), 0);

    watcher.unwatchAll();
    ASSERT_EQ(watcher.size(), 0);

    create_delete_loop(iterations, action_delta);
    ASSERT_EQ(created_no, iterations);
    ASSERT_EQ(deleted_no, iterations);
}

TEST(FSWatcherTest, ModifiyLoop)
{
    FSWatcher watcher;

    // parameters
    int iterations  = 10;
    int modified_no = 0;
    ms action_delta = watcher.check_interval() * 3;

    watcher.watch(WatchDescriptor(working_dir, FSEventType::Modified),
                  [&](FSEvent) { modified_no++; });
    ASSERT_EQ(watcher.size(), 1);

    modify_loop(iterations, action_delta);
    ASSERT_EQ(modified_no, iterations);

    watcher.unwatch(WatchDescriptor(working_dir, FSEventType::Modified));
    ASSERT_EQ(watcher.size(), 0);

    watcher.unwatch(WatchDescriptor(working_dir, FSEventType::Modified));
    ASSERT_EQ(watcher.size(), 0);

    modify_loop(iterations, action_delta);
    ASSERT_EQ(modified_no, iterations);
}

int main(int argc, char **argv)
{
    logging::init_sync();
    logging::log->pipe(std::make_unique<Stdout>());

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
