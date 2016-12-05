#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "utils/fswatcher.hpp"

using namespace utils;

TEST_CASE("FSWatcher init")
{
    FSWatcher watcher;
    watcher.init();
}
