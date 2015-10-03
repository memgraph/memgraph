#include "catch.hpp"
#include "data_structures/map/hashmap.hpp"

TEST_CASE("Lockfree HashMap basic functionality")
{
    lockfree::HashMap<int, int> hashmap;
    // hashmap[32] = 10;
    hashmap.put(32, 10);
    REQUIRE(hashmap[32] == 10);
}
