#include "catch.hpp"
#include "data_structures/bitset/dynamic_bitset.hpp"

TEST_CASE("Dynamic bitset basic functionality")
{
    DynamicBitset<> db;
    db.set(2222255555, 1);
    bool value = db.at(2222255555, 1);
    REQUIRE(value == true);


    db.set(32, 1);
    value = db.at(32, 1);
    REQUIRE(value == true);

    db.clear(32, 1);
    value = db.at(32, 1);
    REQUIRE(value == false);
}
