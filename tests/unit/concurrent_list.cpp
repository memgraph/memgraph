#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "data_structures/concurrent/concurrent_list.hpp"

TEST_CASE("Conncurent List insert")
{
    List<int> list;
    auto it = list.begin();
    it.push(32);
    it.reset();
    REQUIRE(*it == 32);
}

TEST_CASE("Conncurent List iterate")
{
    List<int> list;
    auto it = list.begin();
    it.push(32);
    it.push(7);
    it.push(9);
    it.push(0);
    it.reset();

    REQUIRE(*it == 0);
    it++;
    REQUIRE(*it == 9);
    it++;
    REQUIRE(*it == 7);
    it++;
    REQUIRE(*it == 32);
    it++;
    REQUIRE(it == list.end());
}

TEST_CASE("Conncurent List head remove")
{
    List<int> list;
    auto it = list.begin();
    it.push(32);
    it.reset();

    REQUIRE(it.remove());
    REQUIRE(it.is_removed());
    REQUIRE(!it.remove());

    it.reset();
    REQUIRE(it == list.end());
}

TEST_CASE("Conncurent List remove")
{
    List<int> list;
    auto it = list.begin();
    it.push(32);
    it.push(7);
    it.push(9);
    it.push(0);
    it.reset();

    it++;
    it++;
    REQUIRE(it.remove());
    REQUIRE(it.is_removed());
    REQUIRE(!it.remove());

    it.reset();
    REQUIRE(*it == 0);
    it++;
    REQUIRE(*it == 9);
    it++;
    REQUIRE(*it == 32);
    it++;
    REQUIRE(it == list.end());
}
