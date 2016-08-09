#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "data_structures/map/rh_hashmap.hpp"

class Data
{

private:
    size_t data = 0;
    int key;

public:
    Data(int key) : key(key) {}

    int &get_key() { return key; }
};

TEST_CASE("Robin hood hashmap basic functionality")
{
    RhHashMap<int, Data> map;

    REQUIRE(map.size() == 0);
    REQUIRE(map.insert(new Data(0)));
    REQUIRE(map.size() == 1);
}

TEST_CASE("Robin hood hashmap insert/get check")
{
    RhHashMap<int, Data> map;

    REQUIRE(!map.get(0).is_present());
    auto ptr0 = new Data(0);
    REQUIRE(map.insert(ptr0));
    REQUIRE(map.get(0).is_present());
    REQUIRE(map.get(0).get() == ptr0);
}

TEST_CASE("Robin hood hashmap double insert")
{
    RhHashMap<int, Data> map;

    REQUIRE(map.insert(new Data(0)));
    REQUIRE(!map.insert(new Data(0)));
}

TEST_CASE("Robin hood hashmap")
{
    RhHashMap<int, Data> map;

    for (int i = 0; i < 128; i++) {
        REQUIRE(!map.get(i).is_present());
        REQUIRE(map.insert(new Data(i)));
        REQUIRE(map.get(i).is_present());
    }

    for (int i = 0; i < 128; i++) {
        REQUIRE(map.get(i).is_present());
        REQUIRE(map.get(i).get()->get_key() == i);
    }
}
