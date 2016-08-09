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

    REQUIRE(!map.find(0).is_present());
    auto ptr0 = new Data(0);
    REQUIRE(map.insert(ptr0));
    REQUIRE(map.find(0).is_present());
    REQUIRE(map.find(0).get() == ptr0);
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
        REQUIRE(!map.find(i).is_present());
        REQUIRE(map.insert(new Data(i)));
        REQUIRE(map.find(i).is_present());
    }

    for (int i = 0; i < 128; i++) {
        REQUIRE(map.find(i).is_present());
        REQUIRE(map.find(i).get()->get_key() == i);
    }
}

TEST_CASE("Robin hood hashmap iterate")
{
    RhHashMap<int, Data> map;

    for (int i = 0; i < 128; i++) {
        REQUIRE(!map.find(i).is_present());
        REQUIRE(map.insert(new Data(i)));
        REQUIRE(map.find(i).is_present());
    }

    bool seen[128] = {false};
    for (auto e : map) {
        auto key = e->get_key();
        REQUIRE(!seen[key]);
        seen[key] = true;
    }
    for (int i = 0; i < 128; i++) {
        REQUIRE(seen[i]);
    }
}

TEST_CASE("Robin hood hashmap checked")
{
    RhHashMap<int, Data> map;
    std::map<int, Data *> s_map;

    for (int i = 0; i < 128; i++) {
        int key = std::rand();
        auto data = new Data(key);
        if (map.insert(data)) {
            REQUIRE(s_map.find(key) == s_map.end());
            s_map[key] = data;
        } else {
            REQUIRE(s_map.find(key) != s_map.end());
        }
    }

    for (auto e : map) {
        REQUIRE(s_map.find(e->get_key()) != s_map.end());
    }

    for (auto e : s_map) {
        REQUIRE(map.find(e.first).get() == e.second);
    }
}
