#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "data_structures/map/rh_hashmultimap.hpp"

class Data
{

private:
    size_t data = 0;
    int key;

public:
    Data(int key) : key(key) {}

    int &get_key() { return key; }
};

TEST_CASE("Robin hood hashmultimap basic functionality")
{
    RhHashMultiMap<int, Data> map;

    REQUIRE(map.size() == 0);
    map.add(new Data(0));
    REQUIRE(map.size() == 1);
}

TEST_CASE("Robin hood hashmultimap insert/get check")
{
    RhHashMultiMap<int, Data> map;

    REQUIRE(map.find(0) == map.end());
    auto ptr0 = new Data(0);
    map.add(ptr0);
    REQUIRE(map.find(0) != map.end());
    REQUIRE(*map.find(0) == ptr0);
}

TEST_CASE("Robin hood hashmultimap double insert")
{
    RhHashMultiMap<int, Data> map;

    auto ptr0 = new Data(0);
    auto ptr1 = new Data(0);
    map.add(ptr0);
    map.add(ptr1);

    for (auto e : map) {
        if (ptr0 == e) {
            ptr0 = nullptr;
            continue;
        }
        if (ptr1 == e) {
            ptr1 = nullptr;
            continue;
        }
        REQUIRE(false);
    }
}

TEST_CASE("Robin hood hashmultimap")
{
    RhHashMultiMap<int, Data> map;

    for (int i = 0; i < 128; i++) {
        REQUIRE(map.find(i) == map.end());
        map.add(new Data(i));
        REQUIRE(map.find(i) != map.end());
    }

    for (int i = 0; i < 128; i++) {
        REQUIRE(map.find(i) != map.end());
        REQUIRE(map.find(i)->get_key() == i);
    }
}

TEST_CASE("Robin hood hashmultimap iterate")
{
    RhHashMultiMap<int, Data> map;

    for (int i = 0; i < 128; i++) {
        REQUIRE(map.find(i) == map.end());
        map.add(new Data(i));
        REQUIRE(map.find(i) != map.end());
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

TEST_CASE("Robin hood hashmultimap checked")
{
    RhHashMultiMap<int, Data> map;
    std::multimap<int, Data *> s_map;

    for (int i = 0; i < 1638; i++) {
        int key = (std::rand() % 100) << 3;

        auto data = new Data(key);
        map.add(data);
        s_map.insert(std::pair<int, Data *>(key, data));
    }

    for (auto e : map) {
        auto it = s_map.find(e->get_key());

        while (it != s_map.end() && it->second != e) {
            it++;
        }
        REQUIRE(it->second == e);
    }

    for (auto e : s_map) {
        auto it = map.find(e.first);

        while (it != map.end() && *it != e.second) {
            it++;
        }
        REQUIRE(e.second == *it);
    }
}
