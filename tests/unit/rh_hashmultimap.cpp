#include "gtest/gtest.h"

#include "data_structures/map/rh_hashmultimap.hpp"

class Data
{

private:
    size_t data = 0;
    int key;

public:
    Data(int key) : key(key) {}

    const int &get_key() { return key; }
};

void cross_validate(RhHashMultiMap<int, Data> &map,
                    std::multimap<int, Data *> &s_map);

void cross_validate_weak(RhHashMultiMap<int, Data> &map,
                         std::multimap<int, Data *> &s_map);

TEST(RobinHoodHashmultimap, BasicFunctionality)
{
    RhHashMultiMap<int, Data> map;

    ASSERT_EQ(map.size(), 0);
    map.add(new Data(0));
    ASSERT_EQ(map.size(), 1);
}

TEST(RobinHoodHashmultimap, InsertGetCheck)
{
    RhHashMultiMap<int, Data> map;

    ASSERT_EQ(map.find(0), map.end());
    auto ptr0 = new Data(0);
    map.add(ptr0);
    ASSERT_NE(map.find(0), map.end());
    ASSERT_EQ(*map.find(0), ptr0);
}

TEST(RobinHoodHashmultimap, ExtremeSameKeyValusFull)
{
    RhHashMultiMap<int, Data> map;

    for (int i = 0; i < 128; i++) {
        map.add(new Data(7));
    }
    ASSERT_EQ(map.size(), 128);
    ASSERT_NE(map.find(7), map.end());
    ASSERT_EQ(map.find(0), map.end());
    auto ptr0 = new Data(0);
    map.add(ptr0);
    ASSERT_NE(map.find(0), map.end());
    ASSERT_EQ(*map.find(0), ptr0);
}

TEST(RobinHoodHashmultimap, ExtremeSameKeyValusFullWithRemove)
{
    RhHashMultiMap<int, Data> map;

    for (int i = 0; i < 127; i++) {
        map.add(new Data(7));
    }
    auto ptr = new Data(7);
    map.add(ptr);
    ASSERT_EQ(map.size(), 128);
    ASSERT_EQ(!map.remove(new Data(0)), true);
    ASSERT_EQ(map.remove(ptr), true);
}

TEST(RobinHoodHasmultihmap, RemoveFunctionality)
{
    RhHashMultiMap<int, Data> map;

    ASSERT_EQ(map.find(0), map.end());
    auto ptr0 = new Data(0);
    map.add(ptr0);
    ASSERT_NE(map.find(0), map.end());
    ASSERT_EQ(*map.find(0), ptr0);
    ASSERT_EQ(map.remove(ptr0), true);
    ASSERT_EQ(map.find(0), map.end());
}

TEST(RobinHoodHashmultimap, DoubleInsert)
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
        ASSERT_EQ(true, false);
    }
}

TEST(RobinHoodHashmultimap, FindAddFind)
{
    RhHashMultiMap<int, Data> map;

    for (int i = 0; i < 128; i++) {
        ASSERT_EQ(map.find(i), map.end());
        map.add(new Data(i));
        ASSERT_NE(map.find(i), map.end());
    }

    for (int i = 0; i < 128; i++) {
        ASSERT_NE(map.find(i), map.end());
        ASSERT_EQ(map.find(i)->get_key(), i);
    }
}

TEST(RobinHoodHashmultimap, Iterate)
{
    RhHashMultiMap<int, Data> map;

    for (int i = 0; i < 128; i++) {
        ASSERT_EQ(map.find(i), map.end());
        map.add(new Data(i));
        ASSERT_NE(map.find(i), map.end());
    }

    bool seen[128] = {false};
    for (auto e : map) {
        auto key = e->get_key();
        ASSERT_EQ(!seen[key], true);
        seen[key] = true;
    }
    for (int i = 0; i < 128; i++) {
        ASSERT_EQ(seen[i], true);
    }
}

TEST(RobinHoodHashmultimap, Checked)
{
    RhHashMultiMap<int, Data> map;
    std::multimap<int, Data *> s_map;

    for (int i = 0; i < 1638; i++) {
        int key = (std::rand() % 100) << 3;

        auto data = new Data(key);
        map.add(data);
        s_map.insert(std::pair<int, Data *>(key, data));
    }
    cross_validate(map, s_map);
}

TEST(RobinHoodHashmultimap, CheckedRand)
{
    RhHashMultiMap<int, Data> map;
    std::multimap<int, Data *> s_map;
    std::srand(std::time(0));
    for (int i = 0; i < 164308; i++) {
        int key = (std::rand() % 10000) << 3;

        auto data = new Data(key);
        map.add(data);
        s_map.insert(std::pair<int, Data *>(key, data));
    }
    cross_validate(map, s_map);
}

TEST(RobinHoodHashmultimap, WithRemoveDataChecked)
{
    RhHashMultiMap<int, Data> map;
    std::multimap<int, Data *> s_map;

    std::srand(std::time(0));
    for (int i = 0; i < 162638; i++) {
        int key = (std::rand() % 10000) << 3;
        if ((std::rand() % 2) == 0) {
            auto it = s_map.find(key);
            if (it == s_map.end()) {
                ASSERT_EQ(map.find(key), map.end());
            } else {
                s_map.erase(it);
                ASSERT_EQ(map.remove(it->second), true);
            }
        } else {
            auto data = new Data(key);
            map.add(data);
            s_map.insert(std::pair<int, Data *>(key, data));
        }
    }

    cross_validate(map, s_map);
}

void cross_validate(RhHashMultiMap<int, Data> &map,
                    std::multimap<int, Data *> &s_map)
{

    for (auto e : map) {
        auto it = s_map.find(e->get_key());

        while (it != s_map.end() && it->second != e) {
            it++;
        }
        ASSERT_NE(it, s_map.end());
    }

    for (auto e : s_map) {
        auto it = map.find(e.first);

        while (it != map.end() && *it != e.second) {
            it++;
        }
        ASSERT_NE(it, map.end());
    }
}

void cross_validate_weak(RhHashMultiMap<int, Data> &map,
                         std::multimap<int, Data *> &s_map)
{
    int count = 0;
    int key = 0;
    for (auto e : map) {
        if (e->get_key() == key) {
            count++;
        } else {
            auto it = s_map.find(key);

            while (it != s_map.end() && it->first == key) {
                it++;
                count--;
            }
            ASSERT_EQ(count, 0);
            key = e->get_key();
            count = 1;
        }
    }
    {
        auto it = s_map.find(key);

        while (it != s_map.end() && it->first == key) {
            it++;
            count--;
        }
        ASSERT_EQ(count, 0);
    }

    for (auto e : s_map) {
        if (e.first == key) {
            count++;
        } else {
            auto it = map.find(key);

            while (it != map.end() && it->get_key() == key) {
                it++;
                count--;
            }
            ASSERT_EQ(count, 0);
            key = e.first;
            count = 1;
        }
    }
    {
        auto it = map.find(key);

        while (it != map.end() && it->get_key() == key) {
            it++;
            count--;
        }
        ASSERT_EQ(count, 0);
    }
}
