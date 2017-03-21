#include "gtest/gtest.h"

#include "data_structures/map/rh_hashmultimap.hpp"

class Data {
 private:
  int key;

 public:
  Data(int key) : key(key) {}

  const int &get_key() { return key; }
};

void cross_validate(RhHashMultiMap<int, Data> &map,
                    std::multimap<int, Data *> &s_map);

void cross_validate_weak(RhHashMultiMap<int, Data> &map,
                         std::multimap<int, Data *> &s_map);

TEST(RobinHoodHashmultimap, BasicFunctionality) {
  RhHashMultiMap<int, Data> map;

  ASSERT_EQ(map.size(), 0);
  Data d0(0);
  map.add(&d0);
  ASSERT_EQ(map.size(), 1);
}

TEST(RobinHoodHashmultimap, InsertGetCheck) {
  RhHashMultiMap<int, Data> map;

  ASSERT_EQ(map.find(0), map.end());
  Data d0(0);
  map.add(&d0);
  ASSERT_NE(map.find(0), map.end());
  ASSERT_EQ(*map.find(0), &d0);
}

TEST(RobinHoodHashmultimap, ExtremeSameKeyValusFull) {
  RhHashMultiMap<int, Data> map;

  std::vector<std::unique_ptr<Data>> di;
  di.reserve(128);
  for (int i = 0; i < 128; i++) {
    di.emplace_back(std::make_unique<Data>(7));
    map.add(di[i].get());
  }
  ASSERT_EQ(map.size(), 128);
  ASSERT_NE(map.find(7), map.end());
  ASSERT_EQ(map.find(0), map.end());
  Data d0(0);
  map.add(&d0);
  ASSERT_NE(map.find(0), map.end());
  ASSERT_EQ(*map.find(0), &d0);
}

TEST(RobinHoodHashmultimap, ExtremeSameKeyValusFullWithRemove) {
  RhHashMultiMap<int, Data> map;

  std::vector<std::unique_ptr<Data>> di;
  di.reserve(128);
  for (int i = 0; i < 127; i++) {
    di.emplace_back(std::make_unique<Data>(7));
    map.add(di[i].get());
  }
  Data d7(7);
  map.add(&d7);
  ASSERT_EQ(map.size(), 128);
  Data d0(0);
  ASSERT_EQ(!map.remove(&d0), true);
  ASSERT_EQ(map.remove(&d7), true);
}

TEST(RobinHoodHasmultihmap, RemoveFunctionality) {
  RhHashMultiMap<int, Data> map;

  ASSERT_EQ(map.find(0), map.end());
  Data d0(0);
  map.add(&d0);
  ASSERT_NE(map.find(0), map.end());
  ASSERT_EQ(*map.find(0), &d0);
  ASSERT_EQ(map.remove(&d0), true);
  ASSERT_EQ(map.find(0), map.end());
}

TEST(RobinHoodHashmultimap, DoubleInsert) {
  RhHashMultiMap<int, Data> map;

  Data d0(0);
  Data d01(0);
  map.add(&d0);
  map.add(&d01);

  for (auto e : map) {
    if (&d0 == e) {
      continue;
    }
    if (&d01 == e) {
      continue;
    }
    ASSERT_EQ(true, false);
  }
}

TEST(RobinHoodHashmultimap, FindAddFind) {
  RhHashMultiMap<int, Data> map;

  std::vector<std::unique_ptr<Data>> di;
  di.reserve(128);
  for (int i = 0; i < 128; i++) {
    ASSERT_EQ(map.find(i), map.end());
    di.emplace_back(std::make_unique<Data>(i));
    map.add(di[i].get());
    ASSERT_NE(map.find(i), map.end());
  }

  for (int i = 0; i < 128; i++) {
    ASSERT_NE(map.find(i), map.end());
    ASSERT_EQ(map.find(i)->get_key(), i);
  }
}

TEST(RobinHoodHashmultimap, Iterate) {
  RhHashMultiMap<int, Data> map;

  std::vector<std::unique_ptr<Data>> di;
  di.reserve(128);
  for (int i = 0; i < 128; i++) {
    ASSERT_EQ(map.find(i), map.end());
    di.emplace_back(std::make_unique<Data>(i));
    map.add(di[i].get());
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

TEST(RobinHoodHashmultimap, Checked) {
  RhHashMultiMap<int, Data> map;
  std::multimap<int, Data *> s_map;

  std::vector<std::unique_ptr<Data>> di;
  std::vector<int> keys;
  di.reserve(1638);
  for (int i = 0; i < 1638; ++i) {
    const int key = (std::rand() % 100) << 3;
    keys.push_back(key);
    di.emplace_back(std::make_unique<Data>(key));
  }

  for (int i = 0; i < 1638; i++) {
    map.add(di[i].get());
    s_map.insert(std::pair<int, Data *>(keys[i], di[i].get()));
  }
  cross_validate(map, s_map);
}

TEST(RobinHoodHashmultimap, CheckedRand) {
  RhHashMultiMap<int, Data> map;
  std::multimap<int, Data *> s_map;
  std::srand(std::time(0));

  std::vector<std::unique_ptr<Data>> di;
  std::vector<int> keys;
  di.reserve(164308);
  for (int i = 0; i < 164308; ++i) {
    const int key = (std::rand() % 10000) << 3;
    keys.push_back(key);
    di.emplace_back(std::make_unique<Data>(key));
  }

  for (int i = 0; i < 164308; i++) {
    map.add(di[i].get());
    s_map.insert(std::pair<int, Data *>(keys[i], di[i].get()));
  }
  cross_validate(map, s_map);
}

TEST(RobinHoodHashmultimap, WithRemoveDataChecked) {
  RhHashMultiMap<int, Data> map;
  std::multimap<int, Data *> s_map;

  std::srand(std::time(0));
  std::vector<std::unique_ptr<Data>> di;
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
      di.emplace_back(std::make_unique<Data>(key));
      map.add(di.back().get());
      s_map.insert(std::pair<int, Data *>(key, di.back().get()));
    }
  }

  cross_validate(map, s_map);
}

TEST(RobinhoodHashmultimap, AlignmentCheck) {
  RhHashMultiMap<int, Data> map;
  char *block = static_cast<char *>(std::malloc(20));
  ++block;  // not alligned - offset 1
  EXPECT_DEATH(map.add((Data *)(block)), "not 8-alligned");
}

void cross_validate(RhHashMultiMap<int, Data> &map,
                    std::multimap<int, Data *> &s_map) {
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
                         std::multimap<int, Data *> &s_map) {
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
