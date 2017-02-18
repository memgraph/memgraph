#include "gtest/gtest.h"

#include "data_structures/map/rh_hashmap.hpp"

class Data {
 private:
  size_t data = 0;
  int key;

 public:
  Data(int key) : key(key) {}

  const int &get_key() const { return key; }
};

void cross_validate(RhHashMap<int, Data> &map, std::map<int, Data *> &s_map);

TEST(RobinHoodHashmap, BasicFunctionality) {
  RhHashMap<int, Data> map;

  ASSERT_EQ(map.size(), 0);
  ASSERT_EQ(map.insert(new Data(0)), true);
  ASSERT_EQ(map.size(), 1);
}

TEST(RobinHoodHashmap, RemoveFunctionality) {
  RhHashMap<int, Data> map;

  ASSERT_EQ(map.insert(new Data(0)), true);
  ASSERT_EQ(map.remove(0).is_present(), true);
  ASSERT_EQ(map.size(), 0);
  ASSERT_EQ(!map.find(0).is_present(), true);
}

TEST(RobinHoodHashmap, InsertGetCheck) {
  RhHashMap<int, Data> map;

  ASSERT_EQ(!map.find(0).is_present(), true);
  auto ptr0 = new Data(0);
  ASSERT_EQ(map.insert(ptr0), true);
  ASSERT_EQ(map.find(0).is_present(), true);
  ASSERT_EQ(map.find(0).get(), ptr0);
}

TEST(RobinHoodHashmap, DoubleInsert) {
  RhHashMap<int, Data> map;

  ASSERT_EQ(map.insert(new Data(0)), true);
  ASSERT_EQ(!map.insert(new Data(0)), true);
}

TEST(RobinHoodHashmap, FindInsertFind) {
  RhHashMap<int, Data> map;

  for (int i = 0; i < 128; i++) {
    ASSERT_EQ(!map.find(i).is_present(), true);
    ASSERT_EQ(map.insert(new Data(i)), true);
    ASSERT_EQ(map.find(i).is_present(), true);
  }

  for (int i = 0; i < 128; i++) {
    ASSERT_EQ(map.find(i).is_present(), true);
    ASSERT_EQ(map.find(i).get()->get_key(), i);
  }
}

TEST(RobinHoodHashmap, Iterate) {
  RhHashMap<int, Data> map;

  for (int i = 0; i < 128; i++) {
    ASSERT_EQ(!map.find(i).is_present(), true);
    ASSERT_EQ(map.insert(new Data(i)), true);
    ASSERT_EQ(map.find(i).is_present(), true);
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

TEST(RobinHoodHashmap, Checked) {
  RhHashMap<int, Data> map;
  std::map<int, Data *> s_map;

  for (int i = 0; i < 128; i++) {
    int key = std::rand();
    auto data = new Data(key);
    if (map.insert(data)) {
      ASSERT_EQ(s_map.find(key), s_map.end());
      s_map[key] = data;
    } else {
      ASSERT_NE(s_map.find(key), s_map.end());
    }
  }

  cross_validate(map, s_map);
}

TEST(RobinHoodHashMap, CheckWithRemove) {
  RhHashMap<int, Data> map;
  std::map<int, Data *> s_map;

  for (int i = 0; i < 1280; i++) {
    int key = std::rand() % 100;
    auto data = new Data(key);
    if (map.insert(data)) {
      ASSERT_EQ(s_map.find(key), s_map.end());
      s_map[key] = data;
      cross_validate(map, s_map);
    } else {
      ASSERT_EQ(map.remove(key).is_present(), true);
      ASSERT_EQ(s_map.erase(key), 1);
      cross_validate(map, s_map);
    }
  }

  cross_validate(map, s_map);
}

void cross_validate(RhHashMap<int, Data> &map, std::map<int, Data *> &s_map) {
  for (auto e : map) {
    ASSERT_NE(s_map.find(e->get_key()), s_map.end());
  }

  for (auto e : s_map) {
    ASSERT_EQ(map.find(e.first).get(), e.second);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
