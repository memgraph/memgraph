#include "gtest/gtest.h"

#include <memory>
#include "data_structures/map/rh_hashmap.hpp"

class Data {
 private:
  int key;

 public:
  Data(int key) : key(key) {}

  const int &get_key() const { return key; }
};

void cross_validate(RhHashMap<int, Data> &map, std::map<int, Data *> &s_map);

TEST(RobinHoodHashmap, BasicFunctionality) {
  RhHashMap<int, Data> map;

  ASSERT_EQ(map.size(), 0);
  Data d0(0);
  ASSERT_EQ(map.insert(&d0), true);
  ASSERT_EQ(map.size(), 1);
}

TEST(RobinHoodHashmap, RemoveFunctionality) {
  RhHashMap<int, Data> map;

  Data d0(0);
  ASSERT_EQ(map.insert(&d0), true);
  ASSERT_EQ(map.remove(0).is_present(), true);
  ASSERT_EQ(map.size(), 0);
  ASSERT_EQ(!map.find(0).is_present(), true);
}

TEST(RobinHoodHashmap, InsertGetCheck) {
  RhHashMap<int, Data> map;

  ASSERT_EQ(!map.find(0).is_present(), true);
  Data d0(0);
  ASSERT_EQ(map.insert(&d0), true);
  ASSERT_EQ(map.find(0).is_present(), true);
  ASSERT_EQ(map.find(0).get(), &d0);
}

TEST(RobinHoodHashmap, DoubleInsert) {
  RhHashMap<int, Data> map;

  Data d0(0);
  ASSERT_EQ(map.insert(&d0), true);
  ASSERT_EQ(!map.insert(&d0), true);
}

TEST(RobinHoodHashmap, FindInsertFind) {
  RhHashMap<int, Data> map;

  std::vector<std::unique_ptr<Data>> di;
  di.reserve(128);
  for (int i = 0; i < 128; ++i) di.emplace_back(std::make_unique<Data>(i));
  for (int i = 0; i < 128; i++) {
    ASSERT_EQ(!map.find(i).is_present(), true);
    ASSERT_EQ(map.insert(di[i].get()), true);
    ASSERT_EQ(map.find(i).is_present(), true);
  }

  for (int i = 0; i < 128; i++) {
    ASSERT_EQ(map.find(i).is_present(), true);
    ASSERT_EQ(map.find(i).get()->get_key(), i);
  }
}

TEST(RobinHoodHashmap, Iterate) {
  RhHashMap<int, Data> map;

  std::vector<std::unique_ptr<Data>> di;
  di.reserve(128);
  for (int i = 0; i < 128; ++i) di.emplace_back(std::make_unique<Data>(i));
  for (int i = 0; i < 128; i++) {
    ASSERT_EQ(!map.find(i).is_present(), true);
    ASSERT_EQ(map.insert(di[i].get()), true);
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

  std::vector<std::unique_ptr<Data>> di;
  std::vector<int> key;
  di.reserve(128);
  key.reserve(128);
  for (int i = 0; i < 128; ++i) {
    const int curr_key = std::rand();
    key.emplace_back(curr_key);
    di.emplace_back(std::make_unique<Data>(curr_key));
  }
  for (int i = 0; i < 128; i++) {
    if (map.insert(di[i].get())) {
      ASSERT_EQ(s_map.find(key[i]), s_map.end());
      s_map[key[i]] = di[i].get();
    } else {
      ASSERT_NE(s_map.find(key[i]), s_map.end());
    }
  }

  cross_validate(map, s_map);
}

TEST(RobinHoodHashMap, CheckWithRemove) {
  RhHashMap<int, Data> map;
  std::map<int, Data *> s_map;
  std::vector<std::unique_ptr<Data>> di;
  std::vector<int> key;
  di.reserve(1280);
  key.reserve(1280);
  for (int i = 0; i < 1280; ++i) {
    const int curr_key = std::rand() % 100;
    key.emplace_back(curr_key);
    di.emplace_back(std::make_unique<Data>(curr_key));
  }

  for (int i = 0; i < 1280; i++) {
    if (map.insert(di[i].get())) {
      ASSERT_EQ(s_map.find(key[i]), s_map.end());
      s_map[key[i]] = di[i].get();
      cross_validate(map, s_map);
    } else {
      ASSERT_EQ(map.remove(key[i]).is_present(), true);
      ASSERT_EQ(s_map.erase(key[i]), 1);
      cross_validate(map, s_map);
    }
  }

  cross_validate(map, s_map);
}

TEST(RobinhoodHashmmap, AlignmentCheck) {
  RhHashMap<int, Data> map;
  char *block = static_cast<char *>(std::malloc(20));
  ++block;  // not alligned - offset 1
  EXPECT_DEATH(map.insert((Data *)(block)), "not 8-alligned");
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
