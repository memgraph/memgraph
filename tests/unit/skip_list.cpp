#include <vector>

#include <gtest/gtest.h>

#include <fmt/format.h>
#include <glog/logging.h>

#include "utils/skip_list.hpp"

TEST(SkipList, Int) {
  utils::SkipList<int64_t> list;
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      auto res = acc.insert(i);
      ASSERT_EQ(*res.first, i);
      ASSERT_TRUE(res.second);
    }
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      auto res = acc.insert(i);
      ASSERT_EQ(*res.first, i);
      ASSERT_FALSE(res.second);
    }
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    int64_t val = -10;
    for (auto &item : acc) {
      ASSERT_EQ(item, val);
      ++val;
    }
    ASSERT_EQ(val, 11);
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      auto it = acc.find(i);
      ASSERT_NE(it, acc.end());
      ASSERT_EQ(*it, i);
    }
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      ASSERT_TRUE(acc.remove(i));
    }
    ASSERT_EQ(acc.size(), 0);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      ASSERT_FALSE(acc.remove(i));
    }
    ASSERT_EQ(acc.size(), 0);
  }
}

TEST(SkipList, String) {
  utils::SkipList<std::string> list;
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      std::string str(fmt::format("str{}", i));
      auto res = acc.insert(str);
      ASSERT_EQ(*res.first, str);
      ASSERT_TRUE(res.second);
      ASSERT_NE(str, "");
    }
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      std::string str(fmt::format("str{}", i));
      auto res = acc.insert(str);
      ASSERT_EQ(*res.first, str);
      ASSERT_FALSE(res.second);
      ASSERT_NE(str, "");
    }
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    int64_t pos = 0;
    std::vector<int64_t> order{-1, -10, -2, -3, -4, -5, -6, -7, -8, -9, 0,
                               1,  10,  2,  3,  4,  5,  6,  7,  8,  9};
    for (auto &item : acc) {
      std::string str(fmt::format("str{}", order[pos]));
      ASSERT_EQ(item, str);
      ++pos;
    }
    ASSERT_EQ(pos, 21);
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      std::string str(fmt::format("str{}", i));
      auto it = acc.find(str);
      ASSERT_NE(it, acc.end());
      ASSERT_EQ(*it, str);
    }
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      std::string str(fmt::format("str{}", i));
      ASSERT_TRUE(acc.remove(str));
    }
    ASSERT_EQ(acc.size(), 0);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      std::string str(fmt::format("str{}", i));
      ASSERT_FALSE(acc.remove(str));
    }
    ASSERT_EQ(acc.size(), 0);
  }
}

TEST(SkipList, StringMove) {
  utils::SkipList<std::string> list;
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      std::string str(fmt::format("str{}", i));
      std::string copy(str);
      auto res = acc.insert(std::move(str));
      ASSERT_EQ(str, "");
      ASSERT_EQ(*res.first, copy);
      ASSERT_TRUE(res.second);
    }
    ASSERT_EQ(acc.size(), 21);
  }
  {
    auto acc = list.access();
    for (int64_t i = -10; i <= 10; ++i) {
      std::string str(fmt::format("str{}", i));
      auto res = acc.insert(str);
      ASSERT_EQ(*res.first, str);
      ASSERT_FALSE(res.second);
      ASSERT_NE(str, "");
    }
    ASSERT_EQ(acc.size(), 21);
  }
}

TEST(SkipList, Basic) {
  utils::SkipList<uint64_t> list;

  auto acc = list.access();

  auto insert_item = [&acc](auto item, bool inserted) {
    auto ret = acc.insert(item);
    ASSERT_NE(ret.first, acc.end());
    ASSERT_EQ(*ret.first, item);
    ASSERT_EQ(ret.second, inserted);
  };

  auto find_item = [&acc](auto item, bool found) {
    auto ret = acc.find(item);
    if (found) {
      ASSERT_NE(ret, acc.end());
      ASSERT_EQ(*ret, item);
    } else {
      ASSERT_EQ(ret, acc.end());
    }
  };

  ASSERT_FALSE(acc.contains(5));
  insert_item(5, true);
  insert_item(1, true);
  insert_item(2, true);
  insert_item(3, true);
  insert_item(4, true);
  insert_item(5, false);
  find_item(5, true);
  find_item(6, false);
  ASSERT_TRUE(acc.remove(5));
  ASSERT_FALSE(acc.remove(5));
  ASSERT_FALSE(acc.remove(6));
  ASSERT_EQ(acc.size(), 4);
}

struct OnlyCopyable {
  OnlyCopyable() = default;
  OnlyCopyable(OnlyCopyable &&) = delete;
  OnlyCopyable(const OnlyCopyable &) = default;
  OnlyCopyable &operator=(OnlyCopyable &&) = delete;
  OnlyCopyable &operator=(const OnlyCopyable &) = default;
  uint64_t value;
};

bool operator==(const OnlyCopyable &a, const OnlyCopyable &b) {
  return a.value == b.value;
}
bool operator<(const OnlyCopyable &a, const OnlyCopyable &b) {
  return a.value < b.value;
}

TEST(SkipList, OnlyCopyable) {
  utils::SkipList<OnlyCopyable> list;
  std::vector<OnlyCopyable> vec{{1}, {2}, {3}, {4}, {5}};
  auto acc = list.access();
  auto ret = acc.insert(vec[1]);
  ASSERT_NE(ret.first, acc.end());
  ASSERT_EQ(*ret.first, OnlyCopyable{2});
  ASSERT_TRUE(ret.second);
}

struct OnlyMoveable {
  OnlyMoveable() = default;
  OnlyMoveable(uint64_t val) : value(val) {}
  OnlyMoveable(OnlyMoveable &&) = default;
  OnlyMoveable(const OnlyMoveable &) = delete;
  OnlyMoveable &operator=(OnlyMoveable &&) = default;
  OnlyMoveable &operator=(const OnlyMoveable &) = delete;
  uint64_t value;
};

bool operator==(const OnlyMoveable &a, const OnlyMoveable &b) {
  return a.value == b.value;
}
bool operator<(const OnlyMoveable &a, const OnlyMoveable &b) {
  return a.value < b.value;
}

TEST(SkipList, OnlyMoveable) {
  utils::SkipList<OnlyMoveable> list;
  std::vector<OnlyMoveable> vec;
  vec.push_back({1});
  vec.push_back({2});
  auto acc = list.access();
  auto ret = acc.insert(std::move(vec[1]));
  ASSERT_NE(ret.first, acc.end());
  ASSERT_EQ(*ret.first, OnlyMoveable{2});
  ASSERT_TRUE(ret.second);
}

TEST(SkipList, Const) {
  utils::SkipList<uint64_t> list;

  auto func = [](const utils::SkipList<uint64_t> &lst) {
    auto acc = lst.access();
    return acc.find(5);
  };

  auto acc = list.access();

  CHECK(func(list) == acc.end());
}

struct MapObject {
  uint64_t key;
  std::string value;
};

bool operator==(const MapObject &a, const MapObject &b) {
  return a.key == b.key;
}
bool operator<(const MapObject &a, const MapObject &b) { return a.key < b.key; }

bool operator==(const MapObject &a, const uint64_t &b) { return a.key == b; }
bool operator<(const MapObject &a, const uint64_t &b) { return a.key < b; }

TEST(SkipList, MapExample) {
  utils::SkipList<MapObject> list;
  {
    auto accessor = list.access();

    // Inserts an object into the list.
    ASSERT_TRUE(accessor.insert(MapObject{5, "hello world"}).second);

    // This operation will return an iterator that isn't equal to
    // `accessor.end()`. This is because the comparison operators only use
    // the key field for comparison, the value field is ignored.
    ASSERT_NE(accessor.find(MapObject{5, "this probably isn't desired"}),
              accessor.end());

    // This will also succeed in removing the object.
    ASSERT_TRUE(accessor.remove(MapObject{5, "not good"}));
  }

  {
    auto accessor = list.access();

    // Inserts an object into the list.
    ASSERT_TRUE(accessor.insert({5, "hello world"}).second);

    // This successfully finds the inserted object.
    ASSERT_NE(accessor.find(5), accessor.end());

    // This successfully removes the inserted object.
    ASSERT_TRUE(accessor.remove(5));
  }
}

TEST(SkipList, Move) {
  utils::SkipList<int64_t> list;

  {
    auto acc = list.access();
    for (int64_t i = -1000; i <= 1000; ++i) {
      acc.insert(i);
    }
    ASSERT_EQ(acc.size(), 2001);
  }

  {
    auto acc = list.access();
    int64_t val = -1000;
    for (auto &item : acc) {
      ASSERT_EQ(item, val);
      ++val;
    }
    ASSERT_EQ(val, 1001);
    ASSERT_EQ(acc.size(), 2001);
  }

  utils::SkipList<int64_t> moved(std::move(list));

  {
    auto acc = moved.access();
    int64_t val = -1000;
    for (auto &item : acc) {
      ASSERT_EQ(item, val);
      ++val;
    }
    ASSERT_EQ(val, 1001);
    ASSERT_EQ(acc.size(), 2001);
  }

  {
    auto acc = list.access();
    ASSERT_DEATH(acc.insert(5), "");
  }
}

struct Inception {
  uint64_t id;
  utils::SkipList<uint64_t> data;
};
bool operator==(const Inception &a, const Inception &b) { return a.id == b.id; }
bool operator<(const Inception &a, const Inception &b) { return a.id < b.id; }
bool operator==(const Inception &a, const uint64_t &b) { return a.id == b; }
bool operator<(const Inception &a, const uint64_t &b) { return a.id < b; }

TEST(SkipList, Inception) {
  utils::SkipList<Inception> list;

  {
    for (uint64_t i = 0; i < 5; ++i) {
      utils::SkipList<uint64_t> inner;
      auto acc_inner = inner.access();
      for (uint64_t j = 0; j < 100; ++j) {
        acc_inner.insert(j + 1000 * i);
      }
      ASSERT_EQ(acc_inner.size(), 100);
      auto acc = list.access();
      acc.insert(Inception{i, std::move(inner)});
      auto dead = inner.access();
      ASSERT_DEATH(dead.insert(5), "");
    }
  }

  {
    ASSERT_EQ(list.size(), 5);
    for (uint64_t i = 0; i < 5; ++i) {
      auto acc = list.access();
      auto it = acc.find(i);
      ASSERT_NE(it, acc.end());
      ASSERT_EQ(it->id, i);
      auto acc_inner = it->data.access();
      ASSERT_EQ(acc_inner.size(), 100);
      for (uint64_t j = 0; j < 100; ++j) {
        auto it_inner = acc_inner.find(j + 1000 * i);
        ASSERT_NE(it_inner, acc_inner.end());
        ASSERT_EQ(*it_inner, j + 1000 * i);
      }
    }
  }
}
