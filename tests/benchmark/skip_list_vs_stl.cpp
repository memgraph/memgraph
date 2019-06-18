#include <chrono>
#include <iostream>
#include <map>
#include <set>

#include <benchmark/benchmark.h>

#include "utils/skip_list.hpp"
#include "utils/spin_lock.hpp"

const int kThreadsNum = 8;
const uint64_t kMaxNum = 10000000;

///////////////////////////////////////////////////////////////////////////////
// utils::SkipList set Insert
///////////////////////////////////////////////////////////////////////////////

class SkipListSetInsertFixture : public benchmark::Fixture {
 protected:
  void SetUp(const benchmark::State &state) override {
    if (state.thread_index == 0) {
      list = utils::SkipList<uint64_t>();
    }
  }

 protected:
  utils::SkipList<uint64_t> list;
};

BENCHMARK_DEFINE_F(SkipListSetInsertFixture, Insert)(benchmark::State &state) {
  std::mt19937 gen(state.thread_index);
  std::uniform_int_distribution<uint64_t> dist(0, kMaxNum);
  uint64_t counter = 0;
  while (state.KeepRunning()) {
    auto acc = list.access();
    if (acc.insert(dist(gen)).second) {
      ++counter;
    }
  }
  state.SetItemsProcessed(counter);
}

BENCHMARK_REGISTER_F(SkipListSetInsertFixture, Insert)
    ->ThreadRange(1, kThreadsNum)
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

///////////////////////////////////////////////////////////////////////////////
// std::set Insert
///////////////////////////////////////////////////////////////////////////////

class StdSetInsertFixture : public benchmark::Fixture {
 protected:
  void SetUp(const benchmark::State &state) override {
    if (state.thread_index == 0) {
      container = {};
    }
  }

 protected:
  std::set<uint64_t> container;
  utils::SpinLock lock;
};

BENCHMARK_DEFINE_F(StdSetInsertFixture, Insert)(benchmark::State &state) {
  std::mt19937 gen(state.thread_index);
  std::uniform_int_distribution<uint64_t> dist(0, kMaxNum);
  uint64_t counter = 0;
  while (state.KeepRunning()) {
    std::lock_guard<utils::SpinLock> guard(lock);
    if (container.insert(dist(gen)).second) {
      ++counter;
    }
  }
  state.SetItemsProcessed(counter);
}

BENCHMARK_REGISTER_F(StdSetInsertFixture, Insert)
    ->ThreadRange(1, kThreadsNum)
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

///////////////////////////////////////////////////////////////////////////////
// utils::SkipList set Find
///////////////////////////////////////////////////////////////////////////////

class SkipListSetFindFixture : public benchmark::Fixture {
 protected:
  void SetUp(const benchmark::State &state) override {
    if (state.thread_index == 0 && list.size() == 0) {
      auto acc = list.access();
      for (uint64_t i = 0; i < kMaxNum; ++i) {
        acc.insert(i);
      }
    }
  }

 protected:
  utils::SkipList<uint64_t> list;
};

BENCHMARK_DEFINE_F(SkipListSetFindFixture, Find)(benchmark::State &state) {
  std::mt19937 gen(state.thread_index);
  std::uniform_int_distribution<uint64_t> dist(0, kMaxNum);
  uint64_t counter = 0;
  while (state.KeepRunning()) {
    auto acc = list.access();
    if (acc.find(dist(gen)) != acc.end()) {
      ++counter;
    }
  }
  state.SetItemsProcessed(counter);
}

BENCHMARK_REGISTER_F(SkipListSetFindFixture, Find)
    ->ThreadRange(1, kThreadsNum)
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

///////////////////////////////////////////////////////////////////////////////
// std::set Find
///////////////////////////////////////////////////////////////////////////////

class StdSetFindFixture : public benchmark::Fixture {
 protected:
  void SetUp(const benchmark::State &state) override {
    if (state.thread_index == 0 && container.size() == 0) {
      for (uint64_t i = 0; i < kMaxNum; ++i) {
        container.insert(i);
      }
    }
  }

 protected:
  std::set<uint64_t> container;
  utils::SpinLock lock;
};

BENCHMARK_DEFINE_F(StdSetFindFixture, Find)(benchmark::State &state) {
  std::mt19937 gen(state.thread_index);
  std::uniform_int_distribution<uint64_t> dist(0, kMaxNum);
  uint64_t counter = 0;
  while (state.KeepRunning()) {
    std::lock_guard<utils::SpinLock> guard(lock);
    if (container.find(dist(gen)) != container.end()) {
      ++counter;
    }
  }
  state.SetItemsProcessed(counter);
}

BENCHMARK_REGISTER_F(StdSetFindFixture, Find)
    ->ThreadRange(1, kThreadsNum)
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

///////////////////////////////////////////////////////////////////////////////
// Map tests common
///////////////////////////////////////////////////////////////////////////////

struct MapObject {
  uint64_t key;
  uint64_t value;
};

bool operator==(const MapObject &a, const MapObject &b) {
  return a.key == b.key;
}
bool operator<(const MapObject &a, const MapObject &b) { return a.key < b.key; }
bool operator==(const MapObject &a, uint64_t b) { return a.key == b; }
bool operator<(const MapObject &a, uint64_t b) { return a.key < b; }

///////////////////////////////////////////////////////////////////////////////
// utils::SkipList map Insert
///////////////////////////////////////////////////////////////////////////////

class SkipListMapInsertFixture : public benchmark::Fixture {
 protected:
  void SetUp(const benchmark::State &state) override {
    if (state.thread_index == 0) {
      list = utils::SkipList<MapObject>();;
    }
  }

 protected:
  utils::SkipList<MapObject> list;
};

BENCHMARK_DEFINE_F(SkipListMapInsertFixture, Insert)(benchmark::State &state) {
  std::mt19937 gen(state.thread_index);
  std::uniform_int_distribution<uint64_t> dist(0, kMaxNum);
  uint64_t counter = 0;
  while (state.KeepRunning()) {
    auto acc = list.access();
    if (acc.insert({dist(gen), 0}).second) {
      ++counter;
    }
  }
  state.SetItemsProcessed(counter);
}

BENCHMARK_REGISTER_F(SkipListMapInsertFixture, Insert)
    ->ThreadRange(1, kThreadsNum)
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

///////////////////////////////////////////////////////////////////////////////
// std::map Insert
///////////////////////////////////////////////////////////////////////////////

class StdMapInsertFixture : public benchmark::Fixture {
 protected:
  void SetUp(const benchmark::State &state) override {
    if (state.thread_index == 0) {
      container = {};
    }
  }

 protected:
  std::map<uint64_t, uint64_t> container;
  utils::SpinLock lock;
};

BENCHMARK_DEFINE_F(StdMapInsertFixture, Insert)(benchmark::State &state) {
  std::mt19937 gen(state.thread_index);
  std::uniform_int_distribution<uint64_t> dist(0, kMaxNum);
  uint64_t counter = 0;
  while (state.KeepRunning()) {
    std::lock_guard<utils::SpinLock> guard(lock);
    if (container.insert({dist(gen), 0}).second) {
      ++counter;
    }
  }
  state.SetItemsProcessed(counter);
}

BENCHMARK_REGISTER_F(StdMapInsertFixture, Insert)
    ->ThreadRange(1, kThreadsNum)
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

///////////////////////////////////////////////////////////////////////////////
// utils::SkipList map Find
///////////////////////////////////////////////////////////////////////////////

class SkipListMapFindFixture : public benchmark::Fixture {
 protected:
  void SetUp(const benchmark::State &state) override {
    if (state.thread_index == 0 && list.size() == 0) {
      auto acc = list.access();
      for (uint64_t i = 0; i < kMaxNum; ++i) {
        acc.insert({i, 0});
      }
    }
  }

 protected:
  utils::SkipList<MapObject> list;
};

BENCHMARK_DEFINE_F(SkipListMapFindFixture, Find)(benchmark::State &state) {
  std::mt19937 gen(state.thread_index);
  std::uniform_int_distribution<uint64_t> dist(0, kMaxNum);
  uint64_t counter = 0;
  while (state.KeepRunning()) {
    auto acc = list.access();
    if (acc.find(dist(gen)) != acc.end()) {
      ++counter;
    }
  }
  state.SetItemsProcessed(counter);
}

BENCHMARK_REGISTER_F(SkipListMapFindFixture, Find)
    ->ThreadRange(1, kThreadsNum)
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

///////////////////////////////////////////////////////////////////////////////
// std::map Find
///////////////////////////////////////////////////////////////////////////////

class StdMapFindFixture : public benchmark::Fixture {
 protected:
  void SetUp(const benchmark::State &state) override {
    if (state.thread_index == 0 && container.size() == 0) {
      for (uint64_t i = 0; i < kMaxNum; ++i) {
        container.insert({i, 0});
      }
    }
  }

 protected:
  std::map<uint64_t, uint64_t> container;
  utils::SpinLock lock;
};

BENCHMARK_DEFINE_F(StdMapFindFixture, Find)(benchmark::State &state) {
  std::mt19937 gen(state.thread_index);
  std::uniform_int_distribution<uint64_t> dist(0, kMaxNum);
  uint64_t counter = 0;
  while (state.KeepRunning()) {
    std::lock_guard<utils::SpinLock> guard(lock);
    if (container.find(dist(gen)) != container.end()) {
      ++counter;
    }
  }
  state.SetItemsProcessed(counter);
}

BENCHMARK_REGISTER_F(StdMapFindFixture, Find)
    ->ThreadRange(1, kThreadsNum)
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

BENCHMARK_MAIN();
