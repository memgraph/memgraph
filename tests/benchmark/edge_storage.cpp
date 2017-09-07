#include <benchmark/benchmark.h>
#include <benchmark/benchmark_api.h>
#include <glog/logging.h>
#include <stdint.h>
#include <stdlib.h>
#include <algorithm>
#include <limits>
#include <set>
#include <unordered_set>
#include <utility>
#include <vector>

/** This suite of benchmarks test the data structure to hold edges in a vertex.
 * Various backing data-structures are benchmarked for common operations. */

/** Generates a random pseudo-pointer from a certain range. The range is limited
to ensure we get vertex-pointer collisions (multiple edges for the same vertex)
with some probability.
*/
int64_t random_pointer() { return rand() % 1000; }

// The element of all our collections. In real MG storage these would be Vertex
// and Edge version list pointers. It's a placeholder for 8-byte pointers.
using TElement = std::pair<int64_t, int64_t>;

/**
 * Converts a (beginning, end) pair of iterators into an iterable that can be
 * passed on to itertools. */
template <typename TIterator>
class Iterable {
 public:
  Iterable(const TIterator &begin, const TIterator &end)
      : begin_(begin), end_(end) {}

  Iterable(TIterator &&begin, TIterator &&end)
      : begin_(std::forward<TIterator>(begin)),
        end_(std::forward<TIterator>(end)) {}

  auto begin() { return begin_; };
  auto end() { return end_; };

 private:
  TIterator begin_;
  TIterator end_;
};

/** Keeps elements sorted in a vector. We get log2(n) lookups for known
 * destination vertex, but everything else suffers. */
class SortedVector {
 public:
  void insert(int64_t vertex, int64_t edge) {
    auto loc = std::lower_bound(storage_.begin(), storage_.end(),
                                TElement{vertex, edge});
    // Lower_bound returns iterator to last element if there is no element
    // greater then sought.
    if (loc + 1 == storage_.end()) loc = storage_.end();
    storage_.insert(loc, {vertex, edge});
  }

  // remove assumes the element is present, does not have to check
  void remove(int64_t vertex, int64_t edge) {
    storage_.erase(std::lower_bound(storage_.begin(), storage_.end(),
                                    TElement{vertex, edge}));
  }

  auto iterable_over(int64_t vertex) {
    return Iterable<std::vector<TElement>::iterator>(
        std::lower_bound(storage_.begin(), storage_.end(),
                         TElement{vertex, std::numeric_limits<int64_t>::min()}),
        std::upper_bound(
            storage_.begin(), storage_.end(),
            TElement{vertex, std::numeric_limits<int64_t>::max()}));
  }

  TElement random_element() { return storage_[rand() % storage_.size()]; }

  std::vector<TElement> storage_;
};

/** Keeps elements in an vector with no guaranteed ordering. Generally works OK,
 * but lookups for known destination vertices must be done with a linear scan.
 */
class Vector {
  /** Custom iterator that takes care of skipping elements when used in a
   * known-destination vertex scenario. */
  class VertexIterator {
   public:
    VertexIterator(std::vector<TElement>::const_iterator position)
        : position_(position) {}

    VertexIterator(std::vector<TElement>::const_iterator position,
                   std::vector<TElement>::const_iterator end, int64_t vertex)
        : position_(position), end_(end), vertex_(vertex) {
      update_position();
    }

    VertexIterator &operator++() {
      ++position_;
      if (vertex_ != 0) update_position();
      return *this;
    }

    const TElement &operator*() const { return *position_; }

    bool operator==(const VertexIterator &other) {
      return position_ == other.position_;
    }

    bool operator!=(const VertexIterator &other) { return !(*this == other); }

   private:
    std::vector<TElement>::const_iterator position_;
    // used only for the vertex-matching iterator
    std::vector<TElement>::const_iterator end_;
    int64_t vertex_{0};

    void update_position() {
      position_ = std::find_if(position_, end_, [this](const TElement &e) {
        return e.first == vertex_;
      });
    }
  };

 public:
  void insert(int64_t vertex, int64_t edge) {
    storage_.emplace_back(vertex, edge);
  }

  // remove assumes the element is present, does not have to check
  void remove(int64_t vertex, int64_t edge) {
    auto found =
        std::find(storage_.begin(), storage_.end(), TElement{vertex, edge});
    *found = std::move(storage_.back());
    storage_.pop_back();
  }

  auto iterable_over(int64_t vertex) {
    return Iterable<VertexIterator>(
        VertexIterator(storage_.begin(), storage_.end(), vertex),
        VertexIterator(storage_.end()));
  }

  // Override begin() and end() to return our custom iterator. We need this so
  // all edge iterators are of the same type, which is necessary for the current
  // implementation of query::plan::Expand.
  auto begin() { return VertexIterator(storage_.begin()); }
  auto end() { return VertexIterator(storage_.end()); }

  TElement random_element() { return storage_[rand() % storage_.size()]; }
  std::vector<TElement> storage_;
};

template <typename TIterator>
auto make_iterable(TIterator &&begin, TIterator &&end) {
  return Iterable<TIterator>(std::forward<TIterator>(begin),
                             std::forward<TIterator>(end));
}

class Set {
 public:
  void insert(int64_t vertex, int64_t edge) { storage_.insert({vertex, edge}); }

  // remove assumes the element is present, does not have to check
  void remove(int64_t vertex, int64_t edge) {
    storage_.erase(storage_.find(TElement{vertex, edge}));
  }

  auto iterable_over(int64_t vertex) {
    return Iterable<std::set<TElement>::iterator>(
        storage_.lower_bound(
            TElement{vertex, std::numeric_limits<int64_t>::min()}),
        storage_.upper_bound(
            TElement{vertex, std::numeric_limits<int64_t>::max()}));
  }

  TElement random_element() {
    auto it = storage_.begin();
    int to_remove =
        storage_.size() == 1 ? 0 : rand() % ((int)storage_.size() - 1);
    for (int i = 0; i < to_remove; i++) it++;

    return *it;
  }

  std::set<TElement> storage_;
};

// hash function for the vertex accessor
namespace std {
template <>
struct hash<TElement> {
  size_t operator()(const TElement &e) const { return e.first ^ e.second; };
};
}

class UnorderedMultiset {
 private:
  struct Hash {
    size_t operator()(const TElement &element) const { return element.first; }
  };
  struct Equal {
    bool operator()(const TElement &a, const TElement &b) const {
      return a.first == b.first;
    }
  };

 public:
  void insert(int64_t vertex, int64_t edge) { storage_.insert({vertex, edge}); }

  // remove assumes the element is present, does not have to check
  void remove(int64_t vertex, int64_t edge) {
    storage_.erase(storage_.find(TElement{vertex, edge}));
  }

  auto iterable_over(int64_t vertex) {
    auto start_end = storage_.equal_range(TElement{vertex, 0});
    return Iterable<std::unordered_multiset<TElement, Hash, Equal>::iterator>(
        start_end.first, start_end.second);
  }

  TElement random_element() {
    auto it = storage_.begin();
    int to_remove =
        storage_.size() == 1 ? 0 : rand() % ((int)storage_.size() - 1);
    for (int i = 0; i < to_remove; i++) it++;

    return *it;
  }

  std::unordered_multiset<TElement, Hash, Equal> storage_;
};

template <typename TStorage>
void Insert(benchmark::State &state) {
  TStorage storage;
  for (int i = 0; i < state.range(0); i++)
    storage.insert(random_pointer(), random_pointer());

  int64_t vertex = random_pointer();
  int64_t edge = random_pointer();

  while (state.KeepRunning()) {
    storage.insert(vertex, edge);
    state.PauseTiming();
    storage.remove(vertex, edge);
    state.ResumeTiming();
  }
}

template <typename TStorage>
static void Remove(benchmark::State &state) {
  TStorage storage;
  for (int i = 0; i < state.range(0); i++)
    storage.insert(random_pointer(), random_pointer());

  TElement to_remove;
  while (state.KeepRunning()) {
    state.PauseTiming();
    to_remove = storage.random_element();

    state.ResumeTiming();
    storage.remove(to_remove.first, to_remove.second);
    state.PauseTiming();

    storage.insert(random_pointer(), random_pointer());
    state.ResumeTiming();
  }
}

template <typename TStorage>
static void Iterate(benchmark::State &state) {
  TStorage storage;
  for (int i = 0; i < state.range(0); i++)
    storage.insert(random_pointer(), random_pointer());

  int64_t sum{0};
  while (state.KeepRunning()) {
    for (const auto &elem : storage.storage_) sum += elem.first;
  }
}

template <typename TStorage>
static void IterateOverVertex(benchmark::State &state) {
  TStorage storage;
  for (int i = 0; i < state.range(0); i++)
    storage.insert(random_pointer(), random_pointer());

  TElement e;
  while (state.KeepRunning()) {
    state.PauseTiming();
    e = storage.random_element();

    state.ResumeTiming();
    int64_t sum{0};
    for (const auto &elem : storage.iterable_over(e.first)) sum += elem.first;
  }
}

BENCHMARK_TEMPLATE(Insert, SortedVector)->RangeMultiplier(4)->Range(1, 4096);
BENCHMARK_TEMPLATE(Insert, Vector)->RangeMultiplier(4)->Range(1, 4096);
BENCHMARK_TEMPLATE(Insert, Set)->RangeMultiplier(4)->Range(1, 4096);
BENCHMARK_TEMPLATE(Insert, UnorderedMultiset)
    ->RangeMultiplier(4)
    ->Range(1, 4096);

BENCHMARK_TEMPLATE(Remove, SortedVector)->RangeMultiplier(4)->Range(1, 4096);
BENCHMARK_TEMPLATE(Remove, Vector)->RangeMultiplier(4)->Range(1, 4096);
BENCHMARK_TEMPLATE(Remove, Set)->RangeMultiplier(4)->Range(1, 4096);
BENCHMARK_TEMPLATE(Remove, UnorderedMultiset)
    ->RangeMultiplier(4)
    ->Range(1, 4096);

BENCHMARK_TEMPLATE(Iterate, SortedVector)->RangeMultiplier(4)->Range(1, 4096);
BENCHMARK_TEMPLATE(Iterate, Vector)->RangeMultiplier(4)->Range(1, 4096);
BENCHMARK_TEMPLATE(Iterate, Set)->RangeMultiplier(4)->Range(1, 4096);
BENCHMARK_TEMPLATE(Iterate, UnorderedMultiset)
    ->RangeMultiplier(4)
    ->Range(1, 4096);

BENCHMARK_TEMPLATE(IterateOverVertex, SortedVector)
    ->RangeMultiplier(4)
    ->Range(1, 4096);
BENCHMARK_TEMPLATE(IterateOverVertex, Vector)
    ->RangeMultiplier(4)
    ->Range(1, 4096);
BENCHMARK_TEMPLATE(IterateOverVertex, Set)->RangeMultiplier(4)->Range(1, 4096);
BENCHMARK_TEMPLATE(IterateOverVertex, UnorderedMultiset)
    ->RangeMultiplier(4)
    ->Range(1, 4096);

int main(int argc, char **argv) {
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
