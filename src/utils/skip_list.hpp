// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <optional>
#include <random>
#include <utility>

#include "spdlog/spdlog.h"
#include "utils/bound.hpp"
#include "utils/linux.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/readable_size.hpp"
#include "utils/spin_lock.hpp"
#include "utils/stack.hpp"
#include "utils/stat.hpp"

// This code heavily depends on atomic operations. For a more detailed
// description of how exactly atomic operations work, see:
// https://www.codeproject.com/Articles/1183423/We-make-a-std-shared-mutex-times-faster
// How the specified memory fences influence the generated assembler
// instructions, see: https://www.cl.cam.ac.uk/~pes20/cpp/cpp0xmappings.html

namespace memgraph::utils {

/// This is the maximum height of the list. This value shouldn't be changed from
/// this value because it isn't practical to have skip lists that have larger
/// heights than 32. The probability of heights larger than 32 gets extremely
/// small. Also, the internal implementation can handle a maximum height of 32
/// primarily becase of the height generator (see the `gen_height` function).
constexpr uint64_t kSkipListMaxHeight = 32;

/// This is the height that a node that is accessed from the list has to have in
/// order for garbage collection to be triggered. This causes the garbage
/// collection to be probabilistically triggered on each operation with the
/// list. Each thread that accesses the list can perform garbage collection. The
/// level is determined empirically using benchmarks. A smaller height means
/// that the garbage collection will be triggered more often.
constexpr uint64_t kSkipListGcHeightTrigger = 16;

/// This is the highest layer that will be used by default for item count
/// estimation. It was determined empirically using benchmarks to have an
/// optimal trade-off between performance and accuracy. The function will have
/// an expected maximum error of less than 20% when the key matches 100k
/// elements.
constexpr int kSkipListCountEstimateDefaultLayer = 10;

/// These variables define the storage sizes for the SkipListGc. The internal
/// storage of the GC and the Stack storage used within the GC are all
/// optimized to have block sizes that are a whole multiple of the memory page
/// size.
constexpr uint64_t kSkipListGcBlockSize = 8189;
constexpr uint64_t kSkipListGcStackSize = 8191;

namespace detail {
struct SkipListNode_base {
  // This function generates a binomial distribution using the same technique
  // described here: http://ticki.github.io/blog/skip-lists-done-right/ under
  // "O(1) level generation". The only exception in this implementation is that
  // the special case of 0 is handled correctly. When 0 is passed to `ffs` it
  // returns 0 which is an invalid height. To make the distribution binomial
  // this value is then mapped to `kSkipListMaxSize`.
  static uint32_t gen_height() {
    thread_local std::mt19937 gen{std::random_device{}()};
    static_assert(kSkipListMaxHeight <= 32,
                  "utils::SkipList::gen_height is implemented only for heights "
                  "up to 32!");
    uint32_t value = gen();
    if (value == 0) return kSkipListMaxHeight;
    // The value should have exactly `kSkipListMaxHeight` bits.
    value >>= (32 - kSkipListMaxHeight);
    // ffs = find first set
    //       ^    ^     ^
    return __builtin_ffs(value);
  }
};
}  // namespace detail

/// This is the Node object that represents each element stored in the list. The
/// array of pointers to the next nodes is declared here to a size of 0 so that
/// we can dynamically allocate different sizes of memory for nodes of different
/// heights. Because the array is of length 0 its size is also 0. That means
/// that `sizeof(SkipListNode<TObj>)` is equal to the size of all members
/// without the `nexts` member. When we allocate memory for the node we allocate
/// `sizeof(SkipListNode<TObj>) + height * sizeof(std::atomic<SkipListNode<TObj>
/// *>)`.
///
/// The allocated memory is then used like this:
/// [      Node      ][Node*][Node*][Node*]...[Node*]
///         |            |      |      |         |
///         |            0      1      2      height-1
/// |----------------||-----------------------------|
///   space for the         space for pointers
///   Node structure         to the next Node
template <typename TObj>
struct SkipListNode {
  template <typename TObjUniv>
  SkipListNode(uint8_t height, TObjUniv &&object) : obj(std::forward<TObjUniv>(object)), height(height) {}

  // The items here are carefully placed to minimize padding gaps.

  TObj obj;
  SpinLock lock;
  std::atomic<bool> marked;
  std::atomic<bool> fully_linked;
  static_assert(std::numeric_limits<uint8_t>::max() >= kSkipListMaxHeight, "Maximum height doesn't fit in uint8_t");
  uint8_t height;
  // uint8_t PAD;
  std::atomic<SkipListNode<TObj> *> nexts[0];
};

/// Maximum size of a single SkipListNode instance.
///
/// This can be used to tune the pool allocator for SkipListNode instances.
template <typename TObj>
constexpr size_t MaxSkipListNodeSize() {
  return sizeof(SkipListNode<TObj>) + kSkipListMaxHeight * sizeof(std::atomic<SkipListNode<TObj> *>);
}

/// Get the size in bytes of the given SkipListNode instance.
template <typename TObj>
size_t SkipListNodeSize(const SkipListNode<TObj> &node) {
  return sizeof(node) + node.height * sizeof(std::atomic<SkipListNode<TObj> *>);
}

/// A helper function for determining the skip list layer used for estimating
/// the number of elements in, e.g. a database index. The lower layer we use,
/// the better approximation we get (if we use the lowest layer, we get the
/// exact numbers). However, lower skip list layers contain more elements so we
/// must iterate through more items to get the estimate.
///
/// Our goal is to achieve balance between execution time and approximation
/// precision. The expected number of elements at the k-th skip list layer is N
/// * (1/2)^(k-1), where N is the skip-list size. We choose to iterate through
/// no more than sqrt(N) items for large N when calculating the estimate, so we
/// need to choose the skip-list layer such that N * (1/2)^(k-1) <= sqrt(N).
/// That is equivalent to k >= 1 + 1/2 * log2(N), so we choose k to be 1 +
/// ceil(log2(N) / 2).
///
/// For N small enough (arbitrarily chosen to be 500), we will just use the
/// lowest layer to get the exact numbers. Mostly because this makes writing
/// tests easier.
constexpr uint64_t SkipListLayerForCountEstimation(const uint64_t N) {
  if (N <= 500) return 1;
  return std::min(1 + (utils::Log2(N) + 1) / 2, utils::kSkipListMaxHeight);
}

/// This function is written with the same intent as the function above except
/// that it uses slightly higher layers for estimation because the
/// `average_number_of_equals` estimate has a larger time complexity than the
/// `*count` estimates.
constexpr uint64_t SkipListLayerForAverageEqualsEstimation(const uint64_t N) {
  if (N <= 500) return 1;
  return std::min(1 + ((utils::Log2(N) * 2) / 3 + 1), utils::kSkipListMaxHeight);
}

/// The skip list doesn't have built-in reclamation of removed nodes (objects).
/// This class handles all operations necessary to remove the nodes safely.
///
/// The principal of operation is as follows:
/// Each accessor to the skip list is given an ID. When nodes are garbage
/// collected the ID of the currently newest living accessor is recorded. When
/// that accessor is destroyed the node can be safely destroyed.
/// This is correct because when the skip list removes the node it immediately
/// unlinks it from the structure so no new accessors can reach it. The last
/// possible accessor that can still have a reference to the removed object is
/// the currently living accessor that has the largest ID.
///
/// To enable fast operations this GC stores accessor IDs in a specially crafted
/// structure. It consists of a doubly-linked list of Blocks. Each Block holds
/// the information (alive/dead) for about 500k accessors. When an accessor is
/// destroyed the corresponding bit for the accessor is found in the list and is
/// set. When garbage collection occurs it finds the largest prefix of dead
/// accessors and destroys all nodes that have the largest living accessor ID
/// corresponding to them less than the largest currently found dead accessor.
///
/// Insertion into the dead accessor list is fast because the blocks are large
/// and the corresponding bit can be set atomically. The only times when the
/// collection is blocking is when the structure of the doubly-linked list has
/// to be changed (eg. a new Block has to be allocated and linked into the
/// structure).
template <typename TObj>
class SkipListGc final {
 private:
  using TNode = SkipListNode<TObj>;
  using TDeleted = std::pair<uint64_t, TNode *>;
  using TStack = Stack<TDeleted, kSkipListGcStackSize>;

  static constexpr uint64_t kIdsInField = sizeof(uint64_t) * 8;
  static constexpr uint64_t kIdsInBlock = kSkipListGcBlockSize * kIdsInField;

  struct Block {
    std::atomic<Block *> prev;
    std::atomic<Block *> succ;
    uint64_t first_id;
    std::atomic<uint64_t> field[kSkipListGcBlockSize];
  };

  Block *AllocateBlock(Block *head) {
    std::lock_guard<SpinLock> guard(lock_);
    Block *curr_head = head_.load(std::memory_order_acquire);
    if (curr_head == head) {
      // Construct through allocator so it propagates if needed.
      Allocator<Block> block_allocator(memory_);
      Block *block = block_allocator.allocate(1);
      // `calloc` would be faster, but the API has no such call.
      memset(block, 0, sizeof(Block));
      // Block constructor should not throw.
      block_allocator.construct(block);
      block->prev.store(curr_head, std::memory_order_release);
      block->succ.store(nullptr, std::memory_order_release);
      block->first_id = last_id_;
      last_id_ += kIdsInBlock;
      if (curr_head == nullptr) {
        tail_.store(block, std::memory_order_release);
      } else {
        curr_head->succ.store(block, std::memory_order_release);
      }
      head_.store(block, std::memory_order_release);
      return block;
    } else {
      return curr_head;
    }
  }

 public:
  explicit SkipListGc(MemoryResource *memory) : memory_(memory) {
    static_assert(sizeof(Block) % kLinuxPageSize == 0,
                  "It is recommended that you set the kSkipListGcBlockSize "
                  "constant so that the size of SkipListGc::Block is a "
                  "multiple of the page size.");
  }

  SkipListGc(const SkipListGc &) = delete;
  SkipListGc &operator=(const SkipListGc &) = delete;
  SkipListGc(SkipListGc &&other) = delete;
  SkipListGc &operator=(SkipListGc &&other) = delete;

  ~SkipListGc() { Clear(); }

  uint64_t AllocateId() {
#ifndef NDEBUG
    alive_accessors_.fetch_add(1, std::memory_order_acq_rel);
#endif
    return accessor_id_.fetch_add(1, std::memory_order_acq_rel);
  }

  void ReleaseId(uint64_t id) {
    // This function only needs to acquire a lock when allocating a new block
    // (in the `AllocateBlock` function), but otherwise doesn't need to acquire
    // a lock because it iterates over the linked list and atomically sets its
    // 'dead' bit in the block field. The structure of the linked list can be
    // accessed without a lock because all of the pointers in the list are
    // atomic and their modification is done so that the access is always
    // correct.
    Block *head = head_.load(std::memory_order_acquire);
    if (head == nullptr) {
      head = AllocateBlock(head);
    }
    while (true) {
      MG_ASSERT(head != nullptr, "Missing SkipListGc block!");
      if (id < head->first_id) {
        head = head->prev.load(std::memory_order_acquire);
      } else if (id >= head->first_id + kIdsInBlock) {
        head = AllocateBlock(head);
      } else {
        id -= head->first_id;
        uint64_t field = id / kIdsInField;
        uint64_t bit = id % kIdsInField;
        uint64_t value = 1;
        value <<= bit;
        auto ret = head->field[field].fetch_or(value, std::memory_order_acq_rel);
        MG_ASSERT(!(ret & value), "A SkipList Accessor was released twice!");
        break;
      }
    }
#ifndef NDEBUG
    alive_accessors_.fetch_add(-1, std::memory_order_acq_rel);
#endif
  }

  void Collect(TNode *node) {
    std::unique_lock guard(lock_);
    deleted_.Push({accessor_id_.load(std::memory_order_acquire), node});
  }

  void Run() {
    // This method can be called after any skip list method, including the add method
    // which could have OOMException enabled in its thread so to ensure no exception
    // is thrown while cleaning the skip list, we add the blocker.
    utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_blocker;
    if (!lock_.try_lock()) return;
    OnScopeExit cleanup([&] { lock_.unlock(); });
    Block *tail = tail_.load(std::memory_order_acquire);
    uint64_t last_dead = 0;
    bool remove_block = true;
    while (tail != nullptr && remove_block) {
      for (uint64_t pos = 0; pos < kSkipListGcBlockSize; ++pos) {
        uint64_t field = tail->field[pos].load(std::memory_order_acquire);
        if (field != std::numeric_limits<uint64_t>::max()) {
          if (field != 0) {
            // Here we find the position of the least significant zero bit
            // (using a inverted value and the `ffs` function to find the
            // position of the first set bit). We find this position because we
            // know that all bits that are of less significance are then all
            // ones. That means that the `where_alive` will be the first ID that
            // is still alive. That means that we have a prefix of all dead
            // accessors that have IDs less than `where_alive`.
            int where_alive = __builtin_ffsl(~field) - 1;
            if (where_alive > 0) {
              last_dead = tail->first_id + pos * kIdsInField + where_alive - 1;
            }
          }
          remove_block = false;
          break;
        } else {
          last_dead = tail->first_id + (pos + 1) * kIdsInField - 1;
        }
      }
      Block *next = tail->succ.load(std::memory_order_acquire);
      // Here we also check whether the next block isn't a `nullptr`. If it is
      // `nullptr` that means that this is the last existing block. We don't
      // want to destroy it because we would need to change the `head_` to point
      // to `nullptr`. We can't do that because we can't guarantee that some
      // thread doesn't have a pointer to the block that it got from reading
      // `head_`. We bail out here, this block will be freed next time.
      if (remove_block && next != nullptr) {
        Allocator<Block> block_allocator(memory_);
        MG_ASSERT(tail == tail_.load(std::memory_order_acquire),
                  "Can't remove SkipListGc block that is in the middle!");
        next->prev.store(nullptr, std::memory_order_release);
        tail_.store(next, std::memory_order_release);
        // Destroy the block.
        tail->~Block();
        block_allocator.deallocate(tail, 1);
      }
      tail = next;
    }
    TStack leftover;
    std::optional<TDeleted> item;
    while ((item = deleted_.Pop())) {
      if (item->first < last_dead) {
        size_t bytes = SkipListNodeSize(*item->second);
        item->second->~TNode();
        memory_->Deallocate(item->second, bytes);
      } else {
        leftover.Push(*item);
      }
    }
    while ((item = leftover.Pop())) {
      deleted_.Push(*item);
    }
  }

  MemoryResource *GetMemoryResource() const { return memory_; }

  void Clear() {
    // Delete all allocated blocks.
    Block *head = head_.load(std::memory_order_acquire);
    while (head != nullptr) {
      Allocator<Block> block_allocator(memory_);
      Block *prev = head->prev.load(std::memory_order_acquire);
      head->~Block();
      block_allocator.deallocate(head, 1);
      head = prev;
    }

    // Delete all items that have to be garbage collected.
    std::optional<TDeleted> item;
    while ((item = deleted_.Pop())) {
      size_t bytes = SkipListNodeSize(*item->second);
      item->second->~TNode();
      memory_->Deallocate(item->second, bytes);
    }

    // Reset all variables.
    accessor_id_ = 0;
    head_ = nullptr;
    tail_ = nullptr;
    last_id_ = 0;
  }

 private:
  MemoryResource *memory_;
  SpinLock lock_;
  std::atomic<uint64_t> accessor_id_{0};
  std::atomic<Block *> head_{nullptr};
  std::atomic<Block *> tail_{nullptr};
  uint64_t last_id_{0};
  TStack deleted_;
#ifndef NDEBUG
  std::atomic<uint64_t> alive_accessors_{0};
#endif
};

/// Concurrent skip list. It is mostly lock-free and fine-grained locking is
/// used for conflict resolution.
///
/// The implementation is based on the work described in the paper
/// "A Provably Correct Scalable Concurrent Skip List"
/// http://people.csail.mit.edu/shanir/publications/OPODIS2006-BA.pdf
///
/// The proposed implementation is in Java so the authors don't worry about
/// garbage collection. This implementation uses the garbage collector that is
/// described previously in this file. The garbage collection is triggered
/// occasionally on each operation that is executed on the list through an
/// accessor.
///
/// The implementation has an interface which mimics the interface of STL
/// containers like `std::set` and `std::map`.
///
/// When getting iterators to contained objects it is guaranteed that the
/// iterator will point to a living object while the accessor that produced it
/// is alive. So to say it differently, you *MUST* keep the accessor to the list
/// alive while you still have some iterators to items that are contained in the
/// list. That is the only usage in which the list guarantees that the item that
/// is pointed to by the iterator doesn't get deleted (freed) by another thread.
///
///
/// The first use-case for the skip list is when you want to use the list as a
/// `std::set`. The items are stored sorted and duplicates aren't permitted.
///
///   utils::SkipList<uint64_t> list;
///   {
///     auto accessor = list.access();
///
///     // Inserts item into the skiplist and returns an <Iterator, bool> pair.
///     // The Iterator points to the newly created node and the boolean is
///     // `true` indicating that the item was inserted.
///     accessor.insert(42);
///
///     // Nothing gets inserted because 42 already exist in the list. The
///     // returned iterator points to the existing item in the list and the
///     // boolean is `false` indicating that the insertion failed (an existing
///     // item was returned).
///     accessor.insert(42);
///
///     // Returns an iterator to the element 42.
///     auto it = accessor.find(42);
///
///     // Returns an empty iterator. The iterator is equal to `accessor.end()`.
///     auto it = accessor.find(99);
///
///     // Iterate over all items in the list.
///     for (auto it = accessor.begin(); it != accessor.end(); ++it) {
///       std::cout << *it << std::endl;
///     }
///
///     // The loop can also be range based.
///     for (auto &e : accessor) {
///       std::cout << e << std::endl;
///     }
///
///     // This removes the existing element from the list and returns `true`
///     // indicating that the removal was successful.
///     accessor.remove(42);
///
///     // This returns `false` indicating that the removal failed because the
///     // item doesn't exist in the list.
///     accessor.remove(42);
///   }
///
///   // The accessor is out-of-scope and the 42 object can be deleted (freed)
///   // from main memory now that there is no alive accessor that can
///   // reach it anymore.
///
///
/// In order to use a custom type for the set use-case you have to implement the
/// following operators:
///
///   bool operator==(const custom &a, const custom &b);
///   bool operator<(const custom &a, const custom &b);
///
///
/// Another use-case for the skip list is when you want to use the list as a
/// `std::map`. The items are stored sorted and duplicates aren't permitted.
///
///   struct MapObject {
///     uint64_t key;
///     std::string value;
///   };
///
///   bool operator==(const MapObject &a, const MapObject &b) {
///     return a.key == b.key;
///   }
///   bool operator<(const MapObject &a, const MapObject &b) {
///     return a.key < b.key;
///   }
///
///
/// When using the list as a map it is recommended that the object stored in the
/// list has a single field that is the "key" of the map. It is also recommended
/// that the key field is the first field in the object. The comparison
/// operators should then only use the key field for comparison.
///
///   utils::SkipList<MapObject> list;
///   {
///     auto accessor = list.access();
///
///     // Inserts an object into the list.
///     accessor.insert(MapObject{5, "hello world"});
///
///     // This operation will return an iterator that isn't equal to
///     // `accessor.end()`. This is because the comparison operators only use
///     // the key field for comparison, the value field is ignored.
///     accessor.find(MapObject{5, "this probably isn't desired"});
///
///     // This will also succeed in removing the object.
///     accessor.remove(MapObject{5, "not good"});
///   }
///
///
/// If you want to be able to query the list using only the key field you have
/// to implement two more comparison operators. Those are:
///
///   bool operator==(const MapObject &a, const uint64_t &b) {
///     return a.key == b;
///   }
///   bool operator<(const MapObject &a, const uint64_t &b) {
///     return a.key < b;
///   }
///
///
/// Now you can use the list in a more intuitive way (as a map):
///
///   {
///     auto accessor = list.access();
///
///     // Inserts an object into the list.
///     accessor.insert({5, "hello world"});
///
///     // This successfully finds the inserted object.
///     accessor.find(5);
///
///     // This successfully removes the inserted object.
///     accessor.remove(5);
///   }
///
///
/// For detailed documentation about the operations available, see the
/// documentation in the Accessor class.
///
/// Note: When using the SkipList as a replacement for `std::set` and searching
/// for some object in the list with `find` you will get an `Iterator` to the
/// object that will allow you to change the contents to the object. Do *not*
/// change the contents of the object! If you change the contents of the object
/// the structure of the SkipList will be compromised!
///
/// Note: When using the SkipList as a replacement for `std::map` and searching
/// for some object in the list the same restriction applies as for the
/// `std::set` use-case. The only difference here is that you must *not* change
/// the key of the object. The value can be modified, but you must have in mind
/// that the change can be done from multiple threads simultaneously so the
/// change must be implemented thread-safe inside the object.
///
/// @tparam TObj object type that is stored in the list
template <typename TObj>
class SkipList final : detail::SkipListNode_base {
 private:
  using TNode = SkipListNode<TObj>;

 public:
  /// Allocator type so that STL containers are aware that we need one.
  using allocator_type = Allocator<TNode>;

  class ConstIterator;

  class Iterator final {
   private:
    friend class SkipList;
    friend class ConstIterator;

    Iterator(TNode *node) : node_(node) {}

   public:
    TObj &operator*() const { return node_->obj; }

    TObj *operator->() const { return &node_->obj; }

    bool operator==(const Iterator &other) const { return node_ == other.node_; }

    bool operator!=(const Iterator &other) const { return node_ != other.node_; }

    Iterator &operator++() {
      while (true) {
        node_ = node_->nexts[0].load(std::memory_order_acquire);
        if (node_ != nullptr && node_->marked.load(std::memory_order_acquire)) {
          continue;
        } else {
          return *this;
        }
      }
    }

   private:
    TNode *node_;
  };

  class ConstIterator final {
   private:
    friend class SkipList;

    ConstIterator(TNode *node) : node_(node) {}

   public:
    ConstIterator(const Iterator &it) : node_(it.node_) {}

    const TObj &operator*() const { return node_->obj; }

    const TObj *operator->() const { return &node_->obj; }

    bool operator==(const ConstIterator &other) const { return node_ == other.node_; }

    bool operator!=(const ConstIterator &other) const { return node_ != other.node_; }

    ConstIterator &operator++() {
      while (true) {
        node_ = node_->nexts[0].load(std::memory_order_acquire);
        if (node_ != nullptr && node_->marked.load(std::memory_order_acquire)) {
          continue;
        } else {
          return *this;
        }
      }
    }

   private:
    TNode *node_;
  };

  class Accessor final {
   private:
    friend class SkipList;

    explicit Accessor(SkipList *skiplist) : skiplist_(skiplist), id_(skiplist->gc_.AllocateId()) {}

   public:
    ~Accessor() {
      if (skiplist_ != nullptr) skiplist_->gc_.ReleaseId(id_);
    }

    Accessor(const Accessor &) = delete;
    Accessor &operator=(const Accessor &) = delete;

    Accessor(Accessor &&other) noexcept : skiplist_(other.skiplist_), id_(other.id_) { other.skiplist_ = nullptr; }
    Accessor &operator=(Accessor &&other) noexcept {
      skiplist_ = other.skiplist_;
      id_ = other.id_;
      other.skiplist_ = nullptr;
      return *this;
    }

    /// Functions that return an Iterator (or ConstIterator) to the beginning of
    /// the list.
    Iterator begin() { return Iterator{skiplist_->head_->nexts[0].load(std::memory_order_acquire)}; }
    ConstIterator begin() const { return ConstIterator{skiplist_->head_->nexts[0].load(std::memory_order_acquire)}; }
    ConstIterator cbegin() const { return ConstIterator{skiplist_->head_->nexts[0].load(std::memory_order_acquire)}; }

    /// Functions that return an Iterator (or ConstIterator) to the end of the
    /// list.
    Iterator end() { return Iterator{nullptr}; }
    ConstIterator end() const { return ConstIterator{nullptr}; }
    ConstIterator cend() const { return ConstIterator{nullptr}; }

    std::pair<Iterator, bool> insert(const TObj &object) { return skiplist_->insert(object); }

    /// Inserts an object into the list. It returns an iterator to the item that
    /// is in the list. If the item already exists in the list no insertion is
    /// done and an iterator to the existing item is returned.
    ///
    /// @return Iterator to the item that is in the list
    ///         bool indicates whether the item was inserted into the list
    std::pair<Iterator, bool> insert(TObj &&object) { return skiplist_->insert(std::move(object)); }

    /// Checks whether the key exists in the list.
    ///
    /// @return bool indicating whether the item exists
    template <typename TKey>
    bool contains(const TKey &key) const {
      return skiplist_->template contains(key);
    }

    /// Finds the key in the list and returns an iterator to the item.
    ///
    /// @return Iterator to the item in the list, will be equal to `end()` when
    ///                  the key isn't found
    template <typename TKey>
    Iterator find(const TKey &key) const {
      return skiplist_->template find(key);
    }

    /// Finds the key or the first larger key in the list and returns an
    /// iterator to the item.
    ///
    /// @return Iterator to the item in the list, will be equal to `end()` when
    ///                  no items match the search
    template <typename TKey>
    Iterator find_equal_or_greater(const TKey &key) const {
      return skiplist_->template find_equal_or_greater(key);
    }

    /// Estimates the number of items that are contained in the list that are
    /// identical to the key determined using the equality operator. The default
    /// layer is chosen to optimize duration vs. precision. The lower the layer
    /// used for estimation the higher the duration of the count operation. If
    /// you set the maximum layer for estimation to 1 you will get an exact
    /// count.
    ///
    /// @return uint64_t estimated count of identical items in the list
    template <typename TKey>
    uint64_t estimate_count(const TKey &key, int max_layer_for_estimation = kSkipListCountEstimateDefaultLayer) const {
      return skiplist_->template estimate_count(key, max_layer_for_estimation);
    }

    /// Estimates the number of items that are contained in the list that are
    /// between the lower and upper bounds using the less and equality operator.
    /// The default layer is chosen to optimize duration vs. precision. The
    /// lower the layer used for estimation the higher the duration of the count
    /// operation. If you set the maximum layer for estimation to 1 you will get
    /// an exact count.
    ///
    /// @return uint64_t estimated count of items in the range in the list
    template <typename TKey>
    uint64_t estimate_range_count(const std::optional<utils::Bound<TKey>> &lower,
                                  const std::optional<utils::Bound<TKey>> &upper,
                                  int max_layer_for_estimation = kSkipListCountEstimateDefaultLayer) const {
      return skiplist_->template estimate_range_count(lower, upper, max_layer_for_estimation);
    }

    /// Estimates the average number of objects in the list that have the same
    /// value using the provided equality operator. E.g., if the objects are:
    /// 1, 2, 2, 3, 3, 3; the average number of equals is 2.
    ///
    /// The items in the list must already be ordered by the field that is used
    /// in the equality operator. The default layer is chosen to optimize
    /// duration vs. precision. The lower the layer used for estimation the
    /// higher the duration of the count operation. If you set the maximum layer
    /// for estimation to 1 you will get an exact average number.
    ///
    /// @return uint64_t estimated average number of equal items in the list
    template <typename TCallable>
    uint64_t estimate_average_number_of_equals(
        const TCallable &equal_cmp, int max_layer_for_estimation = kSkipListCountEstimateDefaultLayer) const {
      return skiplist_->template estimate_average_number_of_equals(equal_cmp, max_layer_for_estimation);
    }

    /// Removes the key from the list.
    ///
    /// @return bool indicating whether the removal was successful
    template <typename TKey>
    bool remove(const TKey &key) {
      return skiplist_->template remove(key);
    }

    /// Returns the number of items contained in the list.
    ///
    /// @return size of the list
    uint64_t size() const { return skiplist_->size(); }

   private:
    SkipList *skiplist_{nullptr};
    uint64_t id_{0};
  };

  class ConstAccessor final {
   private:
    friend class SkipList;

    explicit ConstAccessor(const SkipList *skiplist) : skiplist_(skiplist), id_(skiplist->gc_.AllocateId()) {}

   public:
    ~ConstAccessor() {
      if (skiplist_ != nullptr) skiplist_->gc_.ReleaseId(id_);
    }

    ConstAccessor(const ConstAccessor &) = delete;
    ConstAccessor &operator=(const ConstAccessor &) = delete;

    ConstAccessor(ConstAccessor &&other) noexcept : skiplist_(other.skiplist_), id_(other.id_) {
      other.skiplist_ = nullptr;
    }
    ConstAccessor &operator=(ConstAccessor &&other) noexcept {
      skiplist_ = other.skiplist_;
      id_ = other.id_;
      other.skiplist_ = nullptr;
      return *this;
    }

    ConstIterator begin() const { return ConstIterator{skiplist_->head_->nexts[0].load(std::memory_order_acquire)}; }
    ConstIterator cbegin() const { return ConstIterator{skiplist_->head_->nexts[0].load(std::memory_order_acquire)}; }

    ConstIterator end() const { return ConstIterator{nullptr}; }
    ConstIterator cend() const { return ConstIterator{nullptr}; }

    template <typename TKey>
    bool contains(const TKey &key) const {
      return skiplist_->template contains(key);
    }

    template <typename TKey>
    ConstIterator find(const TKey &key) const {
      return skiplist_->template find(key);
    }

    template <typename TKey>
    ConstIterator find_equal_or_greater(const TKey &key) const {
      return skiplist_->template find_equal_or_greater(key);
    }

    template <typename TKey>
    uint64_t estimate_count(const TKey &key, int max_layer_for_estimation = kSkipListCountEstimateDefaultLayer) const {
      return skiplist_->template estimate_count(key, max_layer_for_estimation);
    }

    template <typename TKey>
    uint64_t estimate_range_count(const std::optional<utils::Bound<TKey>> &lower,
                                  const std::optional<utils::Bound<TKey>> &upper,
                                  int max_layer_for_estimation = kSkipListCountEstimateDefaultLayer) const {
      return skiplist_->template estimate_range_count(lower, upper, max_layer_for_estimation);
    }

    template <typename TCallable>
    uint64_t estimate_average_number_of_equals(
        const TCallable &equal_cmp, int max_layer_for_estimation = kSkipListCountEstimateDefaultLayer) const {
      return skiplist_->template estimate_average_number_of_equals(equal_cmp, max_layer_for_estimation);
    }

    uint64_t size() const { return skiplist_->size(); }

   private:
    const SkipList *skiplist_{nullptr};
    uint64_t id_{0};
  };

  explicit SkipList(MemoryResource *memory = NewDeleteResource()) : gc_(memory) {
    static_assert(kSkipListMaxHeight <= 32, "The SkipList height must be less or equal to 32!");
    void *ptr = memory->Allocate(MaxSkipListNodeSize<TObj>());
    // `calloc` would be faster, but the API has no such call.
    memset(ptr, 0, MaxSkipListNodeSize<TObj>());
    // Here we don't call the `SkipListNode` constructor so that the `TObj`
    // doesn't have to have a default constructor. The initialization of the
    // `height` and `lock` variables is done manually.
    // NOTE: The `head_` node doesn't have a valid `TObj` (because we didn't
    // call the constructor), so you mustn't perform any comparisons using its
    // value.
    head_ = static_cast<TNode *>(ptr);
    head_->height = kSkipListMaxHeight;
    new (&head_->lock) utils::SpinLock();
  }

  SkipList(SkipList &&other) noexcept : head_(other.head_), gc_(other.GetMemoryResource()), size_(other.size_.load()) {
    other.head_ = nullptr;
  }

  SkipList &operator=(SkipList &&other) noexcept {
    MG_ASSERT(other.GetMemoryResource() == GetMemoryResource(),
              "Move assignment with different MemoryResource is not supported");
    TNode *head = head_;
    while (head != nullptr) {
      TNode *succ = head->nexts[0].load(std::memory_order_acquire);
      size_t bytes = SkipListNodeSize(*head);
      head->~TNode();
      GetMemoryResource()->Deallocate(head, bytes);
      head = succ;
    }
    head_ = other.head_;
    size_ = other.size_.load();
    other.head_ = nullptr;
    return *this;
  }

  SkipList(const SkipList &) = delete;
  SkipList &operator=(const SkipList &) = delete;

  ~SkipList() {
    if (head_ != nullptr) {
      // Remove all items from the list.
      clear();

      // We need to free the `head_` node manually because we didn't call its
      // constructor (see the note in the `SkipList` constructor). We mustn't
      // call the `TObj` destructor because we didn't call its constructor.
      head_->lock.~SpinLock();
      GetMemoryResource()->Deallocate(head_, SkipListNodeSize(*head_));
    }
  }

  /// Functions that return an accessor to the list. All operations on the list
  /// must be done through the Accessor (or ConstAccessor) proxy object.
  Accessor access() { return Accessor{this}; }
  ConstAccessor access() const { return ConstAccessor{this}; }

  /// The size of the list can be read directly from the list because it is an
  /// atomic operation.
  uint64_t size() const { return size_.load(std::memory_order_acquire); }

  MemoryResource *GetMemoryResource() const { return gc_.GetMemoryResource(); }

  /// This function removes all elements from the list.
  /// NOTE: The function *isn't* thread-safe. It must be called only if there are
  /// no more active accessors using the list.
  void clear() {
    TNode *curr = head_->nexts[0].load(std::memory_order_acquire);
    while (curr != nullptr) {
      TNode *succ = curr->nexts[0].load(std::memory_order_acquire);
      size_t bytes = SkipListNodeSize(*curr);
      curr->~TNode();
      GetMemoryResource()->Deallocate(curr, bytes);
      curr = succ;
    }
    for (int layer = 0; layer < kSkipListMaxHeight; ++layer) {
      head_->nexts[layer] = nullptr;
    }
    size_ = 0;
    gc_.Clear();
  }

  void run_gc() { gc_.Run(); }

 private:
  template <typename TKey>
  int find_node(const TKey &key, TNode *preds[], TNode *succs[]) const {
    int layer_found = -1;
    TNode *pred = head_;
    for (int layer = kSkipListMaxHeight - 1; layer >= 0; --layer) {
      TNode *curr = pred->nexts[layer].load(std::memory_order_acquire);
      // Existence test is missing in the paper.
      while (curr != nullptr && curr->obj < key) {
        pred = curr;
        curr = pred->nexts[layer].load(std::memory_order_acquire);
      }
      // Existence test is missing in the paper.
      if (layer_found == -1 && curr && curr->obj == key) {
        layer_found = layer;
      }
      preds[layer] = pred;
      succs[layer] = curr;
    }
    if (layer_found + 1 >= kSkipListGcHeightTrigger) gc_.Run();
    return layer_found;
  }

  template <typename TObjUniv>
  std::pair<Iterator, bool> insert(TObjUniv &&object) {
    int top_layer = gen_height();
    TNode *preds[kSkipListMaxHeight], *succs[kSkipListMaxHeight];
    if (top_layer >= kSkipListGcHeightTrigger) gc_.Run();
    while (true) {
      int layer_found = find_node(object, preds, succs);
      if (layer_found != -1) {
        TNode *node_found = succs[layer_found];
        if (!node_found->marked.load(std::memory_order_acquire)) {
          while (!node_found->fully_linked.load(std::memory_order_acquire))
            ;
          return {Iterator{node_found}, false};
        }
        continue;
      }

      std::unique_lock<SpinLock> guards[kSkipListMaxHeight];
      TNode *pred, *succ, *prev_pred = nullptr;
      bool valid = true;
      // The paper has a wrong condition here. In the paper it states that this
      // loop should have `(layer <= top_layer)`, but that isn't correct.
      for (int layer = 0; valid && (layer < top_layer); ++layer) {
        pred = preds[layer];
        succ = succs[layer];
        if (pred != prev_pred) {
          guards[layer] = std::unique_lock<SpinLock>(pred->lock);
          prev_pred = pred;
        }
        // Existence test is missing in the paper.
        valid = !pred->marked.load(std::memory_order_acquire) &&
                pred->nexts[layer].load(std::memory_order_acquire) == succ &&
                (succ == nullptr || !succ->marked.load(std::memory_order_acquire));
      }

      if (!valid) continue;

      size_t node_bytes = sizeof(TNode) + top_layer * sizeof(std::atomic<TNode *>);
      void *ptr = GetMemoryResource()->Allocate(node_bytes);
      // `calloc` would be faster, but the API has no such call.
      memset(ptr, 0, node_bytes);
      auto *new_node = static_cast<TNode *>(ptr);
      // Construct through allocator so it propagates if needed.
      Allocator<TNode> allocator(GetMemoryResource());
      allocator.construct(new_node, top_layer, std::forward<TObjUniv>(object));

      // The paper is also wrong here. It states that the loop should go up to
      // `top_layer` which is wrong.
      for (int layer = 0; layer < top_layer; ++layer) {
        new_node->nexts[layer].store(succs[layer], std::memory_order_release);
        preds[layer]->nexts[layer].store(new_node, std::memory_order_release);
      }

      new_node->fully_linked.store(true, std::memory_order_release);
      size_.fetch_add(1, std::memory_order_acq_rel);
      return {Iterator{new_node}, true};
    }
  }

  template <typename TKey>
  bool contains(const TKey &key) const {
    TNode *preds[kSkipListMaxHeight], *succs[kSkipListMaxHeight];
    int layer_found = find_node(key, preds, succs);
    return (layer_found != -1 && succs[layer_found]->fully_linked.load(std::memory_order_acquire) &&
            !succs[layer_found]->marked.load(std::memory_order_acquire));
  }

  template <typename TKey>
  Iterator find(const TKey &key) const {
    TNode *preds[kSkipListMaxHeight], *succs[kSkipListMaxHeight];
    int layer_found = find_node(key, preds, succs);
    if (layer_found != -1 && succs[layer_found]->fully_linked.load(std::memory_order_acquire) &&
        !succs[layer_found]->marked.load(std::memory_order_acquire)) {
      return Iterator{succs[layer_found]};
    }
    return Iterator{nullptr};
  }

  template <typename TKey>
  Iterator find_equal_or_greater(const TKey &key) const {
    TNode *preds[kSkipListMaxHeight], *succs[kSkipListMaxHeight];
    find_node(key, preds, succs);
    if (succs[0] && succs[0]->fully_linked.load(std::memory_order_acquire) &&
        !succs[0]->marked.load(std::memory_order_acquire)) {
      return Iterator{succs[0]};
    }
    return Iterator{nullptr};
  }

  template <typename TKey>
  uint64_t estimate_count(const TKey &key, int max_layer_for_estimation) const {
    MG_ASSERT(max_layer_for_estimation >= 1 && max_layer_for_estimation <= kSkipListMaxHeight,
              "Invalid layer for SkipList count estimation!");

    TNode *preds[kSkipListMaxHeight], *succs[kSkipListMaxHeight];
    int layer_found = find_node(key, preds, succs);
    if (layer_found == -1) {
      return 0;
    }

    uint64_t count = 0;
    TNode *pred = preds[layer_found];
    for (int layer = std::min(layer_found, max_layer_for_estimation - 1); layer >= 0; --layer) {
      uint64_t nodes_traversed = 0;
      TNode *curr = pred->nexts[layer].load(std::memory_order_acquire);
      while (curr != nullptr && curr->obj < key) {
        pred = curr;
        curr = pred->nexts[layer].load(std::memory_order_acquire);
      }
      while (curr != nullptr && curr->obj == key) {
        pred = curr;
        curr = pred->nexts[layer].load(std::memory_order_acquire);
        ++nodes_traversed;
      }
      // Here we assume that the list is perfectly balanced and that each upper
      // layer will have two times less items than the layer below it.
      count += (1ULL << layer) * nodes_traversed;
    }

    return count;
  }

  template <typename TKey>
  uint64_t estimate_range_count(const std::optional<utils::Bound<TKey>> &lower,
                                const std::optional<utils::Bound<TKey>> &upper, int max_layer_for_estimation) const {
    MG_ASSERT(max_layer_for_estimation >= 1 && max_layer_for_estimation <= kSkipListMaxHeight,
              "Invalid layer for SkipList count estimation!");

    TNode *preds[kSkipListMaxHeight], *succs[kSkipListMaxHeight];
    int layer_found = -1;
    if (lower) {
      layer_found = find_node(lower->value(), preds, succs);
    } else {
      for (auto &pred : preds) {
        pred = head_;
      }
      layer_found = kSkipListMaxHeight - 1;
    }
    if (layer_found == -1) {
      return 0;
    }

    uint64_t count = 0;
    TNode *pred = preds[layer_found];
    for (int layer = std::min(layer_found, max_layer_for_estimation - 1); layer >= 0; --layer) {
      uint64_t nodes_traversed = 0;
      TNode *curr = pred->nexts[layer].load(std::memory_order_acquire);
      if (lower) {
        while (curr != nullptr && curr->obj < lower->value()) {
          pred = curr;
          curr = pred->nexts[layer].load(std::memory_order_acquire);
        }
        if (lower->IsExclusive()) {
          while (curr != nullptr && curr->obj == lower->value()) {
            pred = curr;
            curr = pred->nexts[layer].load(std::memory_order_acquire);
          }
        }
      }
      if (upper) {
        while (curr != nullptr && curr->obj < upper->value()) {
          pred = curr;
          curr = pred->nexts[layer].load(std::memory_order_acquire);
          ++nodes_traversed;
        }
        if (upper->IsInclusive()) {
          while (curr != nullptr && curr->obj == upper->value()) {
            pred = curr;
            curr = pred->nexts[layer].load(std::memory_order_acquire);
            ++nodes_traversed;
          }
        }
      } else {
        while (curr != nullptr) {
          pred = curr;
          curr = pred->nexts[layer].load(std::memory_order_acquire);
          ++nodes_traversed;
        }
      }
      // Here we assume that the list is perfectly balanced and that each upper
      // layer will have two times less items than the layer below it.
      count += (1ULL << layer) * nodes_traversed;
    }

    return count;
  }

  template <typename TCallable>
  uint64_t estimate_average_number_of_equals(const TCallable &equal_cmp, int max_layer_for_estimation) const {
    MG_ASSERT(max_layer_for_estimation >= 1 && max_layer_for_estimation <= kSkipListMaxHeight,
              "Invalid layer for SkipList count estimation!");

    // We need to traverse some nodes to make the calculation correct, so find
    // the first layer that has some nodes, starting from the hinted layer.
    TNode *curr = nullptr;
    int layer = max_layer_for_estimation;
    while (curr == nullptr && layer > 0) {
      layer -= 1;
      curr = head_->nexts[layer].load(std::memory_order_acquire);
    }
    if (curr == nullptr) {
      // There are no elements in the list.
      return 0;
    }

    // Traverse the chain of nodes and count how many of them are unique and how
    // many have been visited in total. The traversal is initiated from the
    // determined layer. Then, equality is checked using layer 1 and a return to
    // the desired layer is performed. The traversal over layers looks as
    // follows ("+" are counted nodes, "*" are visited nodes):
    //
    //  10: *              *--*     *-- ...
    //   9: |              |  |     |
    //   8: |           *--*  |     |
    //   7: |           |     |     |
    //   6: |           |     |  *--*
    //   5: |     *--*--*     |  |
    //   4: |     |           |  |
    //   3: |     |           |  |
    //   2: |     |           |  |
    //   1: +--+--+           +--+
    uint64_t unique_count = 0;
    uint64_t nodes_traversed = 0;
    uint64_t traversal_limit = size_.load(std::memory_order_acquire);
    if (layer != 0) {
      // If the layer isn't 0 we don't want to traverse all of the equal items
      // because the whole list can be the same item. That is why we limit the
      // traversal to at most `sqrt(list_size)` items which is a good balance
      // between general correctness and time complexity.
      traversal_limit = static_cast<uint64_t>(std::sqrt(size_.load(std::memory_order_acquire)));
    }
    while (curr != nullptr) {
      // First, traverse the bottom layer to count the items.
      ++unique_count;
      TNode *pred = nullptr;
      uint64_t current_traversed = 0;
      while (curr != nullptr && current_traversed < traversal_limit) {
        if (pred) {
          const auto &pred_obj = pred->obj;
          const auto &curr_obj = curr->obj;
          if (!equal_cmp(pred_obj, curr_obj)) {
            break;
          }
        }
        pred = curr;
        curr = pred->nexts[0].load(std::memory_order_acquire);
        ++current_traversed;
      }
      nodes_traversed += current_traversed;
      // Second, find a node that has the necessary hight to return to the
      // desired layer.
      while (curr != nullptr && curr->height - 1 < layer) {
        curr = curr->nexts[curr->height - 1].load(std::memory_order_acquire);
      }
    }

    MG_ASSERT(unique_count > 0);

    return nodes_traversed / unique_count;
  }

  bool ok_to_delete(TNode *candidate, int layer_found) {
    // The paper has an incorrect check here. It expects the `layer_found`
    // variable to be 1-indexed, but in fact it is 0-indexed.
    return (candidate->fully_linked.load(std::memory_order_acquire) && candidate->height == layer_found + 1 &&
            !candidate->marked.load(std::memory_order_acquire));
  }

  template <typename TKey>
  bool remove(const TKey &key) {
    TNode *node_to_delete = nullptr;
    bool is_marked = false;
    int top_layer = -1;
    TNode *preds[kSkipListMaxHeight], *succs[kSkipListMaxHeight];
    std::unique_lock<SpinLock> node_guard;
    while (true) {
      int layer_found = find_node(key, preds, succs);
      if (is_marked || (layer_found != -1 && ok_to_delete(succs[layer_found], layer_found))) {
        if (!is_marked) {
          node_to_delete = succs[layer_found];
          top_layer = node_to_delete->height;
          node_guard = std::unique_lock<SpinLock>(node_to_delete->lock);
          if (node_to_delete->marked.load(std::memory_order_acquire)) {
            return false;
          }
          node_to_delete->marked.store(true, std::memory_order_release);
          is_marked = true;
        }

        std::unique_lock<SpinLock> guards[kSkipListMaxHeight];
        TNode *pred, *succ, *prev_pred = nullptr;
        bool valid = true;
        // The paper has a wrong condition here. In the paper it states that
        // this loop should have `(layer <= top_layer)`, but that isn't correct.
        for (int layer = 0; valid && (layer < top_layer); ++layer) {
          pred = preds[layer];
          succ = succs[layer];
          if (pred != prev_pred) {
            guards[layer] = std::unique_lock<SpinLock>(pred->lock);
            prev_pred = pred;
          }
          valid = !pred->marked.load(std::memory_order_acquire) &&
                  pred->nexts[layer].load(std::memory_order_acquire) == succ;
        }

        if (!valid) continue;

        // The paper is also wrong here. It states that the loop should start
        // from `top_layer` which is wrong.
        for (int layer = top_layer - 1; layer >= 0; --layer) {
          preds[layer]->nexts[layer].store(node_to_delete->nexts[layer].load(std::memory_order_acquire),
                                           std::memory_order_release);
        }
        gc_.Collect(node_to_delete);
        size_.fetch_add(-1, std::memory_order_acq_rel);
        return true;
      } else {
        return false;
      }
    }
  }

 private:
  TNode *head_{nullptr};
  // gc_ also stores the only copy of `MemoryResource *`, to save space.
  mutable SkipListGc<TObj> gc_;

  std::atomic<uint64_t> size_{0};
};

}  // namespace memgraph::utils
