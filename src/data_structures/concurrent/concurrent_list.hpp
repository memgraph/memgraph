#pragma once

#include <atomic>
#include <utility>
#include "glog/logging.h"
#include "utils/crtp.hpp"

// TODO: reimplement this. It's correct but somewhat inefecient and it could be
// done better.
template <class T>
class ConcurrentList {
 private:
  template <class V>
  static V load(std::atomic<V> &atomic) {
    return atomic.load(std::memory_order_acquire);
  }

  template <class V>
  static void store(std::atomic<V> &atomic,
                    V desired) {  // Maybe could be relaxed
    atomic.store(desired, std::memory_order_release);
  }

  template <class V>
  static bool cas(
      std::atomic<V> &atomic, V expected,
      V desired) {  // Could be relaxed but must be at least Release.
    return atomic.compare_exchange_strong(expected, desired,
                                          std::memory_order_seq_cst);
  }

  template <class V>
  static V *swap(std::atomic<V *> &atomic, V *desired) {  // Could be relaxed
    return atomic.exchange(desired, std::memory_order_seq_cst);
  }

  // Basic element in a ConcurrentList
  class Node {
   public:
    Node(const T &data) : data(data) {}
    Node(T &&data) : data(std::move(data)) {}

    // Carried data
    T data;

    // Next element in list or nullptr if end.
    std::atomic<Node *> next{nullptr};

    // Next removed element in list or nullptr if end.
    std::atomic<Node *> next_rem{nullptr};

    // True if node has logicaly been removed from list.
    std::atomic<bool> removed{false};
  };

  // Base for Mutable and Immutable iterators. Also serves as accessor to the
  // list uses for safe garbage disposall.
  template <class It>
  class IteratorBase : public Crtp<It> {
    friend class ConcurrentList;

   protected:
    IteratorBase() : list(nullptr), curr(nullptr) {}

    IteratorBase(ConcurrentList *list) : list(list) {
      DCHECK(list != nullptr) << "List is nullptr.";
      // Increment number of iterators accessing list.
      list->active_threads_no_++;
      // Start from the begining of list.
      reset();
    }

   public:
    IteratorBase(const IteratorBase &) = delete;

    IteratorBase(IteratorBase &&other)
        : list(other.list), prev(other.prev), curr(other.curr) {
      other.list = nullptr;
      other.curr = nullptr;
      other.prev = nullptr;
    }

    ~IteratorBase() {
      if (list == nullptr) {
        return;
      }

      auto head_rem = load(list->removed);

      // Next IF checks if this thread is responisble for disposall of
      // collected garbage.
      // Fetch could be relaxed
      // There exist possibility that no one will delete garbage at this
      // time but it will be deleted at some other time.
      if (list->active_threads_no_.fetch_sub(1) ==
              1 &&                // I am the last one accessing
          head_rem != nullptr &&  // There is some garbage
          cas<Node *>(list->removed, head_rem,
                      nullptr)  // No new garbage was added.
      ) {
        // Delete all removed node following chain of next_rem starting
        // from head_rem.
        auto now = head_rem;
        do {
          auto next = load(now->next_rem);
          delete now;
          now = next;
        } while (now != nullptr);
      }
    }

    IteratorBase &operator=(IteratorBase const &other) = delete;
    IteratorBase &operator=(IteratorBase &&other) = delete;

    T &operator*() const {
      DCHECK(valid()) << "Not valid data.";
      return curr->data;
    }
    T *operator->() const {
      DCHECK(valid()) << "Not valid data.";
      return &(curr->data);
    }

    bool valid() const { return curr != nullptr; }

    // Iterating is wait free.
    It &operator++() {
      DCHECK(valid()) << "Not valid data.";
      do {
        // We don't care about previous unless it's alive. If it's not alive we
        // are going to look for it again and just incurr performance hit
        // because of constant lookup of previous alive iterator while
        // re-linking.
        if (!curr->removed) prev = curr;
        curr = load(curr->next);
      } while (valid() && is_removed());  // Loop ends if end of list is
                                          // found or if not removed
                                          // element is found.
      return this->derived();
    }
    It &operator++(int) { return operator++(); }

    bool is_removed() {
      DCHECK(valid()) << "Not valid data.";
      return load(curr->removed);
    }

    // Returns IteratorBase to begining
    void reset() {
      prev = nullptr;
      curr = load(list->head);
      if (valid() && is_removed()) {
        operator++();
      }
    }

    // Adds to the begining of list
    // It is lock free but it isn't wait free.
    void push(T &&data) {
      // It could be done with unique_ptr but while this could meen memory
      // leak on excpetion, unique_ptr could meean use after free. Memory
      // leak is less dangerous.
      auto node = new Node(data);
      Node *next = nullptr;
      // Insert at begining of list. Retrys on failure.
      do {
        next = load(list->head);
        // First connect to next.
        store(node->next, next);
        // Then try to set as head.
      } while (!cas(list->head, next, node));

      list->count_.fetch_add(1);
    }

    // True only if this call removed the element. Only reason for fail is
    // if the element is already removed.
    // Remove has deadlock if another thread dies between marking node for
    // removal and the disconnection.
    // This can be improved with combinig the removed flag with prev.next or
    // curr.next
    bool remove() {
      DCHECK(valid()) << "Not valid data.";
      // Try to logically remove it.
      if (cas(curr->removed, false, true)) {
        // I removed it!!!
        // Try to disconnect it from list.
        if (!disconnect()) {
          // Disconnection failed because Node relative location in
          // list changed. Whe firstly must find it again and then try
          // to disconnect it again.
          find_and_disconnect();
        }
        // Add to list of to be garbage collected.
        store(curr->next_rem, swap(list->removed, curr));
        list->count_.fetch_sub(1);
        return true;
      }
      return false;
    }

    friend bool operator==(const It &a, const It &b) {
      return a.curr == b.curr;
    }

    friend bool operator!=(const It &a, const It &b) { return !(a == b); }

   private:
    // Fids current element starting from the begining of the list Retrys
    // until it succesffuly disconnects it.
    void find_and_disconnect() {
      Node *bef = nullptr;
      auto now = load(list->head);
      auto next = load(curr->next);
      while (now != nullptr) {
        if (now == curr) {
          // Found it.
          prev = bef;  // Set the correct previous node in list.
          if (disconnect()) {
            // succesffuly disconnected it.
            return;
          }
          // Let's try again from the begining.
          bef = nullptr;
          now = load(list->head);
        } else if (now == next) {  // Comparison with next is
                                   // optimization for early return.
          return;
        } else {
          // Now isn't the one whe are looking for lets try next one.
          bef = now;
          now = load(now->next);
        }
      }
    }

    // Trys to disconnect currrent element from
    bool disconnect() {
      auto next = load(curr->next);
      if (prev != nullptr) {
        store(prev->next, next);
        if (load(prev->removed)) {
          // previous isn't previous any more.
          return false;
        }
      } else if (!cas(list->head, curr, next)) {
        return false;
      }
      return true;
    }

    ConcurrentList *list;
    Node *prev{nullptr};
    Node *curr;
  };

 public:
  class ConstIterator : public IteratorBase<ConstIterator> {
    friend class ConcurrentList;

   public:
    using IteratorBase<ConstIterator>::IteratorBase;

    const T &operator*() const {
      return IteratorBase<ConstIterator>::operator*();
    }

    const T *operator->() const {
      return IteratorBase<ConstIterator>::operator->();
    }

    operator const T &() const {
      return IteratorBase<ConstIterator>::operator T &();
    }
  };

  class Iterator : public IteratorBase<Iterator> {
    friend class ConcurrentList;

   public:
    using IteratorBase<Iterator>::IteratorBase;
  };

 public:
  ConcurrentList() = default;

  ConcurrentList(ConcurrentList &) = delete;
  ConcurrentList(ConcurrentList &&) = delete;

  ~ConcurrentList() {
    auto now = head.load();
    while (now != nullptr) {
      auto next = now->next.load();
      delete now;
      now = next;
    }
  }

  void operator=(ConcurrentList &) = delete;

  Iterator begin() { return Iterator(this); }

  ConstIterator cbegin() { return ConstIterator(this); }

  Iterator end() { return Iterator(); }

  ConstIterator cend() { return ConstIterator(); }

  std::size_t active_threads_no() { return active_threads_no_.load(); }
  std::size_t size() { return count_.load(); }

 private:
  // TODO: use lazy GC or something else as a garbage collection strategy
  //       use the same principle as in skiplist
  std::atomic<std::size_t> active_threads_no_{0};
  std::atomic<std::size_t> count_{0};
  std::atomic<Node *> head{nullptr};
  std::atomic<Node *> removed{nullptr};
};
