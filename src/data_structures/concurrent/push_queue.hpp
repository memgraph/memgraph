#pragma once

#include <atomic>
#include <mutex>

#include "glog/logging.h"

/** @brief A queue with lock-free concurrent push and
 * single-threaded deletion.
 *
 * Deletions are done via the iterator. Currently only
 * tail-deletion is supported, but individual element
 * deletion should be possible to implement.
 * Iteration can also be concurrent, as long as nobody
 * is deleting.
 *
 * @tparam TElement Type of element stored in the
 * queue.
 */
template <typename TElement>
class ConcurrentPushQueue {
 private:
  // The queue is implemented as as singly-linked list
  // and this is one list element
  class Node {
   public:
    // constructor that accepts only arguments for
    // creating the element. it can accept both rvalue and lvalue
    // refs to an element, or arguments for emplace creation of
    // an element
    template <typename... TArgs>
    Node(TArgs &&... args) : element_(std::forward<TArgs>(args)...) {}

    // element itself and pointer to next Node
    TElement element_;
    Node *next_{nullptr};
  };

  /** @brief Iterator over the queue.
   *
   * Exposes standard forward-iterator ops, plus the
   * delete_tail() function.
   *
   * @tparam TQueue - either const or non-const ConcurrentPushQueue
   */
  template <typename TQueue>
  struct Iterator {
   public:
    Iterator(TQueue &queue, Node *current) : queue_(queue), current_(current) {}

    // copying, moving and destroying is all fine

    bool operator==(const Iterator &rhs) const {
      return rhs.current_ == this->current_;
    }

    bool operator!=(const Iterator &rhs) const { return !(*this == rhs); }

    Iterator &operator++() {
      DCHECK(current_ != nullptr) << "Prefix increment on invalid iterator";
      previous_ = current_;
      current_ = current_->next_;
      return *this;
    }

    Iterator operator++(int) {
      DCHECK(current_ != nullptr) << "Postfix increment on invalid iterator";
      Iterator rval(queue_, current_);
      previous_ = current_;
      current_ = current_->next_;
      return rval;
    }

    TElement &operator*() {
      DCHECK(current_ != nullptr)
          << "Dereferencing operator on invalid iterator";
      return current_->element_;
    }
    TElement *operator->() {
      DCHECK(current_ != nullptr) << "Arrow operator on invalid iterator";
      return &current_->element_;
    }

    /** @brief Deletes the current element of the iterator,
     * and all subsequent elements.
     *
     * After this op this iterator is not valid and is equal
     * to push_queue.end(). Invalidates all the iterators
     * that were equal to this or further away from the head.
     *
     * @return The number of deleted elements.
     */
    size_t delete_tail() {
      // compare and swap queue head
      // if it succeeds, it means that the current node of this
      // iterator was head, and we have just replaced head with nullptr
      // if it fails, it means the current node was not head, and
      // we got the new_head
      auto new_head = current_;
      bool was_head =
          queue_.get().head_.compare_exchange_strong(new_head, nullptr);
      // if we have no previous_ (iterator wasn't incremented and thinks it's
      // head), but in reality we weren't head, then we need to find the actual
      // previous_  node, so we can cut the tail off
      if (!previous_ && !was_head) {
        previous_ = new_head;
        while (previous_->next_ != current_) previous_ = previous_->next_;
      }

      // cut the tail off
      if (previous_) previous_->next_ = nullptr;

      // delete all from current to the end, track deletion count
      size_t deleted = 0;
      previous_ = current_;
      while (current_) {
        previous_ = current_;
        current_ = current_->next_;
        delete previous_;
        deleted++;
      }

      // update the size of the queue and return
      queue_.get().size_ -= deleted;
      return deleted;
    }

   private:
    // the queue we're iterating over
    // use a reference wrapper because it facilitates move and copy
    std::reference_wrapper<TQueue> queue_;
    // the current node of this iterator
    Node *current_{nullptr};
    // the previous node of this iterator
    // used for deleting the current node
    Node *previous_{nullptr};
  };

 public:
  ConcurrentPushQueue() = default;
  ~ConcurrentPushQueue() { begin().delete_tail(); }

  // copy and move ops are disabled due to atomic member variable

  /// Pushes an element to the queue.
  // TODO: review - this is at the same time push and emplace,
  // how should it be called?
  template <typename... TArgs>
  void push(TArgs &&... args) {
    auto node = new Node(std::forward<TArgs>(args)...);
    while (!head_.compare_exchange_strong(node->next_, node)) continue;
    size_++;
  };

  // non-const iterators
  auto begin() { return Iterator<ConcurrentPushQueue>(*this, head_.load()); }
  auto end() { return Iterator<ConcurrentPushQueue>(*this, nullptr); }

  // const iterators
  auto cbegin() const {
    return Iterator<const ConcurrentPushQueue>(*this, head_.load());
  }
  auto cend() const {
    return Iterator<const ConcurrentPushQueue>(*this, nullptr);
  }
  auto begin() const {
    return Iterator<const ConcurrentPushQueue>(*this, head_.load());
  }
  auto end() const {
    return Iterator<const ConcurrentPushQueue>(*this, nullptr);
  }

  auto size() const { return size_.load(); }

 private:
  // head of the queue (last-added element)
  std::atomic<Node *> head_{nullptr};
  // number of elements in the queue
  std::atomic<size_t> size_{0};
};
