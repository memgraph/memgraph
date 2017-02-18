#pragma once

#include <atomic>
#include <memory>

namespace lockfree {

/** @brief Multiple-Producer Single-Consumer Queue
 *  A wait-free (*) multiple-producer single-consumer queue.
 *
 *  features:
 *    - wait-free
 *    - fast producers (only one atomic XCHG and and one atomic store with
 *      release semantics)
 *    - extremely fast consumer (only atomic loads with acquire semantics on
 *      the fast path and atomic loads + atomic XCHG on the slow path)
 *    - no need for order reversion -> pop() is always O(1)
 *    - ABA free
 *
 *  great for using in loggers, garbage collectors etc.
 *
 *  (*) there is a small window of inconsistency from the lock free design
 *      see the url below for details
 *  URL:
 * http://www.1024cores.net/home/lock-free-algorithms/queues/intrusive-mpsc-node-based-queue
 *
 *  mine is not intrusive for better modularity, but with slightly worse
 *  performance because it needs to do two memory allocations instead of
 *  one
 *
 *  @tparam T Type of the items to store in the queue
 */
template <class T>
class MpscQueue {
  struct Node {
    Node(Node* next, std::unique_ptr<T>&& item)
        : next(next), item(std::forward<std::unique_ptr<T>>(item)) {}

    std::atomic<Node*> next;
    std::unique_ptr<T> item;
  };

 public:
  MpscQueue() {
    auto stub = new Node(nullptr, nullptr);
    head.store(stub);
    tail = stub;
  }

  ~MpscQueue() {
    // purge all elements from the queue
    while (pop()) {
    }

    // we are left with a stub, delete that
    delete tail;
  }

  MpscQueue(MpscQueue&) = delete;
  MpscQueue(MpscQueue&&) = delete;

  /** @brief Pushes an item into the queue.
   *
   *  Pushes an item into the front of the queue.
   *
   *  @param item std::unique_ptr<T> An item to push into the queue
   *  @return void
   */
  void push(std::unique_ptr<T>&& item) {
    push(new Node(nullptr, std::forward<std::unique_ptr<T>>(item)));
  }

  /** @brief Pops a node from the queue.
   *
   *  Pops and returns a node from the back of the queue.
   *
   *  @return std::unique_ptr<T> A pointer to the node popped from the
   *      queue, nullptr if nothing was popped
   */
  std::unique_ptr<T> pop() {
    auto tail = this->tail;

    // serialization point wrt producers
    auto next = tail->next.load(std::memory_order_acquire);

    if (next) {
      // remove the last stub from the queue
      // make [2] the next stub and return it's data
      //
      // H --> [n] <- ... <- [2] <--+--[STUB]   +-- T
      //                            |           |
      //                            +-----------+
      this->tail = next;

      // delete the stub node
      // H --> [n] <- ... <- [STUB] <-- T
      delete tail;

      return std::move(next->item);
    }

    return nullptr;
  }

 private:
  std::atomic<Node*> head;
  Node* tail;

  /** @brief Pushes a new node into the queue.
   *
   *  Pushes a new node containing the item into the front of the queue.
   *
   *  @param node Node* A pointer to node you want to push into the queue
   *  @return void
   */
  void push(Node* node) {
    // initial state
    // H --> [3] <- [2] <- [STUB] <-- T

    // serialization point wrt producers, acquire-release
    auto old = head.exchange(node, std::memory_order_acq_rel);

    // after exchange
    // H --> [4]    [3] <- [2] <- [STUB] <-- T

    // this is the window of inconsistency, if the producer is blocked
    // here, the consumer is also blocked. but this window is extremely
    // small, it's followed by a store operation which is a
    // serialization point wrt consumer

    // old holds a pointer to node [3] and we need to link the [3] to a
    // newly created node [4] using release semantics

    // serialization point wrt consumer, release
    old->next.store(node, std::memory_order_release);

    // finally, we have a queue like this
    // H --> [4] <- [3] <- [2] <- [1] <-- T
  }
};
}
