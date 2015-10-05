#ifndef MEMGRAPH_DATA_STRUCTURES_QUEUE_MPSC_QUEUE
#define MEMGRAPH_DATA_STRUCTURES_QUEUE_MPSC_QUEUE

#include <atomic>
#include <memory>

namespace lockfree
{

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
 *  URL: http://www.1024cores.net/home/lock-free-algorithms/queues/intrusive-mpsc-node-based-queue
 *
 *  mine is not intrusive for better modularity, but with slightly worse
 *  performance because it needs to do two memory allocations instead of
 *  one
 *
 *  @tparam T Type of the items to store in the queue
 */
template <class T>
class MpscQueue
{
    struct Node
    {
        std::atomic<Node*> next;
        std::unique_ptr<T> item;
    };

public:
    MpscQueue()
    {
        std::atomic_init(&stub.next, nullptr);
        std::atomic_init(&head, &stub);
        tail = &stub;
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
    void push(std::unique_ptr<T> item)
    {
        push(new Node { nullptr, std::move(item) });
    }

    /** @brief Pops a node from the queue.
     *
     *  Pops and returns a node from the back of the queue.
     *
     *  @return std::unique_ptr<T> A pointer to the node popped from the
     *      queue, nullptr if nothing was popped
     */
    std::unique_ptr<T> pop()
    {
        auto last = tail;
        auto next = tail->next.load(std::memory_order_acquire);

        // there was a time when there weren't any items in the queue so a
        // stub was pushed
        // ... [STUB] <-- T
        if (last == &stub)
        {
            // check if this still is the only item in the queue
            // H --> [STUB] <-- T
            if(next == nullptr)
                return nullptr;

            // if it's not, the queue looks something like this
            // H --> [n] <- ... <- [1] <- [STUB] <- T
            
            // remove the stub from the queue
            // H --> [n] <- ... <- [1] <--+--[STUB]   +-- T
            //                            |           |
            //                            +-----------+
            tail = next;
            last = next;
            next = next->next.load(std::memory_order_acquire);
        }

        if(next)
        {
            // node [1] has next, swap tail to next
            // H --> [3] <- [2] <- [1] <- T
            tail = next;

            // using unique ptr so that last will be deleted on return :D
            auto last_unique = std::unique_ptr<Node>(last);

            // move the item from the last node to the caller
            return std::move(last->item);
        }

        auto first = head.load(std::memory_order_acquire);

        // no need to push the stub if more elements exist in the queue
        // H --> [n] <- ... <- [1] <-- T
        if(tail != first)
            return nullptr;
        
        // the state looks like this
        // H --> [1] <-- T

        // so we need to push the stub in order to pop [1]
        // H --> [STUB] <- [1] <-- T
        push(&stub);

        next = tail->next;

        if(next)
        {
            // remove the item [1] from the queue
            // H --> [n] <- ... <- [STUB] <--+--[1]   +-- T
            //                               |        |
            //                               +--------+
            tail = next;
            return tail;
        }

        return nullptr;
    }

private:
    std::atomic<Node*> head;
    Node* tail;
    Node stub;

    /** @brief Pushes a new node into the queue.
     *
     *  Pushes a new node containing the item into the front of the queue.
     *
     *  @param node Node* A pointer to node you want to push into the queue
     *  @return void
     */
    void push(Node* node)
    {
        // initial state
        // H --> [3] <- [2] <- [1] <-- T
        auto old = head.exchange(node);

        // after exchange
        // H --> [4]    [3] <- [2] <- [1] <-- T

        // old holds a pointer to node [3] and we need to link the [3] to a
        // newly created node [4] using release semantics
        old->next.store(node, std::memory_order_release);

        // finally, we have a queue like this
        // H --> [4] <- [3] <- [2] <- [1] <-- T
    }
};

}

#endif
