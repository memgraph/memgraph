#pragma once

#include <atomic>
#include <memory>

namespace lockfree
{

template <class T, size_t N>
class BoundedSpscQueue
{
public:
    static constexpr size_t size = N;

    BoundedSpscQueue() = default;

    BoundedSpscQueue(const BoundedSpscQueue&) = delete;
    BoundedSpscQueue(BoundedSpscQueue&&) = delete;

    BoundedSpscQueue& operator=(const BoundedSpscQueue&) = delete;

    bool push(const T& item)
    {
        // load the current tail
        // [] [] [1] [2] [3] [4] [5] [$] []
        //        H                   T
        auto t = tail.load(std::memory_order_relaxed);

        // what will next tail be after we push
        // [] [] [1] [2] [3] [4] [5] [$] [ ]
        //        H                   T   T' 
        auto next = increment(t);

        // check if queue is full and do nothing if it is
        // [3] [4] [5] [6] [7] [8] [$] [ 1 ] [2]
        //                          T   T'H
        if(next == head.load(std::memory_order_acquire))
            return false;

        // insert the item into the empty spot
        // [] [] [1] [2] [3] [4] [5] [ ] []
        //        H               T   T' 
        items[t] = item;

        // release the tail to the consumer (serialization point)
        // [] [] [1] [2] [3] [4] [5] [ $ ] []
        //        H                   T T' 
        tail.store(next, std::memory_order_release);

        return true;
    }

    bool pop(T& item)
    {
        // [] [] [1] [2] [3] [4] [5] [$] []
        //        H                   T
        auto h = head.load(std::memory_order_relaxed);

        // [] [] [] [] [ $ ] [] [] [] []
        //              H T        
        if(h == tail.load(std::memory_order_acquire))
            return false;

        // move an item from the queue
        item = std::move(items[h]);

        // serialization point wrt producer
        // [] [] [] [2] [3] [4] [5] [$] []
        //           H               T
        head.store(increment(h), std::memory_order_release);

        return true;
    }

private:
    static constexpr size_t capacity = N + 1;

    std::array<T, capacity> items;
    std::atomic<size_t> head {0}, tail {0};

    size_t increment(size_t idx) const
    {
        return (idx + 1) % capacity;
    }
};

}
