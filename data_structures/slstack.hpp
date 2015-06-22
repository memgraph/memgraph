#ifndef MEMGRAPH_DATA_STRUCTURES_SPINLOCK_STACK_HPP
#define MEMGRAPH_DATA_STRUCTURES_SPINLOCK_STACK_HPP

#include <stack>

#include "utils/sync/spinlock.hpp"

template <class T>
class SpinLockStack
{
public:

    T pop()
    {
        lock.acquire();

        T elem = stack.top();
        stack.pop();

        lock.release();

        return elem;
    }

    void push(const T& elem)
    {
        lock.acquire();
        stack.push(elem);
        lock.release();
    }

private:
    SpinLock lock;
    std::stack<T> stack;
};

#endif
