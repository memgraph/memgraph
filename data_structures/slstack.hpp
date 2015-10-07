#ifndef MEMGRAPH_DATA_STRUCTURES_SPINLOCK_STACK_HPP
#define MEMGRAPH_DATA_STRUCTURES_SPINLOCK_STACK_HPP

#include <stack>

#include "threading/sync/spinlock.hpp"
#include "threading/sync/lockable.hpp"

template <class T>
class SpinLockStack : Lockable<SpinLock>
{
public:

    T pop()
    {
        auto guard = acquire();

        T elem = stack.top();
        stack.pop();

        return elem;
    }

    void push(const T& elem)
    {
        auto guard = acquire();

        stack.push(elem);
    }

private:
    std::stack<T> stack;
};

#endif
