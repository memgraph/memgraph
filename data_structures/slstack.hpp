#pragma once

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
