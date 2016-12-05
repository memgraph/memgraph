#pragma once

#include <list>

#include "threading/sync/lockable.hpp"
#include "threading/sync/spinlock.hpp"

template <typename value_type, typename lock_type = SpinLock>
class LinkedList : public Lockable<lock_type>
{
public:
    std::size_t size() const
    {
        auto guard = this->acquire_unique();
        return data.size();
    }

    void push_front(const value_type &value)
    {
        auto guard = this->acquire_unique();
        data.push_front(value);
    }

    void push_front(value_type &&value)
    {
        auto guard = this->acquire_unique();
        data.push_front(std::forward<value_type>(value));
    }

    void pop_front()
    {
        auto guard = this->acquire_unique();
        data.pop_front();
    }

    // value_type& as return value
    // would not be concurrent
    value_type front()
    {
        auto guard = this->acquire_unique();
        return data.front();
    }

private:
    std::list<value_type> data;
};
