#pragma once

#include <atomic>

namespace mvcc
{

template <class T>
class CreExp
{
public:
    CreExp() = default;
    CreExp(T cre, T exp) : cre_(cre), exp_(exp) {}

    T cre(std::memory_order order = std::memory_order_acquire) const
    {
        return cre_.load(order);
    }

    void cre(T value, std::memory_order order = std::memory_order_release)
    {
        cre_.store(value, order);
    }

    T exp(std::memory_order order = std::memory_order_acquire) const
    {
        return exp_.load(order);
    }

    void exp(T value, std::memory_order order = std::memory_order_release)
    {
        exp_.store(value, order);
    }

private:
    std::atomic<T> cre_ {0}, exp_ {0};
};

}
