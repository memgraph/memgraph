#pragma once

#include <atomic>

namespace mvcc
{

template <class T>
class Version
{
public:
    Version() = default;
    Version(T *older) : older(older) {}

    ~Version() { delete older.load(std::memory_order_seq_cst); }

    // return a pointer to an older version stored in this record
    T *next(std::memory_order order = std::memory_order_seq_cst)
    {
        return older.load(order);
    }

    const T *next(std::memory_order order = std::memory_order_seq_cst) const
    {
        return older.load(order);
    }

    // set the older version of this record
    void next(T *value, std::memory_order order = std::memory_order_seq_cst)
    {
        older.store(value, order);
    }

    // sets if as expected
    bool cas(T *expected, T *set,
             std::memory_order order = std::memory_order_seq_cst)
    {
        return older.compare_exchange_strong(expected, set, order);
    }

private:
    std::atomic<T *> older{nullptr};
};
}
