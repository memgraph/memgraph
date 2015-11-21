#pragma once

#include <atomic>
#include <vector>
#include <unistd.h>

#include "hazard_store.hpp"

class hazard_ptr
{
    static constexpr size_t EMPTY = -1;
    static constexpr uintptr_t NULLPTR = 0;

public:
    hazard_ptr() = default;

    template <class T>
    hazard_ptr(const T* ptr) : ptr(reinterpret_cast<uintptr_t>(ptr))
    {
        if(ptr == nullptr)
            return;

        idx = HazardStore::get().acquire(this->ptr);
    }

    hazard_ptr(const hazard_ptr&) = delete;

    hazard_ptr(hazard_ptr&& other)
    {
        *this = std::move(other);
    }

    ~hazard_ptr()
    {
        reset();
    }

    void reset()
    {
        if(idx == EMPTY)
            return;

        HazardStore::get().release(idx);
        detach();
    }

    hazard_ptr& operator=(hazard_ptr&& other)
    {
        reset();

        ptr = other.ptr;
        idx = other.idx;

        other.detach();

        return *this;
    }

    uintptr_t get() const
    {
        return ptr;
    }

    template <class T>
    operator T*() const
    {
        return reinterpret_cast<T*>(ptr);
    }

    friend bool operator==(const hazard_ptr& lhs, uintptr_t rhs)
    {
        return lhs.ptr == rhs;
    }

    friend bool operator==(uintptr_t lhs, const hazard_ptr& rhs)
    {
        return operator==(rhs, lhs);
    }

    template <class T>
    friend bool operator==(const hazard_ptr& lhs, const T* const rhs)
    {
        return lhs.ptr == reinterpret_cast<uintptr_t>(rhs);
    }

    template <class T>
    friend bool operator==(const T* const lhs, const hazard_ptr& rhs)
    {
        return operator==(rhs, lhs);
    }

    friend bool operator!=(const hazard_ptr& lhs, uintptr_t rhs)
    {
        return !operator==(lhs, rhs);
    }

    friend bool operator!=(uintptr_t lhs, const hazard_ptr& rhs)
    {
        return operator!=(rhs, lhs);
    }

    template <class T>
    friend bool operator!=(const hazard_ptr& lhs, const T* const rhs)
    {
        return !operator==(lhs, rhs);
    }

    template <class T>
    friend bool operator!=(const T* const lhs, const hazard_ptr& rhs)
    {
        return operator!=(rhs, lhs);
    }

private:
    uintptr_t ptr {NULLPTR};
    size_t idx {EMPTY};

    void detach()
    {
        ptr = NULLPTR;
        idx = EMPTY;
    }
};
