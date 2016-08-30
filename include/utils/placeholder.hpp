#pragma once

#include <ext/aligned_buffer.h>
#include <utility>

template <class T>
class Placeholder
{
public:
    Placeholder() = default;

    Placeholder(Placeholder &) = delete;
    Placeholder(Placeholder &&) = delete;

    ~Placeholder()
    {
        if (initialized) get().~T();
    };

    bool is_initialized() { return initialized; }

    T &get() noexcept
    {
        assert(initialized);
        return *data._M_ptr();
    }

    const T &get() const noexcept
    {
        assert(initialized);
        return *data._M_ptr();
    }

    void set(const T &item)
    {
        new (data._M_addr()) T(item);
        initialized = true;
    }

    void set(T &&item)
    {
        new (data._M_addr()) T(std::move(item));
        initialized = true;
    }

    template <class... Args>
    void emplace(Args &&... args)
    {
        new (data._M_addr()) T(args...);
        initialized = true;
    }

private:
    __gnu_cxx::__aligned_buffer<T> data;
    bool initialized = false;
};
