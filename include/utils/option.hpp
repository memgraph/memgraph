#pragma once

#include <cassert>
#include <ext/aligned_buffer.h>
#include <utility>

template <class T>
class Option
{
public:
    Option() {}
    //
    // Option(T item)
    // {
    //     new (data._M_addr()) T(std::forward(item));
    //     initialized = true;
    // }

    Option(T const &item)
    {
        new (data._M_addr()) T(item);
        initialized = true;
    }

    Option(T &&item)
    {
        new (data._M_addr()) T(std::move(item));
        initialized = true;
    }

    Option(Option &other) = default;
    // Containers from std which have strong exception guarantees wont use move
    // constructors and operators wihtout noexcept. "Optimized C++,2016 , Kurt
    // Guntheroth, page: 142, title: Moving instances into std::vector"
    Option(Option &&other) noexcept
    {
        if (other.initialized) {
            data = std::move(other.data);
            other.initialized = false;
            initialized = true;
        }
    }

    ~Option()
    {
        if (initialized) get().~T();
    }

    Option &operator=(Option &other) = default;
    Option &operator=(Option &&other)
    {
        if (initialized) {
            get().~T();
            initialized = false;
        }

        if (other.initialized) {
            data = std::move(other.data);
            other.initialized = false;
            initialized = true;
        }

        return *this;
    }

    bool is_present() const { return initialized; }

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

    T take()
    {
        assert(initialized);
        initialized = false;
        return std::move(*data._M_ptr());
    }

    explicit operator bool() const { return initialized; }

private:
    // Aligned buffer is here to ensure aligment for data of type T. It isn't
    // applicable to just put T field because the field has to be able to be
    // uninitialized to fulfill the semantics of Option class.
    __gnu_cxx::__aligned_buffer<T> data;
    bool initialized = false;
};

template <class T>
auto make_option()
{
    return Option<T>();
}

template <class T>
auto make_option(T &&data)
{
    return Option<T>(std::move(data));
}

template <class T>
auto make_option_const(const T &&data)
{
    return Option<const T>(std::move(data));
}
