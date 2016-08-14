#pragma once

#include <ext/aligned_buffer.h>
#include <utility>

template <class T>
class Option
{
public:
    Option() {}

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

    Option<T> &operator=(Option<T> &&other)
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

    const T &get() const noexcept { assert(initialized); }

    T take()
    {
        assert(initialized);
        initialized = false;
        return std::move(*data._M_ptr());
    }

    explicit operator bool() const { return initialized; }

private:
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
