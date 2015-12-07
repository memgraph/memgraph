#pragma once

#include <utility>
#include <ext/aligned_buffer.h>

template <class T>
class Placeholder
{
public:
    Placeholder() = default;

    Placeholder(Placeholder&) = delete;
    Placeholder(Placeholder&&) = delete;

    ~Placeholder()
    {
        if(initialized)
            get().~T();
    };

    T& get() noexcept
    {
        assert(initialized);
        return *data._M_ptr();
    }

    void set(const T& item)
    {
        new (data._M_addr()) T(item);
        initialized = true;
    }

    void set(T&& item)
    {
        new (data._M_addr()) T(std::move(item));
        initialized = true;
    }

private:
	__gnu_cxx::__aligned_buffer<T> data;
    bool initialized = false;
};
