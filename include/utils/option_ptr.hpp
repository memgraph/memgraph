#pragma once

// Like option just for pointers. More efficent than option.
template <class T>
class OptionPtr
{
public:
    OptionPtr() {}
    OptionPtr(T *ptr) : ptr(ptr) {}

    bool is_present() { return ptr != nullptr; }

    T *get()
    {
        assert(is_present());
        return ptr;
    }

private:
    T *ptr = nullptr;
};

template <class T>
auto make_option_ptr(T *t)
{
    return OptionPtr<T>(t);
}
